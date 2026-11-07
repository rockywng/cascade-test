"""Run test cases on the engine and evaluate their expectations."""
from __future__ import annotations

import os
import shutil
import tempfile
from collections import Counter
from dataclasses import dataclass, field

from . import diff as D
from . import plan as P
from . import snapshots as S
from . import values as V
from .engine import Engine
from .project import Project
from .spec import PIPELINE, TestCase

OP_ALIASES = {"insert": "+I", "+i": "+I", "update_before": "-U", "-u": "-U", "update_after": "+U", "+u": "+U",
              "update": "+U", "delete": "-D", "-d": "-D"}


@dataclass
class Failure:
    title: str
    detail: str = ""


@dataclass
class OutputResult:
    label: str
    columns: list[tuple[str, str]]
    changes: list[dict]
    rows: list[dict]  # final state

    @property
    def names(self) -> list[str]:
        return [n for n, _ in self.columns]

    @property
    def types(self) -> dict:
        return dict(self.columns)


@dataclass
class TestResult:
    case: TestCase
    status: str  # pass | fail | error | skip
    failures: list[Failure] = field(default_factory=list)
    millis: int = 0
    outputs: dict = field(default_factory=dict)  # label -> OutputResult
    notes: list[str] = field(default_factory=list)
    prep: P.Prepared | None = None
    expectation_counts: dict = field(default_factory=dict)  # model -> {expectation: violations}
    job_error: str | None = None

    @property
    def passed(self) -> bool:
        return self.status in ("pass", "skip")

    @property
    def message(self) -> str:
        return "\n".join(f.title + (("\n" + f.detail) if f.detail else "") for f in self.failures)


@dataclass
class Options:
    update_snapshots: bool = False
    ci: bool = False
    coverage: bool = False
    keep_workdir: bool = False


def run(project: Project, cases: list[TestCase], engine: Engine, options: Options | None = None) -> list[TestResult]:
    options = options or Options()
    work = tempfile.mkdtemp(prefix="cascade-test-")
    results: dict[int, TestResult] = {}
    prepared: list[P.Prepared] = []
    try:
        for i, case in enumerate(cases):
            if case.skip:
                results[i] = TestResult(case, "skip", notes=[case.skip])
                continue
            prep = P.prepare(project, case, os.path.join(work, f"t{i:04d}"), coverage=options.coverage)
            if prep.error:
                results[i] = TestResult(case, "error", [Failure(prep.error)], prep=prep)
            else:
                prepared.append(prep)
        payloads = [r.payload for prep in prepared for r in prep.requests]
        raw = engine.run(payloads) if payloads else []
        by_id = {r["id"]: r for r in raw}
        stores: dict[str, S.Store] = {}
        index = {id(c): i for i, c in enumerate(cases)}
        for prep in prepared:
            store = None
            if _uses_snapshots(prep.case):
                store = stores.setdefault(prep.case.file, S.Store(prep.case.file))
            res = evaluate(project, prep, [by_id[r.payload["id"]] for r in prep.requests], options, store)
            results[index[id(prep.case)]] = res
        for s in stores.values():
            s.save()
    finally:
        if options.keep_workdir:
            print(f"cascade-test: kept fixtures and SQL in {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)
    return [results[i] for i in range(len(cases))]


def _uses_snapshots(case: TestCase) -> bool:
    if case.expect.get("snapshot"):
        return True
    return any((e or {}).get("snapshot") for e in (case.expect.get("sinks") or {}).values())


# --- evaluation ------------------------------------------------------------------------------

def evaluate(project: Project, prep: P.Prepared, raw: list[dict], options: Options,
             store: S.Store | None = None) -> TestResult:
    case = prep.case
    res = TestResult(case, "pass", prep=prep, millis=sum(int(r.get("millis", 0)) for r in raw))
    if case.kind == "calls":
        _evaluate_calls(prep, raw, res)
        res.status = "fail" if res.failures else "pass"
        return res
    r = raw[0]
    for o in prep.requests[0].outputs:
        out = (r.get("outputs") or {}).get(o.name)
        if out is None or "rows" not in out:
            continue
        cols = [(c["name"], c["type"]) for c in out["columns"]]
        names = [n for n, _ in cols]
        final = D.materialize(names, out["rows"], o.materialize_key)
        res.outputs[o.name] = OutputResult(o.label, cols, out["rows"], final)

    late = {s: rd.late_rows for s, rd in prep.rendered.items() if rd.late_rows}
    if late:
        res.notes.append("late input (at or behind the watermark when it arrived; event-time windows and joins "
                         "drop it): " + "; ".join(f"{s} row(s) {', '.join(map(str, rows))}" for s, rows in late.items()))

    expect = case.expect
    if r.get("error"):
        res.job_error = r["error"]
        wanted = expect.get("error")
        if wanted is not None:
            if str(wanted) not in r["error"]:
                res.failures.append(Failure(f"the job failed, but the error does not mention '{wanted}'",
                                            _indent(short_error(r["error"]))))
        else:
            res.failures.append(Failure(f"the job failed{' (timeout)' if r.get('phase') == 'timeout' else ''}",
                                        _indent(short_error(r["error"]) + _hint(r["error"]))))
        res.status = "fail" if res.failures else "pass"
        return res
    if expect.get("error") is not None:
        res.failures.append(Failure(f"expected the job to fail mentioning '{expect['error']}', but it succeeded"))

    # outputs under test
    if case.target == PIPELINE:
        by_label = {o.label: o for o in prep.requests[0].outputs if o.role == "sink"}
        for sink, spec in (expect.get("sinks") or {}).items():
            o = by_label.get(sink)
            if o is None:
                from .fixtures import suggest
                res.failures.append(Failure(f"expect.sinks: '{sink}' is not a sink of this pipeline"
                                            f"{suggest(sink, by_label)} (sinks: {', '.join(by_label)})"))
                continue
            _check_output(project, case, prep, res, res.outputs.get(o.name), spec, sink, store, options)
    else:
        name = "target" if case.target not in project.pipeline.sinks else f"sink:{case.target}"
        _check_output(project, case, prep, res, res.outputs.get(name), expect, case.target, store, options)

    # invariants
    for o in prep.requests[0].outputs:
        if o.role != "invariant":
            continue
        out = res.outputs.get(o.name)
        if out and out.rows:
            res.failures.append(Failure(
                f"invariant violated on {o.label}: {o.meta['predicate']} ({len(out.rows)} row(s))",
                _indent(D.table(out.rows, out.names, limit=10))))

    # data-quality expectations
    for o in prep.requests[0].outputs:
        if o.role == "expectations" and o.name in res.outputs:
            out = res.outputs[o.name]
            counts = Counter()
            for row in out.rows:
                for i, n in enumerate(o.meta["names"]):
                    counts[n] += int(row.get(f"e{i}") or 0)
            res.expectation_counts[o.label] = {n: counts[n] for n in o.meta["names"]}
    _check_expectations(project, case, res)
    _check_late(case, prep, res)
    res.status = "fail" if res.failures else "pass"
    return res


def _check_output(project: Project, case: TestCase, prep: P.Prepared, res: TestResult, out: OutputResult | None,
                  spec: dict, target: str, store: S.Store | None, options: Options):
    if out is None:
        res.failures.append(Failure(f"no output captured for {target}"))
        return
    types = out.types
    key = case.key or project.key_of(target)
    ignore = list(case.ignore_columns)
    label = "" if case.target != PIPELINE else f"{target}: "
    unknown = _unknown_columns(spec, out.names)
    if unknown:
        from .fixtures import suggest
        col, where = unknown
        res.failures.append(Failure(f"{label}expected column '{col}' ({where}) is not an output column of "
                                    f"'{target}'{suggest(col, out.names)}", f"  columns: {', '.join(out.names)}"))
        return

    def compare(expected, **kw):
        return D.compare(expected, out.rows, types, key=key, tolerance=case.tolerance, ignore=ignore,
                         base_time=prep.base_time, **kw)

    if "rows" in spec:
        d = compare(list(spec["rows"] or []), ordered=case.ordered)
        if not d.ok:
            res.failures.append(Failure(f"{label}rows differ: {d.summary()}", _indent(D.render_text(d))))
            _show_actual(res, out)
    if spec.get("contains"):
        d = compare(list(spec["contains"]))
        if d.missing or d.changed:
            d.unexpected = []
            res.failures.append(Failure(f"{label}expected rows not found: {len(d.missing) + len(d.changed)} of "
                                        f"{d.expected_count}", _indent(D.render_text(d))))
            _show_actual(res, out)
    if spec.get("excludes"):
        for row in spec["excludes"]:
            d = D.compare([row], out.rows, types, tolerance=case.tolerance, base_time=prep.base_time)
            if d.matched:
                res.failures.append(Failure(f"{label}found a row that must not be produced: "
                                            f"{D.format_row(row)}"))
    if "count" in spec:
        want = spec["count"]
        n = len(out.rows)
        lo, hi = (want.get("min"), want.get("max")) if isinstance(want, dict) else (want, want)
        if (lo is not None and n < int(lo)) or (hi is not None and n > int(hi)):
            res.failures.append(Failure(f"{label}expected {_count_text(want)} row(s), got {n}"))
            _show_actual(res, out)
    if "changelog" in spec:
        expected = []
        for row in spec["changelog"] or []:
            row = dict(row)
            op = str(row.pop("_op", "+I"))
            expected.append({"_op": OP_ALIASES.get(op.lower(), op), **row})
        actual = D.to_dicts(out.names, out.changes)
        cols = ["_op"] + [c for c in dict.fromkeys(k for r in expected for k in r) if c != "_op"]
        d = D.compare(expected, actual, {"_op": "STRING", **types}, columns=cols, tolerance=case.tolerance,
                      ignore=ignore, ordered=case.ordered, base_time=prep.base_time)
        if not d.ok:
            res.failures.append(Failure(f"{label}changelog differs: {d.summary()}", _indent(D.render_text(d))))
            res.notes.append("actual changelog:\n" + _indent(D.table(actual, ["_op"] + out.names)))
    if spec.get("snapshot"):
        _check_snapshot(project, case, prep, res, out, target, key, store, options)


def _count_text(want) -> str:
    if isinstance(want, dict):
        lo, hi = want.get("min"), want.get("max")
        return f"between {lo} and {hi}" if lo is not None and hi is not None else \
            (f"at least {lo}" if lo is not None else f"at most {hi}")
    return str(want)


def _unknown_columns(spec: dict, names: list[str]):
    for where in ("rows", "contains", "excludes", "changelog"):
        for row in spec.get(where) or []:
            if isinstance(row, dict):
                for k in row:
                    if k != "_op" and k not in names:
                        return k, where
    return None


def _show_actual(res: TestResult, out: OutputResult):
    note = f"actual output of {out.label} ({len(out.rows)} row(s)):\n" + _indent(D.table(out.rows, out.names, 25))
    if note not in res.notes:
        res.notes.append(note)


def _check_snapshot(project, case, prep, res, out: OutputResult, target, key, store: S.Store | None,
                    options: Options):
    if store is None:
        return
    nondet = set(project.nondeterministic_columns(target)) | set(case.ignore_columns)
    cols = [c for c in out.names if c not in nondet]
    current = S.plain_rows(out.rows, cols, out.types)
    saved = store.get(case.name, target)
    rel = os.path.relpath(store.path)
    if saved is None:
        if options.ci:
            res.failures.append(Failure(f"no snapshot for {target} in {rel} (record it locally and commit it: "
                                        "cascade-test run --update-snapshots)"))
            return
        store.put(case.name, target, cols, current)
        res.notes.append(f"snapshot recorded for {target} in {rel} ({len(current)} row(s)); review and commit it")
        return
    d = D.compare(saved.get("rows") or [], out.rows, out.types, columns=[c for c in saved.get("columns", cols)
                                                                       if c in out.types and c not in nondet],
                  key=key, tolerance=case.tolerance, base_time=prep.base_time)
    if d.ok:
        return
    if options.update_snapshots:
        store.put(case.name, target, cols, current)
        res.notes.append(f"snapshot for {target} updated ({d.summary(('removed', 'added'))})")
        return
    res.failures.append(Failure(
        f"output of {target} differs from its snapshot: {d.summary(('removed', 'added'))}",
        _indent(D.render_text(d, labels=("in the snapshot, not produced", "produced, not in the snapshot"),
                              names=("snapshot", "now"))) +
        f"\n  if the change is intended: cascade-test run --update-snapshots -k '{case.name}'"))


def _check_expectations(project: Project, case: TestCase, res: TestResult):
    wanted = case.expect.get("expectations") or {}
    for k, v in wanted.items():
        model, name = (k.split(".", 1) if "." in k else (None, k))
        hits = [(m, c) for m, c in res.expectation_counts.items() if (model is None or m == model) and name in c]
        if not hits:
            known = [f"{m}.{n}" for m, c in res.expectation_counts.items() for n in c]
            res.failures.append(Failure(f"expectations: '{k}' is not a data-quality expectation upstream of "
                                        f"{case.target}" + (f" (known: {', '.join(known)})" if known else "")))
            continue
        if len(hits) > 1:
            res.failures.append(Failure(f"expectations: '{k}' is ambiguous; write <model>.{name}"))
            continue
        m, counts = hits[0]
        want = v.get("violations") if isinstance(v, dict) else v
        if want is None and isinstance(v, dict) and v.get("none"):
            want = 0
        got = counts[name]
        if want is not None and int(want) != got:
            res.failures.append(Failure(f"expectation {m}.{name}: expected {want} violating row(s), got {got}"))


def _check_late(case: TestCase, prep: P.Prepared, res: TestResult):
    want = case.expect.get("late")
    if want is None:
        return
    if not isinstance(want, dict):
        res.failures.append(Failure("expect.late is a mapping of source -> row numbers (1-based) or a count"))
        return
    for src, rows in want.items():
        rd = prep.rendered.get(src)
        if rd is None:
            res.failures.append(Failure(f"expect.late: '{src}' is not a source"))
            continue
        got = rd.late_rows
        if isinstance(rows, int):
            if len(got) != rows:
                res.failures.append(Failure(f"expected {rows} late row(s) in {src}, got {len(got)}"
                                            + (f" (rows {', '.join(map(str, got))})" if got else "")))
        elif sorted(rows or []) != got:
            res.failures.append(Failure(f"late rows in {src}: expected {sorted(rows or [])}, got {got}"))


def _evaluate_calls(prep: P.Prepared, raw: list[dict], res: TestResult):
    for req, r in zip(prep.requests, raw):
        o = req.outputs[0]
        spec = o.meta["spec"]
        call = spec["call"]
        if r.get("error"):
            if spec.get("error") is not None and str(spec["error"]) in r["error"]:
                continue
            res.failures.append(Failure(f"{call}: failed", _indent(short_error(r["error"]))))
            continue
        if spec.get("error") is not None:
            res.failures.append(Failure(f"{call}: expected an error mentioning '{spec['error']}', but it succeeded"))
            continue
        out = r["outputs"]["call"]
        typ = out["columns"][0]["type"]
        rows = D.materialize(["result"], out["rows"])
        got = rows[0]["result"] if rows else None
        if "type" in spec and str(spec["type"]).upper() != typ.upper():
            res.failures.append(Failure(f"{call}: returns type {typ}, expected {spec['type']}"))
        if "returns" in spec:
            want = spec["returns"]
            a, b = V.normalize(want, typ, prep.base_time), V.normalize(got, typ, prep.base_time)
            if not V.close(a, b, spec.get("tolerance", prep.case.tolerance)):
                res.failures.append(Failure(f"{call}: expected {V.display(a)}, got {V.display(b)}"))


# --- error messages --------------------------------------------------------------------------

BORING = ("java.util.concurrent.ExecutionException", "JobExecutionException: Job execution failed",
          "Recovery is suppressed by", "TableException: Failed to wait job finish",
          "CompletionException: org.apache.flink")


def short_error(err: str, lines: int = 5) -> str:
    """The informative part of a Flink error chain: wrapper exceptions dropped, root cause kept."""
    parts = [p.strip() for p in err.split("\n") if p.strip()]
    useful = [p for p in parts if not any(b in p for b in BORING)] or parts[-1:]
    useful = [p[len("caused by "):] if i == 0 and p.startswith("caused by ") else p for i, p in enumerate(useful)]
    if len(useful) <= lines:
        return "\n".join(useful)
    return "\n".join(useful[:1] + ["..."] + useful[-(lines - 1):])


HINTS = [
    ("doesn't support consuming update changes",
     "an append-only sink or operator receives an updating stream: declare the sink `mode: upsert` with a "
     "primary key, or make the model append-only (e.g. a windowed aggregate instead of a running one)"),
    ("ExpectationViolation", "a data-quality expectation with `on_violation: fail` was violated; to test that, "
                             "add `expect: {error: <expectation name>}`"),
    ("timed out", "the job did not finish: tests need bounded inputs; check for a source that is not replaced"),
    ("Column '", "a model refers to a column its input does not have"),
    ("jar or artifact resource", "a Java UDF jar is missing: build it first (the function's `jar:` path)"),
    ('Cannot run program "python', "a Python UDF needs an interpreter with PyFlink: pip install "
                                   "apache-flink==<Flink version>, or set CASCADE_PYTHON to one that has it"),
    ("No module named", "a Python UDF needs PyFlink: pip install apache-flink==<Flink version> "
                        "(or point CASCADE_PYTHON at an interpreter that has it)"),
]


def _hint(err: str) -> str:
    for needle, hint in HINTS:
        if needle in err:
            return f"\nhint: {hint}"
    return ""


def _indent(text: str, n: int = 2) -> str:
    pad = " " * n
    return "\n".join(pad + line if line else line for line in text.split("\n"))
