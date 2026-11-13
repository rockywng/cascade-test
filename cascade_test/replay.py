"""Replay testing: run a candidate version on recorded production input and diff its outputs.

Recording (a directory; `cascade-test record` builds one from a topic dump):

    recording.yaml                 optional: {name, description, sources: {orders: {format: json}}}
    <source>.jsonl[.gz]            records in capture order, one per line:
        {"partition": 0, "offset": 41, "timestamp": 1767261600000, "key": "c1", "headers": {...},
         "op": "+I", "row": {...}}
    lookups/<source>.jsonl[.gz]    cassette for a lookup table: its rows (lookup joins are served
                                   from it, so replays never call external systems)

Determinism, which makes a diff worth reading:
  - partitions keep their recorded order and interleaving (or --order timestamp), and each
    partition's watermark advances exactly as in the test framework (min over partitions);
  - lookups come from cassettes, never live systems;
  - columns computed from processing time or randomness are left out of the diff and listed;
  - sinks are captured, so a replay never writes anywhere.

The baseline is another version of the pipeline run on the same input (a path, or git:<ref>) or
golden outputs saved earlier (--golden). Every sink is compared as final state (upsert sinks by
primary key), key-aware where a key is known, with optional numeric tolerance.
"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field

import yaml

from . import diff as D
from . import plan as P
from . import runner as R
from . import timeutil
from .engine import Engine
from .project import Project
from .spec import PIPELINE, Location, TestCase

DEBEZIUM_OPS = {"c": "upsert", "r": "upsert", "u": "upsert", "d": "delete"}
OPS = {"+I": "insert", "+U": "upsert", "-U": None, "-D": "delete", "insert": "insert", "update": "upsert",
       "upsert": "upsert", "delete": "delete"}


class ReplayError(ValueError):
    pass


# --- recordings ---------------------------------------------------------------------------------

def _open(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def _find(directory: str, stem: str) -> str | None:
    for ext in (".jsonl", ".jsonl.gz", ".ndjson", ".ndjson.gz"):
        p = os.path.join(directory, stem + ext)
        if os.path.exists(p):
            return p
    return None


@dataclass
class Recording:
    path: str
    meta: dict
    records: dict = field(default_factory=dict)  # source -> [record]
    lookups: dict = field(default_factory=dict)  # source -> [row]

    @property
    def name(self) -> str:
        return self.meta.get("name") or os.path.basename(os.path.normpath(self.path))

    def count(self) -> int:
        return sum(len(v) for v in self.records.values())


def load_recording(path: str) -> Recording:
    if not os.path.isdir(path):
        raise ReplayError(f"recording not found: {path} (a directory of <source>.jsonl files)")
    meta = {}
    mp = os.path.join(path, "recording.yaml")
    if os.path.exists(mp):
        with open(mp) as f:
            meta = yaml.safe_load(f) or {}
    rec = Recording(path, meta)
    fmt = {s: (v or {}).get("format", "json") for s, v in (meta.get("sources") or {}).items()}
    for name in sorted(os.listdir(path)):
        full = os.path.join(path, name)
        stem = name.split(".")[0]
        if os.path.isfile(full) and name.endswith((".jsonl", ".jsonl.gz", ".ndjson", ".ndjson.gz")):
            with _open(full) as f:
                rec.records[stem] = [normalize_record(json.loads(line), fmt.get(stem, "json"), stem, i + 1)
                                     for i, line in enumerate(f) if line.strip()]
    lk = os.path.join(path, "lookups")
    if os.path.isdir(lk):
        for name in sorted(os.listdir(lk)):
            if name.endswith((".jsonl", ".jsonl.gz")):
                with _open(os.path.join(lk, name)) as f:
                    rec.lookups[name.split(".")[0]] = [json.loads(line) for line in f if line.strip()]
    return rec


def normalize_record(obj: dict, fmt: str = "json", source: str = "?", line: int = 0) -> dict:
    """A cascade record, a `kcat -J` record or a bare row -> a cascade record."""
    if "row" in obj and isinstance(obj.get("row"), (dict, type(None))):
        r = dict(obj)
    elif "payload" in obj and ("partition" in obj or "offset" in obj):  # kcat -C -J
        payload = obj.get("payload")
        value = json.loads(payload) if isinstance(payload, str) and payload.strip() else payload
        r = {"partition": obj.get("partition", 0), "offset": obj.get("offset"), "timestamp": obj.get("ts"),
             "key": obj.get("key"), "headers": _headers(obj.get("headers")), "value": value}
    else:
        r = {"row": obj}
    if "value" in r and "row" not in r:
        value = r.pop("value")
        if fmt == "debezium-json":
            if value is None:  # tombstone after a delete
                r.update(row=None, op=None)
            else:
                body = value.get("payload", value) if isinstance(value, dict) else value
                op = DEBEZIUM_OPS.get(body.get("op"))
                if op is None:
                    raise ReplayError(f"{source} line {line}: unknown Debezium op {body.get('op')!r}")
                r.update(op=op, row=body.get("before") if op == "delete" else body.get("after"))
        else:
            r["row"] = value
    r.setdefault("partition", 0)
    r.setdefault("op", "insert")
    return r


def _headers(h):
    if isinstance(h, list):  # kcat: ["k1", "v1", "k2", "v2"]
        return {h[i]: h[i + 1] for i in range(0, len(h) - 1, 2)}
    return h


def select(records: list[dict], last_ms: int | None = None, limit: int | None = None, order: str = "recorded"):
    out = list(records)
    if order == "timestamp":
        out.sort(key=lambda r: (r.get("timestamp") or 0, r.get("partition") or 0, r.get("offset") or 0))
    if last_ms is not None:
        stamps = [r["timestamp"] for r in out if r.get("timestamp") is not None]
        if stamps:
            cutoff = max(stamps) - last_ms
            out = [r for r in out if r.get("timestamp") is None or r["timestamp"] >= cutoff]
    if limit is not None:
        out = out[:limit]
    return out


def to_fixture(project: Project, source: str, records: list[dict]) -> list[dict]:
    """Records -> fixture rows (lenient: unknown fields dropped; for changelog sources, an update of a
    key not seen yet becomes an insert and a delete of one is skipped, since a recording usually
    starts mid-stream)."""
    info = project.source_info(source)
    names = set(info.names)
    rows, live = [], set()
    for r in records:
        if r.get("row") is None:
            continue
        op = OPS.get(r.get("op") or "insert", "insert")
        if op is None:
            continue
        row = {k: v for k, v in r["row"].items() if k in names}
        row["_partition"] = int(r.get("partition") or 0)
        if info.updating:
            key = tuple(row.get(k) for k in info.primary_key)
            if op == "delete":
                if key not in live:
                    continue
                live.discard(key)
                row = {**{k: row.get(k) for k in info.primary_key}, "_partition": row["_partition"], "_op": "delete"}
            else:
                row["_op"] = "update" if key in live else "insert"
                live.add(key)
        elif op != "insert":
            continue
        rows.append(row)
    return rows


# --- running -------------------------------------------------------------------------------------

@dataclass
class SinkDiff:
    sink: str
    baseline_rows: int | None
    candidate_rows: int | None
    diff: D.Diff | None
    note: str = ""


@dataclass
class ReplayReport:
    project: str
    recording: Recording
    baseline_label: str
    sinks: list[SinkDiff] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    excluded: dict = field(default_factory=dict)  # sink -> nondeterministic columns left out
    replayed: int = 0
    span: str = ""
    millis: int = 0

    @property
    def changed(self) -> bool:
        return bool(self.errors) or any(s.diff is None or not s.diff.ok for s in self.sinks)

    def text(self, style: D.Style | None = None) -> str:
        s = style or D.Style(False)
        out = [s.bold(f"replay: {self.project} on recording '{self.recording.name}' ({self.replayed} records"
                      f"{', ' + self.span if self.span else ''}) vs {self.baseline_label}")]
        for e in self.errors:
            out.append(s.red("  error: " + e))
        for w in self.warnings:
            out.append(s.yellow("  note: " + w))
        for sd in self.sinks:
            if sd.diff is None:
                out.append(s.yellow(f"  ~ {sd.sink}: {sd.note}"))
                continue
            counts = f"{sd.baseline_rows} -> {sd.candidate_rows} rows"
            if sd.diff.ok:
                out.append(s.green(f"  = {sd.sink}: identical ({counts})"))
            else:
                out.append(s.red(f"  ≠ {sd.sink}: {sd.diff.summary(('removed', 'added'))} ({counts})"))
                out.append(_indent(D.render_text(sd.diff, s, limit=10, labels=("removed (baseline only)",
                                                                                   "added (candidate only)"),
                                                  names=("baseline", "candidate")), 4))
        for sink, cols in self.excluded.items():
            out.append(s.dim(f"  {sink}: left out of the diff (nondeterministic): {', '.join(cols)}"))
        return "\n".join(out)

    def markdown(self) -> str:
        icon = "⚠️" if self.changed else "✅"
        out = [f"### {icon} Replay diff: `{self.project}` vs {self.baseline_label}", "",
               f"Recording `{self.recording.name}`: {self.replayed:,} records"
               + (f" ({self.span})" if self.span else "") + ".", ""]
        for e in self.errors:
            out.append(f"> ❌ {e}\n")
        out += ["| sink | baseline rows | candidate rows | changed | removed | added | |", "|---|---|---|---|---|---|---|"]
        for sd in self.sinks:
            if sd.diff is None:
                out.append(f"| `{sd.sink}` | {sd.baseline_rows if sd.baseline_rows is not None else '–'} | "
                           f"{sd.candidate_rows if sd.candidate_rows is not None else '–'} | | | | {sd.note} |")
                continue
            d = sd.diff
            out.append(f"| `{sd.sink}` | {sd.baseline_rows} | {sd.candidate_rows} | {len(d.changed)} | "
                       f"{len(d.missing)} | {len(d.unexpected)} | {'✅' if d.ok else '⚠️'} |")
        out.append("")
        for sd in self.sinks:
            if sd.diff is not None and not sd.diff.ok:
                out.append(f"<details><summary><code>{sd.sink}</code>: {sd.diff.summary(('removed', 'added'))}"
                           f"</summary>\n")
                out.append(D.render_markdown(sd.diff))
                out.append("</details>\n")
        notes = self.warnings + [f"`{k}`: left out of the diff (nondeterministic): {', '.join(v)}"
                                 for k, v in self.excluded.items()]
        if notes:
            out.append("<sub>" + "<br>".join(notes) + "</sub>")
        return "\n".join(out) + "\n"

    def to_json(self) -> dict:
        return {"project": self.project, "recording": self.recording.name, "baseline": self.baseline_label,
                "records": self.replayed, "changed": self.changed, "errors": self.errors, "warnings": self.warnings,
                "sinks": [{"sink": s.sink, "baseline_rows": s.baseline_rows, "candidate_rows": s.candidate_rows,
                           "note": s.note, "diff": D.to_json(s.diff) if s.diff else None} for s in self.sinks]}


def _case(project: Project, given: dict, timeout: int, partitions: dict, config: dict | None = None,
          clock: str | None = None) -> TestCase:
    return TestCase(name="replay", loc=Location("<replay>"), target=PIPELINE, given=given, expect={},
                    timeout=timeout, partitions=partitions, config=dict(config or {}), clock=clock,
                    file=os.path.join(project.root, "tests", "<replay>"))


def given_from(project: Project, rec: Recording, last_ms, limit, order, warnings: list[str]) -> tuple[dict, dict]:
    p = project.pipeline
    given, partitions = {}, {}
    for name, records in rec.records.items():
        if name not in p.sources:
            warnings.append(f"recording has '{name}', which is not a source of {p.name}; ignored")
            continue
        chosen = select(records, last_ms, limit, order)
        given[name] = to_fixture(project, name, chosen)
        parts = {int(r.get("partition") or 0) for r in records}
        if parts:
            partitions[name] = max(parts) + 1
    for name, rows in rec.lookups.items():
        if name not in p.sources:
            warnings.append(f"cassette '{name}' is not a source of {p.name}; ignored")
            continue
        names = set(project.source_info(name).names)
        given[name] = [{k: v for k, v in r.items() if k in names} for r in rows]
    for name, s in p.sources.items():
        if name not in given and not s.copy_of:
            warnings.append(f"source '{name}' has no recording or cassette; it replays as empty")
    return given, partitions


def run_versions(engine: Engine, versions: list[tuple[Project, dict, dict]], timeout: int,
                 config: dict | None = None, clock: str | None = None):
    """Run each (project, given, partitions) as a whole-pipeline job; returns [TestResult].
    `config` is extra Flink configuration for every job (e.g. a savepoint to restore from); `clock`
    freezes the wall-clock functions at that instant (clock.py)."""
    work = tempfile.mkdtemp(prefix="cascade-replay-")
    try:
        preps = []
        for i, (proj, given, parts) in enumerate(versions):
            prep = P.prepare(proj, _case(proj, given, timeout, parts, config, clock), os.path.join(work, f"v{i}"))
            if prep.error:
                raise ReplayError(f"{proj.name}: {prep.error}")
            preps.append(prep)
        raw = engine.run([prep.requests[0].payload for prep in preps])
        return [R.evaluate(proj, prep, [r], R.Options()) for (proj, _, _), prep, r in zip(versions, preps, raw)]
    finally:
        shutil.rmtree(work, ignore_errors=True)


def sink_outputs(res: R.TestResult) -> dict:
    """label -> OutputResult for every captured sink (including quarantines)."""
    return {o.label: res.outputs[o.name] for o in res.prep.requests[0].outputs
            if o.role == "sink" and o.name in res.outputs}


def compare(report: ReplayReport, candidate: Project, cand: dict, base: dict, keys: dict, tolerance, ignore,
            base_project: Project | None = None, frozen: bool = False):
    for sink in list(dict.fromkeys(list(base) + list(cand))):
        b, c = base.get(sink), cand.get(sink)
        if b is None or c is None:
            report.sinks.append(SinkDiff(sink, len(b.rows) if b else None, len(c.rows) if c else None, None,
                                         "new sink" if b is None else "sink removed"))
            continue
        nondet = set(candidate.nondeterministic_columns(sink, frozen))
        if base_project is not None:
            nondet |= set(base_project.nondeterministic_columns(sink, frozen))
        common = [n for n in c.names if n in b.types]
        cols = [n for n in common if n not in nondet and n not in ignore]
        if nondet & set(common):
            report.excluded[sink] = sorted(nondet & set(common))
        added, dropped = [n for n in c.names if n not in b.types], [n for n in b.names if n not in c.types]
        note = ""
        if added or dropped:
            note = "schema changed:" + (f" +{', +'.join(added)}" if added else "") + \
                   (f" -{', -'.join(dropped)}" if dropped else "") + "; compared on common columns"
            report.warnings.append(f"{sink}: {note}")
        key = keys.get(sink) or candidate.key_of(sink)
        d = D.compare(b.rows, c.rows, c.types, columns=cols, key=key, tolerance=tolerance)
        report.sinks.append(SinkDiff(sink, len(b.rows), len(c.rows), d, note))


# --- golden outputs ------------------------------------------------------------------------------

def write_golden(directory: str, outputs: dict):
    os.makedirs(directory, exist_ok=True)
    for label, out in outputs.items():
        with open(os.path.join(directory, f"{label}.jsonl"), "w") as f:
            f.write(json.dumps({"columns": [[n, t] for n, t in out.columns]}) + "\n")
            for row in sorted(out.rows, key=lambda r: json.dumps(r, sort_keys=True, default=str)):
                f.write(json.dumps(row, default=str) + "\n")


def read_golden(directory: str) -> dict:
    outs = {}
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".jsonl"):
            continue
        with open(os.path.join(directory, name)) as f:
            lines = [json.loads(x) for x in f if x.strip()]
        cols = [tuple(c) for c in lines[0]["columns"]] if lines and "columns" in lines[0] else []
        rows = lines[1:] if cols else lines
        outs[name[: -len(".jsonl")]] = R.OutputResult(name[: -len(".jsonl")], cols, [], rows)
    return outs


# --- baselines from git --------------------------------------------------------------------------

class GitBaseline:
    """`git:<ref>`: the project as of <ref>, extracted to a temporary directory."""

    def __init__(self, candidate_path: str, ref: str):
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=os.path.abspath(candidate_path),
                             capture_output=True, text=True)
        if top.returncode != 0:
            raise ReplayError(f"git:{ref}: {candidate_path} is not in a git repository")
        self.top = top.stdout.strip()
        self.rel = os.path.relpath(os.path.abspath(candidate_path), self.top)
        self.ref = ref
        self.dir = tempfile.mkdtemp(prefix="cascade-baseline-")
        archive = subprocess.run(["git", "archive", "--format=tar", ref, "--", self.rel if self.rel != "." else "."],
                                 cwd=self.top, capture_output=True)
        if archive.returncode != 0:
            shutil.rmtree(self.dir, ignore_errors=True)
            raise ReplayError(f"git:{ref}: {archive.stderr.decode().strip()}")
        subprocess.run(["tar", "-x", "-C", self.dir], input=archive.stdout, check=True)
        self.path = os.path.join(self.dir, self.rel)

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def open_baseline(candidate_path: str, spec: str):
    """Returns (path, label, cleanup)."""
    if spec.startswith("git:"):
        g = GitBaseline(candidate_path, spec[4:])
        return g.path, f"`{spec[4:]}`", g.close
    return spec, f"`{os.path.relpath(spec)}`", (lambda: None)


# --- commands ------------------------------------------------------------------------------------

def replay(engine: Engine, candidate: Project, rec: Recording, *, baseline: Project | None = None,
           baseline_label: str = "", golden: str | None = None, update_golden: bool = False,
           last: str | None = None, limit: int | None = None, order: str = "recorded", keys: dict | None = None,
           tolerance=None, ignore=(), timeout: int = 600, savepoint: str | None = None,
           state_drop=(), state_keep=(), clock: str | None = None) -> ReplayReport:
    if (state_drop or state_keep) and not savepoint:
        raise ReplayError("--state-drop/--state-keep need --from-savepoint")
    last_ms = timeutil.parse_duration_ms(last) if last else None
    report = ReplayReport(candidate.name, rec, baseline_label or (f"golden `{golden}`" if golden else "?"))
    state_dir = None
    config = None
    try:
        if savepoint:
            config, state_dir = restore_config(engine, savepoint, state_drop, state_keep, report.warnings)
        return _replay(engine, candidate, rec, report, baseline, golden, update_golden, last, last_ms, limit, order,
                       keys, tolerance, ignore, timeout, config, clock)
    finally:
        if state_dir:
            shutil.rmtree(state_dir, ignore_errors=True)


def restore_config(engine, savepoint: str, drop, keep, warnings: list[str]) -> tuple[dict, str | None]:
    """Flink configuration that starts each replayed job from `savepoint`, after removing operators'
    state from a copy when asked (State Processor API). Returns (config, temporary directory)."""
    from .engine import state_tool
    path = os.path.abspath(savepoint)
    if not os.path.isfile(os.path.join(path, "_metadata")):
        raise ReplayError(f"{savepoint} is not a savepoint (no _metadata file); pass the savepoint directory")
    tmp = None
    if drop or keep:
        tmp = tempfile.mkdtemp(prefix="cascade-state-")
        out = os.path.join(tmp, "savepoint")
        args = ["subset", path, out] + [x for u in drop for x in ("--drop", u)] + [x for u in keep for x in ("--keep", u)]
        res = state_tool(engine.flink_home, args)
        for missing in res.get("not_in_savepoint") or []:
            warnings.append(f"state: '{missing}' is not an operator of the savepoint")
        warnings.append(f"state: restored from a copy of the savepoint without {len(res['removed'])} operator(s)' "
                        f"state ({len(res['kept'])} kept)")
        path = out
    else:
        warnings.append(f"state: restored from savepoint {savepoint}")
    # operators the candidate no longer has must not fail the replay: it is a diff of outputs, and the
    # state-compatibility check (`cascade-test ci`) is what judges the change
    return {"execution.state-recovery.path": path, "execution.state-recovery.ignore-unclaimed-state": "true"}, tmp


def _replay(engine, candidate, rec, report, baseline, golden, update_golden, last, last_ms, limit, order,
            keys, tolerance, ignore, timeout, config, clock=None) -> ReplayReport:
    given, parts = given_from(candidate, rec, last_ms, limit, order, report.warnings)
    report.replayed = sum(len(v) for k, v in given.items() if k in rec.records)
    if last:
        report.span = f"last {last}"
    versions = [(candidate, given, parts)]
    if baseline is not None:
        bgiven, bparts = given_from(baseline, rec, last_ms, limit, order, [])
        versions.append((baseline, bgiven, bparts))
    if clock:
        from .clock import ClockError, instant
        try:
            instant(clock)
        except ClockError as e:
            raise ReplayError(str(e)) from None
        report.warnings.append(f"clock frozen at {clock}: CURRENT_TIMESTAMP, NOW() ... read that instant")
    results = run_versions(engine, versions, timeout, config, clock)
    report.millis = max(r.millis for r in results)
    for label, r in zip(("candidate", "baseline"), results):
        if r.job_error:
            report.errors.append(f"the {label} job failed: {R.short_error(r.job_error).splitlines()[0]}")
    if report.errors:
        return report
    cand = sink_outputs(results[0])
    if baseline is not None:
        compare(report, candidate, cand, sink_outputs(results[1]), keys or {}, tolerance, ignore, baseline, bool(clock))
    elif golden:
        if update_golden or not os.path.isdir(golden):
            write_golden(golden, cand)
            report.warnings.append(f"golden outputs written to {golden} ({len(cand)} sink(s)); commit them")
            report.sinks = [SinkDiff(k, None, len(v.rows), None, "golden written") for k, v in cand.items()]
        else:
            compare(report, candidate, cand, read_golden(golden), keys or {}, tolerance, ignore, None, bool(clock))
    else:
        report.sinks = [SinkDiff(k, None, len(v.rows), None, "no baseline") for k, v in cand.items()]
    return report


def parse_keys(specs: list[str]) -> dict:
    keys = {}
    for s in specs or []:
        if "=" not in s:
            raise ReplayError(f"--key {s}: expected sink=col1,col2")
        sink, cols = s.split("=", 1)
        keys[sink] = [c.strip() for c in cols.split(",") if c.strip()]
    return keys


def cmd_replay(args) -> int:
    style = D.Style(sys.stdout.isatty() and not args.no_color and not os.environ.get("NO_COLOR"))
    candidate = Project(args.path, args.env)
    rec = load_recording(args.recording)
    cleanup = lambda: None  # noqa: E731
    base, label = None, ""
    try:
        if args.baseline:
            path, label, cleanup = open_baseline(args.path, args.baseline)
            base = Project(path, args.env)
        with Engine(args.flink_home, workers=args.workers) as engine:
            report = replay(engine, candidate, rec, baseline=base, baseline_label=label, golden=args.golden,
                            update_golden=args.update_golden, last=args.last, limit=args.limit, order=args.order,
                            keys=parse_keys(args.key), tolerance=args.tolerance, ignore=args.ignore,
                            timeout=args.timeout, savepoint=args.from_savepoint, state_drop=args.state_drop,
                            state_keep=args.state_keep, clock=args.clock)
    finally:
        cleanup()
    print(report.text(style))
    if args.markdown:
        with open(args.markdown, "w") as f:
            f.write(report.markdown())
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report.to_json(), f, indent=2, default=str)
    if report.errors:
        return 2
    return 1 if report.changed and not args.allow_diff else 0


def cmd_record(args) -> int:
    os.makedirs(args.out, exist_ok=True)
    fmt = args.format
    target = os.path.join(args.out, "lookups" if args.lookup else "", f"{args.source}.jsonl")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    n = 0
    with _open(args.input) as src, open(target, "a") as out:
        for i, line in enumerate(src):
            if not line.strip():
                continue
            rec = normalize_record(json.loads(line), fmt, args.source, i + 1)
            if args.lookup:
                if rec.get("row") is not None and rec.get("op") != "delete":
                    out.write(json.dumps(rec["row"]) + "\n")
                    n += 1
                continue
            out.write(json.dumps(rec) + "\n")
            n += 1
    meta_path = os.path.join(args.out, "recording.yaml")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = yaml.safe_load(f) or {}
    meta.setdefault("name", os.path.basename(os.path.normpath(args.out)))
    if not args.lookup:
        meta.setdefault("sources", {})[args.source] = {"format": "cascade"}
    with open(meta_path, "w") as f:
        yaml.safe_dump(meta, f, sort_keys=False)
    print(f"recorded {n} {'lookup row' if args.lookup else 'record'}(s) for {args.source} in {target}")
    return 0


def _load_rows(path: str) -> list[dict]:
    from .fixtures import load_file
    if path.endswith((".jsonl", ".jsonl.gz", ".ndjson")):
        out = []
        with _open(path) as f:
            for i, line in enumerate(f):
                if line.strip():
                    obj = json.loads(line)
                    if "columns" in obj and i == 0 and len(obj) == 1:
                        continue  # golden header
                    rec = normalize_record(obj)
                    if rec.get("row") is not None:
                        row = dict(rec["row"])
                        if "op" in obj or "_op" in row:
                            row.setdefault("_op", obj.get("op"))
                        out.append(row)
        return out
    return load_file(path)


def _infer_types(rows: list[dict]) -> dict:
    types = {}
    for r in rows:
        for k, v in r.items():
            if v is None or k in types and types[k] == "STRING":
                continue
            t = "BIGINT" if isinstance(v, int) and not isinstance(v, bool) else \
                "DOUBLE" if isinstance(v, float) else "BOOLEAN" if isinstance(v, bool) else "STRING"
            if isinstance(v, str):
                try:
                    float(v)
                    t = "DECIMAL"
                except ValueError:
                    t = "STRING"
            if types.get(k) not in (None, t):
                t = "DOUBLE" if {types[k], t} <= {"BIGINT", "DOUBLE", "DECIMAL"} else "STRING"
            types[k] = t
    return types


def cmd_diff(args) -> int:
    style = D.Style(sys.stdout.isatty() and not os.environ.get("NO_COLOR"))
    base, cand = _load_rows(args.baseline), _load_rows(args.candidate)
    key = [k.strip() for k in args.key.split(",")] if args.key else None
    if args.changelog:
        def final(rows):
            names = sorted({k for r in rows for k in r if k not in ("_op", "op")})
            changes = [{"kind": {"insert": "+I", "upsert": "+U", "update": "+U", "delete": "-D", "c": "+I", "r": "+I",
                                 "u": "+U", "d": "-D"}.get(str(r.get("_op") or r.get("op") or "+I"),
                                                           str(r.get("_op") or r.get("op") or "+I")),
                        "values": [r.get(n) for n in names]} for r in rows]
            return D.materialize(names, changes, key)
        base, cand = final(base), final(cand)
    types = _infer_types(base + cand)
    cols = [c for c in types if c not in ("_op", "op") and c not in args.ignore]
    d = D.compare(base, cand, types, columns=cols, key=key, tolerance=args.tolerance)
    print(f"{args.baseline} -> {args.candidate}: {len(base)} -> {len(cand)} rows; "
          f"{d.summary(('removed', 'added'))}")
    if not d.ok:
        print(D.render_text(d, style, labels=("removed (baseline only)", "added (candidate only)"),
                            names=("baseline", "candidate")))
    if args.markdown:
        with open(args.markdown, "w") as f:
            f.write(f"### Output diff: `{args.baseline}` → `{args.candidate}`\n\n"
                    f"{len(base)} → {len(cand)} rows: {d.summary(('removed', 'added'))}\n\n" + D.render_markdown(d))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(D.to_json(d), f, indent=2, default=str)
    return 0 if d.ok else 1


def _indent(text: str, n: int) -> str:
    return "\n".join(" " * n + line if line else line for line in text.split("\n"))
