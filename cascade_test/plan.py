"""Turn a test case into engine requests.

The pipeline's own generated statements are kept, with three substitutions:
  - every source reads its fixture (cascade-dsl's `cascade-fixture` connector), with
    `WATERMARK ... AS SOURCE_WATERMARK()` so the watermarks computed in fixtures.py are the only ones;
  - sinks under test keep their DDL and their real INSERT (upsert materialization, ON CONFLICT), but
    write to `cascade-capture` with the same changelog mode, so an updating stream into an
    append-only sink fails here exactly as it would in production;
  - other sinks and INSERTs are dropped.

Extra outputs ride along in the same job: invariant checks (rows violating a predicate) and
expectation probes (which rows fail each data-quality check, before drop/quarantine/fail).
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field

from cascade_dsl.lower import Stmt, q

from . import fixtures as F
from . import timeutil
from .clock import ClockError
from .project import Project
from .spec import PIPELINE, TestCase


@dataclass
class Output:
    name: str  # engine output name
    role: str  # target | sink | invariant | expectations | call
    label: str  # what to call it in messages (sink name, model, ...)
    target: str | None = None  # the target this output belongs to (pipeline tests: a sink)
    materialize_key: list[str] | None = None
    meta: dict = field(default_factory=dict)


@dataclass
class Request:
    payload: dict
    outputs: list[Output]


@dataclass
class Prepared:
    case: TestCase
    requests: list[Request] = field(default_factory=list)
    rendered: dict = field(default_factory=dict)  # source -> fixtures.Rendered
    error: str | None = None  # the test is broken before it runs (bad fixture, unknown target, ...)
    base_time: str = timeutil.DEFAULT_BASE
    targets: list[str] = field(default_factory=list)  # what the test asserts on
    probed: list[str] = field(default_factory=list)  # models whose expectations are probed


def _uid() -> str:
    return uuid.uuid4().hex[:12]


def prepare(project: Project, case: TestCase, workdir: str, coverage: bool = False) -> Prepared:
    prep = Prepared(case, base_time=case.base_time or timeutil.DEFAULT_BASE)
    try:
        _prepare(project, case, workdir, coverage, prep)
    except (F.FixtureError, timeutil.TimeError) as e:
        prep.error = f"bad fixture: {e}"
    except PlanError as e:
        prep.error = str(e)
    except ClockError as e:
        prep.error = f"bad clock: {e}"
    return prep


class PlanError(ValueError):
    pass


def _prepare(project: Project, case: TestCase, workdir: str, coverage: bool, prep: Prepared):
    p = project.pipeline
    os.makedirs(workdir, exist_ok=True)
    if case.kind == "calls":
        _prepare_calls(project, case, prep)
        return
    if case.target != PIPELINE:
        problem = project.check_target(case.target)
        if problem:
            raise PlanError(problem)
    for src in case.given:
        if src not in p.sources:
            raise PlanError(f"`given` names '{src}', which is not a source{F.suggest(src, p.sources)}")

    # fixtures
    for name, s in p.sources.items():
        spec = case.given.get(name)
        if spec is None and s.copy_of:  # backfill copies replay the same input
            spec = case.given.get(s.copy_of)
        info = project.source_info(name)
        rows, parts = F.expand(spec, info, base_time=prep.base_time,
                               defaults=case.defaults.get(s.copy_of or name), test_dir=case.test_dir,
                               partitions=case.partitions.get(s.copy_of or name))
        prep.rendered[name] = F.render(info, rows, case.watermarks, parts)
        F.write(prep.rendered[name], os.path.join(workdir, "sources", f"{name}.jsonl"))

    # what is captured
    if case.target == PIPELINE:
        sinks = list(p.sinks)
        quarantines = [m for m in p.models if any(e.on_violation == "quarantine" for e in p.models[m].expectations)]
        prep.targets = sinks + [f"{m}.quarantined" for m in quarantines]
        scope = set(p.models)
    else:
        sinks = [case.target] if case.target in p.sinks else []
        quarantines = []
        prep.targets = [case.target]
        rel = project.relation_of(case.target)
        scope = project.upstream(rel) | {rel}

    outputs: list[Output] = []
    setup: list[str] = []
    capture_ids = {s: f"sink-{_uid()}" for s in sinks}
    capture_ids.update({f"{m}__quarantine": f"quarantine-{_uid()}" for m in quarantines})
    python = None
    if any(f.language == "python" for f in p.functions.values()):
        from cascade_dsl.jvm import python_executable
        python = python_executable()
    for st in project.lowered.statements:
        if st.kind == "set" and st.name == "python.files" and python:
            for k in ("python.executable", "python.client.executable"):
                setup.append(f"SET '{k}' = '{python}';")
        if st.kind == "source":
            setup.append(_fixture_source(st, project, os.path.join(workdir, "sources", f"{st.name}.jsonl")).render())
        elif st.kind in ("sink", "quarantine"):
            if st.name in capture_ids:
                mode = p.sinks[st.name].mode if st.kind == "sink" else "append"
                setup.append(st.with_options({"connector": "cascade-capture", "id": capture_ids[st.name],
                                              "mode": mode}).render())
        elif st.kind == "insert":
            if st.name in capture_ids:
                if st.kind == "insert" and st.name in p.sinks:
                    outputs.append(Output(f"sink:{st.name}", "sink", st.name, st.name,
                                          project.materialize_key(st.name)))
                else:
                    model = st.name[: -len("__quarantine")]
                    outputs.append(Output(f"sink:{st.name}", "sink", f"{model}.quarantined", f"{model}.quarantined"))
                outputs[-1].meta = {"insert": st.render(), "table": st.name, "capture": capture_ids[st.name]}
        else:
            setup.append(st.render())
    setup += _test_settings(case)

    if case.target != PIPELINE and case.target not in p.sinks:
        outputs.append(Output("target", "target", case.target, case.target,
                              meta={"query": f"SELECT * FROM {q(project.view_of(case.target))}"}))

    # invariants: rows that violate a predicate (evaluated by Flink, on the final state)
    inv_specs = []
    if case.target == PIPELINE:
        for s, e in (case.expect.get("sinks") or {}).items():
            inv_specs += [(s, x) for x in e.get("invariants") or []]
    else:
        inv_specs += [(case.target, x) for x in case.expect.get("invariants") or []]
    for i, (t, pred) in enumerate(inv_specs):
        if t != PIPELINE and project.check_target(t) is None:
            view = project.view_of(t)
        else:
            raise PlanError(f"invariants: unknown target '{t}'")
        outputs.append(Output(f"invariant:{i}", "invariant", t, t,
                              meta={"query": f"SELECT * FROM {q(view)} WHERE NOT (({pred}) IS TRUE)",
                                    "predicate": pred}))

    # expectation probes
    wanted = set()
    for k in (case.expect.get("expectations") or {}):
        wanted.add(k.split(".")[0] if "." in k else k)
    probe = [m for m in project.models_with_expectations(scope)
             if coverage or m in wanted or any(e.name in wanted for e in p.models[m].expectations)]
    for m in probe:
        exps = p.models[m].expectations
        flags = ", ".join(f"CASE WHEN ({e.check}) IS TRUE THEN 0 ELSE 1 END AS {q('e' + str(i))}"
                          for i, e in enumerate(exps))
        outputs.append(Output(f"expect:{m}", "expectations", m, m,
                              meta={"query": f"SELECT {flags} FROM {q(project.expectation_base(m))}",
                                    "names": [e.name for e in exps]}))
    prep.probed = probe

    payload_outputs = []
    for o in outputs:
        if "insert" in o.meta:
            payload_outputs.append({"name": o.name, "insert": o.meta["insert"], "table": o.meta["table"],
                                    "capture": o.meta["capture"]})
        else:
            payload_outputs.append({"name": o.name, "query": o.meta["query"]})
    if case.clock:
        from . import clock
        when = clock.instant(case.clock, prep.base_time)
        setup = [clock.freeze(s, when)[0] for s in setup]
        for o in payload_outputs:
            for k in ("insert", "query"):
                if k in o:
                    o[k] = clock.freeze(o[k], when)[0]
    prep.requests.append(Request({"id": f"{case.id}#{_uid()}", "setup": setup, "outputs": payload_outputs,
                                  "timeout": case.timeout, "config": {}}, outputs))


def _test_settings(case: TestCase) -> list[str]:
    out = []
    settings = {"parallelism.default": str(case.parallelism or 1)}
    settings.update({k: str(v) for k, v in case.config.items()})
    for k, v in settings.items():
        out.append(f"SET '{k}' = '{str(v).replace(chr(39), chr(39) * 2)}';")
    return out


def _fixture_source(st: Stmt, project: Project, path: str) -> Stmt:
    src = project.pipeline.sources[st.name]
    updating = project.pipeline.relations[st.name].changelog == "updating"
    extra = [x for x in st.extra if not x.startswith("WATERMARK FOR")]
    if src.event_time:
        extra.append(f"WATERMARK FOR {q(src.event_time.column)} AS SOURCE_WATERMARK()")
    return Stmt(st.kind, st.name, st.sql, st.comment, st.columns, extra,
                {"connector": "cascade-fixture", "path": path, "changelog": "all" if updating else "insert-only"})


def _prepare_calls(project: Project, case: TestCase, prep: Prepared):
    setup = [st.render() for st in project.lowered.statements if st.kind in ("set", "function")]
    python = None
    if any(f.language == "python" for f in project.pipeline.functions.values()):
        from cascade_dsl.jvm import python_executable
        python = python_executable()
        if python:
            setup += [f"SET '{k}' = '{python}';" for k in ("python.executable", "python.client.executable")]
    setup += _test_settings(case)
    prep.targets = []
    for i, c in enumerate(case.calls or []):
        out = Output("call", "call", str(c["call"]), meta={"query": f"SELECT {c['call']} AS `result`", "spec": c,
                                                           "index": i})
        prep.requests.append(Request({"id": f"{case.id}#call{i}#{_uid()}", "setup": setup,
                                      "outputs": [{"name": "call", "query": out.meta["query"]}],
                                      "timeout": case.timeout, "config": {}}, [out]))


def script(prep: Prepared) -> str:
    """The SQL a test runs (for --show-sql and debugging)."""
    parts = []
    for r in prep.requests:
        parts += r.payload["setup"]
        for o in r.payload["outputs"]:
            parts.append(o.get("insert") or f"-- output {o['name']}\n{o['query']};")
    return "\n\n".join(parts) + "\n"
