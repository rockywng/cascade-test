"""Follow input rows through a pipeline, one stage at a time.

    cascade-test trace PATH --given rows.yaml            rows: {source: [row, ...]}
    cascade-test trace PATH --recording DIR --sample 5   a seeded random sample of the recording

Every model and sink is run as its own test on the same input, so the output shows what each stage
makes of the rows: the rows it emits, and where rows stop appearing (a filter, a join that found no
partner, a data-quality expectation that rejected them). One job per stage runs on a warm engine, in
parallel; the sample is picked by --seed, so the same sample can be traced again after a change.
"""
from __future__ import annotations

import json
import os
import random
import shutil
import sys
import tempfile

import yaml

from . import plan as P
from . import runner as R
from .engine import Engine
from .project import Project
from .replay import ReplayError, given_from, load_recording, to_fixture
from .spec import Location, TestCase


class TraceError(ValueError):
    pass


def stage_order(project: Project) -> list[str]:
    """Models in dependency order (inputs first), then the sinks."""
    p = project.pipeline
    models = [m for m, mod in p.models.items() if not mod.copy_of]
    order, seen = [], set()

    def visit(name):
        if name in seen or name not in p.models:
            return
        seen.add(name)
        rel = p.relations.get(name)
        for dep in (rel.inputs if rel else []):
            visit(dep)
        if name in models:
            order.append(name)

    for m in models:
        visit(m)
    return order + [s for s in p.sinks]


def sample_given(project: Project, path: str, sample: int, seed: int, warnings: list[str]) -> tuple[dict, dict]:
    """A seeded sample of `sample` records per source of the recording (in recorded order)."""
    rec = load_recording(path)
    rng = random.Random(seed)
    picked = type(rec)(rec.path, rec.meta)
    picked.lookups = rec.lookups
    for name, records in rec.records.items():
        idx = sorted(rng.sample(range(len(records)), min(sample, len(records))))
        picked.records[name] = [records[i] for i in idx]
    return given_from(project, picked, None, None, "recorded", warnings)


def load_given(path: str) -> dict:
    with open(path) as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict) or not all(isinstance(v, list) for v in data.values()):
        raise TraceError(f"{path}: expected a mapping of source -> list of rows")
    return data


def trace(engine: Engine, project: Project, given: dict, partitions: dict | None = None, timeout: int = 120,
          stages: list[str] | None = None, clock: str | None = None) -> list[dict]:
    """[{stage, kind, rows, columns, rejected, error}] for each stage, inputs first."""
    p = project.pipeline
    names = stages or stage_order(project)
    work = tempfile.mkdtemp(prefix="cascade-trace-")
    try:
        preps = []
        for i, name in enumerate(names):
            case = TestCase(name=f"trace-{name}", loc=Location("<trace>"), target=name, given=given, expect={},
                            timeout=timeout, partitions=dict(partitions or {}), clock=clock,
                            file=os.path.join(project.root, "tests", "<trace>"))
            preps.append(P.prepare(project, case, os.path.join(work, f"s{i}")))
        runnable = [(n, pr) for n, pr in zip(names, preps) if not pr.error]
        raw = engine.run([pr.requests[0].payload for _, pr in runnable]) if runnable else []
        results = {}
        for (n, pr), r in zip(runnable, raw):
            results[n] = R.evaluate(project, pr, [r], R.Options())
        out = []
        for n, pr in zip(names, preps):
            entry = {"stage": n, "kind": "sink" if n in p.sinks else "model", "rows": [], "columns": [],
                     "rejected": {}, "error": None}
            if pr.error:
                entry["error"] = pr.error
            else:
                res = results[n]
                if res.job_error:
                    entry["error"] = R.short_error(res.job_error)
                else:
                    o = next((o for k, o in res.outputs.items() if k in ("target", f"sink:{n}")), None)
                    if o is not None:
                        entry["rows"] = o.rows
                        entry["columns"] = [c for c, _ in o.columns]
                    entry["rejected"] = {f"{m}.{e}": c for m, cs in res.expectation_counts.items()
                                         for e, c in cs.items() if c}
            out.append(entry)
        return out
    finally:
        shutil.rmtree(work, ignore_errors=True)


def render(stages: list[dict], given: dict, max_rows: int = 5) -> str:
    lines = ["input"]
    for src, rows in given.items():
        lines.append(f"  {src}: {len(rows)} row(s)")
    prev = sum(len(r) for r in given.values())
    for s in stages:
        lines.append("")
        head = f"{s['kind']} {s['stage']}"
        if s["error"]:
            lines.append(f"{head}: failed: {s['error']}")
            continue
        n = len(s["rows"])
        note = ""
        if n == 0 and prev:
            note = "  <- nothing comes out"
            if s["rejected"]:
                note += " (rejected by " + ", ".join(f"{k}: {v}" for k, v in s["rejected"].items()) + ")"
        elif s["rejected"]:
            note = "  (rejected " + ", ".join(f"{k}: {v}" for k, v in s["rejected"].items()) + ")"
        lines.append(f"{head}: {n} row(s){note}")
        for row in s["rows"][:max_rows]:
            lines.append("  " + json.dumps(row, default=str))
        if n > max_rows:
            lines.append(f"  ... {n - max_rows} more")
        prev = n if n else prev
    return "\n".join(lines)


def run(args) -> int:
    project = Project(args.path, args.env)
    warnings: list[str] = []
    partitions: dict = {}
    try:
        if bool(args.given) == bool(args.recording):
            raise TraceError("give the input as --given FILE or --recording DIR [--sample N]")
        if args.given:
            given = load_given(args.given)
            for name in given:
                if name not in project.pipeline.sources:
                    raise TraceError(f"'{name}' is not a source of {project.name}")
        else:
            given, partitions = sample_given(project, args.recording, args.sample, args.seed, warnings)
    except (TraceError, ReplayError) as e:
        print(f"cascade-test: {e}", file=sys.stderr)
        return 2
    stages = args.stage or None
    with Engine(args.flink_home, workers=args.workers) as engine:
        result = trace(engine, project, given, partitions, args.timeout, stages, args.clock)
    for w in warnings:
        print(f"note: {w}", file=sys.stderr)
    if args.json:
        print(json.dumps({"input": given, "stages": result}, indent=2, default=str))
    else:
        print(render(result, given, args.rows))
    return 1 if any(s["error"] for s in result) else 0
