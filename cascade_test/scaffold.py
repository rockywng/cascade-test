"""`cascade-test new`: write the boilerplate of a test.

Generates a test for a model or sink with one example row for every source it reads (columns
typed from the pipeline, the event-time column as a relative time). With `--record`, it runs the
test and fills `expect.rows` with what the pipeline produced, so writing a test becomes: generate,
edit the input to the case you care about, re-record, review the output, commit.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

import yaml

from . import plan as P
from . import runner as R
from .engine import Engine
from .project import Project
from .spec import PIPELINE, Location, TestCase
from .values import base_type


def example_value(name: str, typ: str, event_time: str | None, i: int = 1):
    b = base_type(typ)
    if name == event_time:
        return f"+{i - 1}m"
    if b in ("STRING", "VARCHAR", "CHAR"):
        return f"{name.split('_')[0][:1] or 'x'}{i}"
    if b in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
        return i
    if b in ("DECIMAL", "NUMERIC", "DOUBLE", "FLOAT"):
        return 10.0 * i
    if b == "BOOLEAN":
        return True
    if b in ("TIMESTAMP", "TIMESTAMP_LTZ"):
        return f"+{i - 1}m"
    if b == "DATE":
        return "2026-01-01"
    return None


def build(project: Project, target: str, name: str | None = None) -> dict:
    p = project.pipeline
    if target != PIPELINE:
        problem = project.check_target(target)
        if problem:
            raise SystemExit(problem)
        sources = [s for s in p.sources if s in project.upstream(project.relation_of(target))]
    else:
        sources = list(p.sources)
    given = {}
    for s in sources:
        src = p.sources[s]
        if src.copy_of:
            continue
        et = src.event_time.column if src.event_time else None
        given[s] = [{c.name: example_value(c.name, c.type.text, et, i) for c in src.columns} for i in (1, 2)]
    test = {"name": name or f"{target.replace('.', '_')}_works", "target": target, "given": given}
    if target == PIPELINE:
        test["expect"] = {"sinks": {k: {"count": 0} for k in p.sinks}}
    else:
        test["expect"] = {"rows": []}
    return test


def scaffold(args) -> int:
    project = Project(args.path, args.env)
    test = build(project, args.target, args.name)
    if args.record:
        case = TestCase(name=test["name"], loc=Location("<new>"), target=args.target, given=test["given"],
                        expect={}, file=os.path.join(project.root, "tests", "new.yaml"))
        work = tempfile.mkdtemp(prefix="cascade-new-")
        try:
            prep = P.prepare(project, case, work, coverage=True)
            if prep.error:
                print(prep.error, file=sys.stderr)
                return 1
            with Engine(getattr(args, "flink_home", None)) as engine:
                raw = engine.run([r.payload for r in prep.requests])
            res = R.evaluate(project, prep, raw, R.Options())
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if res.job_error:
            print(f"the job failed; writing the test without recorded output:\n{R.short_error(res.job_error)}",
                  file=sys.stderr)
        elif args.target == PIPELINE:
            test["expect"] = {"sinks": {o.label: {"rows": _plain(o, project, o.label)}
                                        for k, o in res.outputs.items() if k.startswith("sink:")}}
        else:
            out = next(iter(o for k, o in res.outputs.items() if k in ("target", f"sink:{args.target}")))
            test["expect"] = {"rows": _plain(out, project, args.target)}
            if not out.rows:
                rejected = [f"{m}.{e} rejected {n} row(s)" for m, c in res.expectation_counts.items()
                            for e, n in c.items() if n]
                print(f"note: {args.target} produced no rows for the example input"
                      + (f" ({'; '.join(rejected)})" if rejected else "") + "; edit `given` and re-record",
                      file=sys.stderr)
    text = yaml.safe_dump([test], sort_keys=False, default_flow_style=None, width=110)
    text = "\n".join("  " + line if line else line for line in text.splitlines()) + "\n"
    if not args.write:
        print("tests:\n" + text, end="")
        return 0
    path = os.path.join(project.root, "tests", f"{args.target.replace('.', '_')}_test.yaml")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    exists = os.path.exists(path)
    with open(path, "a") as f:
        if not exists:
            f.write("tests:\n")
        else:
            f.write("\n")
        f.write(text)
    print(f"{'appended to' if exists else 'wrote'} {os.path.relpath(path)}")
    return 0


def _plain(out: R.OutputResult, project: Project, target: str | None = None) -> list[dict]:
    from .snapshots import plain_rows
    nondet = set(project.nondeterministic_columns(target or out.label))
    cols = [c for c in out.names if c not in nondet]
    return plain_rows(out.rows, cols, out.types)

