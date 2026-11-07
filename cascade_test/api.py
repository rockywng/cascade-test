"""Python API: test pipelines from pytest (or a notebook) with the same engine and diff engine.

    from cascade_test import project

    orders = project("examples/orders")          # compiled once, engine shared by the session

    def test_ltv():
        r = orders.run("customer_ltv", given={"orders": [
            {"order_id": "o1", "customer_id": "c1", "amount": 10, "order_ts": "+0s"},
            {"order_id": "o2", "customer_id": "c1", "amount": 5,  "order_ts": "+1m"},
        ]})
        r.assert_rows([{"customer_id": "c1", "lifetime_value": 15, "order_count": 2}])

    def test_whole_pipeline():
        r = orders.pipeline(given={...})
        r.sink("ltv_to_kafka").assert_count(2)

Rows are given exactly as in YAML tests (relative times, `_op`, `_partition`, `_watermark`, ...).
"""
from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import replace

from . import diff as D
from . import plan as P
from . import runner as R
from . import values as V
from .engine import Engine, shared_engine
from .project import Project as _Compiled
from .spec import PIPELINE, Location, TestCase

_projects: dict = {}


def find_root(path: str) -> str:
    """The project containing `path` (a project directory, or any file or directory inside one,
    e.g. a test's __file__)."""
    d = os.path.abspath(path)
    if os.path.isfile(d) and d.endswith((".yaml", ".py")) and os.path.basename(d).startswith("pipeline."):
        return d
    d = d if os.path.isdir(d) else os.path.dirname(d)
    while True:
        if any(os.path.exists(os.path.join(d, f)) for f in ("pipeline.yaml", "pipeline.py")):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            raise FileNotFoundError(f"no cascade project (pipeline.yaml or pipeline.py) at or above {path}")
        d = parent


def project(path: str, env: str | None = None, engine: Engine | None = None) -> "Project":
    """A project, compiled once per process (cached by path and env). `path` may be the project
    directory or any file inside it: `project(__file__)` in a test works from any directory."""
    path = find_root(path)
    key = (path, env)
    if key not in _projects:
        _projects[key] = Project(path, env, engine)
    return _projects[key]


class Project:
    def __init__(self, path: str, env: str | None = None, engine: Engine | None = None):
        self.compiled = _Compiled(path, env)
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine or shared_engine()

    @property
    def pipeline(self):
        return self.compiled.pipeline

    def run(self, target: str, given: dict | None = None, *, watermarks: str = "per_row",
            partitions: dict | None = None, base_time: str | None = None, defaults: dict | None = None,
            parallelism: int | None = None, timeout: int = 60, config: dict | None = None,
            key: list[str] | None = None, tolerance=None) -> "Result":
        """Run a model, `<model>.quarantined`, a sink, or `pipeline` on fixture rows."""
        case = TestCase(name=f"api:{target}", loc=Location("<python>"), target=target, given=dict(given or {}),
                        expect={}, watermarks=watermarks, partitions=dict(partitions or {}), base_time=base_time,
                        defaults=dict(defaults or {}), parallelism=parallelism, timeout=timeout,
                        config=dict(config or {}), key=key, tolerance=tolerance,
                        file=os.path.join(self.compiled.root, "tests", "<python>"))
        return self._execute(case)

    def pipeline_run(self, given: dict | None = None, **kw) -> "Result":
        return self.run(PIPELINE, given, **kw)

    pipeline_test = pipeline_run

    def call(self, expression: str):
        """Evaluate a SQL expression (e.g. a UDF call) and return its value."""
        case = TestCase(name="api:call", loc=Location("<python>"), target=None, given={}, expect={},
                        calls=[{"call": expression}], file=os.path.join(self.compiled.root, "tests", "<python>"))
        work = tempfile.mkdtemp(prefix="cascade-test-")
        try:
            prep = P.prepare(self.compiled, case, work)
            raw = self.engine.run([r.payload for r in prep.requests])[0]
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if raw.get("error"):
            raise AssertionError(f"{expression} failed:\n{R.short_error(raw['error'])}")
        out = raw["outputs"]["call"]
        rows = D.materialize(["result"], out["rows"])
        return V.normalize(rows[0]["result"], out["columns"][0]["type"]) if rows else None

    def _execute(self, case: TestCase) -> "Result":
        work = tempfile.mkdtemp(prefix="cascade-test-")
        try:
            prep = P.prepare(self.compiled, case, work, coverage=True)
            if prep.error:
                raise ValueError(prep.error)
            raw = self.engine.run([r.payload for r in prep.requests])
            res = R.evaluate(self.compiled, prep, raw, R.Options())
        finally:
            shutil.rmtree(work, ignore_errors=True)
        return Result(self.compiled, case, res)


class Result:
    """The outcome of a run. For a pipeline run, `sink(name)` gives each sink's result."""

    def __init__(self, compiled, case: TestCase, res: R.TestResult, output: str | None = None):
        self._compiled = compiled
        self.case = case
        self._res = res
        self.error = res.job_error
        self.expectations = res.expectation_counts
        self.millis = res.millis
        self.late_rows = {s: rd.late_rows for s, rd in res.prep.rendered.items() if rd.late_rows}
        if output is None:
            output = "target" if case.target != PIPELINE and case.target not in compiled.pipeline.sinks \
                else (f"sink:{case.target}" if case.target != PIPELINE else None)
        self._out = res.outputs.get(output) if output else None
        self._target = case.target

    # --- data ----------------------------------------------------------------------------------

    def _require(self) -> R.OutputResult:
        if self.error:
            raise AssertionError(f"the job failed:\n{R.short_error(self.error)}")
        if self._out is None:
            raise AssertionError("this is a pipeline run: pick a sink with .sink(name)")
        return self._out

    @property
    def columns(self) -> list[str]:
        return self._require().names

    @property
    def rows(self) -> list[dict]:
        """Final state, with values as Python types (Decimal, datetime, int, ...)."""
        out = self._require()
        return [{c: V.normalize(r.get(c), out.types[c]) for c in out.names} for r in out.rows]

    @property
    def changelog(self) -> list[dict]:
        out = self._require()
        return [{"_op": c["kind"], **{n: V.normalize(v, out.types[n]) for n, v in zip(out.names, c["values"])}}
                for c in out.changes]

    def sink(self, name: str) -> "Result":
        if self._target != PIPELINE:
            raise ValueError("sink() is for pipeline runs")
        label = name
        for o in self._res.prep.requests[0].outputs:
            if o.label == label:
                return Result(self._compiled, self.case, self._res, o.name)
        raise KeyError(f"'{name}' is not a sink of this pipeline")

    def to_pandas(self):
        import pandas as pd
        return pd.DataFrame(self.rows, columns=self.columns)

    def __len__(self):
        return len(self._require().rows)

    def __repr__(self):
        if self.error:
            return f"<Result {self._target}: FAILED {self.error.splitlines()[0]}>"
        if self._out is None:
            return f"<Result {self._target}: sinks {[o.label for o in self._res.prep.requests[0].outputs]}>"
        return f"<Result {self._target}: {len(self._out.rows)} row(s)>\n" + D.table(self._out.rows, self._out.names, 20)

    # --- assertions ----------------------------------------------------------------------------

    def _check(self, expect: dict, **overrides):
        case = replace(self.case, expect=expect, **overrides)
        res = R.TestResult(case, "pass", prep=self._res.prep)
        out = self._require()
        R._check_output(self._compiled, case, self._res.prep, res, out, expect, self._label(), None, R.Options())
        if res.failures:
            raise AssertionError("\n" + res.message + ("\n" + "\n".join(res.notes) if res.notes else ""))
        return self

    def _label(self) -> str:
        return self._out.label if self._out is not None else str(self._target)

    def assert_rows(self, rows: list[dict], *, ordered: bool = False, key=None, tolerance=None, ignore=()):
        """The final output is exactly these rows (only the columns named are compared)."""
        return self._check({"rows": rows}, ordered=ordered, key=key or self.case.key,
                           tolerance=tolerance if tolerance is not None else self.case.tolerance,
                           ignore_columns=list(ignore))

    def assert_contains(self, rows: list[dict], *, tolerance=None):
        return self._check({"contains": rows}, tolerance=tolerance)

    def assert_excludes(self, rows: list[dict]):
        return self._check({"excludes": rows})

    def assert_count(self, n: int | None = None, *, min: int | None = None, max: int | None = None):  # noqa: A002
        return self._check({"count": n if n is not None else {"min": min, "max": max}})

    def assert_empty(self):
        return self._check({"rows": []})

    def assert_changelog(self, changes: list[dict], *, ordered: bool = True):
        return self._check({"changelog": changes}, ordered=ordered)

    def assert_all(self, predicate, message: str = "predicate"):
        bad = [r for r in self.rows if not predicate(r)]
        if bad:
            raise AssertionError(f"{len(bad)} row(s) violate {message}:\n" + D.table(bad, self.columns, 10))
        return self

    def assert_error(self, text: str = ""):
        if not self.error:
            raise AssertionError(f"expected the job to fail{f' mentioning {text!r}' if text else ''}, but it succeeded")
        if text not in self.error:
            raise AssertionError(f"the job failed, but the error does not mention {text!r}:\n"
                                 f"{R.short_error(self.error)}")
        return self

    def assert_expectation(self, name: str, violations: int):
        model, exp = name.split(".", 1) if "." in name else (None, name)
        for m, counts in self.expectations.items():
            if (model is None or model == m) and exp in counts:
                if counts[exp] != violations:
                    raise AssertionError(f"expectation {m}.{exp}: expected {violations} violating row(s), "
                                         f"got {counts[exp]}")
                return self
        raise AssertionError(f"no expectation '{name}' upstream of {self._target}")
