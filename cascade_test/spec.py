"""Test files: tests/**/*.yaml in a cascade project.

Every file that `cascade test` reads works unchanged; the additions are all optional. See
docs/test-spec.md for the full reference. In short:

    defaults:                       # file-wide settings, any test can override them
      base_time: "2026-01-01 10:00:00"
      rows: {orders: {currency: usd, order_id: "o{n}"}}
    fixtures:                       # named inputs that tests pull in with `use:`
      acme: {customers: [{customer_id: c1, name: Acme}]}
    tests:
      - name: ...
        target: customer_ltv        # a model | <model>.quarantined | a sink | pipeline
        use: [acme]
        given: {orders: [...]}
        parameters: [{amount: 0}, {amount: -1}]   # one test per entry; "${amount}" substituted
        expect: {rows | contains | excludes | count | changelog | empty | error | snapshot |
                 invariants | expectations | late | sinks}
"""
from __future__ import annotations

import copy
import difflib
import glob
import os
import re
from dataclasses import dataclass, field

import yaml

from cascade_dsl.loader import _Loader

TEST_KEYS = {"name", "description", "target", "given", "use", "expect", "ordered", "watermarks", "partitions",
             "base_time", "defaults", "key", "tolerance", "ignore_columns", "parallelism", "timeout", "tags",
             "skip", "only", "parameters", "calls", "config"}
EXPECT_KEYS = {"rows", "contains", "excludes", "count", "changelog", "empty", "error", "snapshot",
               "invariants", "expectations", "late", "sinks"}
OUTPUT_EXPECT_KEYS = EXPECT_KEYS - {"error", "expectations", "late", "sinks"}
FILE_KEYS = {"tests", "defaults", "fixtures"}
DEFAULT_KEYS = {"base_time", "rows", "watermarks", "tolerance", "timeout", "parallelism", "ignore_columns",
                "partitions", "config", "ordered"}
WATERMARK_MODES = ("per_row", "manual", "end_of_input")
PIPELINE = "pipeline"


class SpecError(ValueError):
    pass


@dataclass
class Location:
    file: str
    line: int | None = None

    def __str__(self):
        return f"{self.file}:{self.line}" if self.line else self.file


@dataclass
class TestCase:
    name: str
    loc: Location
    target: str | None
    given: dict
    expect: dict
    description: str | None = None
    ordered: bool = False
    watermarks: str = "per_row"
    partitions: dict = field(default_factory=dict)  # source -> declared partition count
    base_time: str | None = None
    defaults: dict = field(default_factory=dict)  # source -> column defaults
    key: list[str] | None = None
    tolerance: object = None
    ignore_columns: list[str] = field(default_factory=list)
    parallelism: int | None = None
    timeout: int = 60
    tags: list[str] = field(default_factory=list)
    skip: str | None = None
    only: bool = False
    calls: list[dict] | None = None
    config: dict = field(default_factory=dict)
    file: str = ""  # the test file (absolute)
    params: dict | None = None

    @property
    def id(self) -> str:
        return f"{os.path.basename(self.file)}::{self.name}"

    @property
    def kind(self) -> str:
        if self.calls is not None:
            return "calls"
        return "pipeline" if self.target == PIPELINE else "target"

    @property
    def test_dir(self) -> str:
        return os.path.dirname(self.file)


def _suggest(name, options) -> str:
    m = difflib.get_close_matches(str(name), sorted(options), n=1)
    return f" (did you mean '{m[0]}'?)" if m else ""


def test_files(root: str) -> list[str]:
    tests = os.path.join(root, "tests")
    files = glob.glob(os.path.join(tests, "**", "*.yaml"), recursive=True) + \
        glob.glob(os.path.join(tests, "**", "*.yml"), recursive=True)
    return sorted(f for f in files if "__snapshots__" not in f.split(os.sep) and not f.endswith(".snap.yaml"))


def load_file(path: str) -> list[TestCase]:
    rel = os.path.relpath(path)
    with open(path) as f:
        try:
            doc = yaml.load(f, Loader=_Loader) or {}
        except yaml.YAMLError as e:
            raise SpecError(f"{rel}: invalid YAML: {e}")
    if not isinstance(doc, dict):
        raise SpecError(f"{rel}: a test file is a mapping with a `tests` list")
    unknown = set(doc) - FILE_KEYS
    if unknown:
        k = sorted(unknown)[0]
        raise SpecError(f"{rel}: unknown top-level key '{k}'{_suggest(k, FILE_KEYS)}")
    defaults = doc.get("defaults") or {}
    bad = set(defaults) - DEFAULT_KEYS
    if bad:
        k = sorted(bad)[0]
        raise SpecError(f"{rel}: unknown key '{k}' in defaults{_suggest(k, DEFAULT_KEYS)}")
    fixtures = doc.get("fixtures") or {}
    cases: list[TestCase] = []
    seen: set = set()
    for t in doc.get("tests") or []:
        loc = Location(rel, getattr(t, "line", None))
        for case in _expand(t, loc, defaults, fixtures, path):
            if case.name in seen:
                raise SpecError(f"{case.loc}: duplicate test name '{case.name}'")
            seen.add(case.name)
            cases.append(case)
    return cases


def discover(root: str, pattern: str | None = None, tags: list[str] | None = None) -> list[TestCase]:
    cases = []
    for path in test_files(root):
        cases += load_file(path)
    return select(cases, pattern, tags)


def select(cases: list[TestCase], pattern: str | None = None, tags: list[str] | None = None) -> list[TestCase]:
    if pattern:
        cases = [c for c in cases if pattern in c.name or pattern in c.id]
    if tags:
        want = {t for t in tags if not t.startswith("!")}
        avoid = {t[1:] for t in tags if t.startswith("!")}
        cases = [c for c in cases if (not want or want & set(c.tags)) and not avoid & set(c.tags)]
    if any(c.only for c in cases):
        cases = [c for c in cases if c.only]
    return cases


def _expand(t, loc: Location, defaults: dict, fixtures: dict, path: str) -> list[TestCase]:
    if not isinstance(t, dict) or "name" not in t:
        raise SpecError(f"{loc}: each test needs a `name`")
    unknown = set(t) - TEST_KEYS
    if unknown:
        k = sorted(unknown)[0]
        raise SpecError(f"{loc}: unknown key '{k}' in test '{t['name']}'{_suggest(k, TEST_KEYS)}")
    params = t.get("parameters")
    if params is None:
        return [_case(t, loc, defaults, fixtures, path, None)]
    if isinstance(params, dict):
        items = list(params.items())
    elif isinstance(params, list):
        items = [(_label(p, i), p) for i, p in enumerate(params)]
    else:
        raise SpecError(f"{loc}: `parameters` is a list of mappings, or a mapping of case name -> mapping")
    out = []
    for label, p in items:
        if not isinstance(p, dict):
            raise SpecError(f"{loc}: each parameter set must be a mapping, got {p!r}")
        body = _substitute({k: v for k, v in t.items() if k != "parameters"}, p, loc)
        body["name"] = f"{t['name']}[{label}]"
        out.append(_case(body, loc, defaults, fixtures, path, dict(p)))
    return out


def _label(p, i) -> str:
    if isinstance(p, dict) and len(p) <= 3:
        text = ",".join(f"{k}={v}" for k, v in p.items())
        if len(text) <= 40:
            return text
    return str(i + 1)


_PARAM = re.compile(r"\$\{(\w+)\}")


def _substitute(v, params: dict, loc):
    if isinstance(v, str):
        m = _PARAM.fullmatch(v.strip())
        if m:
            if m.group(1) not in params:
                raise SpecError(f"{loc}: unknown parameter '${{{m.group(1)}}}'{_suggest(m.group(1), params)}")
            return copy.deepcopy(params[m.group(1)])

        def rep(mm):
            if mm.group(1) not in params:
                raise SpecError(f"{loc}: unknown parameter '${{{mm.group(1)}}}'{_suggest(mm.group(1), params)}")
            return str(params[mm.group(1)])
        return _PARAM.sub(rep, v)
    if isinstance(v, dict):
        return {k: _substitute(x, params, loc) for k, x in v.items()}
    if isinstance(v, list):
        return [_substitute(x, params, loc) for x in v]
    return v


def _case(t: dict, loc: Location, defaults: dict, fixtures: dict, path: str, params) -> TestCase:
    name = str(t["name"])
    calls = t.get("calls")
    target = t.get("target")
    if calls is None and not target:
        raise SpecError(f"{loc}: test '{name}' needs a `target` (a model, <model>.quarantined, a sink, or "
                        "`pipeline`) or `calls` (function calls to check)")
    if calls is not None:
        if not isinstance(calls, list) or not all(isinstance(c, dict) and "call" in c for c in calls):
            raise SpecError(f"{loc}: `calls` is a list of {{call: <SQL expression>, returns: <value>}}")
    given = {}
    for u in t.get("use") or []:
        if u not in fixtures:
            raise SpecError(f"{loc}: test '{name}' uses unknown fixture '{u}'{_suggest(u, fixtures)}")
        for src, rows in (fixtures[u] or {}).items():
            given[src] = _merge_fixture(given.get(src), rows)
    for src, rows in (t.get("given") or {}).items():
        given[src] = _merge_fixture(given.get(src), rows)

    expect = dict(t.get("expect") or {})
    if calls is None:
        bad = set(expect) - EXPECT_KEYS
        if bad:
            k = sorted(bad)[0]
            raise SpecError(f"{loc}: unknown key '{k}' in expect of '{name}'{_suggest(k, EXPECT_KEYS)}")
        if not expect:
            raise SpecError(f"{loc}: test '{name}' has no `expect` (rows, contains, count, error, snapshot, ...)")
        if target == PIPELINE:
            sinks = expect.get("sinks") or {}
            if not sinks and not expect.get("error") and not expect.get("expectations") and not expect.get("late"):
                raise SpecError(f"{loc}: a pipeline test expects per sink: expect: {{sinks: {{<sink>: {{rows: ...}}}}}}")
            for s, e in sinks.items():
                if not isinstance(e, dict):
                    raise SpecError(f"{loc}: expect.sinks.{s} must be a mapping (rows, contains, count, ...)")
                bad = set(e) - OUTPUT_EXPECT_KEYS
                if bad:
                    raise SpecError(f"{loc}: unknown key '{sorted(bad)[0]}' in expect.sinks.{s}")
        elif "sinks" in expect:
            raise SpecError(f"{loc}: expect.sinks is for `target: pipeline` tests")
        if expect.get("empty"):
            expect["rows"] = []

    wm = t.get("watermarks", defaults.get("watermarks", "per_row"))
    if wm not in WATERMARK_MODES:
        raise SpecError(f"{loc}: watermarks must be one of {', '.join(WATERMARK_MODES)}")
    row_defaults = {**(defaults.get("rows") or {})}
    for src, d in (t.get("defaults") or {}).items():
        row_defaults[src] = {**row_defaults.get(src, {}), **(d or {})}
    partitions = {**(defaults.get("partitions") or {}), **(t.get("partitions") or {})}
    skip = t.get("skip")
    key = t.get("key")
    return TestCase(
        name=name, loc=loc, target=target, given=given, expect=expect, description=t.get("description"),
        ordered=bool(t.get("ordered", defaults.get("ordered", False))), watermarks=wm, partitions=partitions,
        base_time=str(t.get("base_time") or defaults.get("base_time") or "") or None,
        defaults=row_defaults, key=[key] if isinstance(key, str) else key,
        tolerance=t.get("tolerance", defaults.get("tolerance")),
        ignore_columns=list(t.get("ignore_columns") or defaults.get("ignore_columns") or []),
        parallelism=t.get("parallelism", defaults.get("parallelism")),
        timeout=int(t.get("timeout", defaults.get("timeout", 60))),
        tags=list(t.get("tags") or []), skip=(skip if isinstance(skip, str) else "skipped") if skip else None,
        only=bool(t.get("only")), calls=calls, config={**(defaults.get("config") or {}), **(t.get("config") or {})},
        file=os.path.abspath(path), params=params)


def _merge_fixture(prev, new):
    """`use:` fixtures and `given` combine per source: rows are concatenated in order."""
    if prev is None:
        return new
    def rows_of(x):
        return x if isinstance(x, list) else None
    a, b = rows_of(prev), rows_of(new)
    if a is not None and b is not None:
        return a + b
    pa = prev if isinstance(prev, dict) else {"rows": prev}
    pb = new if isinstance(new, dict) else {"rows": new}
    merged = {**pa, **{k: v for k, v in pb.items() if k != "rows"}}
    merged["rows"] = list(pa.get("rows") or []) + list(pb.get("rows") or [])
    return merged
