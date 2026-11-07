"""pytest integration (installed automatically with cascade-test).

- YAML tests in a cascade project's tests/ directory are collected as pytest items, so
  `pytest examples/orders` runs them next to Python tests, with -k, -x, JUnit XML, IDE runners and
  everything else pytest has. All selected YAML tests of a project run in one batch on the shared
  engine; each item then reports its own result.
- Python tests get the `cascade_project` fixture (and can import `cascade_test.project` directly).

Options: --cascade-update-snapshots, --cascade-ci, --flink-home.
"""
from __future__ import annotations

import os

import pytest

from . import runner
from .api import project as _project
from .engine import shared_engine
from .project import Project
from .spec import SpecError, load_file


def pytest_addoption(parser):
    g = parser.getgroup("cascade-test")
    g.addoption("--cascade-update-snapshots", action="store_true", help="re-record cascade snapshots that differ")
    g.addoption("--cascade-ci", action="store_true", help="fail on missing cascade snapshots instead of recording")
    g.addoption("--flink-home", help="Flink distribution for cascade tests (default: $FLINK_HOME)")


def _project_root(path: str) -> str | None:
    d = os.path.dirname(os.path.abspath(path))
    while True:
        if os.path.basename(d) == "tests":
            root = os.path.dirname(d)
            if any(os.path.exists(os.path.join(root, f)) for f in ("pipeline.yaml", "pipeline.py")):
                return root
        parent = os.path.dirname(d)
        if parent == d:
            return None
        d = parent


def pytest_collect_file(parent, file_path):
    p = str(file_path)
    if not p.endswith((".yaml", ".yml")) or "__snapshots__" in p or p.endswith(".snap.yaml"):
        return None
    root = _project_root(p)
    if root is None:
        return None
    with open(p) as f:
        if "tests:" not in f.read():
            return None
    return CascadeFile.from_parent(parent, path=file_path)


class CascadeFile(pytest.File):
    def collect(self):
        try:
            cases = load_file(str(self.path))
        except SpecError as e:
            raise self.CollectError(str(e))
        root = _project_root(str(self.path))
        for case in cases:
            item = CascadeItem.from_parent(self, name=case.name, case=case, root=root)
            for tag in case.tags:
                item.add_marker(tag)
            if case.skip:
                item.add_marker(pytest.mark.skip(reason=case.skip))
            yield item


class CascadeFailure(Exception):
    def __init__(self, result):
        super().__init__(result.message)
        self.result = result


class _Batch:
    """All selected YAML tests of one project, run together the first time any of them runs."""

    def __init__(self, root: str, items: list):
        self.root = root
        self.items = items
        self.results: dict | None = None
        self.error: Exception | None = None

    def run(self, config):
        if self.results is not None or self.error is not None:
            return
        try:
            proj = Project(self.root)
            cases = [i.case for i in self.items]
            opts = runner.Options(update_snapshots=config.getoption("--cascade-update-snapshots"),
                                  ci=config.getoption("--cascade-ci"))
            engine = shared_engine(config.getoption("--flink-home"))
            results = runner.run(proj, cases, engine, opts)
            self.results = {id(r.case): r for r in results}
        except Exception as e:  # noqa: BLE001 - reported on every item of the batch
            self.error = e


class CascadeItem(pytest.Item):
    def __init__(self, *, case, root, **kw):
        super().__init__(**kw)
        self.case = case
        self.root = root

    def runtest(self):
        batch = self.config.stash[_BATCHES][self.root]
        batch.run(self.config)
        if batch.error is not None:
            raise batch.error
        r = batch.results[id(self.case)]
        for note in r.notes:
            if "snapshot" in note:
                self.add_report_section("call", "cascade", note)
        if r.status == "skip":
            pytest.skip(r.notes[0] if r.notes else "skipped")
        if r.status in ("fail", "error"):
            raise CascadeFailure(r)

    def repr_failure(self, excinfo):
        if isinstance(excinfo.value, CascadeFailure):
            r = excinfo.value.result
            notes = ("\n" + "\n".join(r.notes)) if r.notes else ""
            return f"{r.message}{notes}\n  at {r.case.loc}"
        return super().repr_failure(excinfo)

    def reportinfo(self):
        return self.path, (self.case.loc.line or 1) - 1, f"cascade: {self.case.name} ({self.case.target})"


_BATCHES = pytest.StashKey[dict]()


def pytest_collection_modifyitems(session, config, items):
    batches: dict = {}
    for item in items:
        if isinstance(item, CascadeItem) and not item.case.skip:
            batches.setdefault(item.root, []).append(item)
    config.stash[_BATCHES] = {root: _Batch(root, its) for root, its in batches.items()}


@pytest.fixture(scope="session")
def cascade_project():
    """Factory: cascade_project("path/to/project") -> cascade_test.Project (compiled once)."""
    return _project
