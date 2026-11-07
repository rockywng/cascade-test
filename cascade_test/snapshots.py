"""Snapshot (golden output) tests: `expect: {snapshot: true}`.

The first run records the output in tests/__snapshots__/<test file>.snap.yaml (commit it); later
runs compare against it with the diff engine. `--update-snapshots` re-records after an intended
change, and `--ci` fails instead of recording when a snapshot is missing. Columns computed from
processing time or randomness are left out, since they would differ on every run.
"""
from __future__ import annotations

import os
import threading

import yaml

from . import values as V

HEADER = "# cascade-test snapshots. Re-record with: cascade-test run --update-snapshots\n"


def path_for(test_file: str) -> str:
    d = os.path.join(os.path.dirname(test_file), "__snapshots__")
    stem = os.path.splitext(os.path.basename(test_file))[0]
    return os.path.join(d, f"{stem}.snap.yaml")


class _Dumper(yaml.SafeDumper):
    pass


def _str(dumper, s):
    style = "|" if "\n" in s else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", s, style=style)


_Dumper.add_representer(str, _str)


class Store:
    """Snapshots of one test file. Writes happen at the end of the run (`save`)."""

    _locks: dict = {}

    def __init__(self, test_file: str):
        self.path = path_for(test_file)
        self.data: dict = {}
        self.dirty = False
        self.used: set = set()
        if os.path.exists(self.path):
            with open(self.path) as f:
                self.data = yaml.safe_load(f) or {}

    def get(self, test: str, output: str):
        self.used.add(test)
        return ((self.data.get(test) or {}).get(output))

    def put(self, test: str, output: str, columns: list[str], rows: list[dict]):
        self.used.add(test)
        entry = self.data.setdefault(test, {})
        entry[output] = {"columns": columns, "rows": rows}
        self.dirty = True

    def prune(self, known: set[str]) -> list[str]:
        stale = [t for t in self.data if t not in known]
        for t in stale:
            del self.data[t]
            self.dirty = True
        return stale

    def save(self):
        if not self.dirty:
            return
        lock = Store._locks.setdefault(self.path, threading.Lock())
        with lock:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "w") as f:
                f.write(HEADER)
                yaml.dump(dict(sorted(self.data.items())), f, Dumper=_Dumper, sort_keys=False,
                          default_flow_style=None, width=100000, allow_unicode=True)
        self.dirty = False


def plain_rows(rows: list[dict], columns: list[str], types: dict) -> list[dict]:
    """Rows as stable, readable YAML values, sorted so the file diffs well in review."""
    def plain(v, t):
        if v is not None and V.base_type(t) in ("DECIMAL", "NUMERIC"):
            return str(v)  # keep the scale: 12.50, not 12.5
        return V.to_plain(V.normalize(v, t))
    out = [{c: plain(r.get(c), types.get(c)) for c in columns} for r in rows]
    out.sort(key=lambda r: [("" if r[c] is None else str(r[c])) for c in columns])
    return out
