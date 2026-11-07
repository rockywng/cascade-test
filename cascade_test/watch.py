"""`cascade-test watch`: re-run tests on save, on an engine that stays warm.

After the first run, a change usually re-runs in well under a second: the JVM, the MiniCluster and
the planner are already loaded. Only affected tests run: editing a test file re-runs that file;
editing a model's SQL re-runs the tests that read that model (directly or downstream).
"""
from __future__ import annotations

import os
import sys
import time

from . import report, runner
from .engine import Engine
from .project import Project, ProjectError
from .spec import PIPELINE, SpecError, discover


def _mtimes(files) -> dict:
    out = {}
    for f in files:
        try:
            out[f] = os.path.getmtime(f)
        except OSError:
            pass
    return out


def affected(project: Project, cases, changed: set[str]) -> list:
    """Tests to re-run for these changed files."""
    if not changed:
        return cases
    tests = {f for f in changed if os.sep + "tests" + os.sep in f}
    other = changed - tests
    if not other:
        return [c for c in cases if c.file in tests]
    p = project.pipeline
    model_files = {}
    for m in p.models.values():
        loc = m.sql_loc
        if loc and loc.file and os.path.abspath(loc.file) != os.path.abspath(p.file):
            model_files.setdefault(os.path.abspath(loc.file), set()).add(m.name)
    touched = set()
    for f in other:
        if f in model_files:
            touched |= model_files[f]
        else:
            return cases  # pipeline definition, UDFs, fixtures: anything may change
    keep = []
    for c in cases:
        if c.file in tests or c.target == PIPELINE or c.kind == "calls":
            keep.append(c)
            continue
        rel = project.relation_of(c.target)
        if (project.upstream(rel) | {rel}) & touched:
            keep.append(c)
    return keep


def watch(args) -> int:
    path = args.paths or "."
    style = report.style_for(color=False if args.no_color else None)
    print(style.dim("starting the engine..."))
    with Engine(args.flink_home, workers=args.workers) as engine:
        print(style.dim(f"engine ready (Flink {engine.info.get('flink_version')}); watching {os.path.abspath(path)}"))
        seen: dict = {}
        first = True
        while True:
            try:
                project = Project(path, args.env)
                files = project.files()
            except ProjectError as e:
                project, files = None, _all_files(path)
                if _changed(seen, files) or first:
                    _clear(style)
                    print(style.red(str(e)))
                    first = False
                seen = _mtimes(files)
                time.sleep(args.interval)
                continue
            now = _mtimes(files)
            changed = {f for f, m in now.items() if seen.get(f) != m} if not first else set()
            if first or changed:
                seen = now
                try:
                    cases = discover(project.root, args.k, args.tag)
                    todo = affected(project, cases, changed)
                    _clear(style)
                    if changed:
                        print(style.dim("changed: " + ", ".join(os.path.relpath(f) for f in sorted(changed))))
                    t0 = time.time()
                    results = runner.run(project, todo, engine)
                    report.console(results, style=style, elapsed=time.time() - t0)
                    if len(todo) < len(cases):
                        print(style.dim(f"({len(cases) - len(todo)} unaffected test(s) not re-run)"))
                except SpecError as e:
                    print(style.red(str(e)))
                print(style.dim(f"[{time.strftime('%H:%M:%S')}] watching for changes (Ctrl-C to stop)"))
                first = False
            time.sleep(args.interval)


def _changed(seen: dict, files) -> bool:
    return _mtimes(files) != seen


def _all_files(path: str) -> list[str]:
    out = []
    for d, dirs, names in os.walk(path):
        dirs[:] = [x for x in dirs if not x.startswith(".")]
        out += [os.path.join(d, n) for n in names if n.endswith((".yaml", ".yml", ".sql", ".py"))]
    return out


def _clear(style):
    if sys.stdout.isatty():
        print("\033[2J\033[H", end="")
