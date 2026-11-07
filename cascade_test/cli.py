"""cascade-test command line.

    cascade-test [run] [PATH ...]      run tests (default: the project here, or every project below)
    cascade-test watch PATH            re-run affected tests on every save, on a warm engine
    cascade-test new PATH TARGET       scaffold a test for a model/sink (--record fills in the output)
    cascade-test timeline PATH -k T    show a test's event-time timeline: rows, watermarks, late rows
    cascade-test replay PATH ...       replay recorded production data; diff against a baseline/golden
    cascade-test record ...            turn a topic dump (kcat -J, JSON lines) into a recording
    cascade-test diff A B --key k      diff two captured outputs (e.g. shadow vs prod sink dumps)
    cascade-test ci PATH ...           everything a PR needs: tests, coverage, upgrade check, replay diff
    cascade-test doctor                check Java, Flink, cascade-dsl jars and the engine
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import time

from . import report
from .engine import Engine, EngineError
from .project import Project, ProjectError
from .runner import Options
from .spec import SpecError

COMMANDS = {"run", "watch", "new", "timeline", "replay", "record", "diff", "ci", "doctor"}


def find_projects(paths: list[str]) -> list[str]:
    """Each path is a project (has pipeline.yaml/.py) or a directory searched for projects with tests."""
    out = []
    for path in paths or ["."]:
        if os.path.isfile(path) or _is_project(path):
            out.append(path)
            continue
        found = sorted({os.path.dirname(p) for pat in ("pipeline.yaml", "pipeline.py")
                        for p in glob.glob(os.path.join(path, "**", pat), recursive=True)
                        if "/." not in p and os.path.isdir(os.path.join(os.path.dirname(p), "tests"))})
        if not found:
            raise SystemExit(f"cascade-test: no cascade project (pipeline.yaml or pipeline.py) in {path}")
        out += found
    return out


def _is_project(path: str) -> bool:
    return any(os.path.exists(os.path.join(path, f)) for f in ("pipeline.yaml", "pipeline.py"))


def run_suites(paths, engine: Engine, options: Options, pattern=None, tags=None, env=None, show_sql=False):
    """Load, plan and run every project's tests. Returns ([(name, results)], {name: (project, results)})."""
    from . import plan as P
    from . import runner
    from .spec import discover
    suites, detail = [], {}
    for path in find_projects(paths):
        project = Project(path, env)
        cases = discover(project.root, pattern, tags)
        results = runner.run(project, cases, engine, options)
        if show_sql:
            for r in results:
                if r.prep is not None:
                    print(f"-- {r.case.name}\n{P.script(r.prep)}")
        name = project.name if len(paths or ["."]) == 1 and len(find_projects(paths)) == 1 else \
            os.path.relpath(project.root)
        suites.append((name, results))
        detail[name] = (project, results)
    return suites, detail


def cmd_run(args) -> int:
    from . import coverage as C
    style = report.style_for(color=False if args.no_color else None)
    options = Options(update_snapshots=args.update_snapshots, ci=args.ci, coverage=args.coverage or
                      args.min_coverage is not None, keep_workdir=args.keep)
    t0 = time.time()
    with _engine(args) as engine:
        suites, detail = run_suites(args.paths, engine, options, args.k, args.tag, args.env, args.show_sql)
    elapsed = time.time() - t0
    failed = False
    cov_all = []
    for name, results in suites:
        project, _ = detail[name]
        report.console(results, verbose=args.verbose, style=style, elapsed=elapsed if len(suites) == 1 else None,
                       project_name=name if len(suites) > 1 else None)
        failed |= any(r.status in ("fail", "error") for r in results)
        if not results:
            print(f"{name}: no tests found in {os.path.relpath(os.path.join(project.root, 'tests'))}"
                  + (" matching the filter" if args.k or args.tag else ""), file=sys.stderr)
            failed |= not (args.k or args.tag)
        if options.coverage:
            cov = C.compute(project, results)
            cov_all.append(cov)
            print(cov.text())
            if args.min_coverage is not None and cov.total_pct < args.min_coverage:
                print(style.red(f"coverage {cov.total_pct:.0f}% is below --min-coverage {args.min_coverage}"))
                failed = True
    if len(suites) > 1:
        print(f"all projects: {elapsed:.1f}s")
    _outputs(args, suites, cov_all[0] if len(cov_all) == 1 else None, elapsed)
    return 1 if failed else 0


def _outputs(args, suites, cov, elapsed):
    if getattr(args, "junit", None):
        report.junit(suites, args.junit)
    if getattr(args, "json", None):
        report.write_json(suites, args.json)
    md = report.markdown(suites, cov, elapsed)
    if getattr(args, "markdown", None):
        with open(args.markdown, "w") as f:
            f.write(md)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        for _, results in suites:
            report.github_annotations(results)
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a") as f:
                f.write(md)


class _engine:
    """Engine for one command; prints a friendly message when Flink or Java is missing."""

    def __init__(self, args):
        self.args = args
        self.engine = None

    def __enter__(self) -> Engine:
        self.engine = Engine(getattr(self.args, "flink_home", None), workers=getattr(self.args, "workers", None))
        return self.engine

    def __exit__(self, *exc):
        if self.engine:
            self.engine.close()


def cmd_watch(args) -> int:
    from .watch import watch
    return watch(args)


def cmd_new(args) -> int:
    from .scaffold import scaffold
    return scaffold(args)


def cmd_timeline(args) -> int:
    from .timeline import show
    return show(args)


def cmd_replay(args) -> int:
    from .replay import cmd_replay as run
    return run(args)


def cmd_record(args) -> int:
    from .replay import cmd_record as run
    return run(args)


def cmd_diff(args) -> int:
    from .replay import cmd_diff as run
    return run(args)


def cmd_ci(args) -> int:
    from .ci import run
    return run(args)


def cmd_doctor(args) -> int:
    from .engine import dsl_jars, engine_jar, find_flink_home
    ok = True

    def check(label, fn):
        nonlocal ok
        try:
            print(f"  ok    {label}: {fn()}")
        except Exception as e:  # noqa: BLE001 - a diagnostic command reports everything
            ok = False
            print(f"  FAIL  {label}: {e}")
    print("cascade-test doctor")
    home = find_flink_home(args.flink_home)
    check("Flink", lambda: home or (_ for _ in ()).throw(EngineError("set FLINK_HOME to a Flink 2.x distribution")))
    check("cascade-dsl jars", lambda: ", ".join(os.path.basename(j) for j in dsl_jars()))
    if home:
        check("engine jar", lambda: engine_jar(home))
        t0 = time.time()

        def start():
            with Engine(home) as e:
                info = e.info
            return f"Flink {info.get('flink_version')}, started in {time.time() - t0:.1f}s " \
                   f"({info.get('slots')} slots, {info.get('workers')} parallel tests)"
        check("engine", start)
    return 0 if ok else 1


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="cascade-test", description="Test framework for Cascade pipelines",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    sub = ap.add_subparsers(dest="command")

    def common(sp, paths="*"):
        if paths:
            sp.add_argument("paths", nargs=paths, help="project directories (default: here, or every project below)")
        sp.add_argument("--env", help="compile for this environment (vars)")
        sp.add_argument("--flink-home", help="Flink distribution (default: $FLINK_HOME)")
        sp.add_argument("--workers", type=int, help="tests run at once (default: half the CPUs, 2..8)")
        sp.add_argument("--no-color", action="store_true")

    sp = sub.add_parser("run", help="run tests")
    common(sp)
    sp.add_argument("-k", help="only tests whose name (or file::name) contains this text")
    sp.add_argument("-t", "--tag", action="append", help="only tests with this tag (!tag excludes); repeatable")
    sp.add_argument("-v", "--verbose", action="store_true", help="show notes for passing tests too")
    sp.add_argument("-u", "--update-snapshots", action="store_true", help="re-record snapshots that differ")
    sp.add_argument("--ci", action="store_true", help="fail on missing snapshots instead of recording them")
    sp.add_argument("--coverage", action="store_true", help="report model/sink/expectation coverage")
    sp.add_argument("--min-coverage", type=float, help="fail if total coverage is below this percentage")
    sp.add_argument("--junit", help="write JUnit XML here")
    sp.add_argument("--json", help="write results as JSON here")
    sp.add_argument("--markdown", help="write a Markdown summary here (PR comment)")
    sp.add_argument("--show-sql", action="store_true", help="print the SQL each test runs")
    sp.add_argument("--keep", action="store_true", help="keep the fixture files and SQL of the run")
    sp.set_defaults(fn=cmd_run)

    sp = sub.add_parser("watch", help="re-run tests when files change")
    common(sp, paths="?")
    sp.add_argument("-k")
    sp.add_argument("-t", "--tag", action="append")
    sp.add_argument("--interval", type=float, default=0.5)
    sp.set_defaults(fn=cmd_watch)

    sp = sub.add_parser("new", help="scaffold a test for a model or sink")
    sp.add_argument("path")
    sp.add_argument("target", help="model, sink, <model>.quarantined or pipeline")
    sp.add_argument("--name", help="test name (default: <target>_works)")
    sp.add_argument("--record", action="store_true", help="run it and fill expect.rows with the actual output")
    sp.add_argument("--write", action="store_true", help="append to tests/<target>_test.yaml instead of printing")
    sp.add_argument("--env")
    sp.add_argument("--flink-home")
    sp.set_defaults(fn=cmd_new)

    sp = sub.add_parser("timeline", help="show a test's event-time timeline (watermarks, late rows)")
    sp.add_argument("path")
    sp.add_argument("-k", required=True, help="test name (substring)")
    sp.add_argument("--env")
    sp.set_defaults(fn=cmd_timeline)

    sp = sub.add_parser("replay", help="replay a recording against this version and diff the outputs")
    sp.add_argument("path", help="candidate project")
    sp.add_argument("--recording", required=True, help="recording directory (see `cascade-test record`)")
    sp.add_argument("--baseline", help="baseline version: a project path or git:<ref> (e.g. git:origin/main)")
    sp.add_argument("--golden", help="compare with (or --update-golden: write) golden outputs in this directory")
    sp.add_argument("--update-golden", action="store_true")
    sp.add_argument("--last", help="only the most recent span of the recording (e.g. 6h)")
    sp.add_argument("--limit", type=int, help="at most this many records per source")
    sp.add_argument("--order", choices=["recorded", "timestamp"], default="recorded",
                    help="interleave partitions as recorded (default) or by record timestamp")
    sp.add_argument("--key", action="append", default=[], help="sink=col1,col2: diff key for an append sink")
    sp.add_argument("--tolerance", type=float, help="absolute tolerance for numeric columns")
    sp.add_argument("--ignore", action="append", default=[], help="column to leave out of the diff (repeatable)")
    sp.add_argument("--markdown", help="write the diff report as Markdown here (PR comment)")
    sp.add_argument("--json", help="write the diff report as JSON here")
    sp.add_argument("--allow-diff", action="store_true", help="exit 0 even when outputs differ")
    sp.add_argument("--timeout", type=int, default=600)
    common(sp, paths=None)
    sp.set_defaults(fn=cmd_replay)

    sp = sub.add_parser("record", help="import a topic dump (kcat -J or JSON lines) as a recording")
    sp.add_argument("--source", required=True, help="the pipeline source the records belong to")
    sp.add_argument("--input", required=True, help="dump file: kcat -C -J output, or JSON lines")
    sp.add_argument("-o", "--out", required=True, help="recording directory (created or extended)")
    sp.add_argument("--format", choices=["json", "debezium-json"], default="json",
                    help="how message values encode rows")
    sp.add_argument("--lookup", action="store_true", help="store as a lookup cassette (rows of a lookup table)")
    sp.set_defaults(fn=cmd_record)

    sp = sub.add_parser("diff", help="diff two captured outputs (JSON lines, kcat -J, CSV)")
    sp.add_argument("baseline")
    sp.add_argument("candidate")
    sp.add_argument("--key", help="comma-separated key columns")
    sp.add_argument("--changelog", action="store_true", help="inputs are changelogs (_op/op column); compare final state")
    sp.add_argument("--tolerance", type=float)
    sp.add_argument("--ignore", action="append", default=[])
    sp.add_argument("--markdown")
    sp.add_argument("--json")
    sp.set_defaults(fn=cmd_diff)

    sp = sub.add_parser("ci", help="tests + coverage + state-compatibility check + replay diff, one report")
    sp.add_argument("path")
    sp.add_argument("--baseline", help="the deployed version: project path or git:<ref>")
    sp.add_argument("--recording", help="replay this recording against baseline and candidate")
    sp.add_argument("--last", help="replay only the most recent span (e.g. 6h)")
    sp.add_argument("--markdown", default="cascade-test-report.md")
    sp.add_argument("--junit")
    sp.add_argument("--min-coverage", type=float)
    sp.add_argument("--allow-state-loss", action="store_true")
    sp.add_argument("--env")
    sp.add_argument("--flink-home")
    sp.add_argument("--workers", type=int)
    sp.set_defaults(fn=cmd_ci)

    sp = sub.add_parser("doctor", help="check the local setup")
    sp.add_argument("--flink-home")
    sp.set_defaults(fn=cmd_doctor)
    return ap


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or (argv[0] not in COMMANDS and argv[0] not in ("-h", "--help")):
        argv = ["run"] + argv
    args = build_parser().parse_args(argv)
    try:
        sys.stdout.reconfigure(line_buffering=True)  # progress shows up promptly when piped (CI, watch)
    except AttributeError:
        pass
    try:
        return args.fn(args)
    except (ProjectError, SpecError, EngineError) as e:
        print(f"cascade-test: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
