"""`cascade-test ci`: everything a pull request needs, in one command and one report.

  1. the test suite (JUnit XML, annotations, coverage);
  2. a state-compatibility check against the deployed version (cascade-dsl's manifest diff: would
     this change reset state, and for which operators?);
  3. a replay of recorded production data through both versions, diffed sink by sink.

The Markdown report is written for a PR comment (see the GitHub Action in this repository).
Exit code: 1 if tests fail, coverage is too low, or the upgrade would lose state without
--allow-state-loss (or is breaking). Replay differences are reported, not failed: they are often
the point of the change, and a reviewer decides.
"""
from __future__ import annotations

import os
import sys
import time

from . import coverage as C
from . import report
from .engine import Engine
from .project import Project
from .replay import load_recording, open_baseline, replay
from .runner import Options, run as run_tests
from .spec import discover

LEVEL_ICON = {"ok": "✅", "warning": "⚠️", "state_loss": "🟥", "breaking": "⛔"}


def upgrade_check(baseline: Project, candidate: Project):
    from cascade_dsl.upgrade import diff_manifests
    return diff_manifests(baseline.lowered.manifest, candidate.lowered.manifest)


def upgrade_markdown(verdict: str, changes) -> str:
    out = [f"### {LEVEL_ICON.get(verdict, '')} State compatibility: `{verdict}`", ""]
    shown = [c for c in changes if c.level != "ok"]
    if not shown:
        out.append("Every piece of state maps onto the new version.")
    else:
        out += ["| level | change | node | detail |", "|---|---|---|---|"]
        for c in shown:
            detail = c.message.replace("|", "\\|")
            out.append(f"| {LEVEL_ICON.get(c.level, '')} {c.level} | `{c.code}` | `{c.node}` | {detail} |")
    return "\n".join(out) + "\n"


def run(args) -> int:
    t0 = time.time()
    candidate = Project(args.path, args.env)
    failed = False
    sections = []
    cleanup = lambda: None  # noqa: E731
    try:
        with Engine(args.flink_home, workers=args.workers) as engine:
            cases = discover(candidate.root)
            results = run_tests(candidate, cases, engine, Options(ci=True, coverage=True))
            cov = C.compute(candidate, results)
            report.console(results, elapsed=time.time() - t0)
            print(cov.text())
            failed |= any(r.status in ("fail", "error") for r in results)
            if args.min_coverage is not None and cov.total_pct < args.min_coverage:
                print(f"coverage {cov.total_pct:.0f}% is below --min-coverage {args.min_coverage}")
                failed = True
            suites = [(candidate.name, results)]
            sections.append(report.markdown(suites, cov, time.time() - t0))
            if args.junit:
                report.junit(suites, args.junit)
            if os.environ.get("GITHUB_ACTIONS") == "true":
                report.github_annotations(results)

            if args.baseline:
                path, label, cleanup = open_baseline(args.path, args.baseline)
                baseline = Project(path, args.env)
                verdict, changes = upgrade_check(baseline, candidate)
                print(f"state compatibility vs {args.baseline}: {verdict}")
                for c in changes:
                    if c.level != "ok":
                        print(f"  [{c.level:<10}] {c.code:<22} {c.message}")
                sections.append(upgrade_markdown(verdict, changes))
                if verdict == "breaking" or (verdict == "state_loss" and not args.allow_state_loss):
                    failed = True
                if args.recording:
                    rep = replay(engine, candidate, load_recording(args.recording), baseline=baseline,
                                 baseline_label=label, last=args.last)
                    print(rep.text(report.style_for()))
                    sections.append(rep.markdown())
                    failed |= bool(rep.errors)
            elif args.recording:
                print("note: --recording needs --baseline to diff against; skipped", file=sys.stderr)
    finally:
        cleanup()
    body = "<!-- cascade-test-report -->\n## Cascade pipeline report\n\n" + "\n".join(sections)
    with open(args.markdown, "w") as f:
        f.write(body)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(body)
    print(f"report written to {args.markdown} ({time.time() - t0:.1f}s)")
    return 1 if failed else 0
