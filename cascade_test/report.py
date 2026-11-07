"""Reporting test results: console, JUnit XML, Markdown (PR comments / step summaries), JSON and
GitHub Actions annotations."""
from __future__ import annotations

import json
import os
import sys
from xml.sax.saxutils import escape, quoteattr

from .diff import Style

ICONS = {"pass": "PASS", "fail": "FAIL", "error": "ERROR", "skip": "SKIP"}


def style_for(stream=None, color: bool | None = None) -> Style:
    if color is None:
        stream = stream or sys.stdout
        color = hasattr(stream, "isatty") and stream.isatty() and not os.environ.get("NO_COLOR")
    return Style(bool(color))


def _target(r) -> str:
    c = r.case
    if c.kind == "calls":
        return f"{len(c.calls)} call(s)"
    return c.target


def console(results, *, verbose: bool = False, style: Style | None = None, out=None, elapsed: float | None = None,
            project_name: str | None = None) -> None:
    out = out or sys.stdout
    s = style or style_for(out)
    paint = {"pass": s.green, "fail": s.red, "error": s.red, "skip": s.yellow}
    for r in results:
        head = f"{paint[r.status](ICONS[r.status].ljust(5))} {r.case.name}  {s.dim(f'({_target(r)}, {r.millis} ms)')}"
        print(head, file=out)
        if r.status in ("fail", "error"):
            for f in r.failures:
                print("      " + s.bold(f.title), file=out)
                if f.detail:
                    print(_indent(f.detail, 6), file=out)
            for n in r.notes:
                print(_indent(s.dim(n), 6), file=out)
            print(s.dim(f"      at {r.case.loc}"), file=out)
        elif verbose or any("snapshot" in n for n in r.notes):
            for n in r.notes:
                print(_indent(s.dim(n), 6), file=out)
    counts = summary_counts(results)
    parts = [f"{counts['pass']} passed"]
    if counts["fail"]:
        parts.append(s.red(f"{counts['fail']} failed"))
    if counts["error"]:
        parts.append(s.red(f"{counts['error']} broken"))
    if counts["skip"]:
        parts.append(s.yellow(f"{counts['skip']} skipped"))
    tail = f" in {elapsed:.1f}s" if elapsed is not None else ""
    name = f"{project_name}: " if project_name else ""
    print(f"{name}{', '.join(parts)}{tail}", file=out)


def summary_counts(results) -> dict:
    c = {"pass": 0, "fail": 0, "error": 0, "skip": 0}
    for r in results:
        c[r.status] += 1
    return c


def _indent(text: str, n: int) -> str:
    return "\n".join((" " * n + line) if line.strip() else line for line in text.split("\n"))


def junit(suites: list[tuple[str, list]], path: str) -> None:
    """suites: [(suite name, results)]"""
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<testsuites>"]
    for name, results in suites:
        c = summary_counts(results)
        total_ms = sum(r.millis for r in results)
        lines.append(f'  <testsuite name={quoteattr(name)} tests="{len(results)}" failures="{c["fail"]}" '
                     f'errors="{c["error"]}" skipped="{c["skip"]}" time="{total_ms / 1000:.3f}">')
        for r in results:
            attrs = (f'classname={quoteattr(name + "." + os.path.splitext(os.path.basename(r.case.file))[0])} '
                     f'name={quoteattr(r.case.name)} time="{r.millis / 1000:.3f}" file={quoteattr(str(r.case.loc.file))}'
                     + (f' line="{r.case.loc.line}"' if r.case.loc.line else ""))
            if r.status == "pass":
                lines.append(f"    <testcase {attrs}/>")
                continue
            lines.append(f"    <testcase {attrs}>")
            if r.status == "skip":
                lines.append(f"      <skipped message={quoteattr(r.notes[0] if r.notes else 'skipped')}/>")
            else:
                tag = "failure" if r.status == "fail" else "error"
                title = r.failures[0].title if r.failures else r.status
                body = r.message + ("\n\n" + "\n".join(r.notes) if r.notes else "")
                lines.append(f"      <{tag} message={quoteattr(title)}>{escape(body)}</{tag}>")
            lines.append("    </testcase>")
        lines.append("  </testsuite>")
    lines.append("</testsuites>")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


def github_annotations(results, out=None) -> None:
    """`::error file=...,line=...::` lines, so failures show up inline in the PR diff."""
    out = out or sys.stdout
    for r in results:
        if r.status not in ("fail", "error"):
            continue
        msg = (r.message or r.status).replace("%", "%25").replace("\r", "").replace("\n", "%0A")
        line = f",line={r.case.loc.line}" if r.case.loc.line else ""
        print(f"::error file={r.case.loc.file}{line},title=cascade-test: {r.case.name}::{msg}", file=out)


def markdown(suites: list[tuple[str, list]], coverage=None, elapsed: float | None = None) -> str:
    out = []
    total = {"pass": 0, "fail": 0, "error": 0, "skip": 0}
    for _, results in suites:
        for k, v in summary_counts(results).items():
            total[k] += v
    ok = not (total["fail"] or total["error"])
    out.append(f"### {'✅' if ok else '❌'} Pipeline tests: {total['pass']} passed"
               + (f", {total['fail']} failed" if total["fail"] else "")
               + (f", {total['error']} broken" if total["error"] else "")
               + (f", {total['skip']} skipped" if total["skip"] else "")
               + (f" ({elapsed:.1f}s)" if elapsed is not None else ""))
    out.append("")
    for name, results in suites:
        bad = [r for r in results if r.status in ("fail", "error")]
        if not bad:
            continue
        out.append(f"**{name}**\n")
        for r in bad:
            out.append(f"<details><summary>❌ <code>{escape(r.case.name)}</code> ({escape(_target(r))}) — "
                       f"{escape(r.failures[0].title if r.failures else r.status)}</summary>\n")
            out.append("```")
            out.append(r.message)
            for n in r.notes:
                out.append(n)
            out.append(f"at {r.case.loc}")
            out.append("```\n</details>\n")
    if coverage is not None:
        out.append(coverage.markdown())
    return "\n".join(out) + "\n"


def to_json(suites: list[tuple[str, list]]) -> dict:
    return {"suites": [{"name": name, "results": [{
        "name": r.case.name, "file": str(r.case.loc.file), "line": r.case.loc.line, "target": r.case.target,
        "status": r.status, "millis": r.millis, "failures": [{"title": f.title, "detail": f.detail} for f in r.failures],
        "notes": r.notes, "expectations": r.expectation_counts} for r in results]} for name, results in suites]}


def write_json(suites, path: str) -> None:
    with open(path, "w") as f:
        json.dump(to_json(suites), f, indent=2)
