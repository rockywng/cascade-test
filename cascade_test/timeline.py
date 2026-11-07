"""`cascade-test timeline`: what event time does in a test.

Event-time bugs (a window that never fires, a row silently dropped as late, one idle partition
holding everything back) are hard to see from outputs alone. The timeline shows, per source, every
row in replay order with its partition and event time, the source watermark after it, and which
rows were late.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

from . import plan as P
from . import timeutil
from .diff import Style
from .project import Project
from .spec import discover


def render(prep: P.Prepared, project: Project, style: Style | None = None) -> str:
    s = style or Style(False)
    out = []
    for name, rd in prep.rendered.items():
        if not rd.events:
            continue
        src = project.pipeline.sources[name]
        et = src.event_time
        head = f"{s.bold(name)}"
        if et:
            head += s.dim(f"  event time {et.column}, max_out_of_orderness {et.max_out_of_orderness_ms} ms, "
                          f"watermarks {prep.case.watermarks}")
        else:
            head += s.dim("  (no event time)")
        if len(rd.partitions) > 1:
            head += s.dim(f", partitions {rd.partitions}")
        out.append(head)
        out.append(s.dim(f"  {'#':>4}  {'part':>4}  {'op':<3} {'event time':<23}  {'watermark after':<23}  row"))
        for e in rd.events:
            if e.kind == "watermark":
                out.append(s.yellow(f"  {'':>4}  {'':>4}  {'WM':<3} {'':<23}  {timeutil.fmt_ms(e.ts):<23}  "
                                    "explicit _watermark"))
                continue
            if e.kind == "idle":
                out.append(s.yellow(f"  {'':>4}  {e.partition:>4}  {'--':<3} {'':<23}  {timeutil.fmt_ms(e.watermark):<23}  "
                                    "partition idle"))
                continue
            vals = ", ".join(f"{k}: {v}" for k, v in (e.values or {}).items()
                             if not k.startswith("_") and (not et or k != et.column))
            line = (f"  {e.row:>4}  {e.partition:>4}  {e.op:<3} {timeutil.fmt_ms(e.ts) if e.ts is not None else '-':<23}  "
                    f"{timeutil.fmt_ms(e.watermark):<23}  {vals[:70]}")
            out.append(s.red(line + "  LATE") if e.late else line)
        out.append("")
    late = {n: rd.late_rows for n, rd in prep.rendered.items() if rd.late_rows}
    if late:
        out.append(s.red("late rows: " + "; ".join(f"{n} {r}" for n, r in late.items())) +
                   s.dim("  (event time at or behind the watermark on arrival: windows that already fired "
                         "and interval/temporal joins drop them)"))
    out.append(s.dim("at end of input the watermark jumps to +inf: every remaining window fires"))
    return "\n".join(out)


def show(args) -> int:
    project = Project(args.path, args.env)
    cases = [c for c in discover(project.root) if args.k in c.name]
    if not cases:
        print(f"no test matching '{args.k}'", file=sys.stderr)
        return 1
    style = Style(sys.stdout.isatty() and not os.environ.get("NO_COLOR"))
    work = tempfile.mkdtemp(prefix="cascade-timeline-")
    try:
        for case in cases:
            prep = P.prepare(project, case, os.path.join(work, "t"))
            print(style.bold(f"== {case.name}") + style.dim(f"  ({case.loc})"))
            if prep.error:
                print(prep.error)
                continue
            print(render(prep, project, style))
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return 0
