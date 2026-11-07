"""The diff engine, shared by test assertions, snapshots, replay and shadow comparisons.

- Changelog normalization: outputs are compared as final state. A retract changelog
  (+I/-U/+U/-D) is applied as a multiset; with a key (an upsert sink's primary key), +I/+U replace
  the row with that key and -D deletes it, the way the external table applies them.
- Key-aware: with a key, rows are matched per key, so a wrong value shows up as one *changed* row
  with the differing columns, not as a missing row plus an unexpected one.
- Order-insensitive within a key (and overall) unless `ordered` is asked for.
- Numeric tolerance: absolute, global or per column.
- Partial rows: only the columns named in the expected rows are compared.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field

from . import values as V

MAX_PAIRING = 400  # leftover rows beyond this are reported without trying to pair them as "changed"


# --- changelogs ----------------------------------------------------------------------------

def to_dicts(names: list[str], changes: list[dict]) -> list[dict]:
    """Engine rows ({kind, values}) -> dicts with an `_op` column."""
    return [{"_op": c["kind"], **dict(zip(names, c["values"]))} for c in changes]


def materialize(names: list[str], changes: list[dict], key: list[str] | None = None) -> list[dict]:
    """Apply a changelog and return the final rows (as dicts, in first-insert order)."""
    if key:
        idx = [names.index(k) for k in key]
        table: dict = {}
        for c in changes:
            vals = c["values"]
            k = tuple(V.freeze(vals[i]) for i in idx)
            if c["kind"] in ("+I", "+U"):
                table[k] = vals
            elif c["kind"] == "-D":
                table.pop(k, None)
        return [dict(zip(names, v)) for v in table.values()]
    live: dict = {}
    order: list = []
    for c in changes:
        f = V.freeze(c["values"])
        if c["kind"] in ("+I", "+U"):
            if f not in live:
                live[f] = [c["values"], 0]
                order.append(f)
            live[f][1] += 1
        elif f in live and live[f][1] > 0:
            live[f][1] -= 1
    return [dict(zip(names, live[f][0])) for f in order for _ in range(live[f][1])]


# --- comparison ----------------------------------------------------------------------------

@dataclass
class Changed:
    key: tuple | None
    expected: dict
    actual: dict
    columns: list[str]  # the columns that differ


@dataclass
class Diff:
    columns: list[str]
    key: list[str] | None
    missing: list[dict] = field(default_factory=list)  # expected (or baseline) rows not produced
    unexpected: list[dict] = field(default_factory=list)  # produced rows not expected
    changed: list[Changed] = field(default_factory=list)
    matched: int = 0
    order_problem: str | None = None
    expected_count: int = 0
    actual_count: int = 0

    @property
    def ok(self) -> bool:
        return not (self.missing or self.unexpected or self.changed or self.order_problem)

    def counts(self) -> dict:
        return {"matched": self.matched, "changed": len(self.changed), "missing": len(self.missing),
                "unexpected": len(self.unexpected)}

    def summary(self, labels=("missing", "unexpected")) -> str:
        if self.ok:
            return f"{self.matched} row(s) match"
        parts = []
        if self.changed:
            parts.append(f"{len(self.changed)} changed")
        if self.missing:
            parts.append(f"{len(self.missing)} {labels[0]}")
        if self.unexpected:
            parts.append(f"{len(self.unexpected)} {labels[1]}")
        if self.order_problem:
            parts.append("wrong order")
        return ", ".join(parts) + f" ({self.matched} matched)"


def _tol(tolerance, col):
    if isinstance(tolerance, dict):
        return tolerance.get(col, tolerance.get("*"))
    return tolerance


def compare(expected: list[dict], actual: list[dict], types: dict[str, str], *, columns: list[str] | None = None,
            key: list[str] | None = None, tolerance=None, ignore=(), ordered: bool = False,
            base_time=None) -> Diff:
    """Compare expected rows with actual rows (both as dicts of raw values).

    `columns`: the columns to compare (default: every column named in an expected row, or all
    output columns when there are no expected rows)."""
    if columns is None:
        columns = []
        for r in expected:
            for c in r:
                if c not in columns and not c.startswith("_op"):
                    columns.append(c)
        if not columns:
            columns = list(types)
    columns = [c for c in columns if c not in set(ignore)]
    if key and not all(k in columns for k in key):
        key = None
    norm = {c: (lambda v, t=types.get(c): V.normalize(v, t, base_time)) for c in columns}

    def n(row: dict) -> dict:
        return {c: norm[c](row.get(c)) for c in columns}

    def n_expected(row: dict) -> dict:
        # a column the expected row does not mention matches anything (per-row partial rows)
        return {c: (norm[c](row.get(c)) if c in row else V.ANY) for c in columns}

    exp = [n_expected(r) for r in expected]
    act = [n(r) for r in actual]
    d = Diff(columns, key, expected_count=len(exp), actual_count=len(act))
    wild = {c for c in columns if any(e[c] is V.ANY for e in exp)}
    if key and wild & set(key):
        key = d.key = None

    def same(a: dict, b: dict) -> bool:
        return all(V.close(a[c], b[c], _tol(tolerance, c)) for c in columns)

    # rows are bucketed on the columns that must match exactly; within a bucket the first row that
    # matches (with tolerance and wildcards) is taken
    exact = [c for c in columns if c not in wild and (_tol(tolerance, c) is None or not _numeric(types.get(c)))]

    def match_group(es: list[dict], acts: list[dict], k) -> None:
        index = defaultdict(list)
        for i in range(len(acts)):
            index[_t(acts[i], exact)].append(i)
        leftover = []
        for e in es:
            bucket = index.get(_t(e, exact))
            hit = None
            if bucket:
                hit = 0 if len(exact) == len(columns) else next((j for j, i in enumerate(bucket)
                                                                  if same(e, acts[i])), None)
            if hit is None:
                leftover.append(e)
            else:
                bucket.pop(hit)
                d.matched += 1
        extra = [acts[i] for i in sorted(i for b in index.values() for i in b)]
        if k is not None:
            # same key: pair them up as changed rows
            for e, a in zip(leftover, extra):
                d.changed.append(Changed(k, e, a, [c for c in columns if not V.close(e[c], a[c], _tol(tolerance, c))]))
            d.missing += leftover[len(extra):]
            d.unexpected += extra[len(leftover):]
        else:
            d.missing += leftover
            d.unexpected += extra

    if key:
        eg, ag = defaultdict(list), defaultdict(list)
        for r in exp:
            eg[_t(r, key)].append(r)
        for r in act:
            ag[_t(r, key)].append(r)
        for k in list(dict.fromkeys(list(eg) + list(ag))):
            match_group(eg.get(k, []), ag.get(k, []), k)
    else:
        match_group(exp, act, None)
        _pair_similar(d, tolerance)

    if ordered and not (d.missing or d.unexpected or d.changed):
        for i, (e, a) in enumerate(zip(exp, act)):
            if not same(e, a):
                d.order_problem = (f"same rows, different order: position {i + 1} expected "
                                   f"{format_row(e)}, got {format_row(a)}")
                break
    return d


def _numeric(t) -> bool:
    return V.is_numeric(t)


def _t(row: dict, cols: list[str]) -> tuple:
    return tuple(_hashable(row[c]) for c in cols)


def _hashable(v):
    try:
        hash(v)
        return v
    except TypeError:
        return repr(v)


def _pair_similar(d: Diff, tolerance) -> None:
    """Without a key, pair a missing row with the unexpected row that agrees on the most columns
    (at least half), so a single wrong value reads as a change rather than two unrelated rows."""
    if not d.missing or not d.unexpected or len(d.missing) * len(d.unexpected) > MAX_PAIRING ** 2:
        return
    cols = d.columns
    need = max(1, (len(cols) + 1) // 2)
    used: set[int] = set()
    still_missing = []
    for e in d.missing:
        best, best_score = None, -1
        for i, a in enumerate(d.unexpected):
            if i in used:
                continue
            score = sum(1 for c in cols if V.close(e[c], a[c], _tol(tolerance, c)))
            if score > best_score:
                best, best_score = i, score
        if best is not None and best_score >= need and best_score < len(cols):
            used.add(best)
            a = d.unexpected[best]
            d.changed.append(Changed(None, e, a, [c for c in cols if not V.close(e[c], a[c], _tol(tolerance, c))]))
        else:
            still_missing.append(e)
    d.missing = still_missing
    d.unexpected = [a for i, a in enumerate(d.unexpected) if i not in used]


# --- rendering ------------------------------------------------------------------------------

def format_row(row: dict, cols: list[str] | None = None) -> str:
    cols = [c for c in (cols or list(row)) if row.get(c) is not V.ANY]
    return "{" + ", ".join(f"{c}: {V.display(row.get(c))}" for c in cols) + "}"


def format_key(key: list[str] | None, k: tuple | None) -> str:
    if not key or k is None:
        return ""
    return ", ".join(f"{c}={V.display(v)}" for c, v in zip(key, k))


class Style:
    def __init__(self, color: bool):
        self.color = color

    def _w(self, code, s):
        return f"\033[{code}m{s}\033[0m" if self.color else s

    def red(self, s): return self._w("31", s)
    def green(self, s): return self._w("32", s)
    def yellow(self, s): return self._w("33", s)
    def dim(self, s): return self._w("2", s)
    def bold(self, s): return self._w("1", s)


def render_text(d: Diff, style: Style | None = None, limit: int = 20,
                labels=("expected, not produced", "produced, not expected"),
                names=("expected", "got")) -> str:
    s = style or Style(False)
    lines = []
    if d.key:
        lines.append(s.dim(f"rows matched by key ({', '.join(d.key)}); compared columns: {', '.join(d.columns)}"))
    else:
        lines.append(s.dim(f"compared columns: {', '.join(d.columns)}"))
    for ch in d.changed[:limit]:
        where = format_key(d.key, ch.key) or format_row(ch.expected, [c for c in d.columns if c not in ch.columns])
        lines.append(s.yellow(f"  ~ {where}"))
        for c in ch.columns:
            lines.append(f"      {c}: {names[0]} {s.green(V.display(ch.expected[c]))}, "
                         f"{names[1]} {s.red(V.display(ch.actual[c]))}")
    if len(d.changed) > limit:
        lines.append(s.dim(f"  ... {len(d.changed) - limit} more changed row(s)"))
    for label, rows, mark, paint in ((labels[0], d.missing, "-", s.green), (labels[1], d.unexpected, "+", s.red)):
        if rows:
            lines.append(f"  {label}:")
            for r in rows[:limit]:
                lines.append(paint(f"  {mark} {format_row(r, d.columns)}"))
            if len(rows) > limit:
                lines.append(s.dim(f"  ... {len(rows) - limit} more"))
    if d.order_problem:
        lines.append("  " + d.order_problem)
    return "\n".join(lines)


def _md(v) -> str:
    return V.display(v).replace("|", "\\|")


def render_markdown(d: Diff, limit: int = 10, labels=("removed", "added"), names=("baseline", "candidate")) -> str:
    """A compact Markdown rendering (PR comments, step summaries)."""
    out = []
    if d.changed:
        out.append(f"**Changed ({len(d.changed)})**\n")
        out.append(f"| {'key' if d.key else 'row'} | column | {names[0]} | {names[1]} |")
        out.append("|---|---|---|---|")
        for ch in d.changed[:limit]:
            where = format_key(d.key, ch.key) or format_row(ch.expected, [c for c in d.columns if c not in ch.columns])
            for i, c in enumerate(ch.columns):
                out.append(f"| {_md(where) if i == 0 else ''} | `{c}` | {_md(ch.expected[c])} | {_md(ch.actual[c])} |")
        if len(d.changed) > limit:
            out.append(f"\n_... and {len(d.changed) - limit} more changed row(s)_")
        out.append("")
    for label, rows in ((labels[0], d.missing), (labels[1], d.unexpected)):
        if rows:
            out.append(f"**{label.capitalize()} ({len(rows)})**\n")
            out.append("| " + " | ".join(d.columns) + " |")
            out.append("|" + "---|" * len(d.columns))
            for r in rows[:limit]:
                out.append("| " + " | ".join(_md(r.get(c)) for c in d.columns) + " |")
            if len(rows) > limit:
                out.append(f"\n_... and {len(rows) - limit} more_")
            out.append("")
    if d.order_problem:
        out.append(d.order_problem)
    return "\n".join(out)


def to_json(d: Diff, limit: int = 50) -> dict:
    p = V.to_plain
    return {
        "ok": d.ok, "key": d.key, "columns": d.columns, **d.counts(),
        "changed_samples": [{"key": [p(x) for x in ch.key] if ch.key else None,
                             "columns": {c: {"expected": p(ch.expected[c]), "actual": p(ch.actual[c])}
                                         for c in ch.columns}} for ch in d.changed[:limit]],
        "missing_samples": [{c: p(r[c]) for c in d.columns} for r in d.missing[:limit]],
        "unexpected_samples": [{c: p(r[c]) for c in d.columns} for r in d.unexpected[:limit]],
        "order_problem": d.order_problem,
    }


def table(rows: list[dict], cols: list[str], limit: int = 50) -> str:
    """A plain text table of rows (for showing actual output)."""
    shown = rows[:limit]
    cells = [[V.display(r.get(c)) if not isinstance(r.get(c), str) else r.get(c) for c in cols] for r in shown]
    widths = [max([len(c)] + [len(row[i]) for row in cells]) for i, c in enumerate(cols)]
    line = "  ".join(c.ljust(w) for c, w in zip(cols, widths))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(v.ljust(w) for v, w in zip(row, widths)) for row in cells]
    if len(rows) > limit:
        out.append(f"... {len(rows) - limit} more row(s)")
    return "\n".join(out)


def counter_of(rows: list[dict], cols: list[str]) -> Counter:
    return Counter(_t(r, cols) for r in rows)
