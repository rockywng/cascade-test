"""A frozen clock for tests and replays.

Flink reads the wall clock for CURRENT_TIMESTAMP, NOW(), LOCALTIMESTAMP, CURRENT_DATE and friends, so a
pipeline that stamps rows with them (a `_quarantined_at` column, an age filter, "events in the last
hour") gives different output on every run, and a replay of a week-old recording measures its rows
against today. With `clock:` in a test, or `--clock` in a replay, those functions are replaced in the
SQL the engine runs by literals for one fixed instant, so the output depends on the input alone.

    clock: "2026-11-06 12:00:00"        UTC, or with an offset (2026-11-06T12:00:00+02:00)
    clock: base_time                    the test's own base_time

Only the wall-clock functions change. PROCTIME() (processing-time windows and joins) needs the real
clock to fire its timers and is left alone: a pipeline that uses it still replays on wall time.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from . import timeutil

_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")
_PARENS = re.compile(r"\s*\(\s*\)")


class ClockError(ValueError):
    pass


def instant(value, base_time: str | None = None) -> datetime:
    """The frozen instant (UTC) for a `clock:` value."""
    text = str(value).strip()
    if text.lower() == "base_time":
        text = base_time or timeutil.DEFAULT_BASE
    try:
        d = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ClockError(f"cannot read clock '{value}' as a timestamp (example: 2026-11-06 12:00:00)") from None
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)


def _replacement(word: str, d: datetime) -> str | None:
    ms = int(d.timestamp() * 1000)
    ts = d.strftime("%Y-%m-%d %H:%M:%S") + f".{d.microsecond // 1000:03d}"
    w = word.upper()
    if w in ("CURRENT_TIMESTAMP", "NOW", "CURRENT_ROW_TIMESTAMP"):
        return f"TO_TIMESTAMP_LTZ({ms}, 3)"
    if w == "LOCALTIMESTAMP":
        return f"TIMESTAMP '{ts}'"
    if w == "CURRENT_DATE":
        return f"DATE '{d:%Y-%m-%d}'"
    if w in ("CURRENT_TIME", "LOCALTIME"):
        return f"TIME '{d:%H:%M:%S}'"
    if w == "UNIX_TIMESTAMP":
        return f"CAST({ms // 1000} AS BIGINT)"
    return None


# functions that are always written with parentheses, the others without (CURRENT_TIMESTAMP also with)
_CALLS = {"NOW", "CURRENT_ROW_TIMESTAMP", "UNIX_TIMESTAMP"}


def freeze(sql: str, d: datetime) -> tuple[str, int]:
    """`sql` with its wall-clock functions replaced by literals for `d`; and how many were replaced.
    String literals, quoted identifiers and comments are left as they are."""
    out, i, n, count = [], 0, len(sql), 0
    while i < n:
        c = sql[i]
        if c in "'\"`":
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # '' inside a string
                        j += 2
                        continue
                    break
                j += 1
            out.append(sql[i:j + 1])
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j < 0 else j
            out.append(sql[i:j])
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append(sql[i:j])
            i = j
        else:
            m = _WORD.match(sql, i)
            if not m:
                out.append(c)
                i += 1
                continue
            word, end = m.group(0), m.end()
            before = sql[:i].rstrip()
            rep = _replacement(word, d)
            call = _PARENS.match(sql, end)
            if rep and not before.endswith(".") and (word.upper() not in _CALLS or call):
                out.append(rep)
                i = call.end() if call else end
                count += 1
            else:
                out.append(word)
                i = end
    return "".join(out), count
