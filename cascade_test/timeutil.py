"""Timestamps and durations in test files.

Timestamps are UTC unless they carry an offset. Relative times ("+5m", "-30s", "+1h30m") are
offsets from the test's `base_time`, which keeps fixtures short and makes intent obvious:
"the second order arrives five minutes after the first".
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone

UNITS_MS = {"ms": 1, "s": 1000, "sec": 1000, "m": 60_000, "min": 60_000, "h": 3_600_000, "d": 86_400_000}
_DURATION = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|sec|min|s|m|h|d)")
DEFAULT_BASE = "2026-01-01 00:00:00"


class TimeError(ValueError):
    pass


def parse_duration_ms(text) -> int:
    """'5m', '1h30m', '250ms', '90s' or a number of milliseconds."""
    if isinstance(text, (int, float)):
        return int(text)
    s = str(text).strip().lower().replace(" ", "")
    if not s:
        raise TimeError("empty duration")
    pos, total = 0, 0.0
    for m in _DURATION.finditer(s):
        if m.start() != pos:
            break
        total += float(m.group(1)) * UNITS_MS[m.group(2)]
        pos = m.end()
    if pos != len(s):
        raise TimeError(f"cannot read '{text}' as a duration (examples: 500ms, 5s, 2m, 1h30m)")
    return int(total)


def is_relative(value) -> bool:
    return isinstance(value, str) and len(value) > 1 and value.strip()[0] in "+-" and value.strip()[1].isdigit()


def parse_ts(value) -> datetime:
    """A naive UTC datetime."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).replace(tzinfo=None)
    s = str(value).strip().replace("T", " ")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise TimeError(f"cannot read '{value}' as a timestamp (e.g. '2026-01-01 10:00:00' or '+5m')")
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


def resolve(value, base) -> datetime:
    """A timestamp value, absolute or relative to `base`."""
    if is_relative(value):
        s = value.strip()
        sign = -1 if s[0] == "-" else 1
        return parse_ts(base) + timedelta(milliseconds=sign * parse_duration_ms(s[1:]))
    return parse_ts(value)


def epoch_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def from_epoch_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def fmt(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    s = dt.strftime("%Y-%m-%d %H:%M:%S")
    return s + (f".{dt.microsecond // 1000:03d}" if dt.microsecond else "")


def fmt_ms(ms: int | None) -> str:
    if ms is None:
        return "-"
    if ms <= -(2 ** 62):
        return "-inf"
    if ms >= 2 ** 62:
        return "+inf"
    return fmt(from_epoch_ms(ms))
