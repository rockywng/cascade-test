"""Comparing values by Flink column type.

Expected values come from YAML (numbers, strings, relative times); actual values come from the
engine as JSON (decimals and timestamps as strings). Both are normalized by the output column's
type before comparison, so `10`, `10.0` and `"10.00"` are the same DECIMAL, and `"+5m"` is a
timestamp five minutes after the test's base time.
"""
from __future__ import annotations

import math
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation

from . import timeutil

INTEGER = {"TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"}
FLOATING = {"FLOAT", "DOUBLE", "REAL"}
TIMESTAMPS = {"TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"}


def base_type(flink_type: str | None) -> str:
    if not flink_type:
        return ""
    t = flink_type.strip().upper()
    if t.startswith("TIMESTAMP_LTZ") or "WITH LOCAL TIME ZONE" in t:
        return "TIMESTAMP_LTZ"
    return t.split("(")[0].split(" ")[0].split("<")[0]


def is_numeric(flink_type: str | None) -> bool:
    return base_type(flink_type) in INTEGER | FLOATING | {"DECIMAL", "NUMERIC"}


class _Any:
    """An expected value that matches anything: a column the expected row does not mention."""

    def __repr__(self):
        return "*"

    def __hash__(self):
        return 0

    def __eq__(self, other):
        return isinstance(other, _Any)


ANY = _Any()


class Unparseable(tuple):
    """Marks a value that could not be read as its column type (compares unequal to real values)."""


def normalize(value, flink_type: str | None, base_time=None):
    if value is None:
        return None
    base = base_type(flink_type)
    try:
        if base in ("DECIMAL", "NUMERIC"):
            if isinstance(value, float) and not math.isfinite(value):
                return value
            return Decimal(str(value)).normalize()
        if base in FLOATING:
            return float(value)
        if base in INTEGER:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, float) and value.is_integer():
                return int(value)
            return int(str(value))
        if base == "BOOLEAN":
            return value if isinstance(value, bool) else str(value).strip().lower() == "true"
        if base in TIMESTAMPS:
            return timeutil.resolve(value, base_time or timeutil.DEFAULT_BASE)
        if base == "DATE":
            if isinstance(value, date):
                return value.isoformat()
            if timeutil.is_relative(value):
                return timeutil.resolve(value, base_time or timeutil.DEFAULT_BASE).date().isoformat()
            return str(value)[:10]
        if base == "TIME":
            return value.isoformat() if isinstance(value, time) else str(value)
    except (InvalidOperation, ValueError, TypeError, timeutil.TimeError):
        return Unparseable(("<unparseable>", str(value)))
    if base == "CHAR":
        return str(value).rstrip()
    if base in ("VARCHAR", "STRING"):
        return value if isinstance(value, str) else str(value)
    return freeze(value)


def freeze(v):
    if isinstance(v, list):
        return tuple(freeze(x) for x in v)
    if isinstance(v, dict):
        return tuple(sorted((k, freeze(x)) for k, x in v.items()))
    return v


def close(a, b, tolerance) -> bool:
    """Equality with an optional absolute tolerance for numbers. ANY (on either side) matches."""
    if a is ANY or b is ANY or a == b:
        return True
    if tolerance is None or a is None or b is None:
        return False
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)) \
            and not isinstance(a, bool) and not isinstance(b, bool):
        return abs(Decimal(str(a)) - Decimal(str(b))) <= Decimal(str(tolerance))
    return False


def display(v) -> str:
    """Human-friendly rendering of a normalized value."""
    if v is None:
        return "null"
    if v is ANY:
        return "*"
    if isinstance(v, Unparseable):
        return f"<unparseable {v[1]!r}>"
    if isinstance(v, datetime):
        return timeutil.fmt(v)
    if isinstance(v, Decimal):
        s = format(v, "f")
        return s
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return v if v and v.strip() == v and not v.lower() in ("null", "true", "false") else repr(v)
    return str(v)


def to_plain(v):
    """A normalized value as plain YAML/JSON data (snapshots, reports)."""
    if isinstance(v, datetime):
        return timeutil.fmt(v)
    if isinstance(v, Decimal):
        s = format(v, "f")
        return s
    if isinstance(v, tuple) and not isinstance(v, Unparseable):
        return [to_plain(x) for x in v]
    return v
