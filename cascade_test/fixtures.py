"""Fixture rows -> the event script a test source replays.

A source's fixture is a list of rows and markers, replayed in order by cascade-dsl's
`cascade-fixture` connector (one subtask, so the order is exactly the file order):

    - {order_id: o1, amount: 10, order_ts: "+0s"}      a row (relative times use base_time)
    - {_partition: 1, order_id: o2, ...}               a row read from Kafka partition 1
    - {_op: update, customer_id: c1, segment: ent}     changelog sources: insert|update|delete|upsert
    - {_watermark: "+10m"}                             emit a watermark now
    - {_idle: 2}                                       partition 2 goes idle (no longer holds back time)

Watermarks are computed here, deterministically, the way Flink computes them for a partitioned
source: each partition tracks `max event time - max_out_of_orderness`; the source's watermark is
the minimum over partitions that are not idle, and does not advance until every active partition
has data. One slow partition therefore holds back every window, as it does in production.
A row whose event time is at or before the current watermark is *late*; the timeline records it.

A fixture can also be a mapping: {rows: [...], file: fixtures/orders.csv, generate: {...},
partitions: 4, defaults: {...}}.
"""
from __future__ import annotations

import csv
import difflib
import json
import os
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal

import yaml

from . import timeutil
from .values import base_type

MARKERS = {"_op", "_partition", "_watermark", "_idle"}
OPS = ("insert", "update", "delete", "upsert")
NEG_INF = -(2 ** 63)


class FixtureError(ValueError):
    pass


@dataclass
class SourceInfo:
    name: str
    columns: list[tuple[str, str]]  # (name, flink type)
    updating: bool
    primary_key: list[str]
    event_time: str | None
    delay_ms: int = 0

    @property
    def names(self) -> list[str]:
        return [n for n, _ in self.columns]

    def type_of(self, col: str) -> str | None:
        return dict(self.columns).get(col)


@dataclass
class Event:
    seq: int
    kind: str  # row | watermark | idle
    partition: int | None = None
    row: int | None = None  # 1-based index among this source's data rows
    op: str | None = None  # +I -U +U -D
    ts: int | None = None  # event time (ms) of a row; the watermark value for a watermark event
    watermark: int | None = None  # the source's watermark after this event
    late: bool = False
    values: dict | None = None


@dataclass
class Rendered:
    source: str
    lines: list[str]
    events: list[Event] = field(default_factory=list)
    partitions: list[int] = field(default_factory=list)

    @property
    def late_rows(self) -> list[int]:
        return [e.row for e in self.events if e.kind == "row" and e.late]

    @property
    def row_count(self) -> int:
        return sum(1 for e in self.events if e.kind == "row")


# --- loading fixture specs -------------------------------------------------------------------

def load_file(path: str) -> list[dict]:
    """Rows from a .csv (header row; empty cells are null), .jsonl, .json or .yaml file."""
    if not os.path.exists(path):
        raise FixtureError(f"fixture file not found: {path}")
    ext = os.path.splitext(path[:-3] if path.endswith(".gz") else path)[1].lower()
    opener = _open_gz if path.endswith(".gz") else open
    with opener(path) as f:
        if ext == ".csv":
            return [{k: (None if v == "" else v) for k, v in row.items()} for row in csv.DictReader(f)]
        if ext in (".jsonl", ".ndjson"):
            return [json.loads(line) for line in f if line.strip()]
        if ext == ".json":
            data = json.load(f)
        elif ext in (".yaml", ".yml"):
            data = yaml.safe_load(f)
        else:
            raise FixtureError(f"unsupported fixture file type '{ext}' (use .csv, .jsonl, .json or .yaml)")
    if isinstance(data, dict) and "rows" in data:
        data = data["rows"]
    if not isinstance(data, list):
        raise FixtureError(f"{path}: expected a list of rows")
    return data


def _open_gz(path):
    import gzip
    return gzip.open(path, "rt")


def expand(spec, src: SourceInfo, *, base_time, defaults: dict | None = None, test_dir: str = ".",
           partitions: int | None = None) -> tuple[list[dict], int | None]:
    """A fixture spec (list or mapping) -> (rows and markers with defaults/templates/times
    resolved, declared partition count)."""
    if spec is None:
        return [], partitions
    defaults = dict(defaults or {})
    if isinstance(spec, list):
        raw = spec
    elif isinstance(spec, dict):
        unknown = set(spec) - {"rows", "file", "generate", "partitions", "defaults"}
        if unknown:
            raise FixtureError(f"fixture for '{src.name}' has unknown key(s) {sorted(unknown)}; "
                               "use rows, file, generate, partitions, defaults")
        raw = []
        if spec.get("file"):
            raw += load_file(os.path.join(test_dir, spec["file"]))
        if spec.get("generate") is not None:
            raw += generate(spec["generate"], src, base_time, spec.get("partitions") or partitions)
        raw += list(spec.get("rows") or [])
        defaults.update(spec.get("defaults") or {})
        partitions = spec.get("partitions", partitions)
    else:
        raise FixtureError(f"fixture for '{src.name}' must be a list of rows or a mapping")
    out = []
    n = 0
    for i, r in enumerate(raw):
        if not isinstance(r, dict):
            raise FixtureError(f"{src.name} row {i + 1}: each row must be a mapping, got {r!r}")
        if "_watermark" in r or "_idle" in r:
            extra = set(r) - {"_watermark", "_idle", "_partition"}
            if extra:
                raise FixtureError(f"{src.name} row {i + 1}: `_watermark`/`_idle` must be alone in their row")
            m = dict(r)
            if "_watermark" in m:
                if not src.event_time:
                    raise FixtureError(f"source '{src.name}' has no event_time; `_watermark` does not apply")
                m["_watermark"] = timeutil.epoch_ms(timeutil.resolve(_template(m["_watermark"], n, n), base_time))
            out.append(m)
            continue
        n += 1
        is_delete = r.get("_op") == "delete"
        row = {} if is_delete else {k: v for k, v in defaults.items() if k not in r}
        row.update(r)
        out.append({k: _value(_template(v, n, n - 1), src.type_of(k), base_time) if not k.startswith("_") else v
                    for k, v in row.items()})
    return out, partitions


def _template(v, n: int, i: int):
    if isinstance(v, str) and "{" in v:
        return v.replace("{n}", str(n)).replace("{i}", str(i))
    return v


def _value(v, flink_type: str | None, base_time):
    """Resolve relative times and make YAML scalars JSON-friendly for the fixture connector."""
    base = base_type(flink_type)
    if v is None:
        return None
    if base in ("TIMESTAMP", "TIMESTAMP_LTZ"):
        if isinstance(v, (str, datetime, date)) or (isinstance(v, (int, float)) and not isinstance(v, bool)):
            try:
                dt = timeutil.resolve(v, base_time) if isinstance(v, str) else timeutil.parse_ts(v)
            except timeutil.TimeError as e:
                raise FixtureError(str(e))
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if base == "DATE":
        if isinstance(v, str) and timeutil.is_relative(v):
            return timeutil.resolve(v, base_time).date().isoformat()
        if isinstance(v, (date, datetime)):
            return (v.date() if isinstance(v, datetime) else v).isoformat()
    return _jsonable(v)


def _jsonable(v):
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    return v


# --- generated data ---------------------------------------------------------------------------

def generate(spec, src: SourceInfo, base_time, partitions: int | None) -> list[dict]:
    """Seeded synthetic rows: {count, seed, columns: {col: spec}, step, jitter}. Column specs: a
    constant or "{n}" template, {one_of: [...]}, {between: [lo, hi]}, {null_rate: 0.1}."""
    if isinstance(spec, int):
        spec = {"count": spec}
    if not isinstance(spec, dict) or not isinstance(spec.get("count"), int):
        raise FixtureError(f"generate for '{src.name}' needs a row count: generate: 1000 or {{count: 1000, ...}}")
    rnd = random.Random(spec.get("seed", 0))
    step = timeutil.parse_duration_ms(spec.get("step", "1s"))
    jitter = timeutil.parse_duration_ms(spec.get("jitter", 0))
    cols = spec.get("columns") or {}
    unknown = set(cols) - set(src.names)
    if unknown:
        raise FixtureError(f"generate for '{src.name}': unknown column(s) {sorted(unknown)}")
    start = timeutil.parse_ts(timeutil.resolve(spec.get("start", "+0s"), base_time))
    rows = []
    for n in range(1, spec["count"] + 1):
        row = {}
        t = start + timedelta(milliseconds=(n - 1) * step + (rnd.randint(-jitter, jitter) if jitter else 0))
        for name, typ in src.columns:
            cs = cols.get(name)
            if isinstance(cs, dict):
                if rnd.random() < float(cs.get("null_rate", 0)):
                    row[name] = None
                elif "one_of" in cs:
                    row[name] = rnd.choice(list(cs["one_of"]))
                elif "between" in cs:
                    lo, hi = cs["between"]
                    row[name] = _between(rnd, lo, hi, typ)
                else:
                    row[name] = _random_value(rnd, name, typ, n, t, src)
            elif cs is not None:
                row[name] = _template(cs, n, n - 1)
            else:
                row[name] = _random_value(rnd, name, typ, n, t, src)
        if partitions:
            row["_partition"] = (n - 1) % int(partitions)
        rows.append(row)
    return rows


def _between(rnd, lo, hi, typ):
    b = base_type(typ)
    if b in ("DECIMAL", "NUMERIC"):
        scale = 2
        if "," in typ:
            scale = int(typ.split(",")[1].split(")")[0])
        q = Decimal(1).scaleb(-scale)
        return str((Decimal(str(lo)) + (Decimal(str(hi)) - Decimal(str(lo))) * Decimal(rnd.random())).quantize(q))
    if b in ("FLOAT", "DOUBLE"):
        return round(rnd.uniform(float(lo), float(hi)), 6)
    if b in ("TIMESTAMP", "TIMESTAMP_LTZ"):
        lo_t, hi_t = timeutil.parse_ts(lo), timeutil.parse_ts(hi)
        return lo_t + (hi_t - lo_t) * rnd.random()
    return rnd.randint(int(lo), int(hi))


def _random_value(rnd, name, typ, n, t, src: SourceInfo):
    b = base_type(typ)
    if name in src.primary_key:
        return n if b in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT") else f"{name}_{n}"
    if b in ("STRING", "VARCHAR", "CHAR"):
        return f"{name}_{rnd.randint(1, 5)}"
    if b in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
        return rnd.randint(1, 100)
    if b in ("DECIMAL", "NUMERIC"):
        return _between(rnd, 1, 100, typ)
    if b in ("FLOAT", "DOUBLE"):
        return round(rnd.uniform(0, 100), 3)
    if b == "BOOLEAN":
        return rnd.random() < 0.5
    if b in ("TIMESTAMP", "TIMESTAMP_LTZ"):
        return t
    if b == "DATE":
        return t.date()
    return None


# --- rendering: rows + markers -> changelog lines + watermarks --------------------------------

def suggest(name: str, candidates) -> str:
    m = difflib.get_close_matches(name, list(candidates), n=1)
    return f" (did you mean '{m[0]}'?)" if m else ""


def render(src: SourceInfo, rows: list[dict], watermarks: str = "per_row",
           partitions: int | None = None) -> Rendered:
    names = src.names
    out = Rendered(src.name, [])
    parts: list[int] = list(range(int(partitions))) if partitions else []
    for r in rows:
        p = r.get("_partition")
        if p is not None and p not in parts:
            parts.append(p)
        for x in _as_list(r.get("_idle")):
            if x not in parts:
                parts.append(x)
    if not parts:
        parts = [0]
    out.partitions = sorted(parts)
    max_ts: dict = {p: None for p in parts}
    idle: set = set()
    last: list = [None]
    state: dict = {}
    seq = [0]
    n = 0

    def event(**kw):
        seq[0] += 1
        e = Event(seq=seq[0], watermark=last[0], **kw)
        out.events.append(e)
        return e

    def emit_wm(wm):
        if wm is not None and (last[0] is None or wm > last[0]):
            last[0] = wm
            out.lines.append(json.dumps({"watermark": wm}))
            return True
        return False

    def combined():
        active = [p for p in parts if p not in idle]
        if not active or any(max_ts[p] is None for p in active):
            return None
        return min(max_ts[p] for p in active) - src.delay_ms

    def emit_row(kind, row, partition, ts, index, values):
        late = ts is not None and last[0] is not None and ts <= last[0]
        out.lines.append(json.dumps({"kind": kind, "values": [row.get(c) for c in names]}))
        e = event(kind="row", partition=partition, row=index, op=kind, ts=ts, late=late, values=values)
        return e

    def advance(partition, ts):
        if ts is None or watermarks != "per_row" or not src.event_time:
            return
        idle.discard(partition)
        max_ts[partition] = ts if max_ts[partition] is None else max(max_ts[partition], ts)
        emit_wm(combined())
        out.events[-1].watermark = last[0]

    for i, raw in enumerate(rows):
        if "_watermark" in raw:
            if emit_wm(raw["_watermark"]):
                event(kind="watermark", ts=raw["_watermark"])
            continue
        if "_idle" in raw:
            for p in _as_list(raw["_idle"]):
                idle.add(p)
            event(kind="idle", partition=_as_list(raw["_idle"])[0])
            if watermarks == "per_row":
                emit_wm(combined())
                out.events[-1].watermark = last[0]
            continue
        n += 1
        row = dict(raw)
        op = row.pop("_op", "insert")
        partition = row.pop("_partition", 0)
        if partition is None:
            partition = 0
        unknown = [k for k in row if k not in names]
        if unknown:
            raise FixtureError(f"{src.name} row {n}: unknown column '{unknown[0]}'{suggest(unknown[0], names)}; "
                               f"columns are {', '.join(names)}")
        full = {c: row.get(c) for c in names}
        ts = _ts_ms(full.get(src.event_time)) if src.event_time else None
        if op not in OPS:
            raise FixtureError(f"{src.name} row {n}: _op must be one of {', '.join(OPS)}")
        if not src.updating:
            if op != "insert":
                raise FixtureError(f"source '{src.name}' is append-only; `_op: {op}` is not allowed")
            emit_row("+I", full, partition, ts, n, row)
            advance(partition, ts)
            continue
        if not src.primary_key:
            raise FixtureError(f"source '{src.name}' is a changelog source without a primary key")
        key = tuple(full.get(k) for k in src.primary_key)
        if op == "upsert":
            op = "update" if key in state else "insert"
        if op == "insert":
            if key in state:
                raise FixtureError(f"{src.name} row {n}: insert of existing key {_key(src, key)}; use `_op: update`")
            state[key] = full
            emit_row("+I", full, partition, ts, n, row)
            advance(partition, ts)
        elif op == "update":
            before = state.get(key)
            if before is None:
                raise FixtureError(f"{src.name} row {n}: update of unknown key {_key(src, key)}")
            merged = {**before, **row}
            ts = _ts_ms(merged.get(src.event_time)) if src.event_time else None
            state[key] = merged
            emit_row("-U", before, partition, ts, n, row)
            emit_row("+U", merged, partition, ts, n, row)
            advance(partition, ts)
        elif op == "delete":
            before = state.pop(key, None)
            if before is None:
                raise FixtureError(f"{src.name} row {n}: delete of unknown key {_key(src, key)}")
            emit_row("-D", before, partition, None, n, row)
    return out


def _key(src: SourceInfo, key: tuple) -> str:
    return "{" + ", ".join(f"{k}: {v}" for k, v in zip(src.primary_key, key)) + "}"


def _as_list(v) -> list:
    if v is None:
        return []
    return list(v) if isinstance(v, (list, tuple)) else [v]


def _ts_ms(v) -> int | None:
    if v is None:
        return None
    try:
        return timeutil.epoch_ms(timeutil.parse_ts(v))
    except timeutil.TimeError:
        raise FixtureError(f"cannot read '{v}' as a timestamp")


def write(rendered: Rendered, path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for line in rendered.lines:
            f.write(line + "\n")
    return path
