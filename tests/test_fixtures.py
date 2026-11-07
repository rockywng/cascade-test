import json
import os

import pytest

from cascade_test import fixtures as F
from cascade_test import timeutil

BASE = "2026-01-01 10:00:00"
ORDERS = F.SourceInfo("orders", [("order_id", "STRING NOT NULL"), ("amount", "DECIMAL(12, 2)"),
                                 ("order_ts", "TIMESTAMP(3)")], False, [], "order_ts", 5000)
CUSTOMERS = F.SourceInfo("customers", [("customer_id", "STRING NOT NULL"), ("segment", "STRING"),
                                       ("updated_at", "TIMESTAMP(3)")], True, ["customer_id"], "updated_at", 0)


def ms(t):
    return timeutil.epoch_ms(timeutil.resolve(t, BASE))


def rendered(src, rows, mode="per_row", partitions=None, defaults=None):
    rows, parts = F.expand(rows, src, base_time=BASE, defaults=defaults, partitions=partitions)
    return F.render(src, rows, mode, parts)


def lines(r):
    return [json.loads(x) for x in r.lines]


def test_defaults_templates_and_relative_times():
    rows, _ = F.expand([{}, {"amount": 5}], ORDERS, base_time=BASE,
                       defaults={"order_id": "o{n}", "amount": 1, "order_ts": "+{n}m"})
    assert rows == [{"order_id": "o1", "amount": 1, "order_ts": "2026-01-01 10:01:00.000"},
                    {"order_id": "o2", "amount": 5, "order_ts": "2026-01-01 10:02:00.000"}]


def test_per_row_watermarks_and_late_rows():
    r = rendered(ORDERS, [{"order_id": "a", "order_ts": "+10s"}, {"order_id": "b", "order_ts": "+90s"},
                          {"order_id": "c", "order_ts": "+20s"}])
    wms = [x["watermark"] for x in lines(r) if "watermark" in x]
    assert wms == [ms("+5s"), ms("+85s")]
    assert r.late_rows == [3]


def test_slowest_partition_holds_back_the_watermark():
    r = rendered(ORDERS, [{"_partition": 1, "order_id": "a", "order_ts": "+5s"},
                          {"_partition": 0, "order_id": "b", "order_ts": "+3m"},
                          {"_partition": 0, "order_id": "c", "order_ts": "+30s"}], partitions=2)
    assert [x["watermark"] for x in lines(r) if "watermark" in x] == [ms("+0s")]
    assert r.late_rows == []


def test_partition_without_data_blocks_the_watermark_until_idle():
    r = rendered(ORDERS, [{"order_id": "a", "order_ts": "+1m"}], partitions=2)
    assert not [x for x in lines(r) if "watermark" in x]
    r = rendered(ORDERS, [{"order_id": "a", "order_ts": "+1m"}, {"_idle": 1}], partitions=2)
    assert [x["watermark"] for x in lines(r) if "watermark" in x] == [ms("+55s")]


def test_idle_partition_releases_the_watermark():
    r = rendered(ORDERS, [{"_partition": 1, "order_id": "a", "order_ts": "+5s"},
                          {"_partition": 0, "order_id": "b", "order_ts": "+3m"}, {"_idle": 1},
                          {"_partition": 0, "order_id": "c", "order_ts": "+30s"}], partitions=2)
    assert r.late_rows == [3]


def test_manual_and_end_of_input_modes():
    rows = [{"order_id": "a", "order_ts": "+10s"}, {"_watermark": "+1m"}, {"order_id": "b", "order_ts": "+30s"}]
    r = rendered(ORDERS, rows, "manual")
    assert [x["watermark"] for x in lines(r) if "watermark" in x] == [ms("+1m")]
    assert r.late_rows == [2]
    r = rendered(ORDERS, [{"order_id": "a", "order_ts": "+10m"}, {"order_id": "b", "order_ts": "+0s"}],
                 "end_of_input")
    assert not [x for x in lines(r) if "watermark" in x] and r.late_rows == []


def test_changelog_operations():
    r = rendered(CUSTOMERS, [
        {"customer_id": "c1", "segment": "smb", "updated_at": "+0s"},
        {"_op": "update", "customer_id": "c1", "segment": "ent", "updated_at": "+1m"},
        {"_op": "upsert", "customer_id": "c2", "segment": "mid", "updated_at": "+2m"},
        {"_op": "delete", "customer_id": "c1"},
    ])
    kinds = [(x["kind"], x["values"][1]) for x in lines(r) if "kind" in x]
    assert kinds == [("+I", "smb"), ("-U", "smb"), ("+U", "ent"), ("+I", "mid"), ("-D", "ent")]


def test_fixture_errors_are_specific():
    with pytest.raises(F.FixtureError, match="did you mean 'amount'"):
        rendered(ORDERS, [{"order_id": "a", "amout": 1}])
    with pytest.raises(F.FixtureError, match="append-only"):
        rendered(ORDERS, [{"_op": "delete", "order_id": "a"}])
    with pytest.raises(F.FixtureError, match="update of unknown key"):
        rendered(CUSTOMERS, [{"_op": "update", "customer_id": "nope"}])
    with pytest.raises(F.FixtureError, match="no event_time"):
        src = F.SourceInfo("x", [("a", "INT")], False, [], None)
        F.expand([{"_watermark": "+1m"}], src, base_time=BASE)


def test_csv_and_jsonl_files(tmp_path):
    (tmp_path / "o.csv").write_text("order_id,amount,order_ts\na,1.50,2026-01-01 10:00:00\nb,,+1m\n")
    rows, _ = F.expand({"file": "o.csv"}, ORDERS, base_time=BASE, test_dir=str(tmp_path))
    assert rows[0]["amount"] == "1.50" and rows[1]["amount"] is None
    assert rows[1]["order_ts"] == "2026-01-01 10:01:00.000"
    (tmp_path / "o.jsonl").write_text('{"order_id": "a"}\n{"order_id": "b"}\n')
    rows, _ = F.expand({"file": "o.jsonl", "rows": [{"order_id": "c"}]}, ORDERS, base_time=BASE,
                       test_dir=str(tmp_path))
    assert [r["order_id"] for r in rows] == ["a", "b", "c"]


def test_generated_rows_are_seeded_and_typed():
    spec = {"generate": {"count": 50, "seed": 3, "columns": {"amount": {"between": [1, 2]}}}, "partitions": 2}
    a, parts = F.expand(spec, ORDERS, base_time=BASE)
    b, _ = F.expand(spec, ORDERS, base_time=BASE)
    assert a == b and parts == 2 and len(a) == 50
    assert all(1 <= float(r["amount"]) <= 2 for r in a)
    assert {r["_partition"] for r in a} == {0, 1}
    assert a[1]["order_ts"] == "2026-01-01 10:00:01.000"


def test_write(tmp_path):
    r = rendered(ORDERS, [{"order_id": "a", "order_ts": "+0s"}])
    path = F.write(r, os.path.join(str(tmp_path), "sources", "orders.jsonl"))
    assert open(path).read().count("\n") == 2


def test_durations():
    assert timeutil.parse_duration_ms("1h30m") == 5_400_000
    assert timeutil.parse_duration_ms("250ms") == 250
    with pytest.raises(timeutil.TimeError):
        timeutil.parse_duration_ms("5 parsecs")
