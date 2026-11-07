import json

import pytest

from cascade_test import replay as RP


def test_kcat_records_keep_partition_offset_timestamp_headers():
    r = RP.normalize_record({"topic": "orders", "partition": 2, "offset": 7, "ts": 1000, "key": "k",
                             "payload": json.dumps({"order_id": "o1"}), "headers": ["a", "1", "b", "2"]})
    assert r == {"partition": 2, "offset": 7, "timestamp": 1000, "key": "k", "headers": {"a": "1", "b": "2"},
                 "row": {"order_id": "o1"}, "op": "insert"}


def test_debezium_payloads_become_changelog_records():
    def kc(payload):
        return {"partition": 0, "offset": 0, "payload": json.dumps(payload)}
    assert RP.normalize_record(kc({"op": "c", "after": {"id": 1}}), "debezium-json")["op"] == "upsert"
    d = RP.normalize_record(kc({"op": "d", "before": {"id": 1}, "after": None}), "debezium-json")
    assert d["op"] == "delete" and d["row"] == {"id": 1}
    tomb = RP.normalize_record({"partition": 0, "offset": 1, "payload": None}, "debezium-json")
    assert tomb["row"] is None
    with pytest.raises(RP.ReplayError):
        RP.normalize_record(kc({"op": "x", "after": {}}), "debezium-json")


def test_bare_rows_are_records():
    assert RP.normalize_record({"order_id": "o1"})["row"] == {"order_id": "o1"}


def test_select_last_limit_and_order():
    recs = [{"timestamp": t, "partition": p, "offset": i, "row": {}} for i, (t, p) in
            enumerate([(30, 0), (10, 1), (20, 0), (40, 1)])]
    assert [r["timestamp"] for r in RP.select(recs, order="timestamp")] == [10, 20, 30, 40]
    assert [r["timestamp"] for r in RP.select(recs, last_ms=15)] == [30, 40]
    assert len(RP.select(recs, limit=2)) == 2


def test_parse_keys():
    assert RP.parse_keys(["a=x, y", "b=z"]) == {"a": ["x", "y"], "b": ["z"]}
    with pytest.raises(RP.ReplayError):
        RP.parse_keys(["nope"])
