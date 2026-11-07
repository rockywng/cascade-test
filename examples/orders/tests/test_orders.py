"""Python tests use the same engine and assertions as the YAML tests (run with: pytest examples/orders).

The pytest plugin also collects tests/*.yaml, so `pytest` runs both kinds side by side."""
from decimal import Decimal

import pytest

from cascade_test import project

orders = project(__file__)  # the project this test file belongs to


def order(n, customer="c1", amount=10, currency="usd", at=None):
    return {"order_id": f"o{n}", "customer_id": customer, "amount": amount, "currency": currency,
            "order_ts": at or f"+{n}m"}


def test_ltv_per_customer():
    r = orders.run("customer_ltv", given={"orders": [order(1), order(2, amount=5), order(3, "c2", 7)]},
                   base_time="2026-01-01 10:00:00")
    r.assert_rows([
        {"customer_id": "c1", "lifetime_value": 15, "order_count": 2},
        {"customer_id": "c2", "lifetime_value": 7, "order_count": 1},
    ])
    # rows come back as Python values
    by_customer = {row["customer_id"]: row for row in r.rows}
    assert by_customer["c1"]["lifetime_value"] == Decimal("15")


@pytest.mark.parametrize("amount", [0, -1, Decimal("-0.01")])
def test_non_positive_amounts_never_reach_ltv(amount):
    r = orders.run("customer_ltv", given={"orders": [order(1), order(2, amount=amount)]})
    r.assert_rows([{"customer_id": "c1", "order_count": 1}])
    r.assert_expectation("positive_amount", violations=1)


def test_pipeline_sinks():
    r = orders.pipeline_run(given={
        "customers": [{"customer_id": "c1", "name": "Acme", "segment": "smb", "updated_at": "-1h"}],
        "orders": [order(1), order(2, currency="jpy")],
    })
    r.sink("ltv_to_kafka").assert_rows([{"customer_id": "c1", "lifetime_value": 10}])
    r.sink("clean_orders.quarantined").assert_count(1)
    r.sink("revenue_to_lake").assert_all(lambda row: row["revenue"] > 0, "revenue is positive")


def test_fail_expectation_stops_the_job():
    orders.run("clean_orders", given={"orders": [order(1, customer=None)]}).assert_error("has_customer")


def test_sql_expressions_and_udfs():
    assert orders.call("UPPER('usd')") == "USD"
    assert orders.call("CAST('12.345' AS DECIMAL(12, 2))") == Decimal("12.35")
