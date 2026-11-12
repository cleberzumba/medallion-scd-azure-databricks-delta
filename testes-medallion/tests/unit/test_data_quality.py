"""
Tests for the data quality checks in tests/data_quality/checks.py.

Local temporary views stand in for the Unity Catalog tables, so the same SQL
that runs in the Lakeflow Job is exercised here: once on a healthy dataset
(every check must pass) and once on a corrupted SCD Type 2 history (the
checks must catch it).
"""

from datetime import date

import pytest


CUSTOMER_DIM_SCHEMA = (
    "customer_id int, name string, city string, "
    "valid_from date, valid_to date, is_current boolean"
)


@pytest.fixture
def healthy_tables(spark):
    """Registers a consistent Silver + Gold dataset as temp views."""
    views = {
        "silver_dim_customers_type2": spark.createDataFrame(
            [
                (1, "John", "Palo Alto",     date(2026, 1, 15), date(2026, 6, 10), False),
                (1, "John", "Mountain View", date(2026, 6, 10), None,              True),
                (2, "Mary", "Cupertino",     date(2026, 1, 15), None,              True),
            ],
            CUSTOMER_DIM_SCHEMA,
        ),
        "silver_dim_products_type2": spark.createDataFrame(
            [
                ("P1", "iPhone",  "Electronics", 999.0, date(2026, 1, 1), date(2026, 6, 1), False),
                ("P1", "iPhone",  "Electronics", 899.0, date(2026, 6, 1), None,             True),
                ("P2", "T-Shirt", "Apparel",     34.9,  date(2026, 1, 1), None,             True),
            ],
            "product_id string, product_name string, category string, price double, "
            "valid_from date, valid_to date, is_current boolean",
        ),
        "silver_dim_customers_type1": spark.createDataFrame(
            [(1, "John", "Mountain View", date(2026, 6, 10)),
             (2, "Mary", "Cupertino",     date(2026, 1, 15))],
            "customer_id int, name string, city string, update_date date",
        ),
        "silver_fact_orders": spark.createDataFrame(
            [(100, 1, "P1", 999.0), (101, 2, "P2", 34.9)],
            "order_id int, customer_id int, product_id string, amount double",
        ),
        "gold_agg_sales_by_city": spark.createDataFrame(
            [("Mountain View", 1, 999.0), ("Cupertino", 1, 34.9)],
            "city string, total_orders long, total_amount double",
        ),
        "gold_agg_sales_by_category": spark.createDataFrame(
            [("Electronics", 1, 999.0, 1), ("Apparel", 1, 34.9, 1)],
            "category string, total_orders long, total_amount double, unique_products long",
        ),
        "gold_ranking_customers": spark.createDataFrame(
            [(1, "John", "Mountain View", 999.0, 1), (2, "Mary", "Cupertino", 34.9, 2)],
            "customer_id int, name string, city string, total_amount double, ranking int",
        ),
    }
    for name, df in views.items():
        df.createOrReplaceTempView(name)
    yield
    for name in views:
        spark.catalog.dropTempView(name)


def test_all_checks_pass_on_healthy_data(spark, dq, healthy_tables):
    assert dq.run_checks(spark) == []


def test_checks_catch_scd2_history_replayed_out_of_order(spark, dq, healthy_tables):
    # What a re-run without batch control produces: the January batch is
    # replayed after June, closing June's version with January's date and
    # re-opening the old address as current.
    spark.createDataFrame(
        [
            (1, "John", "Palo Alto",     date(2026, 1, 15), date(2026, 6, 10), False),
            (1, "John", "Mountain View", date(2026, 6, 10), date(2026, 1, 15), False),
            (1, "John", "Palo Alto",     date(2026, 1, 15), None,              True),
            (2, "Mary", "Cupertino",     date(2026, 1, 15), None,              True),
        ],
        CUSTOMER_DIM_SCHEMA,
    ).createOrReplaceTempView("silver_dim_customers_type2")

    failed = {name for name, _ in dq.run_checks(spark)}

    assert "silver_dim_customers_type2: no version closes before it opens" in failed
    assert "silver_dim_customers_type2: versions of the same customer_id do not overlap" in failed


def test_checks_catch_two_current_versions(spark, dq, healthy_tables):
    spark.createDataFrame(
        [
            (1, "John", "Palo Alto",     date(2026, 1, 15), None, True),
            (1, "John", "Mountain View", date(2026, 6, 10), None, True),
            (2, "Mary", "Cupertino",     date(2026, 1, 15), None, True),
        ],
        CUSTOMER_DIM_SCHEMA,
    ).createOrReplaceTempView("silver_dim_customers_type2")

    failed = {name for name, _ in dq.run_checks(spark)}

    assert "silver_dim_customers_type2: exactly one current version per customer_id" in failed


def test_checks_catch_orphan_orders_and_gold_mismatch(spark, dq, healthy_tables):
    spark.createDataFrame(
        [(100, 1, "P1", 999.0), (101, 2, "P2", 34.9), (102, 99, "P1", 10.0)],
        "order_id int, customer_id int, product_id string, amount double",
    ).createOrReplaceTempView("silver_fact_orders")

    failed = {name for name, _ in dq.run_checks(spark)}

    assert "silver_fact_orders: every customer_id exists in the customer dimension" in failed
    assert "gold_agg_sales_by_category: total matches the fact table" in failed
