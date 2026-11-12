"""
Tests for the Gold layer (5-gold.py), run against local Delta tables.
"""

from datetime import date

import pytest


@pytest.fixture
def silver_tables(spark, schema):
    """Silver tables as 4-silver.py leaves them after the two batches."""
    spark.createDataFrame(
        [
            # John moved: only the current version should count in Gold.
            (1, "John", "Palo Alto",     date(2026, 1, 15), date(2026, 6, 10), False),
            (1, "John", "Mountain View", date(2026, 6, 10), None,              True),
            (2, "Mary", "Cupertino",     date(2026, 1, 15), None,              True),
            (3, "Paul", "Seattle",       date(2026, 1, 15), None,              True),
        ],
        "customer_id int, name string, city string, "
        "valid_from date, valid_to date, is_current boolean",
    ).write.format("delta").saveAsTable("silver_dim_customers_type2")

    spark.createDataFrame(
        [
            ("P1", "iPhone",  "Electronics", 999.0, date(2026, 1, 1), date(2026, 6, 1), False),
            ("P1", "iPhone",  "Electronics", 899.0, date(2026, 6, 1), None,             True),
            ("P2", "AirPods", "Electronics", 199.0, date(2026, 1, 1), None,             True),
            ("P3", "T-Shirt", "Apparel",     34.9,  date(2026, 1, 1), None,             True),
        ],
        "product_id string, product_name string, category string, price double, "
        "valid_from date, valid_to date, is_current boolean",
    ).write.format("delta").saveAsTable("silver_dim_products_type2")

    spark.createDataFrame(
        [
            (100, 1, "P1", 999.0, date(2026, 2, 10), 2026, 2),
            (101, 1, "P2", 199.0, date(2026, 2, 11), 2026, 2),
            (102, 2, "P1", 899.0, date(2026, 6, 16), 2026, 6),
            (103, 2, "P3", 34.9,  date(2026, 6, 17), 2026, 6),
            (104, 3, "P3", 34.9,  date(2026, 6, 18), 2026, 6),
        ],
        "order_id int, customer_id int, product_id string, amount double, "
        "order_date date, year int, month int",
    ).write.format("delta").saveAsTable("silver_fact_orders")


def test_sales_by_city_uses_current_customer_version(spark, gold, silver_tables):
    gold.silver_to_gold_sales_city(spark)

    rows = {r.city: r for r in spark.read.table("gold_agg_sales_by_city").collect()}

    # John's orders land in his current city; Palo Alto disappears.
    assert set(rows) == {"Mountain View", "Cupertino", "Seattle"}
    assert rows["Mountain View"].total_orders == 2
    assert rows["Mountain View"].total_amount == pytest.approx(1198.0)
    assert rows["Cupertino"].total_amount == pytest.approx(933.9)


def test_sales_by_category_is_not_inflated_by_closed_versions(spark, gold, silver_tables):
    gold.silver_to_gold_sales_by_category(spark)

    rows = {r.category: r for r in spark.read.table("gold_agg_sales_by_category").collect()}

    # P1 has two versions; joining both would count its orders twice.
    assert rows["Electronics"].total_orders == 3
    assert rows["Electronics"].unique_products == 2
    assert rows["Electronics"].total_amount == pytest.approx(999.0 + 199.0 + 899.0)
    assert rows["Apparel"].total_amount == pytest.approx(69.8)


def test_customer_ranking_orders_customers_by_amount_spent(spark, gold, silver_tables):
    gold.silver_to_gold_ranking(spark)

    rows = sorted(spark.read.table("gold_ranking_customers").collect(),
                  key=lambda r: r.ranking)

    assert [(r.ranking, r.name) for r in rows] == [(1, "John"), (2, "Mary"), (3, "Paul")]


def test_gold_tables_are_rebuilt_not_appended(spark, gold, silver_tables):
    gold.silver_to_gold_sales_city(spark)
    gold.silver_to_gold_sales_city(spark)

    assert spark.read.table("gold_agg_sales_by_city").count() == 3
