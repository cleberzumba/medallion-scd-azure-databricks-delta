"""
Tests for the Silver layer (4-silver.py), run against local Delta tables.

The scenario follows the README: a January batch and a June batch in which
two customers move, one customer joins, the iPhone gets cheaper, the T-Shirt
gets more expensive and a new product launches.
"""

from datetime import date

import pytest


# =========================================================
# Test data
# =========================================================
RAW = "abfss://medallion@account.dfs.core.windows.net/raw"

CUSTOMERS = [
    # customer_id, name, city, event_date, _source_file
    ("1", "John", "Palo Alto",     "2026-01-15", f"{RAW}/customers/customers_2026-01.json"),
    ("2", "Mary", "San Jose",      "2026-01-15", f"{RAW}/customers/customers_2026-01.json"),
    ("3", "Paul", "Seattle",       "2026-01-15", f"{RAW}/customers/customers_2026-01.json"),
    ("1", "John", "Mountain View", "2026-06-10", f"{RAW}/customers/customers_2026-06.json"),
    ("2", "Mary", "Cupertino",     "2026-06-10", f"{RAW}/customers/customers_2026-06.json"),
    ("3", "Paul", "Seattle",       "2026-06-10", f"{RAW}/customers/customers_2026-06.json"),
    ("4", "Lisa", "Austin",        "2026-06-10", f"{RAW}/customers/customers_2026-06.json"),
]

PRODUCTS = [
    # product_id, product_name, category, price, launch_date, _source_file
    ("P1", "iPhone",   "Electronics", "999.00", "2025-09-01", f"{RAW}/products/products_2026-01.csv"),
    ("P2", "AirPods",  "Electronics", "199.00", "2025-09-01", f"{RAW}/products/products_2026-01.csv"),
    ("P3", "T-Shirt",  "Apparel",     "29.90",  "2025-01-01", f"{RAW}/products/products_2026-01.csv"),
    ("P4", "Mug",      "Home",        "12.00",  "2025-01-01", f"{RAW}/products/products_2026-01.csv"),
    ("P1", "iPhone",   "Electronics", "899.00", "2025-09-01", f"{RAW}/products/products_2026-06.csv"),
    ("P2", "AirPods",  "Electronics", "199.00", "2025-09-01", f"{RAW}/products/products_2026-06.csv"),
    ("P3", "T-Shirt",  "Apparel",     "34.90",  "2025-01-01", f"{RAW}/products/products_2026-06.csv"),
    ("P4", "Mug",      "Home",        "12.00",  "2025-01-01", f"{RAW}/products/products_2026-06.csv"),
    ("P5", "iPad",     "Electronics", "599.00", "2026-05-01", f"{RAW}/products/products_2026-06.csv"),
]

ORDERS = [
    # order_id, customer_id, product_id, amount, order_date, _source_file
    ("100", "1", "P1", "999.00", "2026-02-10", f"{RAW}/orders/orders_2026-01.parquet"),
    ("101", "2", "P3", "29.90",  "2026-02-11", f"{RAW}/orders/orders_2026-01.parquet"),
    ("102", "3", "P2", "199.00", "2026-02-12", f"{RAW}/orders/orders_2026-01.parquet"),
    ("103", "1", "P4", "12.00",  "2026-02-13", f"{RAW}/orders/orders_2026-01.parquet"),
    ("103", "1", "P4", "12.00",  "2026-02-13", f"{RAW}/orders/orders_2026-06.parquet"),  # replayed
    ("200", "4", "P5", "599.00", "2026-06-15", f"{RAW}/orders/orders_2026-06.parquet"),
    ("201", "2", "P1", "899.00", "2026-06-16", f"{RAW}/orders/orders_2026-06.parquet"),
    ("202", "3", None, "50.00",  "2026-06-17", f"{RAW}/orders/orders_2026-06.parquet"),  # no product
    ("203", "3", "P3", "0",      "2026-06-18", f"{RAW}/orders/orders_2026-06.parquet"),  # zero amount
]


@pytest.fixture
def bronze(spark, schema):
    """Bronze tables (all columns as string, as Auto Loader leaves them)."""
    spark.createDataFrame(
        CUSTOMERS,
        "customer_id string, name string, city string, event_date string, "
        "_source_file string",
    ).write.format("delta").saveAsTable("bronze_raw_customers")

    spark.createDataFrame(
        PRODUCTS,
        "product_id string, product_name string, category string, price string, "
        "launch_date string, _source_file string",
    ).write.format("delta").saveAsTable("bronze_raw_products")

    spark.createDataFrame(
        ORDERS,
        "order_id string, customer_id string, product_id string, amount string, "
        "order_date string, _source_file string",
    ).write.format("delta").saveAsTable("bronze_raw_orders")


@pytest.fixture
def dimensions(spark, schema):
    """Empty SCD dimensions, with the columns the MERGE/INSERT statements expect."""
    spark.sql("""
        CREATE TABLE silver_dim_customers_type1 (
            customer_id INT, name STRING, city STRING, update_date DATE
        ) USING DELTA""")
    spark.sql("""
        CREATE TABLE silver_dim_customers_type2 (
            customer_id INT, name STRING, city STRING,
            valid_from DATE, valid_to DATE, is_current BOOLEAN
        ) USING DELTA""")
    spark.sql("""
        CREATE TABLE silver_dim_products_type2 (
            product_id STRING, product_name STRING, category STRING, price DOUBLE,
            valid_from DATE, valid_to DATE, is_current BOOLEAN
        ) USING DELTA""")


BATCHES = [("2026-01", "2026-01-01"), ("2026-06", "2026-06-01")]


def run_silver(spark, silver):
    """
    Runs the Silver layer the way main() does, minus the Unity Catalog setup.

    The batches are passed explicitly rather than taken from
    discover_batches(), so the SCD tests do not depend on batch discovery,
    which has tests of its own.
    """
    for batch_key, business_date in BATCHES:
        silver.process_batch(spark, batch_key, business_date)
    silver.bronze_to_silver_orders(spark)


# =========================================================
# Batch discovery
# =========================================================
@pytest.mark.xfail(
    strict=True,
    reason="Known bug: BATCH_KEY_PATTERN is inserted into a SQL string literal "
           "as '\\d', which Spark SQL unescapes to 'd', so regexp_extract never "
           "matches and discover_batches() raises. Fix: write the pattern as "
           "r\"(\\\\d{4}-\\\\d{2})\" or pass it without going through SQL.",
)
def test_discover_batches_returns_batches_in_chronological_order(spark, silver, bronze):
    assert silver.discover_batches(spark) == BATCHES


def test_discover_batches_fails_loudly_when_no_batch_is_found(spark, silver, schema):
    spark.createDataFrame(
        [("1", "John", "Palo Alto", "2026-01-15", "customers_latest.json")],
        "customer_id string, name string, city string, event_date string, "
        "_source_file string",
    ).write.format("delta").saveAsTable("bronze_raw_customers")

    with pytest.raises(ValueError, match="No batch could be identified"):
        silver.discover_batches(spark)


# =========================================================
# Cleansing and typing
# =========================================================
def test_customers_staging_holds_only_the_requested_batch(spark, silver, bronze):
    silver.bronze_to_silver_customers(spark, "2026-06")

    rows = {r.customer_id: r for r in spark.read.table("silver_stg_customers").collect()}

    assert set(rows) == {1, 2, 3, 4}
    assert rows[1].city == "Mountain View"
    assert rows[1].event_date == date(2026, 6, 10)


def test_customers_staging_drops_rows_missing_required_fields(spark, silver, schema):
    spark.createDataFrame(
        [
            ("1",  "John", "Palo Alto", "2026-01-15", "customers_2026-01.json"),
            (None, "Ann",  "Austin",    "2026-01-15", "customers_2026-01.json"),
            ("3",  None,   "Austin",    "2026-01-15", "customers_2026-01.json"),
            ("4",  "Bob",  None,        "2026-01-15", "customers_2026-01.json"),
            ("5",  "Eve",  "Austin",    None,         "customers_2026-01.json"),
            ("1",  "John", "Palo Alto", "2026-01-15", "customers_2026-01.json"),
        ],
        "customer_id string, name string, city string, event_date string, "
        "_source_file string",
    ).write.format("delta").saveAsTable("bronze_raw_customers")

    silver.bronze_to_silver_customers(spark, "2026-01")

    ids = [r.customer_id for r in spark.read.table("silver_stg_customers").collect()]
    assert ids == [1]


def test_products_staging_rejects_non_positive_prices(spark, silver, schema):
    spark.createDataFrame(
        [
            ("P1", "iPhone",  "Electronics", "999.00", "2025-09-01", "products_2026-01.csv"),
            ("P2", "Freebie", "Promo",       "0",      "2025-09-01", "products_2026-01.csv"),
            ("P3", "Refund",  "Promo",       "-5",     "2025-09-01", "products_2026-01.csv"),
            (None, "Ghost",   "Promo",       "10",     "2025-09-01", "products_2026-01.csv"),
        ],
        "product_id string, product_name string, category string, price string, "
        "launch_date string, _source_file string",
    ).write.format("delta").saveAsTable("bronze_raw_products")

    silver.bronze_to_silver_products(spark, "2026-01")

    rows = spark.read.table("silver_stg_products").collect()
    assert [(r.product_id, r.price) for r in rows] == [("P1", 999.0)]


def test_fact_orders_is_clean_deduplicated_and_partitioned(spark, silver, bronze):
    silver.bronze_to_silver_orders(spark)

    rows = {r.order_id: r for r in spark.read.table("silver_fact_orders").collect()}

    # 202 has no product, 203 has a zero amount, 103 arrived twice.
    assert set(rows) == {100, 101, 102, 103, 200, 201}
    assert (rows[200].year, rows[200].month) == (2026, 6)
    assert rows[100].amount == 999.0


# =========================================================
# SCD Type 1 and Type 2
# =========================================================
def test_scd_type1_keeps_one_row_per_customer_with_latest_values(
        spark, silver, bronze, dimensions):
    run_silver(spark, silver)

    rows = {r.customer_id: r for r in
            spark.read.table("silver_dim_customers_type1").collect()}

    assert set(rows) == {1, 2, 3, 4}
    assert rows[1].city == "Mountain View"
    assert rows[2].city == "Cupertino"


def test_scd_type2_customers_keeps_address_history(spark, silver, bronze, dimensions):
    run_silver(spark, silver)

    john = sorted(
        spark.read.table("silver_dim_customers_type2")
             .filter("customer_id = 1").collect(),
        key=lambda r: r.valid_from,
    )

    assert [(r.city, r.valid_from, r.valid_to, r.is_current) for r in john] == [
        ("Palo Alto",     date(2026, 1, 15), date(2026, 6, 10), False),
        ("Mountain View", date(2026, 6, 10), None,              True),
    ]


def test_scd_type2_customers_does_not_version_unchanged_customers(
        spark, silver, bronze, dimensions):
    run_silver(spark, silver)

    paul = spark.read.table("silver_dim_customers_type2").filter("customer_id = 3").collect()

    assert len(paul) == 1
    assert paul[0].is_current


def test_scd_type2_products_keeps_price_history(spark, silver, bronze, dimensions):
    run_silver(spark, silver)

    iphone = sorted(
        spark.read.table("silver_dim_products_type2")
             .filter("product_id = 'P1'").collect(),
        key=lambda r: r.valid_from,
    )

    assert [(r.price, r.valid_from, r.valid_to, r.is_current) for r in iphone] == [
        (999.0, date(2026, 1, 1), date(2026, 6, 1), False),
        (899.0, date(2026, 6, 1), None,             True),
    ]


def test_scd_type2_has_exactly_one_current_version_per_key(
        spark, silver, bronze, dimensions):
    run_silver(spark, silver)

    for table, key in [("silver_dim_customers_type2", "customer_id"),
                       ("silver_dim_products_type2", "product_id")]:
        bad = spark.sql(f"""
            SELECT {key} FROM {table}
            GROUP BY {key}
            HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
        """).count()
        assert bad == 0, table


@pytest.mark.xfail(
    strict=True,
    reason="Known bug: every run replays all Bronze batches, so a second run "
           "re-applies January over June and corrupts the SCD Type 2 history. "
           "Fix with a batch control table that skips processed batches.",
)
def test_running_silver_twice_does_not_change_scd2_history(
        spark, silver, bronze, dimensions):
    def snapshot():
        rows = spark.read.table("silver_dim_customers_type2").collect()
        return sorted(tuple("" if v is None else str(v) for v in r) for r in rows)

    run_silver(spark, silver)
    first = snapshot()

    run_silver(spark, silver)
    second = snapshot()

    assert second == first
