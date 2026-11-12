"""
tests/data_quality/checks.py
----------------------------
Automated data quality checks for the Medallion pipeline.

Runs as the `data_quality_checks` task of the Lakeflow Job, right after Gold.
Each check is a SQL query that returns the number of violating rows; any
non-zero result fails the task, which fails the job run and triggers the
on-failure e-mail notification.

What is checked
---------------
SCD Type 2 (customers and products)
    - exactly one current version per business key
    - is_current is true exactly when valid_to is null
    - no version closes before it opens (valid_to < valid_from)
    - versions of the same key do not overlap in time

SCD Type 1 / fact
    - one row per customer in the Type 1 dimension
    - one row per order, every amount positive
    - every order points to a known customer and product

Gold
    - totals reconcile with the Silver fact table
    - the ranking is 1..N with no gaps or ties
    - no Silver or Gold table is empty

The same checks are exercised locally by tests/unit/test_data_quality.py.
"""

from pyspark.sql import SparkSession


# =========================================================
# CONFIGURATION
# =========================================================

# Unity Catalog namespace
CATALOG = "medallion"
SCHEMA  = "medallion_scd"

# Tolerance for comparing monetary totals (floating-point sums)
AMOUNT_TOLERANCE = 0.01


def get_spark():
    """Returns the active SparkSession (reused inside Databricks)."""
    return SparkSession.builder.appName("Medallion-SCD-DataQuality").getOrCreate()


def setup_unity_catalog(spark):
    """Selects the target catalog and schema."""
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
    print(f"Using namespace: {CATALOG}.{SCHEMA}")


# =========================================================
# CHECK DEFINITIONS
# ---------------------------------------------------------
# Each entry: (check name, SQL returning one row with the
# number of violations in a column named `violations`).
# =========================================================
def _scd2_checks(table, key):
    """The four SCD Type 2 invariants for one dimension."""
    return [
        (f"{table}: exactly one current version per {key}", f"""
            SELECT COUNT(*) AS violations FROM (
                SELECT {key}
                FROM {table}
                GROUP BY {key}
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
            )"""),

        (f"{table}: is_current matches valid_to IS NULL", f"""
            SELECT COUNT(*) AS violations
            FROM {table}
            WHERE (is_current AND valid_to IS NOT NULL)
               OR (NOT is_current AND valid_to IS NULL)"""),

        (f"{table}: no version closes before it opens", f"""
            SELECT COUNT(*) AS violations
            FROM {table}
            WHERE valid_to IS NOT NULL AND valid_to < valid_from"""),

        (f"{table}: versions of the same {key} do not overlap", f"""
            SELECT COUNT(*) AS violations FROM (
                SELECT valid_to,
                       LEAD(valid_from) OVER (
                           PARTITION BY {key} ORDER BY valid_from
                       ) AS next_valid_from
                FROM {table}
            )
            WHERE next_valid_from IS NOT NULL
              AND (valid_to IS NULL OR valid_to > next_valid_from)"""),
    ]


def _not_empty(table):
    return (f"{table}: is not empty",
            f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS violations FROM {table}")


CHECKS = [
    # ---- SCD Type 2 ------------------------------------------------------
    *_scd2_checks("silver_dim_customers_type2", "customer_id"),
    *_scd2_checks("silver_dim_products_type2",  "product_id"),

    # ---- SCD Type 1 ------------------------------------------------------
    ("silver_dim_customers_type1: one row per customer_id", """
        SELECT COUNT(*) - COUNT(DISTINCT customer_id) AS violations
        FROM silver_dim_customers_type1"""),

    # ---- Fact ------------------------------------------------------------
    ("silver_fact_orders: one row per order_id", """
        SELECT COUNT(*) - COUNT(DISTINCT order_id) AS violations
        FROM silver_fact_orders"""),

    ("silver_fact_orders: every amount is positive", """
        SELECT COUNT(*) AS violations
        FROM silver_fact_orders
        WHERE amount IS NULL OR amount <= 0"""),

    ("silver_fact_orders: every customer_id exists in the customer dimension", """
        SELECT COUNT(*) AS violations
        FROM silver_fact_orders f
        LEFT ANTI JOIN silver_dim_customers_type2 d
            ON f.customer_id = d.customer_id"""),

    ("silver_fact_orders: every product_id exists in the product dimension", """
        SELECT COUNT(*) AS violations
        FROM silver_fact_orders f
        LEFT ANTI JOIN silver_dim_products_type2 d
            ON f.product_id = d.product_id"""),

    # ---- Gold reconciliation ---------------------------------------------
    ("gold_agg_sales_by_city: total matches the fact table", f"""
        SELECT CASE WHEN ABS(g.total - f.total) > {AMOUNT_TOLERANCE}
                    THEN 1 ELSE 0 END AS violations
        FROM (SELECT COALESCE(SUM(total_amount), 0) AS total
              FROM gold_agg_sales_by_city) g
        CROSS JOIN (
              SELECT COALESCE(SUM(f.amount), 0) AS total
              FROM silver_fact_orders f
              JOIN silver_dim_customers_type2 d
                ON f.customer_id = d.customer_id AND d.is_current) f"""),

    ("gold_agg_sales_by_category: total matches the fact table", f"""
        SELECT CASE WHEN ABS(g.total - f.total) > {AMOUNT_TOLERANCE}
                    THEN 1 ELSE 0 END AS violations
        FROM (SELECT COALESCE(SUM(total_amount), 0) AS total
              FROM gold_agg_sales_by_category) g
        CROSS JOIN (
              SELECT COALESCE(SUM(f.amount), 0) AS total
              FROM silver_fact_orders f
              JOIN silver_dim_products_type2 d
                ON f.product_id = d.product_id AND d.is_current) f"""),

    ("gold_ranking_customers: ranking is 1..N with no gaps or ties", """
        SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT ranking)
                      AND MIN(ranking) = 1
                      AND MAX(ranking) = COUNT(*)
                    THEN 0 ELSE 1 END AS violations
        FROM gold_ranking_customers"""),

    # ---- Completeness ----------------------------------------------------
    _not_empty("silver_dim_customers_type1"),
    _not_empty("silver_dim_customers_type2"),
    _not_empty("silver_dim_products_type2"),
    _not_empty("silver_fact_orders"),
    _not_empty("gold_agg_sales_by_city"),
    _not_empty("gold_agg_sales_by_category"),
    _not_empty("gold_ranking_customers"),
]


# =========================================================
# RUNNER
# =========================================================
def run_checks(spark, checks=CHECKS):
    """
    Runs every check and returns a list of (name, violations) for the ones
    that failed. All checks run even after a failure, so one run reports
    every problem at once.
    """
    failures = []
    for name, sql in checks:
        violations = spark.sql(sql).collect()[0]["violations"]
        status = "PASS" if violations == 0 else f"FAIL ({violations})"
        print(f"  [{status}] {name}")
        if violations != 0:
            failures.append((name, violations))
    return failures


def main():
    spark = get_spark()
    setup_unity_catalog(spark)

    print(f"\n[DATA QUALITY] Running {len(CHECKS)} checks...")
    failures = run_checks(spark)

    if failures:
        summary = "\n".join(f"  - {name}: {v} violation(s)" for name, v in failures)
        raise AssertionError(
            f"{len(failures)} data quality check(s) failed:\n{summary}"
        )

    print(f"\nAll {len(CHECKS)} data quality checks passed.")


if __name__ == "__main__":
    main()
