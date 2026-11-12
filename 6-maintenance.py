"""
6-maintenance.py
----------------
Delta table maintenance for the Medallion pipeline.

Runs after 5-gold.py:

    - OPTIMIZE + ZORDER BY on Silver tables (join keys)
    - OPTIMIZE on Bronze tables (compaction only)
    - VACUUM on every table (7-day retention)

Kept as a separate task so a maintenance failure never forces Silver or Gold
to be recomputed, and so it can later move to its own schedule (e.g. weekly).

Next step: run 8-sql_queries.py for validation queries and join types.
"""

from pyspark.sql import SparkSession


# =========================================================
# CONFIGURATION
# =========================================================

# Unity Catalog namespace
CATALOG = "medallion"
SCHEMA  = "medallion_scd"

# Regex used to extract the batch key from the source file name recorded in
# Bronze. Matches the YYYY-MM segment of names like "customers_2026-01.json".
BATCH_KEY_PATTERN = r"(\d{4}-\d{2})"


def get_spark():
    """
    Returns the active SparkSession.

    Inside a Databricks notebook or job the session already exists, so this
    simply reuses it. Outside Databricks it creates a local one.
    """
    return SparkSession.builder.appName("Medallion-SCD-ETL").getOrCreate()


def setup_unity_catalog(spark):
    """Selects the target catalog and schema."""
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
    print(f"Using namespace: {CATALOG}.{SCHEMA}")


# =========================================================
# MAINTENANCE — Delta table optimization
# =========================================================
def optimize_tables(spark):
    """
    Compacts small files and clusters data by the most-filtered columns.

    OPTIMIZE
        Every MERGE and overwrite leaves behind small files. Left unchecked
        this becomes the small-files problem: thousands of tiny Parquet files,
        each one a separate read. OPTIMIZE rewrites them into fewer, larger
        files.

    ZORDER BY
        Physically co-locates rows sharing similar values in the given columns.
        Combined with Delta's data skipping, a query filtering on those columns
        reads far fewer files. Chosen here on the join keys, since every Gold
        aggregation joins fact to dimension on them.

        Note: a partition column cannot be used in ZORDER — the partitioning
        already provides that clustering.
    """
    print("\n[MAINTENANCE] Optimizing Delta tables...")

    targets = [
        ("silver_fact_orders",         "customer_id, product_id"),
        ("silver_dim_customers_type2", "customer_id"),
        ("silver_dim_products_type2",  "product_id"),
        ("silver_dim_customers_type1", "customer_id"),
    ]

    for table, zorder_cols in targets:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_cols})")
        print(f"  OPTIMIZE {table} ZORDER BY ({zorder_cols})")

    # Bronze tables are append-only and already partitioned by ingestion date,
    # so plain compaction is enough — no clustering key needed.
    for table in ["bronze_raw_customers", "bronze_raw_orders", "bronze_raw_products"]:
        spark.sql(f"OPTIMIZE {table}")
        print(f"  OPTIMIZE {table}")

    print("Optimization completed.")


def vacuum_tables(spark, retention_hours=168):
    """
    Removes data files no longer referenced by any recent table version.

    OPTIMIZE and MERGE do not delete the old files — they only stop referencing
    them, which is what keeps time travel working. VACUUM is what actually
    reclaims that storage.

    The retention window is the trade-off: anything older than it can no longer
    be time-travelled to. The 168-hour (7-day) default is the Databricks
    safeguard, and it also protects readers whose queries started before the
    files were superseded.
    """
    print(f"\n[MAINTENANCE] Vacuuming (retention: {retention_hours}h)...")

    tables = [
        "bronze_raw_customers", "bronze_raw_orders", "bronze_raw_products",
        "silver_stg_customers", "silver_stg_products",
        "silver_dim_customers_type1", "silver_dim_customers_type2",
        "silver_dim_products_type2", "silver_fact_orders",
        "gold_agg_sales_by_city", "gold_agg_sales_by_category",
        "gold_ranking_customers",
    ]

    for table in tables:
        spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")

    print(f"Vacuum completed on {len(tables)} tables.")


def main():
    """Compacts, clusters and vacuums the pipeline's Delta tables."""
    spark = get_spark()
    setup_unity_catalog(spark)

    optimize_tables(spark)
    vacuum_tables(spark)

    print("\nMaintenance completed.")


main()
