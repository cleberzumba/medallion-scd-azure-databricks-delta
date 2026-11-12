# Medallion Architecture + SCD + PySpark — Azure Databricks Lakehouse

A production-grade **Lakehouse pipeline** built and deployed on **Azure Databricks** with **PySpark**, **Delta Lake** and **Unity Catalog**.

The project brings together five pillars of modern Data Engineering:

- **Medallion Architecture** — Bronze, Silver and Gold layers
- **Slowly Changing Dimensions** — Type 1 and Type 2 implemented with Delta Lake `MERGE`
- **Auto Loader** — incremental multi-format ingestion (JSON, Parquet, CSV) from ADLS Gen2
- **Unity Catalog governance** — external locations, tags, RBAC, column masking and row-level security
- **Lakeflow Jobs orchestration** — one task per layer, with dependencies, retries and a schedule

---

## Physical Data Model

![Data Model](docs/data_model.png)

## Pipeline Workflow

![Workflow](docs/workflow.png)

---

## The scenario

A retail company receives data from three source systems, each delivering a different file format — a pattern that shows up in almost every enterprise environment:

| Source | Format | Simulates |
|--------|--------|-----------|
| **Customers** | JSON | CRM REST API (Salesforce, HubSpot) |
| **Orders** | Parquet | CDC pipeline (Debezium, Fivetran) |
| **Products** | CSV | Legacy ERP export |

Two batches demonstrate the dimensions changing over time:

**January 2026** — 3 customers, 4 products, 4 orders.

**June 2026** — customers move (John → Mountain View, Mary → Cupertino), a new customer joins, the iPhone price drops from $999 to $899, the T-Shirt goes up from $29.90 to $34.90, and a new product launches.

Because the product dimension is versioned, an order placed in February still joins to the $999 price, while a June order joins to $899. That is the **point-in-time join** that SCD Type 2 exists to enable.

---

## Architecture

### Storage layout (ADLS Gen2)

```
abfss://medallion@<storage-account>.dfs.core.windows.net/
│
├── raw/                          ← source files land here
│   ├── customers/  *.json
│   ├── orders/     *.parquet
│   └── products/   *.csv
│
├── lakehouse/                    ← external Delta tables
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── _system/                      ← managed by Auto Loader and Unity Catalog
    ├── checkpoints/
    ├── schemas/
    └── catalog/
```

### Tables

Namespace: `medallion.medallion_scd.<table>`

**Bronze** — raw ingestion, no transformation
- `bronze_raw_customers` · `bronze_raw_orders` · `bronze_raw_products`

**Silver** — cleansed and modeled
- `silver_stg_customers` · `silver_stg_products` — staging
- `silver_dim_customers_type1` — SCD Type 1 (overwrite)
- `silver_dim_customers_type2` — SCD Type 2 (address history)
- `silver_dim_products_type2` — SCD Type 2 (price history)
- `silver_fact_orders` — fact table, partitioned by year/month

**Gold** — business aggregations
- `gold_agg_sales_by_city` · `gold_agg_sales_by_category` · `gold_ranking_customers`

### Authentication

No credentials appear anywhere in the code. Unity Catalog handles storage access through:

```
Azure Access Connector (Managed Identity)
        ↓  Storage Blob Data Contributor
Storage Credential  →  External Location  →  abfss://...
```

---

## Repository structure

Files are numbered in execution order.

```
medallion-scd-azure-databricks-delta/
│
├── README.md
│
├── docs/
│   ├── data_model.png / .svg       ← physical data model (ERD)
│   └── workflow.png / .svg         ← end-to-end pipeline
│
├── 1-unity_catalog_setup.sql       ← governance: catalog, grants, tags, masking, row filters
├── 2-create_tables.py              ← DDL: 12 external Delta tables
├── 3-etl_autoloader.py             ← Bronze ingestion via Auto Loader
├── 4-silver.py                     ← Silver: cleansing, SCD Type 1 and Type 2, fact table
├── 5-gold.py                       ← Gold: business aggregations
├── 6-maintenance.py                ← Delta maintenance: OPTIMIZE, ZORDER, VACUUM
├── 7-validation.sql                ← post-run integrity checks
├── 8-sql_queries.py                ← query demos: all join types, unions, time travel
│
├── workflow_job.json               ← Lakeflow Job definition (serverless, 4 tasks)
│
└── sample_data/                    ← files to upload to the raw zone
    ├── customers/  *.json
    ├── orders/     *.parquet
    └── products/   *.csv
```

---

## How the pipeline runs

```
raw/ files
    │
    ▼  3-etl_autoloader.py   — Auto Loader, trigger(availableNow)
Bronze tables
    │
    ▼  4-silver.py           — cleansing, SCD merges, fact table
Silver tables
    │
    ▼  5-gold.py             — business aggregations
Gold tables
    │
    ▼  6-maintenance.py      — OPTIMIZE + ZORDER, VACUUM
    │
    ▼  7-validation.sql      — integrity checks
BI / analytics
```

### Batch-based processing

Auto Loader ingests every available file at once, so Bronze holds several batches side by side. Processing them together would make deduplication pick an arbitrary version of each entity and leave SCD Type 2 with no history to record.

`4-silver.py` therefore replays Bronze **one batch at a time, in chronological order**, identifying each batch by its source file. The SCD merges run once per batch — which is exactly what produces the version history.

The batch list is not hardcoded. `discover_batches()` derives it from the `_source_file` values already recorded in Bronze, so a new file dropped in the raw zone is picked up with no code change.

### Delta table maintenance

Every `MERGE` and overwrite leaves small files behind, and superseded files are kept for time travel. `6-maintenance.py` keeps the tables healthy:

- **`OPTIMIZE`** — compacts small files into fewer, larger ones (the small-files problem).
- **`ZORDER BY`** — co-locates rows on the join keys (`customer_id`, `product_id`) so Delta's data skipping reads fewer files.
- **`VACUUM`** — removes files no longer referenced, with a 7-day retention window that also bounds how far back time travel can go.

---

## Setup

### 1. Azure resources

| Resource | Purpose |
|----------|---------|
| Storage Account (ADLS Gen2, HNS enabled) | data lake |
| Container | holds `raw/`, `lakehouse/`, `_system/` |
| Access Connector for Azure Databricks | managed identity for Unity Catalog |
| Azure Databricks workspace (**Premium**) | Unity Catalog requires Premium |

Grant the Access Connector the **Storage Blob Data Contributor** role on the storage account.

### 2. Unity Catalog

In the Databricks Catalog Explorer:

1. Create a **Storage Credential** of type `Azure Managed Identity`, pointing to the Access Connector resource ID.
2. Create an **External Location** with that credential, pointing to `abfss://<container>@<account>.dfs.core.windows.net/`.
3. Run **Test connection** — Read, List, Write and Delete must succeed.

### 3. Configuration

Adjust the storage path at the top of `2-create_tables.py` and `3-etl_autoloader.py`:

```python
CATALOG      = "medallion"
SCHEMA       = "medallion_scd"
STORAGE_BASE = "abfss://<container>@<account>.dfs.core.windows.net"
```

`4-silver.py`, `5-gold.py` and `6-maintenance.py` work through table names only, so they need just `CATALOG` and `SCHEMA`.

### 4. Execution order

```
1. 1-unity_catalog_setup.sql   (sections 2–6)   — catalog, schema, grants
2. 2-create_tables.py                           — 12 external tables
3. Upload sample_data/ to raw/
4. 3-etl_autoloader.py                          — Bronze
5. 4-silver.py                                  — Silver
6. 5-gold.py                                    — Gold
7. 6-maintenance.py                             — OPTIMIZE, ZORDER, VACUUM
8. 8-sql_queries.py                             — query demos (optional)
9. 7-validation.sql                             — verify results
10. 1-unity_catalog_setup.sql  (sections 7–8)   — masking + row filters
```

Sections 7 and 8 come last on purpose: the row filter hides non-current SCD versions from anyone outside `data_engineers`, which would obscure the history during validation.

`2-create_tables.py` and `3-etl_autoloader.py` only *select* the catalog and schema — they never create them. A `CREATE CATALOG` in those scripts would omit the `MANAGED LOCATION` and silently place the catalog in the metastore root, and `IF NOT EXISTS` would then stop the setup script from ever correcting it. Step 1 is therefore mandatory.

### 5. Scheduling with Lakeflow Jobs

Import `workflow_job.json` into **Jobs & Pipelines** to run the pipeline on a schedule with **Lakeflow Jobs** (formerly Databricks Workflows). The job runs on serverless compute, pulls the scripts straight from this repository (`git_source`), and has one task per layer:

```
bronze_ingestion ──► silver_transformations ──► gold_aggregations ──► table_maintenance
```

| Task | Script | Retries |
|------|--------|---------|
| `bronze_ingestion` | `3-etl_autoloader.py` | 2 (checkpointed, safe to re-run) |
| `silver_transformations` | `4-silver.py` | 0 (SCD merges must not be replayed blindly) |
| `gold_aggregations` | `5-gold.py` | 2 (overwrite, idempotent) |
| `table_maintenance` | `6-maintenance.py` | 1 |

The schedule (daily at 03:00, `America/Sao_Paulo`) ships paused; unpause it in the job settings.

---

## Governance

`1-unity_catalog_setup.sql` applies:

**Documentation** — a `COMMENT` on every table, visible in Catalog Explorer.

**Tags** — `layer`, `domain`, `scd_type`, `contains_pii` at table level; `pii` and `classification` at column level. Enables queries like "every table containing PII".

**RBAC** — three personas with different reach:

| Group | Access |
|-------|--------|
| `data_engineers` | full access to all layers |
| `data_analysts` | read-only on Gold |
| `data_scientists` | read-only on Silver and Gold |

Bronze is never exposed outside the pipeline.

**Column masking** — a function checks group membership and returns either the real value or `XXXX`. Same table, same query, different result per identity.

**Row-level security** — a row filter restricts non-engineers to `is_current = true`, hiding the historical audit trail while keeping it intact for the pipeline.

---

## PySpark techniques demonstrated

**Columns** — `select`, `selectExpr`, `col`, `withColumn`, `cast`, `alias`, `lit`

**Rows** — `filter` / `where`, `dropDuplicates`, `dropna`, `fillna`, `orderBy`, `limit`

**Joins** — inner, left, right, outer, cross, left_anti, left_semi, `union`

**Dates** — `to_date`, `year`, `month`, `current_date`, `date_add`

**Aggregation** — `groupBy`, `agg`, `count`, `countDistinct`, `sum`

**Windows** — `Window.orderBy`, `row_number`

**Delta Lake** — `MERGE` via both the `DeltaTable` API and SQL, schema evolution, partitioning, time travel (`VERSION AS OF`), `OPTIMIZE`, `ZORDER BY`, `VACUUM`

**Streaming** — Auto Loader `cloudFiles`, schema inference and evolution, checkpointing, `trigger(availableNow=True)`

---

## Design decisions

**External tables over managed tables.** Explicit control of physical location, portability across workspaces, and clearer auditability.

**Bronze keeps everything as string.** Casting during ingestion means a dirty record either breaks the job or gets silently dropped. Storing raw allows reprocessing without going back to the source.

**Staging is overwritten, dimensions are merged.** Staging only feeds the current batch; dimensions accumulate and must preserve what already exists.

**`MERGE` instead of separate `INSERT` and `UPDATE`.** A single ACID transaction — no window where a dimension could end up with two current versions or none.

**Fact partitioned by year/month, not by day.** Daily partitions on this volume would create thousands of tiny files, the classic small-files problem.

**`trigger(availableNow=True)` instead of continuous streaming.** Data arrives once per day; keeping compute running around the clock buys nothing.

**Batches are discovered, not declared.** The pipeline reads the batch keys from Bronze rather than from a list in the source. A hardcoded date list would silently skip any file the developer forgot to add — the kind of failure that produces no error and no data.

**One job task per layer.** Bronze, Silver, Gold and maintenance run as separate tasks. A failure in an aggregation can be repaired on its own without replaying the SCD merges, the job UI shows where time is spent, and retries are tuned per task.

**Explicit maintenance instead of predictive optimization.** Predictive optimization automates `OPTIMIZE` and `VACUUM`, but only for Unity Catalog managed tables. Since this project uses external tables, maintenance runs as its own task. With managed tables, that task could be dropped and liquid clustering could replace `ZORDER`.

---

## References

- [Databricks — Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Databricks — Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Databricks — Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
- [Delta Lake — MERGE](https://docs.delta.io/latest/delta-update.html)
- [Unity Catalog on Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/)
- [Kimball Group — Slowly Changing Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
