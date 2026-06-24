# Northwind Azure data engineering pipeline

An end-to-end pipeline built on Azure that takes the Northwind Traders dataset, simulates thousands of incremental transactions, and turns them into a star schema that supports real sales and customer analytics — built around the same fundamentals (incremental loads, layered transformation, change tracking) used in production data platforms.

See `architecture.png` for the full diagram and `LESSONS_LEARNED.md` for what changed (and why) over the course of building this.

## What this pipeline is for

Two business questions drove the modeling decisions in the Gold layer:

**1. Sales performance over time, by employee and by region.**
`fact_sales` joined with `dim_employees` and `dim_date` lets you answer "who sold what, when, and where" — and because `dim_employees` is built with SCD Type 2, an employee's historical title or region at the time of a sale is preserved even if it has since changed. A current-state-only table would silently misattribute past sales to a person's *current* role.

**2. Customer value and activity over time.**
`fact_sales` joined with `dim_customers` supports revenue-per-customer and recency analysis. `dim_customers` is also SCD Type 2, so a customer who relocated mid-history doesn't have their past orders incorrectly reattributed to their new location — past sales stay tied to where the customer was *at the time*.

Both questions only return correct answers because of a deliberate modeling choice: **history-sensitive dimensions live in Gold with SCD Type 2, not in Silver.** Silver's job is to be a clean, current-state mirror of the source; Gold's job is to know what changed and when.

## Why it's built this way

| Decision | Reasoning |
|---|---|
| Incremental load via watermark, not full reload | Mirrors how real source systems are extracted from without re-pulling unchanged history every run |
| One Delta table per source table in Silver | Keeps lineage traceable — every Silver column has an obvious origin, no business logic mixed in yet |
| SCD Type 2 only in Gold | Silver does straightforward dedup; Gold's Delta MERGE is the only place that decides "this is a change worth tracking" |
| Surrogate keys via `monotonically_increasing_id()` | Decouples Gold from source-system keys, and is what makes SCD Type 2 possible — a natural key alone can't represent two historical versions of the same entity |
| Bronze partitioned by load timestamp, read recursively in Silver | Each ingestion run lands in its own folder so a run with zero new rows can never overwrite previously landed data |
| Data volume generated synthetically (50K+ orders, 130K+ line items) | The original dataset only ships a handful of rows — a synthetic generator (seasonal date weighting, valid foreign keys against existing dimensions) was used to produce enough volume to make the incremental load and Gold layer behave like a real workload |

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Azure Data Factory — metadata-driven, config + watermark table pattern |
| Storage | Azure Data Lake Storage Gen2 |
| Transformation | Azure Databricks (PySpark) |
| Storage format | Parquet (Bronze) → Delta Lake (Silver, Gold) |
| Monitoring | Azure Monitor + Action Groups, ADF failure-path alerting |
| Auth | Service Principal (App Registration) for Databricks → ADLS |

## Architecture

```
MySQL (source)
    │  Azure Data Factory — incremental load via config + watermark table
    ▼
Bronze (ADLS, Parquet)        — raw, partitioned by load timestamp
    │  Databricks / PySpark — clean, cast, dedup
    ▼
Silver (ADLS, Delta)          — one table per source table, current-state
    │  Databricks / PySpark — Delta MERGE, surrogate keys, SCD Type 2
    ▼
Gold (ADLS, Delta)             — star schema
    │
    ▼
Power BI ready
```

Full diagram: `architecture.png`

## Data engineering fundamentals demonstrated

- **Incremental extraction** using a watermark column tracked in a config table, rather than full reloads
- **Metadata-driven pipeline** — adding a new source table is a config row, not a pipeline change
- **Medallion architecture** with a clear contract for what each layer is responsible for
- **Slowly Changing Dimensions** — both Type 1 (overwrite) and Type 2 (full history via Delta MERGE), applied deliberately rather than uniformly
- **Surrogate key generation** and first-load detection (`DeltaTable.isDeltaTable()`) to handle initial vs. incremental writes differently
- **Partition-safe Bronze writes** so an incremental run with no new data can't destroy previously ingested data
- **Pipeline observability** — Azure Monitor alerts on pipeline success/failure, plus failure-path branching inside ADF to specific Web Activities

## Repo structure

```
├── README.md
├── LESSONS_LEARNED.md
├── architecture.png
├── images/
│   └── adf_pipeline.png        # ADF pipeline canvas screenshot
└── notebooks/
    ├── silver_notebook.py      # Bronze → Silver, all 7 tables
    └── gold_notebook.py        # Silver → Gold star schema, all 7 tables
```

## Scale

The original Northwind dataset has only a few hundred rows — not enough to meaningfully exercise an incremental load or a star schema. A Python data generator (seasonal date distribution, valid foreign keys drawn from existing customers/employees/products/shippers) was used to produce **50,000+ orders and 130,000+ order line items**, giving the pipeline enough volume to behave like a real incremental workload rather than a one-shot toy load.
