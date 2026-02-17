# Analytics Lab – E-commerce Data Platform (Assignment)

## 1) Goal
Build a small, production-minded data platform that:
- ingests messy raw business files,
- transforms them into analytics-friendly models,
- exposes decision-useful outputs for finance, operations, and growth.

This project intentionally keeps raw data noisy (duplicates, late rows, schema drift, malformed records) and applies cleaning logic in the pipeline.

## 2) Scope and assumptions
- Raw data lands daily as CSV files in `data/raw/dt=YYYY-MM-DD/`.
- Source files include:
	- `products.csv`
	- `orders.csv`
	- `order_lines.csv`
	- `refunds.csv`
- Data quality issues are expected and handled in staging.

## 3) Architecture
- **Data generation (raw simulation):** Python script creates intentionally messy files.
- **Ingestion/raw layer (dbt):** `raw_*` models read all landing files with minimal transformation and preserve source fidelity.
- **Staging/clean layer (dbt):** `stg_*` models cast types, deduplicate by business keys, and filter clearly invalid records.
- **Warehouse:** DuckDB file at `data/warehouse.duckdb`.

### Main model layers
- Raw ingestion views:
	- `raw_products`
	- `raw_orders`
	- `raw_order_lines`
	- `raw_refunds`
- Cleaned staging tables:
	- `stg_products`
	- `stg_orders`
	- `stg_order_lines`
	- `stg_refunds`

## 4) Modeling choices
- **Deduplication:** latest record wins using `row_number()` over business keys and file/event timestamps.
- **Type normalization:** `try_cast(...)` keeps bad values from crashing the pipeline.
- **Invalid record handling:** malformed rows are tolerated in raw ingestion; impossible business keys are filtered in staging.
- **Target schema principle:** staging and marts expose only business-relevant columns, not every raw field.

## 5) Required business outputs
Two output views should be exposed from marts:
- **Products view** (product attributes for reporting)
- **Sales view** (line-level sales + refund-adjusted metrics)

The sales view is designed to answer:
1. **Daily net quantity by date** (gross quantity - refunded quantity)
2. **Top 10 fashion SKUs by gross quantity in last 30 days**

Example business queries:

```sql
-- Daily net quantity by date
select order_date, sum(net_quantity) as daily_net_qty
from sales_view
group by 1
order by 1;
```

```sql
-- Top 10 fashion SKUs by gross quantity (last 30 days)
select sku, sum(gross_quantity) as gross_qty_30d
from sales_view
where product_type = 'fashion'
	and order_date >= current_date - interval 30 day
group by 1
order by 2 desc
limit 10;
```

## 6) Data quality and testing
Implemented data tests include:
- `not_null` checks on critical primary keys.
- `unique` checks on primary keys.
- relationship checks between linked entities:
	- order lines -> orders
	- order lines -> products
	- refunds -> order lines

Additionally, a data quality report view can include checks such as:
- row count anomalies,
- duplicate counts,
- orphan foreign keys.

## 7) Incremental strategy
Current implementation can be rebuilt end-to-end quickly in DuckDB for this assignment.

Production incremental strategy:
- Keep raw ingestion as append-only by file date (`dt=...`).
- Use incremental staging models keyed by natural/business keys (`order_id`, `order_line_id`, etc.).
- Reprocess a rolling backfill window (for late-arriving records) such as last 7 days.
- Use `merge`-style upserts (or delete+insert by partition/date) to keep latest version.

## 8) Orchestration
Single-command orchestration is supported through dbt.

Run end-to-end:

```bash
cd dbt/analytics_project
uv run dbt build --profiles-dir .
```

This command runs models + tests in dependency order.

## 9) How to run from scratch
1. Generate raw files with the Python generator into `data/raw`.
2. Build models and tests with dbt:

```bash
cd dbt/analytics_project
uv run dbt build --profiles-dir .
```

3. Inspect DuckDB output in `data/warehouse.duckdb`.

## 10) Tradeoffs
- Chose robustness over strict parsing in raw layer to avoid load failures from malformed lines.
- Applied stricter filtering in staging to preserve trustworthy reporting tables.
- Kept scope minimal for timebox (focused on ingestion, staging quality, and business-ready outputs).

## 11) What I would improve with more time
- Add explicit marts models for `products_view` and `sales_view` if not already materialized.
- Add freshness tests and source-level anomaly checks.
- Add snapshotting for slowly changing product attributes.
- Add CI workflow (`dbt build` on pull requests).
- Add observability: run metadata, row-count dashboards, and alerting on failed checks.
