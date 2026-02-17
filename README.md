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
- Data quality issues are expected and handled in transformations.

## 3) Architecture
- **Data generation (raw simulation):** Python script creates intentionally messy files.
- **Ingestion/modeling:** dbt project with bronze/silver/gold layers.
- **Warehouse:** DuckDB file at `data/warehouse.duckdb`.

### Layered model design
#### Bronze (raw landing, minimal transformation)
- `bronze_products`
- `bronze_orders`
- `bronze_order_lines`
- `bronze_refunds`

#### Silver (clean and conformed)
- `silver_products`
- `silver_orders`
- `silver_order_lines`
- `silver_refunds`

#### Gold (analytics/star-schema)
- Fact:
	- `gold_fct_sales`
- Dimensions:
	- `gold_dim_products`
	- `gold_dim_orders`
	- `gold_dim_customers`
	- `gold_dim_dates`

## 4) Modeling choices
- **Deduplication:** latest record wins using `row_number()` over business keys and file/event timestamps.
- **Type normalization:** `try_cast(...)` avoids pipeline failures from bad values.
- **Invalid record handling:** malformed rows tolerated in bronze ingestion, invalid keys filtered in silver.
- **Target schema principle:** gold exposes business-ready entities for BI, not all raw columns.

## 5) Required business outputs
The model supports the required outputs directly from `gold_fct_sales` (joined to `gold_dim_products` when needed):

1. **Daily net quantity by date**
2. **Top 10 fashion SKUs by gross quantity in last 30 days**

Example queries:

```sql
-- Daily net quantity by date
select
	order_date,
	sum(net_quantity) as daily_net_qty
from gold.gold_fct_sales
group by 1
order by 1;
```

```sql
-- Top 10 fashion SKUs by gross quantity (last 30 days)
select
	f.sku,
	sum(f.gross_quantity) as gross_qty_30d
from gold.gold_fct_sales f
join gold.gold_dim_products p
	on f.product_id = p.product_id
where p.product_type = 'fashion'
	and f.order_date >= current_date - interval 30 day
group by 1
order by 2 desc
limit 10;
```

## 6) Data quality and testing
Implemented data tests include:
- `not_null` checks on critical keys,
- `unique` checks on primary keys,
- `relationships` checks across key joins (silver and gold).

Additional examples included:
- Python unit test: `tests/test_finance_ingest.py` (validates `normalize_columns`).
- dbt singular data test: `tests/silver_order_lines_non_negative_quantity.sql` (fails if any `quantity < 0` in `silver_order_lines`).

Current project status: `dbt build` passes with all models and tests.

Run tests:

```bash
uv run pytest
cd dbt/analytics_project
uv run dbt test --profiles-dir .
```

## 7) Incremental strategy
Current behavior:
- Bronze is incremental append from daily files (`dt=*`) and persists previously ingested rows.
- Bronze ingestion is idempotent by source file path (`_source_file`): already ingested files are skipped on reruns.
- Silver is incremental with `merge` upserts on business keys:
	- `silver_orders` on `order_id`
	- `silver_order_lines` on `order_line_id`
	- `silver_products` on `product_id`
	- `silver_refunds` on `refund_id`

Effect:
- New rows are inserted in silver.
- Changed rows are updated in silver.
- Bronze remains append-only landing data unless a `--full-refresh` is explicitly run.

## 8) Orchestration
Single-command orchestration via dbt:

```bash
cd dbt/analytics_project
uv run dbt build --profiles-dir .
```

How tests run:
- `dbt build` runs models, then tests (schema tests + singular data tests).
- If any test fails, command exits non-zero and the run is marked failed.

Daily run + status flag (Windows):
- Script: [orchestration/run_daily_dbt.ps1](orchestration/run_daily_dbt.ps1)
- It runs `uv run dbt build --profiles-dir .`
- It writes a status file: [logs/daily_dbt_status.json](logs/daily_dbt_status.json)
	- `status = PASS` when everything succeeds
	- `status = FAIL` when a model/test fails

Schedule it daily with Windows Task Scheduler:
- Program/script: `powershell.exe`
- Arguments: `-ExecutionPolicy Bypass -File C:\Users\45255\analytics-lab\orchestration\run_daily_dbt.ps1`

## 9) Power BI integration
This project uses DuckDB (`data/warehouse.duckdb`) as the analytics warehouse, with curated star-schema tables in the `gold` schema.

### 9.1 Prerequisites
- Power BI Desktop installed.
- DuckDB ODBC driver installed on the same machine as Power BI Desktop.
- Latest dbt run completed so gold tables are populated:

```bash
cd dbt/analytics_project
uv run dbt build --profiles-dir .
```

### 9.2 Create the ODBC DSN (Windows)
1. Open **ODBC Data Sources (64-bit)**.
2. Go to **System DSN** (or **User DSN**) and click **Add**.
3. Choose **DuckDB Driver**.
4. Set:
	- Name: `analytics_lab_duckdb`
	- Database file: `C:\Users\45255\analytics-lab\data\warehouse.duckdb`
5. Save DSN.

### 9.3 Connect from Power BI Desktop
1. Open Power BI Desktop.
2. **Home > Get data > ODBC**.
3. Select DSN `analytics_lab_duckdb`.
4. In Navigator, select these `gold` tables:
	- `gold_fct_sales`
	- `gold_dim_products`
	- `gold_dim_orders`
	- `gold_dim_customers`
	- `gold_dim_dates`
5. Click **Load**.

### 9.4 Relationship model (star schema)
In Model view, confirm/create relationships:
- `gold_fct_sales.product_id` -> `gold_dim_products.product_id`
- `gold_fct_sales.order_id` -> `gold_dim_orders.order_id`
- `gold_fct_sales.customer_id` -> `gold_dim_customers.customer_id`
- `gold_fct_sales.order_date` -> `gold_dim_dates.date_day`

Recommended relationship settings:
- Cardinality: Many-to-one (`gold_fct_sales` many side).
- Cross-filter direction: Single (dimension filters fact).
- Active relationships enabled.

### 9.5 Measures to create in Power BI
Create these DAX measures in `gold_fct_sales`:

```DAX
Gross Sales = SUM(gold_fct_sales[gross_sales_amount])
Refund Amount = SUM(gold_fct_sales[refund_amount])
Net Sales = SUM(gold_fct_sales[net_sales_amount])
Gross Quantity = SUM(gold_fct_sales[gross_quantity])
Net Quantity = SUM(gold_fct_sales[net_quantity])
```

### 9.6 Recommended visuals
- Line chart: `gold_dim_dates[date_day]` vs `Net Quantity`.
- Bar chart: Top 10 `gold_dim_products[sku]` by `Gross Quantity`, filtered to fashion.
- KPI cards: `Gross Sales`, `Refund Amount`, `Net Sales`.
- Slicers: date, product type, country, customer.

<img width="1230" height="593" alt="image" src="https://github.com/user-attachments/assets/08ec67d9-c3f9-4465-a9c5-bc755e8e8ef7" />



### 9.7 Refresh behavior and daily operations
- Power BI reads whatever exists in `warehouse.duckdb` at refresh time.
- Daily process should be:
  1. Land raw files in `data/raw/dt=YYYY-MM-DD/`.
  2. Run orchestration script: `orchestration/run_daily_dbt.ps1`.
  3. Confirm [logs/daily_dbt_status.json](logs/daily_dbt_status.json) has `status = PASS`.
  4. Refresh Power BI dataset/report.

### 9.8 Troubleshooting
- If no tables appear in Navigator, verify DSN points to the correct `warehouse.duckdb` file.
- If visuals are blank, verify relationships are active and key columns match types.
- If data looks stale, rerun dbt build and refresh the report.
- If dbt failed, inspect [logs/daily_dbt_status.json](logs/daily_dbt_status.json) and `dbt/analytics_project/logs/dbt.log`.

## 10) Tradeoffs
- Prioritized robust ingestion and reliable tests over extensive business metric breadth.
- Kept transformations explicit and readable for presentation and review.
- Added extra dimensions for BI readiness even beyond minimum assignment requirements.

## 11) What I would improve with more time
- Add freshness/source tests at bronze level.
- Add semantic KPI layer (gross margin, refund rate, AOV, repeat rate).
- Add CI pipeline (`dbt build` + docs generation on pull requests).
- Add observability dashboards for row-count deltas and test failures.

## 12) Presentation script (meeting walkthrough)
Use this flow to present the solution in 5-7 minutes:

1. **Problem framing (30s)**
	- Stakeholders need trustworthy daily reporting from messy file extracts.
	- Data includes duplicates, late-arriving records, schema drift, and bad rows.

2. **Architecture decision (60s)**
	- Explain the bronze/silver/gold approach and why it fits reliability + analytics.
	- Bronze preserves fidelity, silver standardizes quality, gold is BI-ready.

3. **Data quality strategy (60s)**
	- Show key tests: `not_null`, `unique`, and relationship checks.
	- Mention that the pipeline tolerates messy raw input without breaking downstream models.

4. **Business outputs (90s)**
	- Demonstrate daily net quantity trend.
	- Demonstrate top fashion SKUs in last 30 days.
	- Optionally show refund impact trend from `gold_fct_sales`.

5. **BI readiness (60s)**
	- Explain star-schema relationships for Power BI (`gold_fct_sales` + dimensions).
	- Mention slicers: date, product type, country, customer.

6. **Tradeoffs and next steps (60s)**
	- Tradeoff: focused on robustness and correctness over broad metric catalog.
	- Next steps: freshness monitoring, CI/CD, semantic KPI layer, and alerting.

## 13) dbt implementation notes (what is used and why)

### Why silver models use `materialized='incremental'`
Silver is the persistent curated layer and is configured for merge upserts (`incremental_strategy='merge'` + `unique_key`).

Why this is used in this project:
- Daily raw files land continuously, so silver should not be rebuilt from scratch each day.
- New business keys should be inserted.
- Existing business keys should be updated when late-arriving or corrected values appear.

Current keys used for merge upserts:
- `silver_orders`: `order_id`
- `silver_order_lines`: `order_line_id`
- `silver_products`: `product_id`
- `silver_refunds`: `refund_id`

### What `macros/generate_schema_name.sql` does and whether it is used
`generate_schema_name` is a dbt naming macro that is invoked implicitly by dbt when relations are created. It does not need to be called manually from model SQL.

This project uses it to override dbt's default schema naming behavior:
- Profile target schema is `main` (`profiles.yml`).
- Models specify custom schemas (`bronze`, `silver`, `gold`).
- Macro keeps exact schema names (`silver`) instead of default prefixed names (for example `main_silver`).

Conclusion: this macro is actively used and required to keep the intended schema layout.

### What `schema.yml` files do and whether they are used
`schema.yml` files define model metadata and data quality contracts. They are actively used by dbt tests in this project.

Used files:
- `models/staging/ecommerce/schema.yml`
- `models/marts/ecommerce/schema.yml`

What they enforce:
- Key quality (`not_null`, `unique`)
- Referential integrity (`relationships`) between tables

How they run:
- `dbt test` runs these tests directly.
- `dbt build` also runs them as part of orchestration.
- Test failures produce non-zero exit code and fail the daily job.
