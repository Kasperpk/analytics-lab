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

Current project status: `dbt build` passes with all models and tests.

## 7) Incremental strategy
For this assignment, full rebuild is used (fast on DuckDB).

Production-oriented approach:
- append by file date at bronze,
- incremental merge in silver on natural keys,
- rolling backfill window for late-arriving records,
- periodic quality snapshots and anomaly alerts.

## 8) Orchestration
Single-command orchestration via dbt:

```bash
cd dbt/analytics_project
uv run dbt build --profiles-dir .
```

## 9) Power BI integration
Recommended semantic model:
- `gold_fct_sales` as fact table.
- Relationships to dimensions:
	- `gold_fct_sales.product_id` -> `gold_dim_products.product_id`
	- `gold_fct_sales.order_id` -> `gold_dim_orders.order_id`
	- `gold_fct_sales.customer_id` -> `gold_dim_customers.customer_id`
	- `gold_fct_sales.order_date` -> `gold_dim_dates.date_day`

Use dimensions for slicers (date, product type, country, customer segments) and fact metrics for visuals.

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
