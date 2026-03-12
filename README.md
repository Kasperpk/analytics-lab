# Analytics Lab

A portable, one-command analytics platform built with **dbt + DuckDB**.
Clone it, run one command, and you have a fully-tested data warehouse with European macroeconomic indicators ready to query.

## Quick Start (Eurostat Macro Analytics)

```bash
git clone <repo-url> && cd analytics-lab
uv sync                        # install Python + dbt dependencies
uv run pipeline eurostat       # extract → transform → test → done
```

That single command:
1. Pulls **6 datasets from the Eurostat API** (GDP, unemployment, inflation, government finances, consumer confidence, interest rates)
2. Lands them as Parquet files in `data/raw/eurostat/`
3. Builds **18 dbt models** (6 bronze → 6 silver → 6 gold) into a local DuckDB warehouse
4. Runs **29 data quality tests** (not-null, unique, referential integrity)

Results land in `data/warehouse.duckdb` — query with any DuckDB client:

```sql
-- GDP growth by country (last 5 years)
select country_code, year, gdp_current_meur, real_gdp_growth_pct
from gold.gold_fct_gdp
where cast(year as integer) >= 2019
order by country_code, year;

-- Monthly macro dashboard for Germany
select period, headline_inflation_pct, unemployment_rate_total,
       consumer_confidence, interest_rate_3m
from gold.gold_fct_macro_overview
where country_code = 'DE'
order by period desc
limit 24;

-- Countries with highest youth unemployment (latest month)
select country_code, month, youth_unemployment_rate
from gold.gold_fct_unemployment
where month = (select max(month) from gold.gold_fct_unemployment)
order by youth_unemployment_rate desc
limit 10;

-- Government debt-to-GDP ratio across the EU
select country_code, year, gross_debt_pct_gdp, deficit_surplus_pct_gdp
from gold.gold_fct_gov_finance
where cast(year as integer) = 2023
order by gross_debt_pct_gdp desc;
```

### What's in the Gold Layer

| Model | Grain | Description |
|---|---|---|
| `gold_dim_countries` | 1 row / country | Country names, EU & Euro area membership flags |
| `gold_fct_gdp` | country × year | Headline GDP, real growth, demand components |
| `gold_fct_unemployment` | country × month | Total, youth, male/female rates + gender gap |
| `gold_fct_inflation` | country × month | Headline, food, housing, transport, energy |
| `gold_fct_gov_finance` | country × year | Deficit/surplus and debt (% GDP & M EUR) |
| `gold_fct_macro_overview` | country × month | Wide dashboard joining all monthly indicators |

### Eurostat Pipeline Architecture

```
Eurostat JSON API
       │
       ▼
 ┌─────────────┐    ┌────────┐    ┌────────┐    ┌────────┐
 │  Extraction  │───▶│ Bronze │───▶│ Silver │───▶│  Gold  │
 │  (Python)    │    │ (raw)  │    │(typed) │    │ (mart) │
 └─────────────┘    └────────┘    └────────┘    └────────┘
   6 datasets        6 tables     6 tables      6 tables
   Parquet files     DuckDB       DuckDB        DuckDB
```

### Other Pipelines

```bash
uv run pipeline ecommerce   # build e-commerce star schema (requires data in data/raw/dt=*)
uv run pipeline all          # run every pipeline
```

On Mac/Linux you can also use Make:

```bash
make setup      # install deps + dbt packages
make eurostat   # run Eurostat pipeline
make help       # show all targets
```

### Scheduling Automatic Refreshes

**Mac / Linux (cron)** — weekly refresh every Monday at 06:00:

```bash
crontab -e
# add this line:
0 6 * * 1  cd /path/to/analytics-lab && bash orchestration/run_eurostat.sh
```

**Windows (Task Scheduler)**:

```
Program:   powershell.exe
Arguments: -ExecutionPolicy Bypass -File C:\path\to\analytics-lab\orchestration\run_daily_dbt.ps1
```

**GitHub Actions** — runs automatically every Monday, or trigger manually from the Actions tab.
The workflow uploads `warehouse.duckdb` as a downloadable artifact (30-day retention).

---


## Prerequisites

### Minimum (runs the Eurostat pipeline)

- **Python 3.13+**
- **[uv](https://docs.astral.sh/uv/)** — Python package manager (`curl -LsSf https://astral.sh/uv/install.sh | sh` or `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`)
- **Git** (to clone the repo)

That's it — `uv sync` handles everything else (dbt, DuckDB, Polars, etc.).

### Clone and bootstrap

```bash
git clone <repo-url>
cd analytics-lab
uv sync
uv run pipeline eurostat   # full pipeline: extract → build → test
```

### Reproduce local pipeline results

From repository root:

```bash
uv run pipeline eurostat     # Eurostat macro-analytics
uv run pipeline ecommerce   # E-commerce star schema
uv run pipeline all          # Everything
```

Or directly with dbt:

```bash
cd dbt/analytics_project
uv run dbt build --profiles-dir .
```

### Run daily orchestration

**Mac / Linux:**

```bash
bash orchestration/run_eurostat.sh
```

**Windows:**

```powershell
powershell -ExecutionPolicy Bypass -File .\orchestration\run_daily_dbt.ps1
```

Output status file: `logs/eurostat_status.json` or `logs/daily_dbt_status.json` (PASS/FAIL + timestamp).
