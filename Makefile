# analytics-lab — convenience targets
# Windows users: use `uv run pipeline` directly (Make requires WSL/Git Bash).

.DEFAULT_GOAL := help

# ── Setup ──────────────────────────────────────────────────────────────────────

.PHONY: setup
setup: ## Install Python deps + dbt packages
	uv sync
	cd dbt/analytics_project && uv run dbt deps --profiles-dir .

# ── Pipelines ──────────────────────────────────────────────────────────────────

.PHONY: eurostat
eurostat: ## Run the Eurostat macroeconomic pipeline (extract → build → test)
	uv run pipeline

# ── Utilities ──────────────────────────────────────────────────────────────────

.PHONY: query
query: ## Open DuckDB CLI connected to the warehouse
	uv run python -c "import duckdb; db=duckdb.connect('data/warehouse.duckdb'); \
		print('Connected to warehouse.duckdb — gold schema ready.'); \
		print('Try:  SELECT * FROM gold.gold_fct_macro_overview LIMIT 10;'); \
		db.close()"
	uv run duckdb data/warehouse.duckdb

.PHONY: clean
clean: ## Remove generated data (parquet + DuckDB warehouse)
	rm -rf data/raw/eurostat/
	rm -f data/warehouse.duckdb data/warehouse.duckdb.wal

.PHONY: test
test: ## Run Python unit tests + dbt tests
	uv run pytest
	cd dbt/analytics_project && uv run dbt test --profiles-dir .

# ── Help ───────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
