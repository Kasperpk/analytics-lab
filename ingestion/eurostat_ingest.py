"""
Eurostat Macroeconomic Data Extractor
=====================================
Pulls key macroeconomic indicators from the Eurostat JSON API
and writes them as Parquet files into data/raw/eurostat/ for dbt to ingest.

Datasets extracted:
  - nama_10_gdp       GDP and main components (annual, current prices & volumes)
  - une_rt_m           Unemployment rate by sex and age (monthly)
  - prc_hicp_manr      HICP – annual rate of change (monthly)
  - gov_10dd_edpt1     Government deficit/surplus, debt (annual)
  - ei_bsco_m          Consumer confidence indicator (monthly)
  - irt_st_m           Money market interest rates (monthly)

Usage:
    uv run python ingestion/eurostat_ingest.py
"""

import json
import requests
import polars as pl
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

RAW_PATH = Path("data/raw/eurostat")
RAW_PATH.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
TIMEOUT = 120  # seconds per API call

# ── Dataset definitions ────────────────────────────────────────────────────────
# Each entry: (dataset_code, human_label, filter_params as URL query params)

DATASETS = [
    {
        "code": "nama_10_gdp",
        "label": "gdp",
        "description": "GDP and main components — current prices, chain linked volumes",
        "params": {
            "unit": ["CP_MEUR", "CLV10_MEUR"],
            "na_item": ["B1GQ", "P3", "P5G", "P6", "P7", "B11"],
        },
    },
    {
        "code": "une_rt_m",
        "label": "unemployment",
        "description": "Unemployment rate by sex and age, monthly, seasonally adjusted",
        "params": {
            "s_adj": ["SA"],
            "age": ["TOTAL", "Y_LT25", "Y25-74"],
            "unit": ["PC_ACT"],
            "sex": ["T", "M", "F"],
        },
    },
    {
        "code": "prc_hicp_manr",
        "label": "hicp_inflation",
        "description": "HICP — monthly annual rate of change (inflation)",
        "params": {
            "coicop": ["CP00", "CP01", "CP04", "CP07", "NRG"],
        },
    },
    {
        "code": "gov_10dd_edpt1",
        "label": "gov_deficit_debt",
        "description": "Government deficit/surplus and debt — annual, % of GDP and million EUR",
        "params": {
            "unit": ["PC_GDP", "MIO_EUR"],
            "sector": ["S13"],
            "na_item": ["B9", "GD"],
        },
    },
    {
        "code": "ei_bsco_m",
        "label": "consumer_confidence",
        "description": "Consumer confidence indicator — monthly, seasonally adjusted",
        "params": {
            "indic": ["BS-CSMCI"],
            "s_adj": ["SA"],
        },
    },
    {
        "code": "irt_st_m",
        "label": "interest_rates",
        "description": "Money market interest rates — monthly averages",
        "params": {
            "int_rt": ["IRT_M3", "IRT_M6", "IRT_M12"],
        },
    },
]


def build_url(dataset_code: str, params: dict) -> str:
    """Build the Eurostat JSON-stat API URL with filter parameters."""
    query_parts = []
    for key, values in params.items():
        for v in values:
            query_parts.append((key, v))
    query_string = urlencode(query_parts)
    return f"{BASE_URL}/{dataset_code}?{query_string}&format=JSON&lang=EN"


def parse_json_stat(data: dict, dataset_code: str, label: str) -> pl.DataFrame:
    """Parse Eurostat JSON-stat v2 response into a long-form Polars DataFrame."""
    # Extract dimension metadata
    dim_ids = data["id"]  # ordered list of dimension names
    dim_sizes = data["size"]  # size of each dimension
    dimensions = {}
    for dim_id in dim_ids:
        dim_data = data["dimension"][dim_id]
        cat = dim_data["category"]
        index = cat["index"]
        labels = cat.get("label", {})
        # Build position -> code mapping
        if isinstance(index, dict):
            pos_to_code = {v: k for k, v in index.items()}
        else:
            pos_to_code = {i: c for i, c in enumerate(index)}
        dimensions[dim_id] = pos_to_code

    # Extract values (sparse: key is flat index as string -> value)
    values = data.get("value", {})
    if not values:
        return None

    # Calculate strides for flat-index -> multi-dimensional index conversion
    strides = []
    for i in range(len(dim_sizes)):
        stride = 1
        for j in range(i + 1, len(dim_sizes)):
            stride *= dim_sizes[j]
        strides.append(stride)

    # Build rows from the sparse value map
    rows = []
    for flat_idx_str, val in values.items():
        flat_idx = int(flat_idx_str)
        row = {}
        remainder = flat_idx
        for i, dim_id in enumerate(dim_ids):
            pos = remainder // strides[i]
            remainder = remainder % strides[i]
            row[dim_id] = dimensions[dim_id].get(pos, str(pos))
        row["obs_value"] = str(val) if val is not None else ""
        rows.append(row)

    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.lit(dataset_code).alias("dataset_code"),
        pl.lit(label).alias("dataset_label"),
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("extracted_at"),
    )
    return df


def extract_dataset(dataset: dict) -> pl.DataFrame | None:
    """Fetch a single Eurostat dataset via JSON API and return as a Polars DataFrame."""
    code = dataset["code"]
    params = dataset["params"]
    label = dataset["label"]

    url = build_url(code, params)
    print(f"  Fetching {code} ({label})...")
    print(f"  URL: {url[:120]}...")

    try:
        resp = requests.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  WARNING: Failed to fetch {code}: {exc}")
        return None

    try:
        data = resp.json()
    except json.JSONDecodeError as exc:
        print(f"  WARNING: Invalid JSON from {code}: {exc}")
        return None

    if "id" not in data:
        print(f"  WARNING: Unexpected response format for {code}")
        return None

    df = parse_json_stat(data, code, label)
    if df is None or len(df) == 0:
        print(f"  WARNING: No data rows parsed for {code}")
        return None

    return df


def rename_geo_column(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize the geo column name across datasets."""
    for col in df.columns:
        if col.lower() == "geo" and col != "geo":
            return df.rename({col: "geo"})
    return df


def main() -> None:
    print(f"Eurostat extraction started at {datetime.now(timezone.utc).isoformat()}")
    print(f"Output directory: {RAW_PATH.resolve()}\n")

    for dataset in DATASETS:
        df = extract_dataset(dataset)
        if df is None:
            continue

        df = rename_geo_column(df)

        output_file = RAW_PATH / f"{dataset['label']}.parquet"
        df.write_parquet(output_file)
        print(f"  -> Saved {len(df):,} rows to {output_file}\n")

    print("Eurostat extraction complete.")


if __name__ == "__main__":
    main()
