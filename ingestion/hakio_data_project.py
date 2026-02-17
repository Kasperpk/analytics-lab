#!/usr/bin/env python3
import argparse
import csv
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass
class Config:
    output_dir: Path
    days: int
    start_date: date
    seed: int
    min_orders_per_day: int
    max_orders_per_day: int


def money(value: float) -> str:
    return str(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def random_ts(day: date, min_days_ago: int = 0, max_days_ago: int = 0) -> str:
    # Late-arriving records: event timestamp can be older than delivery day
    lag = random.randint(min_days_ago, max_days_ago) if max_days_ago > 0 else 0
    event_day = day - timedelta(days=lag)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(event_day.year, event_day.month, event_day.day, hour, minute, second).isoformat()


def daterange(start: date, days: int) -> List[date]:
    return [start + timedelta(days=i) for i in range(days)]


def choose_schema(base_cols: List[str], optional_cols: List[str], drift_cols: List[str]) -> List[str]:
    mode = random.choices(
        population=["base", "missing_optional", "extra_columns"],
        weights=[0.5, 0.3, 0.2],
        k=1,
    )[0]

    if mode == "base":
        return base_cols + optional_cols
    if mode == "missing_optional":
        keep = [c for c in optional_cols if random.random() > 0.5]
        return base_cols + keep
    return base_cols + optional_cols + random.sample(drift_cols, k=random.randint(1, len(drift_cols)))


def write_csv(
    file_path: Path,
    rows: List[Dict[str, str]],
    base_cols: List[str],
    optional_cols: List[str],
    drift_cols: List[str],
    bad_raw_lines: List[str],
) -> None:
    cols = choose_schema(base_cols, optional_cols, drift_cols)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()

        for row in rows:
            out = {}
            for c in cols:
                if c in row:
                    out[c] = row[c]
                elif c.startswith("new_"):
                    out[c] = random.choice(["A", "B", "C", "", "legacy"])
                else:
                    out[c] = ""
            writer.writerow(out)

        # Intentionally malformed lines (bad records)
        for bad in bad_raw_lines:
            f.write(bad + "\n")


def build_products_master(n: int = 120) -> List[Dict[str, str]]:
    product_types = ["fashion", "noos", "home", "beauty"]
    categories = ["tops", "bottoms", "accessories", "shoes", "skincare", "kitchen"]
    brands = ["Northwind", "Contoso", "Fabrikam", "AdventureWorks", "Tailspin"]
    currencies = ["USD", "EUR", "GBP"]

    products = []
    for i in range(1, n + 1):
        sku = f"SKU-{i:05d}"
        base_cost = round(random.uniform(3.5, 120.0), 2)
        row = {
            "product_id": f"P{i:05d}",
            "sku": sku,
            "product_name": f"Product {i}",
            "product_type": random.choice(product_types),
            "category": random.choice(categories),
            "brand": random.choice(brands),
            "cost_price": money(base_cost),
            "list_price": money(base_cost * random.uniform(1.2, 2.8)),
            "is_active": random.choice(["true", "true", "true", "false"]),
            "currency": random.choice(currencies),
            "created_at": datetime(2024, random.randint(1, 12), random.randint(1, 28), 12, 0, 0).isoformat(),
        }
        products.append(row)
    return products


def generate_daily_data(cfg: Config) -> None:
    random.seed(cfg.seed)

    products_master = build_products_master()
    order_counter = 1
    line_counter = 1
    refund_counter = 1

    countries = ["US", "DE", "SE", "NL", "FR", "GB"]
    currencies = {"US": "USD", "DE": "EUR", "SE": "SEK", "NL": "EUR", "FR": "EUR", "GB": "GBP"}
    payment_methods = ["card", "paypal", "klarna", "gift_card"]
    order_statuses = ["placed", "paid", "shipped", "delivered", "cancelled"]
    refund_reasons = ["damaged", "late_delivery", "wrong_size", "customer_remorse", "other"]

    summary: List[Tuple[str, int, int, int, int]] = []

    for day in daterange(cfg.start_date, cfg.days):
        day_folder = cfg.output_dir / f"dt={day.isoformat()}"

        # ----- products.csv -----
        todays_products = []
        for p in random.sample(products_master, k=random.randint(80, len(products_master))):
            row = p.copy()
            if random.random() < 0.08:
                # occasional cost updates
                row["cost_price"] = money(float(row["cost_price"]) * random.uniform(0.9, 1.1))
            todays_products.append(row)

        # duplicates
        if todays_products:
            todays_products.extend(random.choices(todays_products, k=max(1, len(todays_products) // 20)))

        # bad record objects (still parseable CSV), plus malformed line appended by raw text
        todays_products.append(
            {
                "product_id": "",
                "sku": "???",
                "product_name": "BROKEN_PRODUCT",
                "product_type": "unknown",
                "category": "",
                "brand": "???",
                "cost_price": "not_a_number",
                "list_price": "-19.99",
                "is_active": "maybe",
                "currency": "USDX",
                "created_at": "2026-99-99T25:61:61",
            }
        )

        write_csv(
            day_folder / "products.csv",
            todays_products,
            base_cols=["product_id", "sku", "product_name", "product_type", "cost_price"],
            optional_cols=["category", "brand", "list_price", "is_active", "currency", "created_at"],
            drift_cols=["new_material_group", "new_supplier_id"],
            bad_raw_lines=["this,is,a,malformed,row,with,too,many,columns,boom"],
        )

        # ----- orders.csv -----
        orders = []
        n_orders = random.randint(cfg.min_orders_per_day, cfg.max_orders_per_day)

        for _ in range(n_orders):
            country = random.choice(countries)
            order_id = f"O{day.strftime('%Y%m%d')}-{order_counter:06d}"
            order_counter += 1

            order = {
                "order_id": order_id,
                "customer_id": f"C{random.randint(1, 40000):06d}",
                "order_ts": random_ts(day, 0, 5),  # late events included
                "country": country,
                "currency": currencies[country],
                "payment_method": random.choice(payment_methods),
                "shipping_amount": money(random.choice([0, 2.99, 4.99, 7.99])),
                "discount_amount": money(random.choice([0, 0, 5, 10, 15])),
                "order_status": random.choice(order_statuses),
                "source_file_date": day.isoformat(),
            }
            orders.append(order)

        # duplicates
        if orders:
            orders.extend(random.choices(orders, k=max(1, len(orders) // 25)))

        # bad records
        orders.append(
            {
                "order_id": "O-BAD-001",
                "customer_id": "",
                "order_ts": "not-a-timestamp",
                "country": "XX",
                "currency": "???",
                "payment_method": "cash_in_pigeon",
                "shipping_amount": "free",
                "discount_amount": "-999999",
                "order_status": "teleported",
                "source_file_date": "2026/13/40",
            }
        )

        write_csv(
            day_folder / "orders.csv",
            orders,
            base_cols=["order_id", "customer_id", "order_ts", "country", "currency"],
            optional_cols=["payment_method", "shipping_amount", "discount_amount", "order_status", "source_file_date"],
            drift_cols=["new_marketing_channel", "new_device_type"],
            bad_raw_lines=[",,,,,", "broken|delimiter|line|not|csv"],
        )

        # ----- order_lines.csv -----
        lines = []
        for o in orders:
            if not o.get("order_id", "").startswith("O20"):
                continue  # skip obvious bad order row for normal generation
            num_lines = random.randint(1, 4)
            for _ in range(num_lines):
                product = random.choice(products_master)
                qty = random.choice([1, 1, 1, 2, 3])
                unit_price = float(product["list_price"])
                line = {
                    "order_line_id": f"L{day.strftime('%Y%m%d')}-{line_counter:07d}",
                    "order_id": o["order_id"],
                    "product_id": product["product_id"],
                    "sku": product["sku"],
                    "quantity": str(qty),
                    "unit_price": money(unit_price),
                    "line_discount_amount": money(random.choice([0, 0, 1.5, 3.0, 5.0])),
                    "tax_rate": random.choice(["0.00", "0.07", "0.10", "0.20", "0.25"]),
                    "line_ts": random_ts(day, 0, 7),  # can be older than file date
                }
                line_counter += 1
                lines.append(line)

        # duplicates
        if lines:
            lines.extend(random.choices(lines, k=max(1, len(lines) // 30)))

        # bad lines: orphan keys, negative qty, invalid price
        lines.append(
            {
                "order_line_id": "L-BAD-ORPHAN",
                "order_id": "O19000101-000001",
                "product_id": "P99999",
                "sku": "SKU-DOES-NOT-EXIST",
                "quantity": "-3",
                "unit_price": "NaN",
                "line_discount_amount": "9999999",
                "tax_rate": "2.5",
                "line_ts": "yesterday",
            }
        )

        write_csv(
            day_folder / "order_lines.csv",
            lines,
            base_cols=["order_line_id", "order_id", "product_id", "quantity", "unit_price"],
            optional_cols=["sku", "line_discount_amount", "tax_rate", "line_ts"],
            drift_cols=["new_warehouse_id", "new_fulfillment_mode"],
            bad_raw_lines=["too,few,cols", "L-RAW-BAD,not,proper,\"unterminated quote"],
        )

        # ----- refunds.csv -----
        refunds = []
        valid_lines = [l for l in lines if l.get("order_line_id", "").startswith("L20")]
        for ln in valid_lines:
            if random.random() < 0.10:
                qty = max(1, int(ln["quantity"])) if ln["quantity"].lstrip("-").isdigit() else 1
                unit_price = float(ln["unit_price"]) if ln["unit_price"].replace(".", "", 1).isdigit() else 0.0
                max_refund = qty * unit_price
                refund_amount = round(random.uniform(1.0, max(1.0, max_refund)), 2)

                refunds.append(
                    {
                        "refund_id": f"R{day.strftime('%Y%m%d')}-{refund_counter:07d}",
                        "order_line_id": ln["order_line_id"],
                        "order_id": ln["order_id"],
                        "refund_ts": random_ts(day, 0, 10),
                        "refund_amount": money(refund_amount),
                        "refund_reason": random.choice(refund_reasons),
                        "refund_status": random.choice(["approved", "pending", "rejected"]),
                    }
                )
                refund_counter += 1

        # duplicates
        if refunds:
            refunds.extend(random.choices(refunds, k=max(1, len(refunds) // 20)))

        # bad refund records
        refunds.append(
            {
                "refund_id": "R-BAD-001",
                "order_line_id": "L-NOPE",
                "order_id": "O-NOPE",
                "refund_ts": "32/13/2026",
                "refund_amount": "-12.34",
                "refund_reason": "",
                "refund_status": "unknown_state",
            }
        )

        write_csv(
            day_folder / "refunds.csv",
            refunds,
            base_cols=["refund_id", "order_line_id", "order_id", "refund_ts", "refund_amount"],
            optional_cols=["refund_reason", "refund_status"],
            drift_cols=["new_refund_processor", "new_case_id"],
            bad_raw_lines=["R-BAD-RAW,this,is,not,valid,csv,for,normal,parsers"],
        )

        summary.append((day.isoformat(), len(todays_products), len(orders), len(lines), len(refunds)))

    # Summary output
    print(f"\nGenerated raw landing data in: {cfg.output_dir.resolve()}")
    print("Daily counts (including duplicates + injected bad records):")
    print("date         products  orders  order_lines  refunds")
    for d, p, o, l, r in summary:
        print(f"{d}  {p:8d}  {o:6d}  {l:11d}  {r:7d}")


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Generate messy raw e-commerce data extracts.")
    parser.add_argument("--output-dir", default="data/raw", help="Output landing root folder")
    parser.add_argument("--days", type=int, default=7, help="How many delivery days to generate")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD; defaults to (today - days + 1)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--min-orders-per-day", type=int, default=120)
    parser.add_argument("--max-orders-per-day", type=int, default=260)

    args = parser.parse_args()

    if args.start_date:
        start = date.fromisoformat(args.start_date)
    else:
        start = date.today() - timedelta(days=args.days - 1)

    if args.min_orders_per_day > args.max_orders_per_day:
        raise ValueError("--min-orders-per-day cannot be greater than --max-orders-per-day")

    return Config(
        output_dir=Path(args.output_dir),
        days=args.days,
        start_date=start,
        seed=args.seed,
        min_orders_per_day=args.min_orders_per_day,
        max_orders_per_day=args.max_orders_per_day,
    )


if __name__ == "__main__":
    config = parse_args()
    generate_daily_data(config)