import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import List, Optional

import pandas as pd

DEFAULT_DATE_STR = "2025-03-15"
DEFAULT_INPUT_DIR = "data/input"
DEFAULT_OUTPUT_DIR = "Partie_2/output"


def load_customers(customers_path: Path) -> pd.DataFrame:
    df = pd.read_csv(customers_path)
    return df


def load_refunds(refunds_path: Path) -> pd.DataFrame:
    df = pd.read_csv(refunds_path)

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["amount"] = -df["amount"].abs()
    refunds_by_order = df.groupby("order_id", as_index=False)["amount"].sum()
    refunds_by_order = refunds_by_order.rename(columns={"amount": "refunds_eur"})
    return refunds_by_order


def load_orders_json(orders_path: Path) -> pd.DataFrame:
    with open(orders_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    orders_df = pd.json_normalize(raw, sep=".")
    date_col = orders_df.get("order_date", orders_df.get("created_at"))
    orders_df["order_date"] = pd.to_datetime(date_col).dt.date.astype(str)
    return orders_df




# -----------------------------------------------------------




def explode_items(orders_df: pd.DataFrame) -> pd.DataFrame:
    exploded = orders_df.explode("items", ignore_index=True)
    items_df = pd.json_normalize(exploded["items"]).add_prefix("item_")
    
    result = pd.concat([exploded.drop("items", axis=1), items_df], axis=1)
    
    result = result.rename(columns={"item_qty": "item_quantity"})
   
    return result


def apply_business_rules(
    orders_items_df: pd.DataFrame,
    customers_df: pd.DataFrame,
) -> (pd.DataFrame, pd.DataFrame):

    paid_mask = orders_items_df["payment_status"].astype(str).str.lower().eq("paid")
    filtered = orders_items_df[paid_mask].copy()

    customers_active = customers_df[customers_df["is_active"] == True].copy()
    filtered = filtered.merge(
        customers_active[["customer_id", "city"]],
        on="customer_id",
        how="inner",
    )

    negative_price_mask = filtered["item_unit_price"].astype(float) < 0
    rejected = filtered[negative_price_mask].copy()
    kept = filtered[~negative_price_mask].copy()

    first_order_indices = kept.drop_duplicates(subset=["order_id"], keep="first").index
    kept["is_first_order_row"] = kept.index.isin(first_order_indices)

    return kept, rejected


def aggregate_orders(
    cleaned_items_df: pd.DataFrame, refunds_by_order: pd.DataFrame
) -> pd.DataFrame:

    tmp = cleaned_items_df.copy()
    tmp["item_quantity"] = pd.to_numeric(tmp["item_quantity"], errors="coerce").fillna(0.0)
    tmp["item_unit_price"] = pd.to_numeric(tmp["item_unit_price"], errors="coerce").fillna(0.0)
    tmp["line_amount_eur"] = tmp["item_quantity"] * tmp["item_unit_price"]

    per_order = (
        tmp.groupby(
            ["order_id", "customer_id", "channel", "city", "order_date"], as_index=False
        )
        .agg(
            items_sold=("item_quantity", "sum"),
            gross_revenue_eur=("line_amount_eur", "sum"),
        )
    )

    per_order = per_order.merge(refunds_by_order, on="order_id", how="left")
    per_order["refunds_eur"] = per_order["refunds_eur"].fillna(0.0)
    per_order["net_revenue_eur"] = per_order["gross_revenue_eur"] + per_order["refunds_eur"]
    return per_order


def build_daily_city_sales(per_order: pd.DataFrame) -> pd.DataFrame:
    per_day_city = (
        per_order.groupby(["order_date", "city", "channel"], as_index=False)
        .agg(
            orders_count=("order_id", "nunique"),
            unique_customers=("customer_id", "nunique"),
            items_sold=("items_sold", "sum"),
            gross_revenue_eur=("gross_revenue_eur", "sum"),
            refunds_eur=("refunds_eur", "sum"),
        )
    )
    per_day_city["net_revenue_eur"] = per_day_city["gross_revenue_eur"] + per_day_city["refunds_eur"]
    per_day_city = per_day_city.rename(columns={"order_date": "date"})
    return per_day_city[
        [
            "date",
            "city",
            "channel",
            "orders_count",
            "unique_customers",
            "items_sold",
            "gross_revenue_eur",
            "refunds_eur",
            "net_revenue_eur",
        ]
    ]


def write_outputs(
    per_order: pd.DataFrame,
    per_day_city: pd.DataFrame,
    rejected_items: pd.DataFrame,
    output_dir: Path,
    target_date: dt.date,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_name = f"daily_summary_{target_date.strftime('%Y%m%d')}.csv"
    per_day_city.to_csv(output_dir / csv_name, index=False, sep=";", encoding="utf-8")

    if not rejected_items.empty:
        rej_name = f"rejected_items_{target_date.strftime('%Y%m%d')}.csv"
        rejected_items.to_csv(output_dir / rej_name, index=False, sep=";", encoding="utf-8")

    db_path = output_dir / "sales.db"
    with sqlite3.connect(db_path) as conn:
        per_order.to_sql("orders_clean", conn, if_exists="append", index=False)
        per_day_city.to_sql("daily_city_sales", conn, if_exists="append", index=False)


def main() -> None:
    # args = parse_args()
    input_dir = Path(DEFAULT_INPUT_DIR)
    output_dir = Path(DEFAULT_OUTPUT_DIR)

    target_date = dt.date.fromisoformat(DEFAULT_DATE_STR)

    orders_path = input_dir / f"orders_{target_date.strftime('%Y-%m-%d')}.json"
    customers_path = input_dir / "customers.csv"
    refunds_path = input_dir / "refunds.csv"

    customers_df = load_customers(customers_path)
    refunds_by_order = load_refunds(refunds_path)
    orders_df = load_orders_json(orders_path)
    orders_items_df = explode_items(orders_df)

    cleaned_items_df, rejected_items_df = apply_business_rules(orders_items_df, customers_df)
    per_order = aggregate_orders(cleaned_items_df, refunds_by_order)
    per_day_city = build_daily_city_sales(per_order)

    write_outputs(per_order, per_day_city, rejected_items_df, output_dir, target_date)


if __name__ == "__main__":
    main()


