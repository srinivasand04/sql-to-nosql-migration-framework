"""
olist_adapter.py
────────────────
Converts Olist Kaggle CSV files (Brazilian E-Commerce) to the
column names expected by the sql_to_nosql_migration pipeline.

Olist Kaggle dataset:
  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Place the raw Olist files inside:
  data/olist_raw/

This script outputs clean CSVs to:
  data/ecommerce_dataset/

Mapping:
  olist_customers_dataset.csv    → users.csv
  olist_products_dataset.csv     → products.csv
  olist_orders_dataset.csv       → orders.csv
  olist_order_items_dataset.csv  → order_items.csv
  olist_order_reviews_dataset.csv→ reviews.csv
  (events synthesized from orders + items)
"""

import os, sys
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)

ROOT     = os.path.dirname(os.path.abspath(__file__))
RAW_DIR  = os.path.join(ROOT, "data/olist_raw")
OUT_DIR  = os.path.join(ROOT, "ecommerce_dataset")
os.makedirs(OUT_DIR, exist_ok=True)

def check_raw_files():
    required = [
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_reviews_dataset.csv",
    ]
    missing = [f for f in required if not os.path.exists(os.path.join(RAW_DIR, f))]
    if missing:
        print("\n❌  Missing Olist files in data/olist_raw/:")
        for m in missing:
            print(f"     • {m}")
        print("\n  Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        print("  Then place ALL CSV files into:  data/olist_raw/\n")
        sys.exit(1)

def adapt_users(max_rows=5000):
    print("  → users.csv ...")
    df = pd.read_csv(os.path.join(RAW_DIR, "olist_customers_dataset.csv"))
    # Olist columns: customer_id, customer_unique_id, customer_zip_code_prefix,
    #                customer_city, customer_state
    df = df.head(max_rows).reset_index(drop=True)
    df["id"]         = df.index + 1
    df["name"]       = "User_" + df["customer_unique_id"].str[:8]
    df["email"]      = "user" + df["id"].astype(str) + "@example.com"
    df["country"]    = df["customer_state"].fillna("BR")
    df["created_at"] = pd.to_datetime("2021-01-01") + pd.to_timedelta(
        np.random.randint(0, 730, len(df)), unit="D"
    )
    df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d")

    out = df[["id", "name", "email", "country", "created_at"]]
    out.to_csv(os.path.join(OUT_DIR, "users.csv"), index=False)
    print(f"     ✓ {len(out):,} users")
    return out

def adapt_products(max_rows=2000):
    print("  → products.csv ...")
    df = pd.read_csv(os.path.join(RAW_DIR, "olist_products_dataset.csv"))
    # Olist: product_id, product_category_name, product_weight_g, ...
    df = df.head(max_rows).reset_index(drop=True)
    df["id"]          = df.index + 1
    df["name"]        = "Product_" + df["product_id"].str[:8]
    df["category"]    = df["product_category_name"].fillna("general").str.replace("_"," ").str.title()
    df["price"]       = np.round(np.random.uniform(5, 500, len(df)), 2)
    df["stock"]       = np.random.randint(0, 500, len(df))
    df["created_at"]  = pd.to_datetime("2021-01-01") + pd.to_timedelta(
        np.random.randint(0, 730, len(df)), unit="D"
    )
    df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d")
    # Save mapping: olist product_id → our int id
    product_id_map = dict(zip(df["product_id"], df["id"]))

    out = df[["id", "name", "category", "price", "stock", "created_at"]]
    out.to_csv(os.path.join(OUT_DIR, "products.csv"), index=False)
    print(f"     ✓ {len(out):,} products")
    return out, product_id_map

def adapt_orders(users_df, max_rows=10000):
    print("  → orders.csv ...")
    df = pd.read_csv(os.path.join(RAW_DIR, "olist_orders_dataset.csv"))
    # Olist: order_id, customer_id, order_status, order_purchase_timestamp, ...
    df = df.head(max_rows).reset_index(drop=True)
    df["id"]          = df.index + 1
    # Map customer_id to our sequential user ids (random assignment for anonymized data)
    df["user_id"]     = np.random.randint(1, len(users_df) + 1, len(df))
    df["status"]      = df["order_status"].fillna("delivered")
    df["total"]       = np.round(np.random.uniform(20, 800, len(df)), 2)
    df["created_at"]  = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    ).dt.strftime("%Y-%m-%d").fillna("2022-01-01")
    # Save mapping: olist order_id → our int id
    order_id_map = dict(zip(df["order_id"], df["id"]))

    out = df[["id", "user_id", "status", "total", "created_at"]]
    out.to_csv(os.path.join(OUT_DIR, "orders.csv"), index=False)
    print(f"     ✓ {len(out):,} orders")
    return out, order_id_map

def adapt_order_items(order_id_map, product_id_map):
    print("  → order_items.csv ...")
    df = pd.read_csv(os.path.join(RAW_DIR, "olist_order_items_dataset.csv"))
    # Olist: order_id, order_item_id, product_id, seller_id, price, freight_value
    df["order_id_int"]   = df["order_id"].map(order_id_map)
    df["product_id_int"] = df["product_id"].map(product_id_map)

    # Keep only rows where both keys exist in our mapped tables
    df = df.dropna(subset=["order_id_int", "product_id_int"])
    df = df.reset_index(drop=True)
    df["id"]         = df.index + 1
    df["order_id"]   = df["order_id_int"].astype(int)
    df["product_id"] = df["product_id_int"].astype(int)
    df["quantity"]   = np.random.randint(1, 4, len(df))
    df["unit_price"] = np.round(df["price"].fillna(50.0), 2)

    out = df[["id", "order_id", "product_id", "quantity", "unit_price"]]
    out.to_csv(os.path.join(OUT_DIR, "order_items.csv"), index=False)
    print(f"     ✓ {len(out):,} order items")
    return out

def adapt_reviews(users_df, product_id_map, max_rows=5000):
    print("  → reviews.csv ...")
    df = pd.read_csv(os.path.join(RAW_DIR, "olist_order_reviews_dataset.csv"))
    # Olist: review_id, order_id, review_score, review_comment_message,
    #        review_creation_date, review_answer_timestamp
    df = df.head(max_rows).reset_index(drop=True)
    df["id"]          = df.index + 1
    df["user_id"]     = np.random.randint(1, len(users_df) + 1, len(df))
    df["product_id"]  = np.random.choice(list(product_id_map.values()), len(df))
    df["rating"]      = df["review_score"].fillna(3).astype(int).clip(1, 5)
    df["comment"]     = df["review_comment_message"].fillna("No comment").str[:200]
    df["created_at"]  = pd.to_datetime(
        df["review_creation_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d").fillna("2022-06-01")

    out = df[["id", "user_id", "product_id", "rating", "comment", "created_at"]]
    out.to_csv(os.path.join(OUT_DIR, "reviews.csv"), index=False)
    print(f"     ✓ {len(out):,} reviews")
    return out

def synthesize_events(users_df, product_id_map, n=15000):
    print("  → events.csv  (synthesized from order behaviour) ...")
    event_types = ["page_view", "add_to_cart", "purchase", "search", "login", "logout", "wishlist"]
    rows = []
    prod_ids = list(product_id_map.values())
    base = datetime(2022, 1, 1)
    for i in range(1, n + 1):
        etype = random.choice(event_types)
        rows.append({
            "id":         i,
            "user_id":    random.randint(1, len(users_df)),
            "event_type": etype,
            "page":       f"/product/{random.choice(prod_ids)}" if etype in ("page_view","add_to_cart","purchase") else "/home",
            "session_id": f"sess_{random.randint(1000,9999)}",
            "created_at": (base + timedelta(
                days=random.randint(0, 600),
                seconds=random.randint(0, 86400)
            )).strftime("%Y-%m-%d %H:%M:%S"),
        })
    pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, "events.csv"), index=False)
    print(f"     ✓ {n:,} events (synthesized)")


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*60)
    print("  OLIST → SQL_NoSQL Project Adapter")
    print("="*60)
    print(f"\n  Source : {RAW_DIR}")
    print(f"  Output : {OUT_DIR}\n")

    check_raw_files()

    print("  Converting tables...\n")
    users_df, *_              = [adapt_users()]
    products_df, product_map  = adapt_products()
    orders_df,   order_map    = adapt_orders(users_df)
    adapt_order_items(order_map, product_map)
    adapt_reviews(users_df, product_map)
    synthesize_events(users_df, product_map)

    print(f"\n  ✅  All 6 tables ready in: {OUT_DIR}")
    print("  Now run:  python main.py\n")
