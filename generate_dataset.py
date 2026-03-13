"""
Dataset Generator for E-Commerce SQL-to-NoSQL Migration Prototype
Generates realistic synthetic CSV files for all 6 tables.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

random.seed(42)
np.random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "ecommerce_dataset")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── 1. USERS ────────────────────────────────────────────────────────────────
def generate_users(n=200):
    first_names = ["Alice", "Bob", "Carlos", "Diana", "Eve", "Frank", "Grace",
                   "Hank", "Iris", "Jack", "Karen", "Leo", "Mia", "Nora", "Oscar"]
    last_names  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                   "Miller", "Davis", "Wilson", "Taylor"]
    countries   = ["US", "UK", "FR", "DE", "IN", "CA", "AU"]

    rows = []
    for i in range(1, n + 1):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        rows.append({
            "id": i,
            "name": f"{fn} {ln}",
            "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
            "country": random.choice(countries),
            "created_at": (datetime(2021, 1, 1) + timedelta(days=random.randint(0, 900))).date(),
            "age": random.randint(18, 65),
        })
    return pd.DataFrame(rows)

# ── 2. PRODUCTS ──────────────────────────────────────────────────────────────
def generate_products(n=100):
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty"]
    adjectives = ["Pro", "Ultra", "Smart", "Premium", "Basic", "Elite"]
    nouns      = ["Widget", "Gadget", "Device", "Item", "Product", "Tool"]

    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id": i,
            "name": f"{random.choice(adjectives)} {random.choice(nouns)} {i}",
            "category": random.choice(categories),
            "price": round(random.uniform(5.0, 500.0), 2),
            "stock": random.randint(0, 500),
            "created_at": (datetime(2021, 1, 1) + timedelta(days=random.randint(0, 900))).date(),
        })
    return pd.DataFrame(rows)

# ── 3. ORDERS ────────────────────────────────────────────────────────────────
def generate_orders(n=500, user_ids=None):
    statuses = ["pending", "shipped", "delivered", "cancelled"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id": i,
            "user_id": random.choice(user_ids),
            "status": random.choice(statuses),
            "total_amount": 0.0,  # filled after order_items
            "created_at": (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).date(),
        })
    return pd.DataFrame(rows)

# ── 4. ORDER ITEMS ───────────────────────────────────────────────────────────
def generate_order_items(order_ids, product_ids, products_df):
    rows = []
    item_id = 1
    price_map = dict(zip(products_df["id"], products_df["price"]))
    for oid in order_ids:
        n_items = random.randint(1, 5)
        chosen_products = random.sample(list(product_ids), min(n_items, len(product_ids)))
        for pid in chosen_products:
            qty = random.randint(1, 4)
            rows.append({
                "id": item_id,
                "order_id": oid,
                "product_id": pid,
                "quantity": qty,
                "unit_price": price_map[pid],
                "subtotal": round(price_map[pid] * qty, 2),
            })
            item_id += 1
    return pd.DataFrame(rows)

# ── 5. REVIEWS ───────────────────────────────────────────────────────────────
def generate_reviews(n=400, user_ids=None, product_ids=None):
    texts = [
        "Great product!", "Not worth the price.", "Excellent quality.",
        "Decent but could be better.", "Absolutely love it!",
        "Would not recommend.", "Fast shipping, good item.", "Average experience.",
    ]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id": i,
            "user_id": random.choice(user_ids),
            "product_id": random.choice(product_ids),
            "rating": random.randint(1, 5),
            "review_text": random.choice(texts),
            "created_at": (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).date(),
        })
    return pd.DataFrame(rows)

# ── 6. EVENTS ────────────────────────────────────────────────────────────────
def generate_events(n=2000, user_ids=None, product_ids=None):
    event_types = ["page_view", "add_to_cart", "purchase", "search", "login", "logout"]
    rows = []
    for i in range(1, n + 1):
        etype = random.choice(event_types)
        rows.append({
            "id": i,
            "user_id": random.choice(user_ids),
            "event_type": etype,
            "product_id": random.choice(product_ids) if etype in ["page_view", "add_to_cart", "purchase"] else None,
            "session_id": f"sess_{random.randint(1000, 9999)}",
            "timestamp": datetime(2022, 1, 1) + timedelta(
                days=random.randint(0, 730),
                seconds=random.randint(0, 86400)
            ),
            "metadata": f'{{"ip": "192.168.{random.randint(1,255)}.{random.randint(1,255)}"}}',
        })
    return pd.DataFrame(rows)


# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating e-commerce dataset...")

    users_df    = generate_users(200)
    products_df = generate_products(100)
    orders_df   = generate_orders(500, user_ids=list(users_df["id"]))
    order_items_df = generate_order_items(
        list(orders_df["id"]), list(products_df["id"]), products_df
    )
    # Backfill total_amount on orders
    totals = order_items_df.groupby("order_id")["subtotal"].sum()
    orders_df["total_amount"] = orders_df["id"].map(totals).round(2)

    reviews_df = generate_reviews(400, list(users_df["id"]), list(products_df["id"]))
    events_df  = generate_events(2000, list(users_df["id"]), list(products_df["id"]))

    files = {
        "users.csv":       users_df,
        "products.csv":    products_df,
        "orders.csv":      orders_df,
        "order_items.csv": order_items_df,
        "reviews.csv":     reviews_df,
        "events.csv":      events_df,
    }

    for fname, df in files.items():
        path = os.path.join(OUTPUT_DIR, fname)
        df.to_csv(path, index=False)
        print(f"  ✓ {fname:20s}  ({len(df):,} rows)")

    print(f"\nDataset written to: {OUTPUT_DIR}")
