"""
data/loader.py
──────────────
Loads the Olist Brazilian E-Commerce dataset (Kaggle).
Mirrors TacticAI's data/loader.py pattern:
  - Auto-downloads via kaggle API if not cached
  - Returns clean DataFrames + FK relationship map
  - Disk cache for fast re-runs

Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
9 real relational tables, ~100k orders
"""

import os
import pickle
import zipfile
import pandas as pd
import numpy as np
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent
RAW_DIR   = ROOT / "data" / "olist_raw"
CACHE_DIR = ROOT / "data"
CACHE_FILE = CACHE_DIR / "olist_cache.pkl"

# ── FK relationship map ───────────────────────────────────────────────────────
# (child_table, child_col, parent_table, parent_col)
FK_RELATIONSHIPS = [
    ("orders",            "customer_id",  "customers",   "customer_id"),
    ("order_items",       "order_id",     "orders",      "order_id"),
    ("order_items",       "product_id",   "products",    "product_id"),
    ("order_items",       "seller_id",    "sellers",     "seller_id"),
    ("order_reviews",     "order_id",     "orders",      "order_id"),
    ("order_payments",    "order_id",     "orders",      "order_id"),
    ("products",          "product_category_name", "product_category_name_translation", "product_category_name"),
    ("customers",         "customer_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix"),
    ("sellers",           "seller_zip_code_prefix",   "geolocation", "geolocation_zip_code_prefix"),
]

# ── File → table name mapping ─────────────────────────────────────────────────
OLIST_FILES = {
    "customers":                       "olist_customers_dataset.csv",
    "orders":                          "olist_orders_dataset.csv",
    "order_items":                     "olist_order_items_dataset.csv",
    "order_reviews":                   "olist_order_reviews_dataset.csv",
    "order_payments":                  "olist_order_payments_dataset.csv",
    "products":                        "olist_products_dataset.csv",
    "sellers":                         "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
    "geolocation":                     "olist_geolocation_dataset.csv",
}


def _try_kaggle_download():
    """Attempt to download via kaggle API. Requires ~/.kaggle/kaggle.json"""
    try:
        import kaggle  # noqa: F401
        print("  Kaggle API found. Downloading olistbr/brazilian-ecommerce ...")
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        os.system(
            f"kaggle datasets download -d olistbr/brazilian-ecommerce "
            f"--path {RAW_DIR} --unzip -q"
        )
        print("  ✓ Download complete.")
        return True
    except Exception as e:
        print(f"  ⚠ Kaggle auto-download failed: {e}")
        print(f"  Manual download: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        print(f"  Place CSVs in: {RAW_DIR}\n")
        return False


def _check_raw_files():
    if not RAW_DIR.exists():
        return False
    present = set(f.name for f in RAW_DIR.glob("*.csv"))
    needed  = set(OLIST_FILES.values())
    return needed.issubset(present)


def _load_raw_tables():
    """Load all 9 Olist CSVs into DataFrames."""
    tables = {}
    for name, fname in OLIST_FILES.items():
        path = RAW_DIR / fname
        df   = pd.read_csv(path, low_memory=False)
        tables[name] = df
        print(f"    {name:<45} {len(df):>7,} rows  {len(df.columns):>3} cols")
    return tables


def _compute_table_stats(tables):
    """Compute per-table stats used as GNN node features."""
    stats = {}
    for name, df in tables.items():
        null_ratio  = df.isnull().mean().mean()
        cardinality = df.nunique().mean() / max(len(df), 1)
        # FK degree
        fk_out = sum(1 for c, _, _, _ in FK_RELATIONSHIPS if c == name)
        fk_in  = sum(1 for _, _, p, _ in FK_RELATIONSHIPS if p == name)
        stats[name] = {
            "row_count":     len(df),
            "col_count":     len(df.columns),
            "null_ratio":    round(null_ratio, 4),
            "cardinality":   round(min(cardinality, 1.0), 4),
            "fk_out_degree": fk_out,
            "fk_in_degree":  fk_in,
            "memory_mb":     round(df.memory_usage(deep=True).sum() / 1e6, 3),
        }
    return stats


def load_olist(use_cache=True, force_download=False):
    """
    Main entry point. Returns (tables, stats, FK_RELATIONSHIPS).

    Args:
        use_cache:       Load from disk cache if available (fast re-runs)
        force_download:  Re-download even if files exist

    Returns:
        tables (dict[str, DataFrame])
        stats  (dict[str, dict])   — node feature source
        FK_RELATIONSHIPS (list)
    """
    # Cache hit
    if use_cache and CACHE_FILE.exists() and not force_download:
        print("  Loading from cache ...")
        with open(CACHE_FILE, "rb") as f:
            data = pickle.load(f)
        print(f"  ✓ {len(data['tables'])} tables loaded from {CACHE_FILE.name}")
        return data["tables"], data["stats"], FK_RELATIONSHIPS

    # Download if needed
    if force_download or not _check_raw_files():
        success = _try_kaggle_download()
        if not success and not _check_raw_files():
            raise FileNotFoundError(
                f"Olist CSVs not found in {RAW_DIR}. "
                "Download manually from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
            )

    print("  Loading raw tables ...")
    tables = _load_raw_tables()
    stats  = _compute_table_stats(tables)

    # Cache to disk
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "wb") as f:
        pickle.dump({"tables": tables, "stats": stats}, f)
    print(f"  ✓ Cached to {CACHE_FILE.name}")

    return tables, stats, FK_RELATIONSHIPS


def print_schema_summary(tables, stats):
    """Print a formatted schema overview."""
    print(f"\n  {'Table':<45} {'Rows':>8}  {'Cols':>5}  {'Null%':>6}  {'FK-out':>7}  {'FK-in':>6}")
    print("  " + "─" * 85)
    for name, s in stats.items():
        print(
            f"  {name:<45} {s['row_count']:>8,}  {s['col_count']:>5}  "
            f"{s['null_ratio']*100:>5.1f}%  {s['fk_out_degree']:>7}  {s['fk_in_degree']:>6}"
        )
    print(f"\n  FK relationships: {len(FK_RELATIONSHIPS)}")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  Olist Data Loader — test run")
    print("=" * 60 + "\n")
    tables, stats, fks = load_olist(use_cache=False)
    print_schema_summary(tables, stats)
