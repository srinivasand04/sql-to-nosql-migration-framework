"""
schema_profiler.py
──────────────────
Loads all CSV tables and produces a structural profile of each table:
  - Row/column counts
  - Data types
  - Null ratios
  - Cardinality estimates
  - Foreign key candidates (heuristic: column name ends with _id)
  - Cross-table relationship map
"""

import os
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ── Data structures ──────────────────────────────────────────────────────────

@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_ratio: float
    unique_count: int
    cardinality_ratio: float          # unique / total
    is_primary_key_candidate: bool
    is_foreign_key_candidate: bool
    referenced_table: Optional[str]   # inferred from column name


@dataclass
class TableProfile:
    name: str
    row_count: int
    col_count: int
    size_bytes: int                   # estimated in-memory size
    columns: List[ColumnProfile] = field(default_factory=list)
    foreign_keys: List[dict] = field(default_factory=list)   # [{col, ref_table, ref_col}]


# ── Helpers ──────────────────────────────────────────────────────────────────

# Map table names to their primary key column
KNOWN_PRIMARY_KEYS = {
    "users":       "id",
    "products":    "id",
    "orders":      "id",
    "order_items": "id",
    "reviews":     "id",
    "events":      "id",
}

# Heuristic FK map: (table, column) → (referenced_table, referenced_column)
KNOWN_FOREIGN_KEYS = {
    ("orders",      "user_id"):    ("users",    "id"),
    ("order_items", "order_id"):   ("orders",   "id"),
    ("order_items", "product_id"): ("products", "id"),
    ("reviews",     "user_id"):    ("users",    "id"),
    ("reviews",     "product_id"): ("products", "id"),
    ("events",      "user_id"):    ("users",    "id"),
    ("events",      "product_id"): ("products", "id"),
}


def _infer_referenced_table(col_name: str, known_tables: List[str]) -> Optional[str]:
    """Heuristic: if col ends with _id, try to match stem to a known table."""
    if col_name.endswith("_id"):
        stem = col_name[:-3]           # remove '_id'
        # exact match or plural
        for t in known_tables:
            if t == stem or t == stem + "s":
                return t
    return None


# ── Core class ───────────────────────────────────────────────────────────────

class SchemaProfiler:
    """Loads the e-commerce CSV dataset and produces table/column profiles."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.dataframes: Dict[str, pd.DataFrame] = {}
        self.profiles:   Dict[str, TableProfile]  = {}

    # ── Public API ────────────────────────────────────────────────────────────

    def load(self) -> "SchemaProfiler":
        """Load all CSV files found in data_dir."""
        for fname in sorted(os.listdir(self.data_dir)):
            if fname.endswith(".csv"):
                table_name = fname.replace(".csv", "")
                path = os.path.join(self.data_dir, fname)
                self.dataframes[table_name] = pd.read_csv(path, low_memory=False)
        return self

    def profile(self) -> Dict[str, TableProfile]:
        """Build a profile for every loaded table."""
        table_names = list(self.dataframes.keys())

        for tname, df in self.dataframes.items():
            col_profiles = []
            pk_col = KNOWN_PRIMARY_KEYS.get(tname, "id")

            for col in df.columns:
                n_rows   = len(df)
                n_null   = int(df[col].isna().sum())
                n_unique = int(df[col].nunique(dropna=True))
                card_ratio = n_unique / n_rows if n_rows > 0 else 0.0

                is_pk = (col == pk_col) and (card_ratio > 0.95)
                is_fk = col.endswith("_id") and not is_pk
                ref_table = None
                if is_fk:
                    key = (tname, col)
                    if key in KNOWN_FOREIGN_KEYS:
                        ref_table = KNOWN_FOREIGN_KEYS[key][0]
                    else:
                        ref_table = _infer_referenced_table(col, table_names)

                col_profiles.append(ColumnProfile(
                    name=col,
                    dtype=str(df[col].dtype),
                    null_ratio=round(n_null / n_rows, 4) if n_rows else 0,
                    unique_count=n_unique,
                    cardinality_ratio=round(card_ratio, 4),
                    is_primary_key_candidate=is_pk,
                    is_foreign_key_candidate=is_fk,
                    referenced_table=ref_table,
                ))

            # Build explicit FK list
            fks = []
            for col_name, (ref_table, ref_col) in KNOWN_FOREIGN_KEYS.items():
                if col_name[0] == tname:
                    fks.append({
                        "column":         col_name[1],
                        "ref_table":      ref_table,
                        "ref_column":     ref_col,
                    })

            size_bytes = df.memory_usage(deep=True).sum()

            self.profiles[tname] = TableProfile(
                name=tname,
                row_count=len(df),
                col_count=len(df.columns),
                size_bytes=int(size_bytes),
                columns=col_profiles,
                foreign_keys=fks,
            )

        return self.profiles

    # ── Reporting ─────────────────────────────────────────────────────────────

    def print_report(self):
        if not self.profiles:
            self.profile()

        print("=" * 60)
        print("  SCHEMA PROFILER REPORT")
        print("=" * 60)

        for tname, tp in self.profiles.items():
            print(f"\n┌─ Table: {tname.upper()}")
            print(f"│  Rows: {tp.row_count:,}   Columns: {tp.col_count}   "
                  f"In-memory: {tp.size_bytes / 1024:.1f} KB")

            # Columns
            print("│  Columns:")
            for cp in tp.columns:
                flags = []
                if cp.is_primary_key_candidate: flags.append("PK")
                if cp.is_foreign_key_candidate: flags.append(f"FK→{cp.referenced_table}")
                flag_str = f"  [{', '.join(flags)}]" if flags else ""
                print(f"│    • {cp.name:<20} {cp.dtype:<12} "
                      f"null={cp.null_ratio:.2%}  "
                      f"card={cp.cardinality_ratio:.2%}{flag_str}")

            # FK relationships
            if tp.foreign_keys:
                print("│  Relationships:")
                for fk in tp.foreign_keys:
                    print(f"│    {tname}.{fk['column']} → "
                          f"{fk['ref_table']}.{fk['ref_column']}")

        print("\n" + "=" * 60)


# ── Quick test ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    #data_dir = sys.argv[1] if len(sys.argv) > 1 else "data/ecommerce_dataset"
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "ecommerce_dataset"
    profiler = SchemaProfiler(data_dir)
    profiler.load()
    profiler.print_report()
