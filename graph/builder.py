"""
graph/builder.py
────────────────
Converts Olist schema statistics into PyTorch Geometric Data objects.

Directly mirrors TacticAI's graph/builder.py pattern:
  TacticAI:  freeze_frame (17 players) → graph per corner kick
  Thesis:    schema stats (9 tables)   → graph per workload scenario

Node features (per table):
  [row_count_norm, col_count_norm, write_ratio, read_ratio,
   null_ratio, cardinality, fk_out_norm, fk_in_norm]

Edge features (per FK relationship):
  [join_frequency, fan_out_ratio, is_hot_join]

Node labels (migration strategy):
  0 = EMBED       (child embedded inside parent document)
  1 = REFERENCE   (separate collection, reference by ID)
  2 = DENORMALIZE (snapshot fields copied into related docs)

Workload simulation:
  To create multiple training graphs (like TacticAI's 815 corner kicks),
  we generate N workload variations with randomized query patterns.
  Each variation produces a differently-weighted graph → different labels.
"""

import torch
import numpy as np
import random
from torch_geometric.data import Data
from typing import Optional

# ── Label constants ───────────────────────────────────────────────────────────
EMBED       = 0
REFERENCE   = 1
DENORMALIZE = 2
LABEL_NAMES = {0: "EMBED", 1: "REFERENCE", 2: "DENORMALIZE"}

# ── Workload query templates (table access frequencies) ───────────────────────
# Mirrors TacticAI's weighted corner kick templates
BASE_QUERY_WEIGHTS = {
    "customers":                       {"read": 0.15, "write": 0.03},
    "orders":                          {"read": 0.25, "write": 0.10},
    "order_items":                     {"read": 0.20, "write": 0.10},
    "order_reviews":                   {"read": 0.08, "write": 0.05},
    "order_payments":                  {"read": 0.10, "write": 0.05},
    "products":                        {"read": 0.12, "write": 0.02},
    "sellers":                         {"read": 0.05, "write": 0.01},
    "product_category_name_translation":{"read": 0.03, "write": 0.00},
    "geolocation":                     {"read": 0.02, "write": 0.00},
}

# Hot join pairs (FK pairs that appear together in queries)
HOT_JOIN_PAIRS = {
    ("orders", "customers"),
    ("order_items", "orders"),
    ("order_items", "products"),
    ("order_reviews", "orders"),
    ("order_payments", "orders"),
}


def _simulate_workload(stats: dict, seed: Optional[int] = None) -> dict:
    """
    Simulate a workload scenario with randomized query patterns.
    Returns per-table workload metrics.
    """
    rng = np.random.RandomState(seed)
    workload = {}
    for table, base in BASE_QUERY_WEIGHTS.items():
        # Add noise to simulate different workload patterns
        read_ratio  = float(np.clip(base["read"]  + rng.normal(0, 0.04), 0.01, 0.99))
        write_ratio = float(np.clip(base["write"] + rng.normal(0, 0.02), 0.00, 0.99))
        total = read_ratio + write_ratio
        workload[table] = {
            "read_ratio":  read_ratio  / total,
            "write_ratio": write_ratio / total,
            "access_freq": read_ratio + write_ratio,
        }
    return workload


def _compute_edge_features(child: str, parent: str, workload: dict,
                            stats: dict) -> list:
    """
    Compute edge features for a FK relationship child→parent.

    Features:
      join_frequency  — how often this join appears in queries
      fan_out_ratio   — avg rows in child per parent row (normalized)
      is_hot_join     — 1 if this is a known high-frequency join
    """
    child_freq  = workload.get(child, {}).get("access_freq", 0.1)
    parent_freq = workload.get(parent, {}).get("access_freq", 0.1)
    join_freq   = float(child_freq * parent_freq)

    child_rows  = stats.get(child,  {}).get("row_count", 100)
    parent_rows = stats.get(parent, {}).get("row_count", 100)
    fan_out     = float(np.clip(child_rows / max(parent_rows, 1), 0.0, 20.0) / 20.0)

    is_hot = 1.0 if (child, parent) in HOT_JOIN_PAIRS or (parent, child) in HOT_JOIN_PAIRS else 0.0

    return [join_freq, fan_out, is_hot]


def _assign_label(child: str, parent: str, stats: dict,
                  workload: dict) -> int:
    """
    Rule-based label assignment — used as silver labels for GNN training.
    GNN learns to replicate and generalise these decisions using graph structure.

    Rules (aligned with thesis mapping engine):
      EMBED        → child is small, join is hot, fan-out < 50
      DENORMALIZE  → join is very hot, parent needs child summary
      REFERENCE    → default / large child / low join frequency
    """
    child_rows  = stats.get(child,  {}).get("row_count", 1000)
    parent_rows = stats.get(parent, {}).get("row_count", 1000)
    fan_out     = child_rows / max(parent_rows, 1)
    join_freq   = workload.get(child, {}).get("access_freq", 0.1)
    write_ratio = workload.get(child, {}).get("write_ratio", 0.3)

    is_hot      = (child, parent) in HOT_JOIN_PAIRS or (parent, child) in HOT_JOIN_PAIRS

    if fan_out < 10 and is_hot and write_ratio < 0.4:
        return EMBED
    elif is_hot and fan_out < 50 and join_freq > 0.15:
        return DENORMALIZE
    else:
        return REFERENCE


def build_graph(stats: dict, fk_relationships: list,
                workload: Optional[dict] = None,
                seed: Optional[int] = None) -> Data:
    """
    Build a single PyTorch Geometric Data object from schema stats + workload.

    Args:
        stats:            Table statistics from data/loader.py
        fk_relationships: List of (child, child_col, parent, parent_col)
        workload:         Pre-computed workload dict (or None → simulate fresh)
        seed:             Random seed for workload simulation

    Returns:
        PyG Data object with:
          x           — node feature matrix [N, 8]
          edge_index  — edge connectivity [2, E]
          edge_attr   — edge features [E, 3]
          y           — node labels [N] (migration strategy)
          table_names — list of table names (for viz)
    """
    if workload is None:
        workload = _simulate_workload(stats, seed=seed)

    table_names = list(stats.keys())
    table_idx   = {name: i for i, name in enumerate(table_names)}
    N           = len(table_names)

    # ── Node feature normalization constants ──────────────────────────────────
    max_rows = max(s["row_count"]  for s in stats.values()) + 1
    max_cols = max(s["col_count"]  for s in stats.values()) + 1
    max_fk   = max(max(s["fk_out_degree"], s["fk_in_degree"]) for s in stats.values()) + 1

    # ── Build node feature matrix [N, 8] ─────────────────────────────────────
    x = []
    for name in table_names:
        s   = stats[name]
        wl  = workload.get(name, {"read_ratio": 0.7, "write_ratio": 0.3, "access_freq": 0.1})
        row = [
            s["row_count"]     / max_rows,        # 0: row count (norm)
            s["col_count"]     / max_cols,         # 1: col count (norm)
            wl["write_ratio"],                     # 2: write ratio
            wl["read_ratio"],                      # 3: read ratio
            s["null_ratio"],                       # 4: null ratio
            s["cardinality"],                      # 5: avg cardinality
            s["fk_out_degree"] / max_fk,           # 6: FK out-degree (norm)
            s["fk_in_degree"]  / max_fk,           # 7: FK in-degree (norm)
        ]
        x.append(row)

    x = torch.tensor(x, dtype=torch.float)

    # ── Build edge index + edge features ─────────────────────────────────────
    edge_src, edge_dst, edge_feats = [], [], []
    labels_per_node = {}

    for child, _, parent, _ in fk_relationships:
        if child not in table_idx or parent not in table_idx:
            continue
        ci, pi = table_idx[child], table_idx[parent]
        feats  = _compute_edge_features(child, parent, workload, stats)

        # Add both directions (undirected graph — same as TacticAI proximity edges)
        edge_src += [ci, pi]
        edge_dst += [pi, ci]
        edge_feats += [feats, feats]

        # Assign label to child node (the one being migrated)
        label = _assign_label(child, parent, stats, workload)
        labels_per_node[ci] = label

    edge_index = torch.tensor([edge_src, edge_dst], dtype=torch.long)
    edge_attr  = torch.tensor(edge_feats, dtype=torch.float)

    # Default label for root/lookup tables: REFERENCE
    y = torch.tensor(
        [labels_per_node.get(i, REFERENCE) for i in range(N)],
        dtype=torch.long
    )

    return Data(
        x=x,
        edge_index=edge_index,
        edge_attr=edge_attr,
        y=y,
        table_names=table_names,
        num_nodes=N,
    )


def build_dataset(stats: dict, fk_relationships: list,
                  n_scenarios: int = 300) -> list:
    """
    Build N workload scenario graphs for training.

    TacticAI had 815 corner kick graphs.
    We generate 300 workload variation graphs from the same Olist schema.
    Each has slightly different workload weights → different node features + labels.

    Args:
        stats:         Table stats from loader
        fk_relationships: FK list
        n_scenarios:   How many graphs to generate (default 300)

    Returns:
        List of PyG Data objects
    """
    graphs = []
    for seed in range(n_scenarios):
        g = build_graph(stats, fk_relationships, seed=seed)
        graphs.append(g)
    return graphs


def print_graph_summary(graph: Data):
    """Print a human-readable graph summary."""
    print(f"\n  Nodes      : {graph.num_nodes}")
    print(f"  Edges      : {graph.edge_index.shape[1]}")
    print(f"  Node feats : {graph.x.shape[1]}")
    print(f"  Edge feats : {graph.edge_attr.shape[1]}")
    print(f"\n  {'Table':<45}  Label")
    print("  " + "─" * 55)
    for i, name in enumerate(graph.table_names):
        label = LABEL_NAMES[graph.y[i].item()]
        print(f"  {name:<45}  {label}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
    from data.loader import load_olist, print_schema_summary

    print("\n" + "=" * 60)
    print("  Graph Builder — test run")
    print("=" * 60 + "\n")

    tables, stats, fks = load_olist()
    print_schema_summary(tables, stats)

    print("\n  Building single scenario graph ...")
    g = build_graph(stats, fks, seed=0)
    print_graph_summary(g)

    print(f"\n  Building dataset of 300 workload scenarios ...")
    dataset = build_dataset(stats, fks, n_scenarios=300)
    label_counts = torch.zeros(3)
    for d in dataset:
        for lbl in d.y:
            label_counts[lbl.item()] += 1
    print(f"  Label distribution — EMBED: {label_counts[0]:.0f}  "
          f"REFERENCE: {label_counts[1]:.0f}  "
          f"DENORMALIZE: {label_counts[2]:.0f}")
