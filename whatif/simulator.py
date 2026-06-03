"""
whatif/simulator.py
────────────────────
What-If workload simulator — mirrors TacticAI's whatif/simulator.py.

TacticAI:  Move a player (dx, dy) → Δreception probability
Thesis:    Change workload params (write_ratio, access_freq) → Δmigration decision

Purpose:
  Answers: "If our orders table suddenly gets 3× more writes,
            should we change the embedding strategy?"

  Lets you perturb any table's workload features and see how the
  GNN's migration recommendation changes — with probability deltas.

Usage:
    sim = WorkloadSimulator(model, graph, table_names)
    result = sim.perturb("orders", write_ratio=0.8)
    sim.print_delta_table(result)
"""

import torch
import torch.nn.functional as F
import numpy as np
import copy
from torch_geometric.data import Data
from typing import Optional


LABEL_NAMES = {0: "EMBED", 1: "REFERENCE", 2: "DENORMALIZE"}


class WorkloadSimulator:
    """
    Perturbs workload features for one table and measures the change
    in GNN migration predictions for all tables in the schema graph.

    Args:
        model:       Trained SchemaGCN or SchemaGAT
        graph:       PyG Data object (baseline scenario)
        table_names: List of table names (from graph.table_names)
    """

    def __init__(self, model, graph: Data, table_names: list[str]):
        self.model       = model
        self.graph       = graph
        self.table_names = table_names
        self.table_idx   = {name: i for i, name in enumerate(table_names)}

        # Baseline predictions
        self.model.eval()
        with torch.no_grad():
            logits = self.model(graph.x, graph.edge_index, graph.edge_attr)
            self.baseline_probs = F.softmax(logits, dim=-1).numpy()  # [N, 3]

    def _perturb_features(self, table_name: str, **kwargs) -> torch.Tensor:
        """
        Clone node feature matrix and apply perturbations for one table.

        Supported kwargs (map to node feature indices):
          write_ratio  → feature index 2
          read_ratio   → feature index 3
          null_ratio   → feature index 4
          cardinality  → feature index 5
          row_count_norm → feature index 0
        """
        FEAT_MAP = {
            "row_count_norm": 0,
            "col_count_norm": 1,
            "write_ratio":    2,
            "read_ratio":     3,
            "null_ratio":     4,
            "cardinality":    5,
            "fk_out_norm":    6,
            "fk_in_norm":     7,
        }
        x_new = self.graph.x.clone()
        idx   = self.table_idx[table_name]

        for feat_name, new_val in kwargs.items():
            if feat_name in FEAT_MAP:
                fi = FEAT_MAP[feat_name]
                x_new[idx, fi] = float(np.clip(new_val, 0.0, 1.0))
            else:
                raise ValueError(f"Unknown feature: {feat_name}. "
                                 f"Valid: {list(FEAT_MAP.keys())}")

        # Re-normalise read+write to sum ≤ 1
        wr = x_new[idx, 2].item()
        rr = x_new[idx, 3].item()
        if wr + rr > 1.0:
            total = wr + rr
            x_new[idx, 2] = wr / total
            x_new[idx, 3] = rr / total

        return x_new

    def perturb(self, table_name: str, **kwargs) -> dict:
        """
        Run a what-if perturbation on one table's workload features.

        Returns a result dict with:
          - baseline_probs   [N, 3]
          - perturbed_probs  [N, 3]
          - delta_pp         [N, 3]  (percentage-point change)
          - decisions        list of (table, baseline_label, new_label, changed)
        """
        if table_name not in self.table_idx:
            raise ValueError(f"Table '{table_name}' not found. "
                             f"Available: {self.table_names}")

        x_new = self._perturb_features(table_name, **kwargs)

        self.model.eval()
        with torch.no_grad():
            logits_new = self.model(x_new, self.graph.edge_index, self.graph.edge_attr)
            new_probs  = F.softmax(logits_new, dim=-1).numpy()

        delta_pp  = (new_probs - self.baseline_probs) * 100.0

        decisions = []
        for i, name in enumerate(self.table_names):
            base_label = int(self.baseline_probs[i].argmax())
            new_label  = int(new_probs[i].argmax())
            decisions.append({
                "table":          name,
                "baseline_label": LABEL_NAMES[base_label],
                "new_label":      LABEL_NAMES[new_label],
                "changed":        base_label != new_label,
                "delta_embed_pp": round(delta_pp[i, 0], 2),
                "delta_ref_pp":   round(delta_pp[i, 1], 2),
                "delta_denorm_pp":round(delta_pp[i, 2], 2),
            })

        return {
            "perturbed_table":  table_name,
            "perturbation":     kwargs,
            "baseline_probs":   self.baseline_probs,
            "perturbed_probs":  new_probs,
            "delta_pp":         delta_pp,
            "decisions":        decisions,
        }

    def print_delta_table(self, result: dict):
        """
        Print formatted probability delta table.
        Mirrors TacticAI's Δprobability table output.
        """
        table   = result["perturbed_table"]
        kwargs  = result["perturbation"]
        changes = [d for d in result["decisions"] if d["changed"]]

        print(f"\n  What-If: '{table}'  ← perturbation: {kwargs}")
        print(f"  {'Node':<45}  {'Baseline':>10}  {'New':>12}  "
              f"{'ΔEMBED':>8}  {'ΔREF':>7}  {'ΔDENORM':>8}")
        print("  " + "─" * 100)

        for d in result["decisions"]:
            changed_marker = "  ⚠ CHANGED" if d["changed"] else ""
            print(
                f"  {d['table']:<45}  {d['baseline_label']:>10}  {d['new_label']:>12}  "
                f"{d['delta_embed_pp']:>+7.1f}pp  {d['delta_ref_pp']:>+6.1f}pp  "
                f"{d['delta_denorm_pp']:>+7.1f}pp{changed_marker}"
            )

        print(f"\n  Decision changes: {len(changes)} / {len(result['decisions'])} tables affected")
        for d in changes:
            print(f"    {d['table']}:  {d['baseline_label']} → {d['new_label']}")

    def sweep(self, table_name: str, feature: str,
              values: list) -> list:
        """
        Sweep a feature across a range of values and record decision changes.
        Useful for finding migration strategy thresholds.

        Returns list of (value, decisions_changed, result)
        """
        results = []
        for v in values:
            r = self.perturb(table_name, **{feature: v})
            n_changed = sum(1 for d in r["decisions"] if d["changed"])
            results.append((v, n_changed, r))

        print(f"\n  Sweep: {table_name}.{feature}")
        print(f"  {'Value':>8}  {'Changes':>9}")
        print("  " + "─" * 22)
        for v, nc, _ in results:
            bar = "█" * nc
            print(f"  {v:>8.2f}  {nc:>9}  {bar}")

        return results


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data.loader import load_olist
    from graph.builder import build_graph
    from models.gcn import SchemaGCN

    print("\n" + "=" * 60)
    print("  What-If Simulator — test run")
    print("=" * 60 + "\n")

    tables, stats, fks = load_olist()
    g     = build_graph(stats, fks, seed=0)
    model = SchemaGCN(in_channels=8, hidden=64, num_classes=3)
    model.eval()

    sim = WorkloadSimulator(model, g, g.table_names)

    # Simulate: orders becomes write-heavy
    result = sim.perturb("orders", write_ratio=0.85, read_ratio=0.15)
    sim.print_delta_table(result)

    # Sweep write_ratio for order_items
    sweep_vals = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    sim.sweep("order_items", "write_ratio", sweep_vals)
