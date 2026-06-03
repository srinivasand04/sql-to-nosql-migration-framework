#!/usr/bin/env python3
"""
main.py
───────
Master's Thesis — Data Engineering & AI, ESILV Paris
  Intelligent SQL-to-NoSQL Schema Transformation
  using Graph Neural Networks, Deep Learning & ML

Pipeline:
  1. Data      — Load Olist (Kaggle, 9 real tables) via kaggle API or cache
  2. Graph     — Build schema graph (tables=nodes, FK=edges) → PyG Data objects
  3. Baseline  — XGBoost + RandomForest on flat node features
  4. GNN       — Train GCN and GAT on 300 workload-variation graphs
  5. LSTM      — Train workload forecaster on query sequences
  6. Cost ML   — XGBoost multi-output cost predictor + SHAP
  7. What-If   — Workload perturbation simulator
  8. Summary   — Model comparison + final recommendation

Architecture inspired by TacticAI-Lite (GCN/GAT on StatsBomb data).
Same PyTorch Geometric stack, applied to database schema migration.
"""

import os
import sys
import time
import json
import torch
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from data.loader          import load_olist, print_schema_summary
from graph.builder        import build_graph, build_dataset, print_graph_summary
from ml.baseline          import run_baseline
from models.gcn           import SchemaGCN, count_parameters
from models.gat           import SchemaGAT
from models.lstm_forecaster import WorkloadLSTM, generate_query_sequences
from train.trainer        import GNNTrainer, LSTMTrainer
from ml.cost_predictor    import train_cost_predictor
from whatif.simulator     import WorkloadSimulator


def banner(text: str, width: int = 62):
    pad = (width - len(text) - 2) // 2
    print(f"\n{'═'*pad} {text} {'═'*pad}")


def step(n: int, label: str):
    print(f"\n{'═'*62}")
    print(f"  STEP {n}: {label}")
    print(f"{'═'*62}")


def main():
    t_start = time.time()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  SQL-to-NoSQL Migration via GNN + DL + ML                ║")
    print("║  Master's Thesis — Data Engineering & AI, ESILV Paris   ║")
    print("╚══════════════════════════════════════════════════════════╝")

    results = {}

    # ── STEP 1: Data ──────────────────────────────────────────────────────────
    step(1, "DATA LOADING  (Olist Kaggle Dataset)")
    tables, stats, fks = load_olist(use_cache=True)
    print_schema_summary(tables, stats)
    results["n_tables"] = len(tables)
    results["total_rows"] = sum(s["row_count"] for s in stats.values())

    # ── STEP 2: Graph Construction ────────────────────────────────────────────
    step(2, "SCHEMA GRAPH CONSTRUCTION  (PyTorch Geometric)")
    baseline_graph = build_graph(stats, fks, seed=0)
    print_graph_summary(baseline_graph)

    print(f"\n  Building 300 workload-variation graphs for training ...")
    dataset = build_dataset(stats, fks, n_scenarios=300)
    print(f"  ✓ {len(dataset)} graphs  |  {dataset[0].num_nodes} nodes each  "
          f"|  {dataset[0].edge_index.shape[1]} edges each")

    # Train / val split (same ratio as TacticAI)
    split       = int(len(dataset) * 0.8)
    train_graphs = dataset[:split]
    val_graphs   = dataset[split:]

    # ── STEP 3: ML Baseline ───────────────────────────────────────────────────
    step(3, "ML BASELINE  (XGBoost + RandomForest)")
    baseline_results = run_baseline(dataset, test_ratio=0.2)
    results["rf_acc"]  = baseline_results["rf_accuracy"]
    results["xgb_acc"] = baseline_results["xgb_accuracy"]

    # ── STEP 4: GNN Training ─────────────────────────────────────────────────
    step(4, "GNN TRAINING  (GCN + GAT — PyTorch Geometric)")

    in_ch = dataset[0].x.shape[1]   # 8 node features

    # GCN
    print("\n  ── GCN ──────────────────────────────────────────────")
    gcn = SchemaGCN(in_channels=in_ch, hidden=64, num_classes=3, dropout=0.3)
    print(f"  Parameters: {count_parameters(gcn):,}")
    gcn_trainer = GNNTrainer(gcn, dataset, lr=1e-3, epochs=200, patience=15,
                              model_name="GCN")
    gcn_results = gcn_trainer.train()
    results["gcn_val_acc"]    = gcn_results["best_val_acc"]
    results["gcn_epochs"]     = gcn_results["epochs_trained"]

    # GAT
    print("\n  ── GAT ──────────────────────────────────────────────")
    gat = SchemaGAT(in_channels=in_ch, hidden=64, heads=4, num_classes=3, dropout=0.2)
    print(f"  Parameters: {count_parameters(gat):,}")
    gat_trainer = GNNTrainer(gat, dataset, lr=1e-3, epochs=200, patience=15,
                              model_name="GAT")
    gat_results = gat_trainer.train()
    results["gat_val_acc"]    = gat_results["best_val_acc"]
    results["gat_epochs"]     = gat_results["epochs_trained"]

    # ── STEP 5: LSTM Workload Forecaster ─────────────────────────────────────
    step(5, "LSTM WORKLOAD FORECASTER  (Deep Learning)")
    table_names = baseline_graph.table_names
    n_tables    = len(table_names)

    print("  Generating query log sequences ...")
    X_seq, y_seq, _ = generate_query_sequences(
        table_names, n_sequences=1000, seq_len=30, seed=42
    )
    print(f"  ✓ {len(X_seq)} sequences  |  seq_len=30  |  vocab={n_tables} tables")

    lstm = WorkloadLSTM(num_tables=n_tables, embed_dim=32,
                        hidden_dim=128, num_layers=2, dropout=0.3)
    lstm_trainer = LSTMTrainer(lstm, X_seq, y_seq, lr=1e-3,
                               epochs=100, patience=10, batch_size=32)
    lstm_res = lstm_trainer.train()
    results["lstm_val_acc"] = lstm_res["best_val_acc"]

    # Forecast next hot tables using trained LSTM
    print("\n  Hot table forecast (next 100 queries):")
    seed_seq = list(X_seq[0][:10])
    forecast = lstm.forecast_hot_tables(seed_seq, steps=100)
    hot_tables = sorted(forecast.items(), key=lambda x: -x[1])[:3]
    for tid, freq in hot_tables:
        print(f"    {table_names[tid]:<45}  {freq*100:.1f}% of next queries")
    results["lstm_top_hot_table"] = table_names[hot_tables[0][0]]

    # ── STEP 6: ML Cost Predictor ─────────────────────────────────────────────
    step(6, "CLOUD COST PREDICTION  (XGBoost + SHAP)")
    cost_predictor, cost_metrics = train_cost_predictor(n_samples=2000)
    results["cost_r2_avg"] = np.mean([m["r2"] for m in cost_metrics.values()])

    # Predict cost for current Olist workload
    total_rows  = sum(s["row_count"] for s in stats.values())
    storage_gb  = total_rows * 1.5 / 1e6    # rough: 1.5KB avg doc size
    read_m      = 10.0
    write_m     = 2.5
    network_gb  = storage_gb * 2.0

    # Use best GNN prediction for schema_type
    best_gnn  = gcn if results["gcn_val_acc"] >= results["gat_val_acc"] else gat
    best_gnn.eval()
    with torch.no_grad():
        probs = best_gnn.predict_proba(baseline_graph.x, baseline_graph.edge_index)
    # Majority vote schema_type across all tables
    schema_type = int(probs.argmax(dim=-1).mode().values.item())

    cost_pred = cost_predictor.predict_single(
        storage_gb=storage_gb, read_m=read_m, write_m=write_m,
        network_gb=network_gb, schema_type=schema_type, n_collections=5
    )
    best_cloud = min(cost_pred, key=cost_pred.get)

    print(f"\n  Workload estimate: {storage_gb:.3f} GB storage, "
          f"{read_m:.0f}M reads/mo, {write_m:.1f}M writes/mo")
    print(f"\n  {'Provider':<15}  {'Cost/month':>12}")
    print("  " + "─" * 30)
    for provider, cost in cost_pred.items():
        marker = "  ← best" if provider == best_cloud else ""
        print(f"  {provider:<15}  ${cost:>10.4f}{marker}")
    results["best_cloud"] = best_cloud
    results["best_cost"]  = cost_pred[best_cloud]

    # ── STEP 7: What-If Simulation ────────────────────────────────────────────
    step(7, "WHAT-IF SIMULATION  (workload perturbation)")
    best_model = gcn if results["gcn_val_acc"] >= results["gat_val_acc"] else gat
    sim = WorkloadSimulator(best_model, baseline_graph, table_names)

    # Scenario: orders becomes write-heavy (e.g. Black Friday)
    result_bf = sim.perturb("orders", write_ratio=0.75, read_ratio=0.25)
    sim.print_delta_table(result_bf)

    # Sweep write_ratio for order_items
    print("\n  Threshold sweep — order_items write_ratio:")
    sim.sweep("order_items", "write_ratio",
              [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])

    # ── STEP 8: Final Summary ─────────────────────────────────────────────────
    elapsed = round(time.time() - t_start, 2)
    banner("FINAL RESULTS")

    gnn_best_name = "GCN" if results["gcn_val_acc"] >= results["gat_val_acc"] else "GAT"
    gnn_best_acc  = max(results["gcn_val_acc"], results["gat_val_acc"])
    baseline_best = max(results["rf_acc"], results["xgb_acc"])

    print(f"""
  ┌──────────────────────────────────────────────────────────┐
  │           MIGRATION STRATEGY CLASSIFICATION              │
  ├──────────────────────────────────────────────────────────┤
  │  Dataset    : Olist (Kaggle) — {results['n_tables']} tables, {results['total_rows']:,} rows
  │  Graphs     : 300 workload-variation scenarios           │
  ├──────────────────────────────────────────────────────────┤
  │  ML Baseline (XGBoost)  : {results['xgb_acc']*100:>5.1f}%                   │
  │  ML Baseline (RF)       : {results['rf_acc']*100:>5.1f}%                   │
  │  GCN accuracy           : {results['gcn_val_acc']*100:>5.1f}%  (ep {results['gcn_epochs']:>3})         │
  │  GAT accuracy           : {results['gat_val_acc']*100:>5.1f}%  (ep {results['gat_epochs']:>3})         │
  │  Best GNN               : {gnn_best_name} (+{(gnn_best_acc - baseline_best)*100:.1f}pp vs baseline)         │
  ├──────────────────────────────────────────────────────────┤
  │  LSTM val accuracy      : {results['lstm_val_acc']*100:>5.1f}%                   │
  │  Forecasted hot table   : {results['lstm_top_hot_table']:<30}│
  ├──────────────────────────────────────────────────────────┤
  │  Cost model avg R²      : {results['cost_r2_avg']:>5.4f}                  │
  │  Best cloud provider    : {results['best_cloud']:<15}             │
  │  Est. monthly cost      : ${results['best_cost']:.4f}                    │
  ├──────────────────────────────────────────────────────────┤
  │  Pipeline time          : {elapsed}s                          │
  └──────────────────────────────────────────────────────────┘
    """)

    # Save results JSON
    out_dir = ROOT / "outputs" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    save_path = out_dir / "pipeline_results.json"
    # Make results JSON-serialisable
    json_results = {k: (float(v) if isinstance(v, (np.floating, float)) else v)
                    for k, v in results.items()}
    with open(save_path, "w") as f:
        json.dump(json_results, f, indent=2)
    print(f"  Results saved → {save_path.relative_to(ROOT)}\n")


if __name__ == "__main__":
    main()
