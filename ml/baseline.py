"""
ml/baseline.py
──────────────
Classical ML baseline — XGBoost + RandomForest on node features.

Purpose:
  Establish a benchmark accuracy BEFORE applying GNN.
  GNN should outperform this because it uses graph structure
  (neighbour context) not just per-node tabular features.

  This mirrors TacticAI's implicit baseline (random = 5.7% top-1).
  Here we have a stronger baseline: XGBoost on flat features.

Features used (same 8 as GNN node features):
  row_count_norm, col_count_norm, write_ratio, read_ratio,
  null_ratio, cardinality, fk_out_norm, fk_in_norm
"""

import numpy as np
import torch
from torch_geometric.data import Data
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import label_binarize
from sklearn.metrics import classification_report, accuracy_score
import xgboost as xgb
import warnings
warnings.filterwarnings("ignore")

LABEL_NAMES = ["EMBED", "REFERENCE", "DENORMALIZE"]


def dataset_to_numpy(graphs: list[Data]):
    """
    Flatten PyG graphs into (X, y) numpy arrays.
    Each node becomes one sample — graph structure is ignored (that's the point).
    """
    X_list, y_list = [], []
    for g in graphs:
        X_list.append(g.x.numpy())
        y_list.append(g.y.numpy())
    X = np.vstack(X_list)
    y = np.concatenate(y_list)
    return X, y


def train_random_forest(X_train, y_train, X_test, y_test):
    """Train RandomForest and return accuracy + report."""
    clf = RandomForestClassifier(
        n_estimators=200,
        max_depth=6,
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced",
    )
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    acc    = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, target_names=LABEL_NAMES, zero_division=0)
    return clf, acc, report


def train_xgboost(X_train, y_train, X_test, y_test):
    """Train XGBoost and return accuracy + report."""
    clf = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        use_label_encoder=False,
        eval_metric="mlogloss",
        random_state=42,
        verbosity=0,
    )
    clf.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    y_pred = clf.predict(X_test)
    acc    = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, target_names=LABEL_NAMES, zero_division=0)
    return clf, acc, report


def run_baseline(graphs: list[Data], test_ratio: float = 0.2) -> dict:
    """
    Run full baseline evaluation. Returns dict with accuracies for both models.
    """
    X, y = dataset_to_numpy(graphs)

    # Train / test split (preserve order — later scenarios = test)
    split = int(len(X) * (1 - test_ratio))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    print(f"  Train: {len(X_train)} samples  |  Test: {len(X_test)} samples")
    print(f"  Label distribution — "
          f"EMBED: {(y==0).sum()}  REFERENCE: {(y==1).sum()}  DENORMALIZE: {(y==2).sum()}")

    # RandomForest
    print("\n  ── RandomForest ─────────────────────────────────────")
    rf_clf, rf_acc, rf_report = train_random_forest(X_train, y_train, X_test, y_test)
    print(f"  Accuracy: {rf_acc:.4f}  ({rf_acc*100:.1f}%)")
    print(rf_report)

    # XGBoost
    print("  ── XGBoost ──────────────────────────────────────────")
    xgb_clf, xgb_acc, xgb_report = train_xgboost(X_train, y_train, X_test, y_test)
    print(f"  Accuracy: {xgb_acc:.4f}  ({xgb_acc*100:.1f}%)")
    print(xgb_report)

    print("\n  ── Baseline Summary ─────────────────────────────────")
    print(f"  RandomForest accuracy : {rf_acc*100:.1f}%")
    print(f"  XGBoost accuracy      : {xgb_acc*100:.1f}%")
    print(f"  → GNN must beat: {max(rf_acc, xgb_acc)*100:.1f}%  to justify graph structure")

    return {
        "rf_accuracy":   rf_acc,
        "xgb_accuracy":  xgb_acc,
        "rf_model":      rf_clf,
        "xgb_model":     xgb_clf,
        "X_train":       X_train,
        "X_test":        X_test,
        "y_train":       y_train,
        "y_test":        y_test,
    }


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data.loader import load_olist
    from graph.builder import build_dataset

    print("\n" + "=" * 60)
    print("  ML Baseline — test run")
    print("=" * 60 + "\n")

    tables, stats, fks = load_olist()
    graphs = build_dataset(stats, fks, n_scenarios=300)
    results = run_baseline(graphs)
