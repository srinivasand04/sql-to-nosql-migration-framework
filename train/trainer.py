"""
train/trainer.py
────────────────
Training loop for GCN, GAT, and LSTM models.

Directly mirrors TacticAI-Lite's train/trainer.py:
  - Top-1 accuracy metric
  - Early stopping with patience=15
  - MLflow experiment logging
  - Per-epoch train/val curves saved

GNN training note:
  Each "sample" is a full schema graph (9 nodes, ~18 edges).
  We train over 300 workload-variation graphs (like TacticAI's 815 corner kicks).
  Full-batch training — no mini-batching needed for 9-node graphs.
"""

import os
import time
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from torch_geometric.data import DataLoader as PyGDataLoader
from pathlib import Path
from typing import Optional

try:
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False

ROOT = Path(__file__).resolve().parent.parent


# ── GNN Trainer ───────────────────────────────────────────────────────────────

class GNNTrainer:
    """
    Trains GCN or GAT models on schema graphs.

    Args:
        model:        SchemaGCN or SchemaGAT instance
        graphs:       List of PyG Data objects
        lr:           Learning rate (default 1e-3)
        epochs:       Max epochs (default 200)
        patience:     Early stopping patience (default 15)
        val_ratio:    Fraction of graphs used for validation (default 0.2)
        experiment:   MLflow experiment name
        model_name:   "GCN" or "GAT" — for logging
    """

    def __init__(self, model, graphs: list, lr: float = 1e-3,
                 epochs: int = 200, patience: int = 15,
                 val_ratio: float = 0.2,
                 experiment: str = "schema_migration",
                 model_name: str = "GCN"):

        self.model      = model
        self.graphs     = graphs
        self.lr         = lr
        self.epochs     = epochs
        self.patience   = patience
        self.experiment = experiment
        self.model_name = model_name

        # Train / val split
        split = int(len(graphs) * (1 - val_ratio))
        self.train_graphs = graphs[:split]
        self.val_graphs   = graphs[split:]

        self.optimizer  = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-4)
        self.scheduler  = torch.optim.lr_scheduler.ReduceLROnPlateau(
            self.optimizer, patience=7, factor=0.5
        )
        self.criterion  = nn.CrossEntropyLoss()

        self.train_losses, self.val_losses       = [], []
        self.train_accs,   self.val_accs         = [], []
        self.best_val_acc  = 0.0
        self.best_state    = None

    def _accuracy(self, logits, y):
        pred = logits.argmax(dim=-1)
        return (pred == y).float().mean().item()

    def _run_epoch(self, graphs: list, training: bool) -> tuple[float, float]:
        """Run one epoch over a list of graphs. Returns (avg_loss, avg_acc)."""
        self.model.train(training)
        total_loss, total_acc, n = 0.0, 0.0, 0

        for g in graphs:
            logits = self.model(g.x, g.edge_index, g.edge_attr)
            loss   = self.criterion(logits, g.y)
            acc    = self._accuracy(logits, g.y)

            if training:
                self.optimizer.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                self.optimizer.step()

            total_loss += loss.item()
            total_acc  += acc
            n += 1

        return total_loss / n, total_acc / n

    def train(self) -> dict:
        """Run full training loop with early stopping."""
        t0             = time.time()
        no_improve     = 0

        # MLflow setup
        if MLFLOW_AVAILABLE:
            mlflow.set_experiment(self.experiment)
            mlflow.start_run(run_name=self.model_name)
            mlflow.log_params({
                "model":      self.model_name,
                "lr":         self.lr,
                "epochs":     self.epochs,
                "patience":   self.patience,
                "train_size": len(self.train_graphs),
                "val_size":   len(self.val_graphs),
                "params":     sum(p.numel() for p in self.model.parameters()),
            })

        print(f"\n  Training {self.model_name}  |  "
              f"train={len(self.train_graphs)}  val={len(self.val_graphs)}")
        print(f"  {'Epoch':>6}  {'Train Loss':>11}  {'Train Acc':>10}  "
              f"{'Val Loss':>9}  {'Val Acc':>8}  {'LR':>8}")
        print("  " + "─" * 65)

        for epoch in range(1, self.epochs + 1):
            train_loss, train_acc = self._run_epoch(self.train_graphs, training=True)

            with torch.no_grad():
                val_loss, val_acc = self._run_epoch(self.val_graphs, training=False)

            self.scheduler.step(val_loss)
            lr_now = self.optimizer.param_groups[0]["lr"]

            self.train_losses.append(train_loss)
            self.val_losses.append(val_loss)
            self.train_accs.append(train_acc)
            self.val_accs.append(val_acc)

            if MLFLOW_AVAILABLE:
                mlflow.log_metrics({
                    "train_loss": train_loss, "val_loss": val_loss,
                    "train_acc":  train_acc,  "val_acc":  val_acc,
                }, step=epoch)

            if epoch % 10 == 0 or epoch == 1:
                print(f"  {epoch:>6}  {train_loss:>11.4f}  {train_acc*100:>9.1f}%  "
                      f"{val_loss:>9.4f}  {val_acc*100:>7.1f}%  {lr_now:.2e}")

            # Early stopping
            if val_acc > self.best_val_acc:
                self.best_val_acc = val_acc
                self.best_state   = {k: v.clone() for k, v in self.model.state_dict().items()}
                no_improve        = 0
            else:
                no_improve += 1
                if no_improve >= self.patience:
                    print(f"\n  ⏹  Early stop at epoch {epoch}  "
                          f"(best val acc: {self.best_val_acc*100:.1f}%)")
                    break

        # Restore best weights
        if self.best_state is not None:
            self.model.load_state_dict(self.best_state)

        elapsed = time.time() - t0

        # Save model
        save_dir = ROOT / "outputs" / "results"
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / f"{self.model_name.lower()}_best.pt"
        torch.save(self.best_state, save_path)
        print(f"\n  ✓ Best model saved → {save_path.name}")

        if MLFLOW_AVAILABLE:
            mlflow.log_metric("best_val_acc", self.best_val_acc)
            mlflow.log_artifact(str(save_path))
            mlflow.end_run()

        return {
            "model_name":    self.model_name,
            "best_val_acc":  self.best_val_acc,
            "epochs_trained": len(self.train_losses),
            "elapsed_s":     round(elapsed, 2),
            "train_losses":  self.train_losses,
            "val_losses":    self.val_losses,
            "train_accs":    self.train_accs,
            "val_accs":      self.val_accs,
        }


# ── LSTM Trainer ──────────────────────────────────────────────────────────────

class LSTMTrainer:
    """
    Trains WorkloadLSTM on synthetic query log sequences.

    Args:
        model:       WorkloadLSTM instance
        sequences:   np.array [N, seq_len] — input token sequences
        labels:      np.array [N, seq_len] — target tokens (next-step)
        lr:          Learning rate
        epochs:      Max epochs
        patience:    Early stopping patience
        batch_size:  Mini-batch size
    """

    def __init__(self, model, sequences: np.ndarray, labels: np.ndarray,
                 lr: float = 1e-3, epochs: int = 100, patience: int = 10,
                 batch_size: int = 32, experiment: str = "schema_migration"):

        self.model      = model
        self.lr         = lr
        self.epochs     = epochs
        self.patience   = patience
        self.batch_size = batch_size
        self.experiment = experiment

        # Train / val split
        split    = int(len(sequences) * 0.8)
        self.X_tr, self.y_tr = sequences[:split], labels[:split]
        self.X_va, self.y_va = sequences[split:], labels[split:]

        self.optimizer = torch.optim.Adam(model.parameters(), lr=lr)
        self.criterion = nn.CrossEntropyLoss()

    def _batch_loss_acc(self, X, y, training: bool):
        self.model.train(training)
        total_loss, total_acc, n_batches = 0.0, 0.0, 0

        indices = np.arange(len(X))
        if training:
            np.random.shuffle(indices)

        for start in range(0, len(X), self.batch_size):
            idx   = indices[start:start + self.batch_size]
            xb    = torch.tensor(X[idx], dtype=torch.long)
            yb    = torch.tensor(y[idx], dtype=torch.long)

            logits, _ = self.model(xb)               # [B, T, V]
            B, T, V   = logits.shape
            loss      = self.criterion(logits.view(B * T, V), yb.view(B * T))
            acc       = (logits.argmax(-1) == yb).float().mean().item()

            if training:
                self.optimizer.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                self.optimizer.step()

            total_loss += loss.item()
            total_acc  += acc
            n_batches  += 1

        return total_loss / n_batches, total_acc / n_batches

    def train(self) -> dict:
        best_val_acc, no_improve, best_state = 0.0, 0, None

        if MLFLOW_AVAILABLE:
            mlflow.set_experiment(self.experiment)
            mlflow.start_run(run_name="LSTM")

        print(f"\n  Training LSTM Workload Forecaster")
        print(f"  {'Epoch':>6}  {'Train Loss':>11}  {'Train Acc':>10}  "
              f"{'Val Loss':>9}  {'Val Acc':>8}")
        print("  " + "─" * 55)

        for epoch in range(1, self.epochs + 1):
            tr_loss, tr_acc = self._batch_loss_acc(self.X_tr, self.y_tr, training=True)
            with torch.no_grad():
                va_loss, va_acc = self._batch_loss_acc(self.X_va, self.y_va, training=False)

            if epoch % 10 == 0 or epoch == 1:
                print(f"  {epoch:>6}  {tr_loss:>11.4f}  {tr_acc*100:>9.1f}%  "
                      f"{va_loss:>9.4f}  {va_acc*100:>7.1f}%")

            if va_acc > best_val_acc:
                best_val_acc = va_acc
                best_state   = {k: v.clone() for k, v in self.model.state_dict().items()}
                no_improve   = 0
            else:
                no_improve += 1
                if no_improve >= self.patience:
                    print(f"\n  ⏹  Early stop at epoch {epoch}  "
                          f"(best val acc: {best_val_acc*100:.1f}%)")
                    break

        if best_state:
            self.model.load_state_dict(best_state)

        save_path = ROOT / "outputs" / "results" / "lstm_best.pt"
        torch.save(best_state, save_path)
        print(f"  ✓ LSTM saved → {save_path.name}")

        if MLFLOW_AVAILABLE:
            mlflow.log_metric("best_val_acc_lstm", best_val_acc)
            mlflow.end_run()

        return {"best_val_acc": best_val_acc, "epochs_trained": epoch}
