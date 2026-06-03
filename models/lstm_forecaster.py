"""
models/lstm_forecaster.py
─────────────────────────
LSTM-based workload forecaster — the Deep Learning layer.

Purpose:
  GNNs model STRUCTURAL relationships (which tables are connected).
  LSTM models TEMPORAL relationships (which tables are accessed over time).

  Input:  A sequence of table access events  [t1, t2, t3, ..., tN]
          e.g. [orders, order_items, products, customers, orders, ...]
  Output: Predicted next table access

This answers: "given the last K queries, what table will be hit next?"
→ Used to update workload features dynamically before GNN inference.

Architecture:
  Embedding(num_tables → embed_dim)
  LSTM(embed_dim → hidden_dim, num_layers=2, dropout=0.3)
  Linear(hidden_dim → num_tables)
  → CrossEntropyLoss

Real-world analogy:
  Like predicting the next word in a sentence, but for database queries.
  Enables proactive migration — if the model forecasts a table will become hot,
  the GNN can recommend pre-emptive denormalization.

Training data:
  Simulated query log sequences from workload_analyzer (with temporal ordering).
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Optional


class WorkloadLSTM(nn.Module):
    """
    LSTM that predicts the next table to be accessed in a query sequence.

    Args:
        num_tables:  Number of distinct tables (vocabulary size)
        embed_dim:   Table embedding dimension (default 32)
        hidden_dim:  LSTM hidden state dimension (default 128)
        num_layers:  Stacked LSTM layers (default 2)
        dropout:     Dropout between LSTM layers (default 0.3)
    """

    def __init__(self, num_tables: int, embed_dim: int = 32,
                 hidden_dim: int = 128, num_layers: int = 2,
                 dropout: float = 0.3):
        super().__init__()

        self.num_tables = num_tables
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers

        # Table ID → dense embedding (like word embeddings in NLP)
        self.embedding = nn.Embedding(num_tables, embed_dim)

        # Stacked LSTM
        self.lstm = nn.LSTM(
            input_size=embed_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )

        self.dropout = nn.Dropout(dropout)
        self.classifier = nn.Linear(hidden_dim, num_tables)

        self._init_weights()

    def _init_weights(self):
        for name, param in self.lstm.named_parameters():
            if "weight_ih" in name:
                nn.init.xavier_uniform_(param.data)
            elif "weight_hh" in name:
                nn.init.orthogonal_(param.data)
            elif "bias" in name:
                nn.init.zeros_(param.data)
        nn.init.xavier_uniform_(self.classifier.weight)
        nn.init.zeros_(self.classifier.bias)

    def forward(self, x, hidden=None):
        """
        Args:
            x:      Token sequence [batch, seq_len] — table IDs
            hidden: Optional initial hidden state

        Returns:
            logits: [batch, seq_len, num_tables]
            hidden: final hidden state (for stateful inference)
        """
        emb = self.embedding(x)                    # [B, T, embed_dim]
        out, hidden = self.lstm(emb, hidden)        # [B, T, hidden_dim]
        out = self.dropout(out)
        logits = self.classifier(out)               # [B, T, num_tables]
        return logits, hidden

    def predict_next(self, sequence: list[int], device="cpu") -> tuple[int, torch.Tensor]:
        """
        Given a list of table IDs, predict the next table.

        Args:
            sequence: List of table IDs (e.g. [2, 0, 1, 2, 0])
            device:   torch device

        Returns:
            (predicted_table_id, probability_vector)
        """
        self.eval()
        with torch.no_grad():
            x = torch.tensor([sequence], dtype=torch.long).to(device)
            logits, _ = self.forward(x)
            last_logits = logits[0, -1, :]           # last position only
            probs = F.softmax(last_logits, dim=-1)
            pred  = probs.argmax().item()
        return pred, probs

    def forecast_hot_tables(self, seed_sequence: list[int],
                            steps: int = 10, device="cpu") -> dict:
        """
        Autoregressively forecast next `steps` table accesses.
        Returns access frequency → used to update node features for GNN.
        """
        self.eval()
        seq      = list(seed_sequence)
        counts   = {i: 0 for i in range(self.num_tables)}

        with torch.no_grad():
            for _ in range(steps):
                pred, _ = self.predict_next(seq[-20:], device=device)  # window=20
                counts[pred] += 1
                seq.append(pred)

        total = max(sum(counts.values()), 1)
        return {k: v / total for k, v in counts.items()}


# ── Data generation for LSTM training ─────────────────────────────────────────

def generate_query_sequences(table_names: list[str],
                             workload_weights: Optional[dict] = None,
                             n_sequences: int = 500,
                             seq_len: int = 30,
                             seed: int = 42) -> tuple:
    """
    Generate synthetic query log sequences for LSTM training.

    Each sequence = ordered list of table accesses (table IDs).
    Labels = next table in sequence (shifted by 1 — standard LM objective).

    Returns:
        sequences: np.array [n_sequences, seq_len]
        labels:    np.array [n_sequences, seq_len]  (next-token)
        table_names: list
    """
    rng = np.random.RandomState(seed)
    n   = len(table_names)
    idx = {name: i for i, name in enumerate(table_names)}

    # Default access probabilities based on typical e-commerce workload
    if workload_weights is None:
        # orders and order_items are accessed most frequently
        base_prob = np.ones(n) * 0.5
        for i, name in enumerate(table_names):
            if "order" in name:   base_prob[i] = 3.0
            if "customer" in name: base_prob[i] = 2.0
            if "product" in name:  base_prob[i] = 2.0
        base_prob /= base_prob.sum()
    else:
        base_prob = np.array([
            workload_weights.get(name, {}).get("access_freq", 0.1)
            for name in table_names
        ])
        base_prob = base_prob / base_prob.sum()

    sequences = []
    for _ in range(n_sequences):
        # Markov-like transitions: current table influences next
        seq  = [rng.choice(n, p=base_prob)]
        for _ in range(seq_len):
            curr = seq[-1]
            # Add locality: 30% chance to re-access same or related table
            if rng.random() < 0.3:
                # Stay in neighborhood (simulate transaction joining)
                neighbors = [curr, min(curr + 1, n - 1), max(curr - 1, 0)]
                nxt = rng.choice(neighbors)
            else:
                nxt = rng.choice(n, p=base_prob)
            seq.append(nxt)
        sequences.append(seq[:-1])   # input
        # labels = seq[1:]  → next token prediction

    X = np.array(sequences)                  # [N, seq_len]
    y = np.array([s[1:] + [s[-1]] for s in  # shift by 1
                  [list(rng.choice(n, p=base_prob, size=seq_len+1))
                   for _ in range(n_sequences)]])

    # Re-generate properly
    X_out, y_out = [], []
    for _ in range(n_sequences):
        full = list(rng.choice(n, p=base_prob, size=seq_len + 1))
        X_out.append(full[:-1])
        y_out.append(full[1:])

    return np.array(X_out), np.array(y_out), table_names


def count_parameters(model: nn.Module) -> int:
    return sum(p.numel() for p in model.parameters() if p.requires_grad)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data.loader import load_olist

    print("\n" + "=" * 60)
    print("  WorkloadLSTM — forward pass test")
    print("=" * 60 + "\n")

    tables, stats, fks = load_olist()
    table_names = list(stats.keys())
    n = len(table_names)

    model = WorkloadLSTM(num_tables=n, embed_dim=32, hidden_dim=128,
                         num_layers=2, dropout=0.3)
    print(f"  Parameters : {count_parameters(model):,}")
    print(f"  Tables     : {n}")

    # Forward pass
    dummy_seq = torch.randint(0, n, (4, 20))   # batch=4, seq_len=20
    logits, _ = model(dummy_seq)
    print(f"  Input  shape: {dummy_seq.shape}")
    print(f"  Output shape: {logits.shape}")

    # Next table prediction
    seed_seq = [2, 0, 1, 2, 1, 3, 0]
    pred_id, probs = model.predict_next(seed_seq)
    print(f"\n  Seed sequence: {[table_names[i] for i in seed_seq]}")
    print(f"  Predicted next: {table_names[pred_id]}  ({probs[pred_id]*100:.1f}%)")

    # Forecast
    forecast = model.forecast_hot_tables(seed_seq, steps=50)
    hot = sorted(forecast.items(), key=lambda x: -x[1])[:3]
    print(f"\n  Forecasted hot tables (next 50 queries):")
    for tid, freq in hot:
        print(f"    {table_names[tid]:<45}  {freq*100:.1f}%")
