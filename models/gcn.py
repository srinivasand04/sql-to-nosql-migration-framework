"""
models/gcn.py
─────────────
Graph Convolutional Network for SQL→NoSQL migration strategy classification.

Directly ported from TacticAI-Lite's models/gcn.py:
  TacticAI:  GCN predicts corner kick receiver (node → 1 probability)
  Thesis:    GCN predicts migration strategy per table (node → 3 classes)

Architecture:
  GCNConv(8→64)  + BatchNorm1d(64)  + ReLU + Dropout(0.3)
  GCNConv(64→64) + BatchNorm1d(64)  + ReLU + Dropout(0.3)
  GCNConv(64→32) + ReLU
  Linear(32→3)   → CrossEntropyLoss

Why GCN works here:
  Each table's embedding absorbs context from its FK neighbors.
  After 3 layers: a table "knows" about tables 3 hops away.
  e.g. order_items sees: orders → customers AND products → categories
  → richer embedding than flat XGBoost features.

Reference: Kipf & Welling (2017), ICLR
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv


class SchemaGCN(nn.Module):
    """
    GCN for NoSQL migration strategy classification.

    Args:
        in_channels:  Number of node features (default 8)
        hidden:       Hidden dimension (default 64)
        num_classes:  Output classes — embed/reference/denormalize (default 3)
        dropout:      Dropout rate (default 0.3)
    """

    def __init__(self, in_channels: int = 8, hidden: int = 64,
                 num_classes: int = 3, dropout: float = 0.3):
        super().__init__()

        self.conv1 = GCNConv(in_channels, hidden)
        self.bn1   = nn.BatchNorm1d(hidden)

        self.conv2 = GCNConv(hidden, hidden)
        self.bn2   = nn.BatchNorm1d(hidden)

        self.conv3 = GCNConv(hidden, hidden // 2)

        self.classifier = nn.Linear(hidden // 2, num_classes)

        self.dropout = dropout

        self._init_weights()

    def _init_weights(self):
        nn.init.xavier_uniform_(self.classifier.weight)
        nn.init.zeros_(self.classifier.bias)

    def forward(self, x, edge_index, edge_attr=None):
        """
        Args:
            x:          Node features  [N, in_channels]
            edge_index: Edge indices   [2, E]
            edge_attr:  Edge features  [E, edge_feat_dim] (ignored by GCN,
                        used by GAT — kept for unified API)

        Returns:
            logits: [N, num_classes]
        """
        # Layer 1
        x = self.conv1(x, edge_index)
        x = self.bn1(x)
        x = F.relu(x)
        x = F.dropout(x, p=self.dropout, training=self.training)

        # Layer 2
        x = self.conv2(x, edge_index)
        x = self.bn2(x)
        x = F.relu(x)
        x = F.dropout(x, p=self.dropout, training=self.training)

        # Layer 3
        x = self.conv3(x, edge_index)
        x = F.relu(x)

        # Classification head
        logits = self.classifier(x)
        return logits

    def embed(self, x, edge_index):
        """Return node embeddings (before classification head) — for visualization."""
        with torch.no_grad():
            x = F.relu(self.bn1(self.conv1(x, edge_index)))
            x = F.relu(self.bn2(self.conv2(x, edge_index)))
            x = F.relu(self.conv3(x, edge_index))
        return x

    def predict_proba(self, x, edge_index):
        """Return softmax probabilities [N, 3]."""
        logits = self.forward(x, edge_index)
        return F.softmax(logits, dim=-1)


def count_parameters(model: nn.Module) -> int:
    return sum(p.numel() for p in model.parameters() if p.requires_grad)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data.loader import load_olist
    from graph.builder import build_graph

    print("\n" + "=" * 60)
    print("  SchemaGCN — forward pass test")
    print("=" * 60 + "\n")

    tables, stats, fks = load_olist()
    g = build_graph(stats, fks, seed=0)

    model = SchemaGCN(in_channels=8, hidden=64, num_classes=3, dropout=0.3)
    print(f"  Parameters: {count_parameters(model):,}")

    model.eval()
    with torch.no_grad():
        logits = model(g.x, g.edge_index)
        probs  = model.predict_proba(g.x, g.edge_index)

    print(f"  Input  shape: {g.x.shape}")
    print(f"  Output shape: {logits.shape}")
    print(f"\n  Node predictions:")
    label_names = ["EMBED", "REFERENCE", "DENORMALIZE"]
    for i, name in enumerate(g.table_names):
        pred = probs[i].argmax().item()
        conf = probs[i].max().item()
        print(f"    {name:<45}  {label_names[pred]:<12}  ({conf*100:.1f}%)")
