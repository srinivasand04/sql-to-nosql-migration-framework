"""
models/gat.py
─────────────
Graph Attention Network for SQL→NoSQL migration strategy classification.

Directly ported from TacticAI-Lite's models/gat.py:
  TacticAI:  GAT predicts corner kick receiver using attention over players
  Thesis:    GAT predicts migration strategy using attention over FK neighbors

Architecture:
  GATConv(8→64, heads=4, concat=True)   + BatchNorm1d(256) + ELU + Dropout(0.2)
  GATConv(256→64, heads=4, concat=True) + BatchNorm1d(256) + ELU + Dropout(0.2)
  GATConv(256→32, heads=1, concat=False)
  Linear(32→3) → CrossEntropyLoss

Why GAT matters here:
  Not all FK neighbors are equally important.
  GAT learns: "the parent table matters more than a distant lookup table."
  e.g. order_items → orders should get more attention weight than
       order_items → geolocation (via long FK chain)
  Attention weights are visualizable → thesis explainability contribution.

Reference: Veličković et al. (2018), ICLR
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GATConv


class SchemaGAT(nn.Module):
    """
    GAT for NoSQL migration strategy classification.

    Args:
        in_channels:  Node feature dimension (default 8)
        hidden:       Hidden dim per attention head (default 64)
        heads:        Number of attention heads (default 4)
        num_classes:  Output classes (default 3)
        dropout:      Dropout rate (default 0.2)
    """

    def __init__(self, in_channels: int = 8, hidden: int = 64,
                 heads: int = 4, num_classes: int = 3, dropout: float = 0.2):
        super().__init__()

        self.dropout = dropout

        # Layer 1: multi-head attention, concatenate heads
        self.conv1 = GATConv(in_channels, hidden, heads=heads, concat=True,
                             dropout=dropout)
        self.bn1   = nn.BatchNorm1d(hidden * heads)

        # Layer 2: multi-head attention, concatenate heads
        self.conv2 = GATConv(hidden * heads, hidden, heads=heads, concat=True,
                             dropout=dropout)
        self.bn2   = nn.BatchNorm1d(hidden * heads)

        # Layer 3: single head, no concat → output dim = hidden//2
        self.conv3 = GATConv(hidden * heads, hidden // 2, heads=1, concat=False,
                             dropout=dropout)

        self.classifier = nn.Linear(hidden // 2, num_classes)

        self._init_weights()

    def _init_weights(self):
        nn.init.xavier_uniform_(self.classifier.weight)
        nn.init.zeros_(self.classifier.bias)

    def forward(self, x, edge_index, edge_attr=None, return_attention=False):
        """
        Args:
            x:                Node features [N, in_channels]
            edge_index:       Edge indices  [2, E]
            edge_attr:        Edge features (unused — GAT learns its own weights)
            return_attention: If True, also return attention weights from layer 3

        Returns:
            logits [N, num_classes]
            (optionally) attention weights from final layer
        """
        # Layer 1
        x = self.conv1(x, edge_index)
        x = self.bn1(x)
        x = F.elu(x)
        x = F.dropout(x, p=self.dropout, training=self.training)

        # Layer 2
        x = self.conv2(x, edge_index)
        x = self.bn2(x)
        x = F.elu(x)
        x = F.dropout(x, p=self.dropout, training=self.training)

        # Layer 3 — optionally capture attention weights for visualization
        if return_attention:
            x, (attn_edge_index, attn_weights) = self.conv3(
                x, edge_index, return_attention_weights=True
            )
            x = F.elu(x)
            logits = self.classifier(x)
            return logits, (attn_edge_index, attn_weights)
        else:
            x = self.conv3(x, edge_index)
            x = F.elu(x)
            logits = self.classifier(x)
            return logits

    def embed(self, x, edge_index):
        """Return node embeddings before classification head."""
        with torch.no_grad():
            x = F.elu(self.bn1(self.conv1(x, edge_index)))
            x = F.elu(self.bn2(self.conv2(x, edge_index)))
            x = F.elu(self.conv3(x, edge_index))
        return x

    def predict_proba(self, x, edge_index):
        """Return softmax probabilities [N, 3]."""
        logits = self.forward(x, edge_index)
        return F.softmax(logits, dim=-1)

    def get_attention_weights(self, x, edge_index):
        """
        Return attention weights for all edges (layer 3).
        Used for visualization — analogous to TacticAI's attention heatmaps.

        Returns:
            attn_edge_index: [2, E]
            attn_weights:    [E, 1] — how much each table attends to each neighbor
        """
        self.eval()
        with torch.no_grad():
            _, (ei, aw) = self.forward(x, edge_index, return_attention=True)
        return ei, aw


def count_parameters(model: nn.Module) -> int:
    return sum(p.numel() for p in model.parameters() if p.requires_grad)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data.loader import load_olist
    from graph.builder import build_graph

    print("\n" + "=" * 60)
    print("  SchemaGAT — forward pass test")
    print("=" * 60 + "\n")

    tables, stats, fks = load_olist()
    g = build_graph(stats, fks, seed=0)

    model = SchemaGAT(in_channels=8, hidden=64, heads=4, num_classes=3, dropout=0.2)
    print(f"  Parameters: {count_parameters(model):,}")

    model.eval()
    with torch.no_grad():
        logits = model(g.x, g.edge_index)
        probs  = model.predict_proba(g.x, g.edge_index)

    print(f"  Input  shape: {g.x.shape}")
    print(f"  Output shape: {logits.shape}")

    print(f"\n  Node predictions with attention:")
    ei, aw = model.get_attention_weights(g.x, g.edge_index)
    label_names = ["EMBED", "REFERENCE", "DENORMALIZE"]
    for i, name in enumerate(g.table_names):
        pred = probs[i].argmax().item()
        conf = probs[i].max().item()
        print(f"    {name:<45}  {label_names[pred]:<12}  ({conf*100:.1f}%)")

    print(f"\n  Attention weight shape: {aw.shape}")
    print(f"  Attention range: [{aw.min():.4f}, {aw.max():.4f}]")
