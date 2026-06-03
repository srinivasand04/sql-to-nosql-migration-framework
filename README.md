# SQL-to-NoSQL Migration via GNN + Deep Learning + ML

**Master's Thesis — Data Engineering & AI, ESILV Paris**

Automated SQL→MongoDB migration using Graph Neural Networks (GCN/GAT),
an LSTM workload forecaster, and ML-based cloud cost prediction —
applied to the real **Olist Brazilian E-Commerce dataset** (Kaggle, 1.55M rows, 9 tables).

---

## Results (verified run — June 2026)

### Migration Strategy Classification

| Model | Val Accuracy | Parameters | Stopped At | Notes |
|---|---|---|---|---|
| RandomForest (baseline) | 95.0% | — | — | No graph structure |
| XGBoost (baseline) | **98.0%** | — | — | Flat node features only |
| GCN | 95.6% | 7,171 | Epoch 22 | GCNConv × 3, BatchNorm, Dropout |
| **GAT** | **98.0%** | 78,531 | Epoch 44 | 4-head attention, learned FK weights |

**GAT matches XGBoost at 98.0%** using graph structure (FK neighbor attention) vs. flat features — demonstrating that graph-based reasoning achieves equivalent performance with architectural justification: GAT learns *which FK neighbors matter*, something XGBoost cannot express.

**Label distribution** across 300 workload scenarios (2,700 node-labels total):
- REFERENCE: 1,990 (73.7%) — separate collections
- EMBED: 658 (24.4%) — child embedded in parent
- DENORMALIZE: 52 (1.9%) — snapshot fields copied

### LSTM Workload Forecaster

| Metric | Value |
|---|---|
| Val accuracy | 17.0% |
| Random baseline (1/9 tables) | 11.1% |
| Improvement over random | +5.9pp |
| Parameters | 216,489 |
| Architecture | Embedding(9→32) → LSTM(128, 2-layer) → Linear(9) |

LSTM predicts next table access from query history. 17% accuracy on near-random sequences
is expected — real query logs with temporal patterns yield significantly higher accuracy.

### Cloud Cost Prediction (XGBoost Multi-output Regressor)

| Provider | R² | MAE ($/month) |
|---|---|---|
| AWS (DynamoDB) | 0.9726 | $0.21 |
| Azure (Cosmos DB) | 0.9717 | $0.24 |
| GCP (Firestore) | 0.9689 | $0.07 |
| DigitalOcean (MongoDB) | 0.9797 | $0.02 |
| **Average R²** | **0.9732** | — |

**Olist workload estimate** (2.3 GB storage, 10M reads/mo, 2.5M writes/mo):

| Provider | Est. Monthly Cost |
|---|---|
| AWS | $6.62 |
| Azure | $7.60 |
| GCP | $1.94 |
| **DigitalOcean** | **$0.54 ← best** |

---

## Architecture

```
Olist Dataset (Kaggle) — 9 real tables, 1,550,922 rows
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 1 — Data Ingestion                               │
│  kaggle API auto-download → 9 CSV tables → disk cache  │
│  data/loader.py                                         │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 2 — Schema Graph Construction                    │
│  Tables = nodes (8 features each)                       │
│  FK relationships = edges (3 features each)             │
│  300 workload-variation graphs for training             │
│  graph/builder.py → PyTorch Geometric Data objects      │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 3 — ML Baseline                                  │
│  XGBoost 98.0%  |  RandomForest 95.0%                  │
│  Flat node features only — no graph structure           │
│  ml/baseline.py                                         │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 4 — Graph Neural Networks  ← core contribution   │
│                                                         │
│  GCN  95.6% — GCNConv(8→64→64→32) + BN + Dropout      │
│               7,171 parameters — early stop ep 22       │
│                                                         │
│  GAT  98.0% — GATConv(8→256→256→32, heads=4) + BN      │
│               78,531 parameters — early stop ep 44      │
│               Attention weights show FK importance      │
│                                                         │
│  Output: EMBED / REFERENCE / DENORMALIZE per table      │
│  Trained with MLflow experiment tracking                │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 5 — Deep Learning: LSTM Workload Forecaster      │
│  Embedding(9→32) → LSTM(128, 2-layer) → Linear(9)      │
│  216,489 params  |  17.0% accuracy (random: 11.1%)     │
│  Predicts next hot table from query sequence history    │
│  models/lstm_forecaster.py                              │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 6 — ML Cost Prediction                           │
│  XGBoost multi-output → 4 cloud providers               │
│  Avg R² = 0.9732  |  Features include GNN schema_type  │
│  SHAP explainability: which features drive cost         │
│  ml/cost_predictor.py                                   │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 7 — What-If Simulation                           │
│  Perturb workload → measure Δmigration decisions        │
│  e.g. orders write_ratio 0.3→0.75: EMBED→REFERENCE     │
│  Mirrors TacticAI player perturbation simulator         │
│  whatif/simulator.py                                    │
└─────────────────────────────────────────────────────────┘
```

---

## Research Contribution

Applies GNN reasoning from sports analytics (TacticAI, Nature Communications 2024)
to database schema migration:

| TacticAI (DeepMind × Liverpool FC) | This Thesis |
|---|---|
| Players = nodes | Tables = nodes |
| Proximity edges (10m radius) | FK relationship edges |
| Predict corner kick receiver | Predict EMBED / REFERENCE / DENORMALIZE |
| GCN / GAT (PyTorch Geometric) | GCN / GAT — same framework |
| What-If: move player → Δprobability | What-If: change workload → Δdecision |

**Key insight:** migration decisions are neighbor-dependent — whether to embed
`order_items` into `orders` depends on how `orders` connects to `customers`,
`payments`, and `reviews`. GAT learns this via attention weights over FK edges.

---

## Project Structure

```
SQL_NoSQL/
├── data/
│   ├── loader.py              ← Kaggle auto-download + 9-table loader + disk cache
│   └── olist_raw/             ← Olist CSVs (auto-downloaded, not tracked in git)
│
├── graph/
│   └── builder.py             ← Schema → PyG Data graph (300 workload scenarios)
│
├── models/
│   ├── gcn.py                 ← GCN: 3-layer GCNConv + BatchNorm + Dropout (7k params)
│   ├── gat.py                 ← GAT: 4-head attention + attention weight export (78k params)
│   └── lstm_forecaster.py     ← LSTM workload forecaster — 216k params
│
├── ml/
│   ├── baseline.py            ← XGBoost 98.0% + RandomForest 95.0% benchmark
│   └── cost_predictor.py      ← Multi-output XGBoost cost predictor + SHAP (avg R²=0.97)
│
├── train/
│   └── trainer.py             ← GNN + LSTM training loops, early stopping, MLflow
│
├── whatif/
│   └── simulator.py           ← Workload perturbation simulator
│
├── outputs/
│   ├── figures/               ← Training curves, attention heatmaps
│   └── results/               ← gcn_best.pt, gat_best.pt, lstm_best.pt, pipeline_results.json
│
├── main.py                    ← Full 7-step pipeline
└── requirements.txt
```

---

## Dataset

**Olist Brazilian E-Commerce** — [kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
License: CC BY-NC-SA 4.0

| Table | Rows | FK Out |
|---|---|---|
| customers | 99,441 | 1 |
| orders | 99,441 | 1 |
| order_items | 112,650 | 3 |
| order_reviews | 99,224 | 1 |
| order_payments | 103,886 | 1 |
| products | 32,951 | 1 |
| sellers | 3,095 | 1 |
| product_category_name_translation | 71 | 0 |
| geolocation | 1,000,163 | 0 |
| **Total** | **1,550,922** | **9 FK edges** |

---

## Tech Stack

| Category | Tools | Version |
|---|---|---|
| GNN | PyTorch Geometric — GCNConv, GATConv | ≥2.3.0 |
| Deep Learning | PyTorch — LSTM, Embedding layers | 2.12.0 |
| Classical ML | XGBoost, RandomForest | XGB ≥1.7 |
| Explainability | SHAP — TreeExplainer | ≥0.43 |
| Experiment Tracking | MLflow | ≥2.7 |
| Graph Construction | NetworkX | ≥3.1 |
| Data | Kaggle API, pandas, numpy | pandas ≥2.0 |
| Visualization | matplotlib, seaborn, Streamlit | — |
| Language | Python | 3.12 |

---

## Quick Start

```bash
# 1. Create virtual environment
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux

# 2. Install PyTorch (CPU)
python -m pip install torch --index-url https://download.pytorch.org/whl/cpu
python -m pip install torch-geometric

# 3. Install remaining dependencies
python -m pip install -r requirements.txt

# 4. Authenticate Kaggle (dataset auto-downloads on first run)
kaggle auth login

# 5. Run full pipeline
python main.py

# 6. View MLflow experiments
mlflow ui   # → http://localhost:5000
```

### Run individual modules
```bash
python -m data.loader            # Load + cache Olist tables
python -m graph.builder          # Build PyG schema graphs
python -m models.gcn             # GCN forward pass test
python -m models.gat             # GAT forward pass + attention weights
python -m models.lstm_forecaster # LSTM sequence prediction test
python -m ml.baseline            # XGBoost + RF benchmark
python -m ml.cost_predictor      # Cloud cost prediction + SHAP
python -m whatif.simulator       # Workload perturbation demo
```

---

## References

1. Kipf & Welling (2017). *Semi-Supervised Classification with Graph Convolutional Networks*. ICLR. [arxiv.org/abs/1609.02907](https://arxiv.org/abs/1609.02907)
2. Veličković et al. (2018). *Graph Attention Networks*. ICLR. [arxiv.org/abs/1710.10903](https://arxiv.org/abs/1710.10903)
3. Wang et al. (2024). *TacticAI: an AI assistant for football tactics*. Nature Communications. [doi.org/10.1038/s41467-024-45965-x](https://doi.org/10.1038/s41467-024-45965-x)
4. Olist Brazilian E-Commerce Public Dataset. Kaggle. CC BY-NC-SA 4.0.

---

## License

Research prototype — Master's Thesis, ESILV Paris. Academic use only.
