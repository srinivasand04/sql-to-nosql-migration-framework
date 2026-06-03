# SQL-to-NoSQL Migration via GNN + Deep Learning + ML

**Master's Thesis — Data Engineering & AI, ESILV Paris**

Automated SQL→MongoDB migration using Graph Neural Networks (GCN/GAT),
an LSTM workload forecaster, and ML-based cloud cost prediction —
applied to the real Olist Brazilian E-Commerce dataset (Kaggle).

---

## Architecture Overview

```
Olist Dataset (Kaggle, 9 real tables, ~600k rows)
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 1 — Data Ingestion                               │
│  kaggle API → 9 CSV tables → disk cache (olist_cache)  │
│  data/loader.py                                         │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 2 — Schema Graph Construction                    │
│  Tables = nodes  |  FK relationships = edges            │
│  Node features: row_count, write_ratio, cardinality...  │
│  Edge features: join_frequency, fan_out, is_hot_join    │
│  300 workload-variation graphs for training             │
│  graph/builder.py → PyTorch Geometric Data objects      │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 3 — ML Baseline                                  │
│  XGBoost + RandomForest on flat node features           │
│  (no graph structure — benchmark for GNN to beat)       │
│  ml/baseline.py                                         │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 4 — Graph Neural Networks  ← core contribution   │
│                                                         │
│  GCN: GCNConv(8→64) + BN + ReLU + Dropout(0.3)         │
│       GCNConv(64→64) + BN + ReLU + Dropout(0.3)        │
│       GCNConv(64→32) + Linear(32→3)                     │
│       6,913 parameters                                  │
│                                                         │
│  GAT: GATConv(8→64, heads=4) + BN + ELU               │
│       GATConv(256→64, heads=4) + BN + ELU              │
│       GATConv(256→32, heads=1) + Linear(32→3)           │
│       Attention = learned FK neighbor importance        │
│                                                         │
│  Output: EMBED / REFERENCE / DENORMALIZE per table      │
│  models/gcn.py  |  models/gat.py                        │
│  train/trainer.py — early stopping, MLflow logging      │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 5 — Deep Learning: LSTM Workload Forecaster      │
│  Embedding(9→32) → LSTM(128, 2 layers) → Linear(9)     │
│  Predicts next table to be accessed from query history  │
│  Enables proactive migration before tables become hot   │
│  models/lstm_forecaster.py                              │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 6 — ML Cost Prediction                           │
│  XGBoost multi-output regressor → cost per cloud        │
│  Features: storage, reads, writes, schema_type (GNN)    │
│  SHAP explainability: which features drive cost         │
│  ml/cost_predictor.py                                   │
└──────────────────────┬──────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  LAYER 7 — What-If Simulation                           │
│  Perturb workload → measure Δmigration decisions        │
│  e.g. "orders write_ratio 0.3→0.8" → which tables      │
│  flip from EMBED to REFERENCE?                          │
│  whatif/simulator.py                                    │
└─────────────────────────────────────────────────────────┘
```

---

## Research Contribution

This project applies GNN reasoning proven in sports analytics (TacticAI,
Nature Communications 2024) to database schema migration:

| TacticAI               | This thesis                         |
|------------------------|-------------------------------------|
| Players = nodes        | Tables = nodes                      |
| Proximity edges        | FK relationship edges               |
| Corner kick receiver   | embed / reference / denormalize     |
| GCN / GAT (PyG)        | GCN / GAT (same framework)          |
| What-If: move player   | What-If: change workload            |

Key insight: migration decisions are **neighbor-dependent** — whether to embed
`order_items` into `orders` depends on how `orders` connects to `customers`,
`payments`, and `reviews`. GNNs capture this; flat ML cannot.

---

## Project Structure

```
SQL_NoSQL/
│
├── data/
│   ├── loader.py              ← Kaggle download + 9-table loader + cache
│   └── olist_raw/             ← Olist CSVs (auto-downloaded or place manually)
│
├── graph/
│   └── builder.py             ← Schema → PyG Data graph (mirrors TacticAI builder.py)
│
├── models/
│   ├── gcn.py                 ← GCN: 3-layer GCNConv + BatchNorm + Dropout
│   ├── gat.py                 ← GAT: 4-head attention, attention weight export
│   └── lstm_forecaster.py     ← LSTM workload forecaster (Deep Learning layer)
│
├── ml/
│   ├── baseline.py            ← XGBoost + RandomForest baseline
│   └── cost_predictor.py      ← ML cost predictor + SHAP explainability
│
├── train/
│   └── trainer.py             ← GNN + LSTM training loops, early stopping, MLflow
│
├── whatif/
│   └── simulator.py           ← Workload perturbation (mirrors TacticAI simulator.py)
│
├── outputs/
│   ├── figures/               ← Training curves, attention plots
│   └── results/               ← Saved models + pipeline_results.json
│
├── main.py                    ← Full pipeline runner
└── requirements.txt
```

---

## Dataset

**Olist Brazilian E-Commerce Public Dataset**
[kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Table | Rows | FK relationships |
|---|---|---|
| customers | 99,441 | — |
| orders | 99,441 | → customers |
| order_items | 112,650 | → orders, products, sellers |
| order_reviews | 99,224 | → orders |
| order_payments | 103,886 | → orders |
| products | 32,951 | → category_translation |
| sellers | 3,095 | → geolocation |
| product_category_name_translation | 71 | — |
| geolocation | 1,000,163 | — |

9 tables, 9 FK edges → schema graph. 300 workload-variation graphs for training.

---

## Quick Start

### 1. Install dependencies

```bash
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
pip install torch-geometric
pip install -r requirements.txt
```

### 2. Set up Kaggle API (optional — for auto-download)

```bash
# Place your kaggle.json in ~/.kaggle/kaggle.json
# Download from: kaggle.com → Account → API → Create New Token
```

Or manually download the dataset and place CSVs in `data/olist_raw/`.

### 3. Run full pipeline

```bash
python main.py
```

### 4. Run individual modules

```bash
python -m data.loader            # Test data loading
python -m graph.builder          # Test graph construction
python -m models.gcn             # Test GCN forward pass
python -m models.gat             # Test GAT forward pass
python -m models.lstm_forecaster # Test LSTM forward pass
python -m ml.baseline            # Run ML baseline only
python -m ml.cost_predictor      # Run cost predictor only
python -m whatif.simulator       # Test What-If simulator
```

### 5. View MLflow experiments

```bash
mlflow ui
# Open http://localhost:5000
```

---

## Tech Stack

| Category | Tools |
|---|---|
| GNN | PyTorch Geometric — GCNConv, GATConv |
| Deep Learning | PyTorch — LSTM, Embedding |
| Classical ML | XGBoost, RandomForest, scikit-learn |
| Explainability | SHAP |
| Experiment tracking | MLflow |
| Graph construction | NetworkX, PyTorch Geometric |
| Data | Kaggle API, pandas |
| Visualization | matplotlib, seaborn, Streamlit |

---

## Expected Output

```
STEP 1: DATA LOADING
  9 tables loaded | 600k+ total rows

STEP 2: SCHEMA GRAPH CONSTRUCTION
  9 nodes | 18 edges | 300 training graphs

STEP 3: ML BASELINE
  XGBoost accuracy : 71.2%
  RandomForest     : 68.4%

STEP 4: GNN TRAINING
  GCN val accuracy : 79.3%  (early stop ep 87)
  GAT val accuracy : 76.1%  (early stop ep 54)
  Best: GCN (+8.1pp vs XGBoost baseline)

STEP 5: LSTM WORKLOAD FORECASTER
  Val accuracy : 43.7%  (random baseline: 11.1%)

STEP 6: CLOUD COST PREDICTION
  XGBoost R² avg : 0.9821
  Best provider  : DigitalOcean ($0.0031/month)

STEP 7: WHAT-IF SIMULATION
  orders write_ratio 0.3→0.8:
    order_items: EMBED → REFERENCE  ⚠ CHANGED
```

---

## References

1. Kipf & Welling (2017). *Semi-Supervised Classification with GCNs*. ICLR.
2. Veličković et al. (2018). *Graph Attention Networks*. ICLR.
3. Wang et al. (2024). *TacticAI: an AI assistant for football tactics*. Nature Communications.
4. Olist Brazilian E-Commerce Dataset. Kaggle.

---

## License

Research prototype — Master's thesis, ESILV Paris. Academic use.
