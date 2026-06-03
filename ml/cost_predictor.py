"""
ml/cost_predictor.py
─────────────────────
ML-based cloud cost predictor — replaces the formula from cost_estimator.py.

Old approach: Cost = α·Storage + β·Reads + γ·Writes + ...  (hardcoded constants)
New approach: XGBoost multi-output regressor trained on realistic cost samples.

Why ML for cost?
  Cloud pricing is non-linear:
    - AWS DynamoDB has tiered read/write pricing
    - MongoDB Atlas jumps at tier boundaries (M10→M20→M30)
    - GCP Firestore has free tier + per-op pricing
  A simple linear formula misses these step-functions.
  XGBoost captures them well with tree splits.

Features:
  storage_gb, read_ops_millions, write_ops_millions,
  network_gb, schema_type (0=embed/1=ref/2=denorm, from GNN),
  n_collections, avg_doc_size_kb

Targets (4 cloud providers):
  aws_cost_usd, azure_cost_usd, gcp_cost_usd, digitalocean_cost_usd

Training data:
  Synthetically generated from realistic pricing tiers.
  (Real billing data not available — this is standard in academic cost modeling)
"""

import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.multioutput import MultiOutputRegressor
import shap
import warnings
warnings.filterwarnings("ignore")


# ── Cloud pricing parameters (current 2024 approximate rates) ─────────────────
PRICING = {
    "aws": {          # DynamoDB on-demand
        "storage_per_gb":  0.25,
        "read_per_million": 0.25,
        "write_per_million": 1.25,
        "network_per_gb":  0.09,
        "free_tier_reads_m": 0.0,
    },
    "azure": {        # Cosmos DB serverless
        "storage_per_gb":  0.25,
        "read_per_million": 0.28,
        "write_per_million": 1.40,
        "network_per_gb":  0.087,
        "free_tier_reads_m": 0.0,
    },
    "gcp": {          # Firestore
        "storage_per_gb":  0.18,
        "read_per_million": 0.06,
        "write_per_million": 0.18,
        "network_per_gb":  0.12,
        "free_tier_reads_m": 0.05,  # 50k free reads/day ≈ 1.5M/month
    },
    "digitalocean": { # Managed MongoDB
        "storage_per_gb":  0.10,
        "read_per_million": 0.01,
        "write_per_million": 0.05,
        "network_per_gb":  0.01,
        "free_tier_reads_m": 0.0,
    },
}


def _compute_true_cost(storage_gb, read_m, write_m, network_gb,
                       schema_type, n_collections, avg_doc_kb, provider):
    """
    Compute cost using realistic pricing formula (our 'ground truth' for training).
    Includes non-linearities: tier jumps, free tiers, schema-type penalties.
    """
    p = PRICING[provider]

    # Schema type multiplier (embed = smaller docs = less storage; denorm = more)
    schema_mult = {0: 0.8, 1: 1.0, 2: 1.3}.get(schema_type, 1.0)

    # Atlas tier jump simulation (MongoDB Atlas M10=$57/mo for first 10GB)
    atlas_base = 0.0
    if provider in ("aws", "azure") and storage_gb > 5:
        atlas_base = 2.50   # simulate tier base fee

    storage_cost = storage_gb * p["storage_per_gb"] * schema_mult
    read_cost    = max(read_m - p["free_tier_reads_m"], 0) * p["read_per_million"]
    write_cost   = write_m * p["write_per_million"]
    network_cost = network_gb * p["network_per_gb"]

    # Collection overhead (more collections = more index memory = slight cost)
    collection_overhead = n_collections * 0.002

    total = atlas_base + storage_cost + read_cost + write_cost + network_cost + collection_overhead

    # Add some realistic noise (billing varies ±5%)
    return max(total * np.random.uniform(0.95, 1.05), 0.001)


def generate_training_data(n_samples: int = 2000, seed: int = 42) -> tuple:
    """
    Generate synthetic cost training data covering realistic parameter ranges.
    """
    rng = np.random.RandomState(seed)

    X, y = [], []
    for _ in range(n_samples):
        storage_gb    = rng.exponential(2.0)          # most workloads < 10GB
        read_m        = rng.exponential(5.0)
        write_m       = rng.exponential(1.5)
        network_gb    = rng.exponential(3.0)
        schema_type   = rng.randint(0, 3)             # 0=embed, 1=ref, 2=denorm
        n_collections = rng.randint(3, 15)
        avg_doc_kb    = rng.uniform(1, 50)

        features = [storage_gb, read_m, write_m, network_gb,
                    schema_type, n_collections, avg_doc_kb]
        costs    = [
            _compute_true_cost(storage_gb, read_m, write_m, network_gb,
                               schema_type, n_collections, avg_doc_kb, p)
            for p in ["aws", "azure", "gcp", "digitalocean"]
        ]
        X.append(features)
        y.append(costs)

    return np.array(X), np.array(y)


FEATURE_NAMES = [
    "storage_gb", "read_ops_millions", "write_ops_millions",
    "network_gb", "schema_type", "n_collections", "avg_doc_size_kb"
]
TARGET_NAMES  = ["aws", "azure", "gcp", "digitalocean"]


class CloudCostPredictor:
    """
    XGBoost-based multi-output cost predictor + SHAP explainability.
    """

    def __init__(self):
        base = xgb.XGBRegressor(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            verbosity=0,
        )
        self.model = MultiOutputRegressor(base, n_jobs=-1)
        self._fitted = False

    def fit(self, X_train, y_train):
        self.model.fit(X_train, y_train)
        self._fitted = True
        return self

    def predict(self, X) -> np.ndarray:
        """Returns cost predictions [N, 4] for all 4 providers."""
        return np.maximum(self.model.predict(X), 0.0)

    def predict_single(self, storage_gb: float, read_m: float, write_m: float,
                       network_gb: float, schema_type: int,
                       n_collections: int = 5, avg_doc_kb: float = 10.0) -> dict:
        """Convenience method for single workload prediction."""
        x = np.array([[storage_gb, read_m, write_m, network_gb,
                       schema_type, n_collections, avg_doc_kb]])
        preds = self.predict(x)[0]
        return dict(zip(TARGET_NAMES, preds))

    def shap_explain(self, X_sample: np.ndarray, provider_idx: int = 0):
        """
        Compute SHAP values for a given provider model.
        Returns shap_values array — use viz/viz.py to plot.
        """
        estimator = self.model.estimators_[provider_idx]
        explainer  = shap.TreeExplainer(estimator)
        shap_vals  = explainer.shap_values(X_sample)
        return shap_vals, explainer


def train_cost_predictor(n_samples: int = 2000) -> tuple:
    """Train and evaluate the cost predictor. Returns (model, metrics)."""

    print("  Generating cost training data ...")
    X, y = generate_training_data(n_samples=n_samples)

    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"  Train: {len(X_tr)}  Test: {len(X_te)}")

    predictor = CloudCostPredictor()
    print("  Fitting XGBoost multi-output regressor ...")
    predictor.fit(X_tr, y_tr)

    y_pred = predictor.predict(X_te)

    metrics = {}
    print(f"\n  {'Provider':<15}  {'MAE ($/mo)':>12}  {'R²':>8}")
    print("  " + "─" * 40)
    for i, name in enumerate(TARGET_NAMES):
        mae = mean_absolute_error(y_te[:, i], y_pred[:, i])
        r2  = r2_score(y_te[:, i], y_pred[:, i])
        metrics[name] = {"mae": mae, "r2": r2}
        print(f"  {name:<15}  ${mae:>10.4f}  {r2:>8.4f}")

    return predictor, metrics


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  Cloud Cost Predictor (ML) — test run")
    print("=" * 60 + "\n")

    predictor, metrics = train_cost_predictor()

    print("\n  Sample prediction:")
    result = predictor.predict_single(
        storage_gb=2.5, read_m=10.0, write_m=2.5,
        network_gb=5.0, schema_type=0, n_collections=5
    )
    best = min(result, key=result.get)
    for provider, cost in result.items():
        marker = "  ← best" if provider == best else ""
        print(f"    {provider:<15}  ${cost:.4f}/month{marker}")
