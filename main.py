#!/usr/bin/env python3
"""
main.py
───────
Master's Thesis Prototype:
  Intelligent SQL-to-NoSQL Schema Transformation & Cost Estimation

Pipeline:
  1. Generate / load dataset
  2. Schema Profiler
  3. Workload Analyzer
  4. Mapping Engine
  5. NoSQL Schema Generator
  6. Cost Estimator
  7. Migration recommendation + export
"""

import os
import sys
import json
import time
import subprocess

# ── Path setup ────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
SRC_DIR    = os.path.join(BASE_DIR, "src")
DATA_DIR = os.path.join(BASE_DIR, "ecommerce_dataset")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
##sys.path.insert(0, SRC_DIR)

from schema_profiler    import SchemaProfiler
from workload_analyzer  import WorkloadAnalyzer
from mapping_engine     import MappingEngine
from cost_estimator     import CostEstimator
from nosql_generator    import NoSQLGenerator
from cloud_cost_comparator     import CloudCostComparator


# ── Helpers ───────────────────────────────────────────────────────────────────

def separator(title: str = ""):
    width = 62
    if title:
        pad = (width - len(title) - 2) // 2
        print(f"\n{'─' * pad} {title} {'─' * pad}\n")
    else:
        print("\n" + "─" * width + "\n")


def step(n: int, label: str):
    print(f"\n{'═' * 62}")
    print(f"  STEP {n}: {label}")
    print(f"{'═' * 62}")


# ── Pipeline ──────────────────────────────────────────────────────────────────

def main():
    t0 = time.time()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   SQL-to-NoSQL Schema Transformation Prototype           ║")
    print("║   Master's Thesis — Data Engineering & AI, ESILV Paris   ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # ── STEP 0: Dataset ───────────────────────────────────────────────────────
    step(0, "DATASET PREPARATION")
    if not os.path.exists(DATA_DIR) or len(os.listdir(DATA_DIR)) == 0:
        print("  Dataset not found. Generating synthetic e-commerce data...")
        ##--gen_script = os.path.join(BASE_DIR, "data", "generate_dataset.py")
        gen_script = os.path.join(BASE_DIR, "generate_dataset.py")
        subprocess.run([sys.executable, gen_script], check=True)
    else:
        csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
        print(f"  Found {len(csv_files)} CSV files in {DATA_DIR}")
        for f in sorted(csv_files):
            print(f"    • {f}")

    # ── STEP 1: Schema Profiler ───────────────────────────────────────────────
    step(1, "SCHEMA PROFILER")
    profiler = SchemaProfiler(DATA_DIR)
    profiler.load()
    profiles = profiler.profile()
    profiler.print_report()

    # ── STEP 2: Workload Analyzer ─────────────────────────────────────────────
    step(2, "WORKLOAD ANALYZER")
    analyzer = WorkloadAnalyzer(n_simulated_queries=10_000)
    workload = analyzer.analyze()
    analyzer.print_report()

    # ── STEP 3: Mapping Engine ────────────────────────────────────────────────
    step(3, "MAPPING ENGINE  (rule-based)")
    engine = MappingEngine(workload, profiles)
    mapping = engine.run()
    engine.print_report(mapping)

    # ── STEP 4: NoSQL Schema Generator ───────────────────────────────────────
    step(4, "NOSQL SCHEMA GENERATOR")
    generator = NoSQLGenerator(profiles, mapping)
    schemas   = generator.generate()
    generator.print_report(schemas)

    # Export schemas to JSON
    schema_path = os.path.join(OUTPUT_DIR, "mongodb_schemas.json")
    generator.export_schemas_json(schemas, schema_path)

    # ── STEP 5: Cost Estimator ────────────────────────────────────────────────
    step(5, "COST ESTIMATOR")
    estimator = CostEstimator(profiles, workload)
    cost_result = estimator.estimate()
    estimator.print_report(cost_result)

    # ── STEP 6: Cloud Cost Comparison ─────────────────────────
    step(6, "CLOUD PROVIDER COST COMPARISON")

    storage_gb = cost_result.storage.total_storage_gb
    reads = workload.total_queries * 0.75
    writes = workload.total_queries * 0.25
    network_gb = cost_result.storage.total_storage_gb * 170

    cloud = CloudCostComparator(storage_gb, reads, writes, network_gb)
    cloud.print_report()

    # ── FINAL RECOMMENDATION ──────────────────────────────────────────────────
    t1 = time.time()
    separator("MIGRATION RECOMMENDATION")

    c = cost_result.costs
    s = cost_result.storage

    print(f"  ┌─────────────────────────────────────────────────────────┐")
    print(f"  │         FINAL MIGRATION RECOMMENDATION                  │")
    print(f"  ├─────────────────────────────────────────────────────────┤")
    print(f"  │  Source      : Relational SQL (6 tables, CSV export)    │")
    print(f"  │  Target      : MongoDB  (Document Store)                │")
    print(f"  │  Confidence  : {mapping.confidence:.1%}                                │")
    print(f"  ├─────────────────────────────────────────────────────────┤")
    print(f"  │  Collections generated : {len(schemas.collections)}                            │")
    print(f"  │  Indexes recommended   : {sum(len(c2.indexes) for c2 in schemas.collections)}                           │")
    print(f"  │  Embedding decisions   : {sum(len(c2.embedding_decisions) for c2 in mapping.collection_mappings)}                            │")
    print(f"  ├─────────────────────────────────────────────────────────┤")
    print(f"  │  Estimated data size   : {s.total_storage_gb * 1024:>7.2f} MB                    │")
    print(f"  │  Est. monthly cost     :   ${c.total_monthly_usd:.4f}                    │")
    print(f"  │  RDS baseline cost     :   ${c.rds_baseline_usd:.4f}                    │")
    sign = "SAVING" if c.savings_usd >= 0 else "EXTRA"
    print(f"  │  Cost delta            :   ${abs(c.savings_usd):.4f} {sign} ({abs(c.savings_pct):.1f}%)          │")
    print(f"  ├─────────────────────────────────────────────────────────┤")
    print(f"  │  Key Embedding Decisions:                               │")
    print(f"  │    ↘ users  ← orders ← order_items  (3-table embed)    │")
    print(f"  │    ↗ reviews → products  (reference + snapshot)        │")
    print(f"  │    ⏱ events → time-series collection                   │")
    print(f"  ├─────────────────────────────────────────────────────────┤")
    print(f"  │  Output files:                                          │")
    print(f"  │    • {os.path.relpath(schema_path, BASE_DIR):<53}│")
    print(f"  └─────────────────────────────────────────────────────────┘")

    print(f"\n  Pipeline completed in {t1 - t0:.2f}s")
    print()

    # ── Save summary JSON ─────────────────────────────────────────────────────
    summary = {
        "recommendation": {
            "nosql_model":  mapping.recommended_nosql_model,
            "target_db":    "MongoDB",
            "confidence":   mapping.confidence,
        },
        "schema_stats": {
            t: {"rows": p.row_count, "columns": p.col_count}
            for t, p in profiles.items()
        },
        "workload": {
            "total_queries":    workload.total_queries,
            "read_write_ratio": workload.read_write_ratio,
            "join_ratio":       workload.overall_join_ratio,
        },
        "cost": {
            "storage_gb":      cost_result.storage.total_storage_gb,
            "monthly_usd":     cost_result.costs.total_monthly_usd,
            "rds_baseline_usd":cost_result.costs.rds_baseline_usd,
            "savings_pct":     cost_result.costs.savings_pct,
        },
        "collections": [cs.name for cs in schemas.collections],
        "rules_fired":  mapping.rule_activations,
    }
    summary_path = os.path.join(OUTPUT_DIR, "migration_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Summary saved → {summary_path}\n")


if __name__ == "__main__":
    main()
