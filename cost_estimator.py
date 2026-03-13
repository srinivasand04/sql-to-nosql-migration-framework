"""
cost_estimator.py
─────────────────
Simplified cloud cost model for the migrated MongoDB deployment.

Cost = α·StorageCost + β·QueryCost + γ·WriteCost + δ·IndexCost

Cloud pricing constants are inspired by MongoDB Atlas M10 cluster (2024 list prices)
but are intentionally simplified for research demonstration.

Outputs:
  - Storage estimate (GB)
  - Monthly compute cost ($)
  - Monthly storage cost ($)
  - Monthly IO cost ($)
  - Total estimated monthly cost ($)
  - Comparison: relational baseline vs NoSQL
"""

import math
from dataclasses import dataclass, field
from typing import Dict


# ── Pricing constants (simplified) ───────────────────────────────────────────

PRICING = {
    # MongoDB Atlas M10 tier (us-east-1, approximate)
    "storage_gb_per_month_usd":    0.25,    # $/GB/month (NVMe SSD)
    "read_op_per_million_usd":     0.30,    # $/million read operations
    "write_op_per_million_usd":    1.00,    # $/million write operations
    "index_storage_per_gb_usd":    0.25,    # same as data storage
    "network_egress_per_gb_usd":   0.09,    # $/GB outbound
    "nosql_overhead_factor":       1.25,    # document overhead vs raw CSV
    # Relational baseline (Aurora MySQL Serverless v2 approximation)
    "rds_storage_per_gb_usd":      0.10,
    "rds_io_per_million_usd":      0.20,
    "rds_compute_per_hr_usd":      0.12,
    "rds_compute_hours_per_month": 730,
}

# Embedding/denormalization inflates data size
DOCUMENT_INFLATION_FACTOR = 1.4   # embedded docs store repeated field names

# Query load assumptions
QUERIES_PER_DAY    = 50_000
WRITE_RATIO        = 0.25
READ_RATIO         = 0.75
DAYS_PER_MONTH     = 30
AVG_DOC_FETCH_SIZE = 2.5   # KB per document read
NETWORK_EGRESS_PCT = 0.10  # 10% of read data leaves VPC


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class StorageEstimate:
    raw_csv_mb:         float
    nosql_data_gb:      float
    index_size_gb:      float
    total_storage_gb:   float


@dataclass
class IOEstimate:
    monthly_reads:     int
    monthly_writes:    int
    network_egress_gb: float


@dataclass
class CostBreakdown:
    storage_cost_usd:  float
    read_cost_usd:     float
    write_cost_usd:    float
    index_cost_usd:    float
    network_cost_usd:  float
    total_monthly_usd: float
    # Comparison
    rds_baseline_usd:  float
    savings_usd:       float
    savings_pct:       float


@dataclass
class CostEstimateResult:
    storage:    StorageEstimate
    io:         IOEstimate
    costs:      CostBreakdown
    per_table:  Dict[str, dict] = field(default_factory=dict)
    notes:      list            = field(default_factory=list)


# ── Estimator ─────────────────────────────────────────────────────────────────

class CostEstimator:

    def __init__(self, schema_profiles, workload_stats):
        self.sp = schema_profiles
        self.ws = workload_stats

    def estimate(self) -> CostEstimateResult:
        # ── 1. Storage ────────────────────────────────────────────────────────
        raw_bytes = sum(tp.size_bytes for tp in self.sp.values())
        raw_mb    = raw_bytes / (1024 ** 2)
        raw_gb    = raw_bytes / (1024 ** 3)

        # NoSQL document overhead (field names repeated per document, embedding)
        nosql_data_gb = raw_gb * PRICING["nosql_overhead_factor"] * DOCUMENT_INFLATION_FACTOR
        # Indexes: typically 10-20% of data size
        index_size_gb = nosql_data_gb * 0.15
        total_storage_gb = nosql_data_gb + index_size_gb

        storage = StorageEstimate(
            raw_csv_mb=round(raw_mb, 2),
            nosql_data_gb=round(nosql_data_gb, 4),
            index_size_gb=round(index_size_gb, 4),
            total_storage_gb=round(total_storage_gb, 4),
        )

        # ── 2. IO ─────────────────────────────────────────────────────────────
        monthly_queries = QUERIES_PER_DAY * DAYS_PER_MONTH
        monthly_reads   = int(monthly_queries * READ_RATIO)
        monthly_writes  = int(monthly_queries * WRITE_RATIO)
        # Network: fraction of reads * avg doc size leaving VPC
        network_egress_gb = (monthly_reads * AVG_DOC_FETCH_SIZE * NETWORK_EGRESS_PCT) / (1024)

        io = IOEstimate(
            monthly_reads=monthly_reads,
            monthly_writes=monthly_writes,
            network_egress_gb=round(network_egress_gb, 4),
        )

        # ── 3. Cost breakdown ─────────────────────────────────────────────────
        p = PRICING
        storage_cost = total_storage_gb * p["storage_gb_per_month_usd"]
        read_cost    = (monthly_reads  / 1_000_000) * p["read_op_per_million_usd"]
        write_cost   = (monthly_writes / 1_000_000) * p["write_op_per_million_usd"]
        index_cost   = index_size_gb * p["index_storage_per_gb_usd"]
        network_cost = network_egress_gb * p["network_egress_per_gb_usd"]
        total_nosql  = storage_cost + read_cost + write_cost + index_cost + network_cost

        # RDS baseline
        rds_storage = total_storage_gb * p["rds_storage_per_gb_usd"]
        rds_io      = ((monthly_reads + monthly_writes) / 1_000_000) * p["rds_io_per_million_usd"]
        rds_compute = p["rds_compute_per_hr_usd"] * p["rds_compute_hours_per_month"]
        total_rds   = rds_storage + rds_io + rds_compute

        savings     = total_rds - total_nosql
        savings_pct = (savings / total_rds * 100) if total_rds else 0

        costs = CostBreakdown(
            storage_cost_usd=round(storage_cost, 4),
            read_cost_usd=round(read_cost, 4),
            write_cost_usd=round(write_cost, 4),
            index_cost_usd=round(index_cost, 4),
            network_cost_usd=round(network_cost, 4),
            total_monthly_usd=round(total_nosql, 4),
            rds_baseline_usd=round(total_rds, 4),
            savings_usd=round(savings, 4),
            savings_pct=round(savings_pct, 2),
        )

        # ── 4. Per-table breakdown ────────────────────────────────────────────
        per_table = {}
        total_rows = sum(tp.row_count for tp in self.sp.values()) or 1
        for tname, tp in self.sp.items():
            share = tp.row_count / total_rows
            per_table[tname] = {
                "rows":          tp.row_count,
                "raw_kb":        round(tp.size_bytes / 1024, 1),
                "nosql_kb":      round((tp.size_bytes * DOCUMENT_INFLATION_FACTOR * PRICING["nosql_overhead_factor"]) / 1024, 1),
                "access_freq":   self.ws.table_access_frequency.get(tname, 0),
                "est_cost_usd":  round(total_nosql * share, 4),
            }

        notes = [
            "Cost model uses simplified Atlas M10-equivalent pricing.",
            "Actual costs depend on cluster tier, region, and replication.",
            "Embedding reduces join cost at the expense of document size.",
            f"Document inflation factor: {DOCUMENT_INFLATION_FACTOR}× (embedded field names).",
            f"Index overhead: 15% of data storage.",
        ]

        return CostEstimateResult(storage=storage, io=io, costs=costs,
                                  per_table=per_table, notes=notes)

    # ── Report ────────────────────────────────────────────────────────────────

    def print_report(self, result: CostEstimateResult):
        c = result.costs
        s = result.storage
        io = result.io

        print("=" * 60)
        print("  COST ESTIMATOR REPORT")
        print("=" * 60)

        print(f"\n  ── Storage ──────────────────────────────────")
        print(f"  Raw CSV size          : {s.raw_csv_mb:.2f} MB")
        print(f"  MongoDB data (est.)   : {s.nosql_data_gb * 1024:.2f} MB")
        print(f"  Index overhead (15%)  : {s.index_size_gb * 1024:.2f} MB")
        print(f"  Total storage         : {s.total_storage_gb * 1024:.2f} MB  "
              f"({s.total_storage_gb:.4f} GB)")

        print(f"\n  ── Monthly IO ───────────────────────────────")
        print(f"  Queries/day           : {QUERIES_PER_DAY:,}")
        print(f"  Monthly reads         : {io.monthly_reads:,}")
        print(f"  Monthly writes        : {io.monthly_writes:,}")
        print(f"  Network egress        : {io.network_egress_gb:.4f} GB")

        print(f"\n  ── MongoDB Cost Breakdown ───────────────────")
        print(f"  Storage               : ${c.storage_cost_usd:.4f}")
        print(f"  Read operations       : ${c.read_cost_usd:.4f}")
        print(f"  Write operations      : ${c.write_cost_usd:.4f}")
        print(f"  Index storage         : ${c.index_cost_usd:.4f}")
        print(f"  Network egress        : ${c.network_cost_usd:.4f}")
        print(f"  ─────────────────────────────────────────────")
        print(f"  TOTAL (MongoDB/month) : ${c.total_monthly_usd:.4f}")

        print(f"\n  ── vs Relational Baseline (RDS) ─────────────")
        print(f"  RDS estimated/month   : ${c.rds_baseline_usd:.4f}")
        sign = "saving" if c.savings_usd >= 0 else "extra cost"
        print(f"  Difference            : ${abs(c.savings_usd):.4f} {sign} "
              f"({abs(c.savings_pct):.1f}%)")

        print(f"\n  ── Per-Table Breakdown ──────────────────────")
        print(f"  {'Table':<20} {'Rows':>6}  {'Raw KB':>7}  {'NoSQL KB':>8}  "
              f"{'Accesses':>9}  {'Est. Cost':>9}")
        for tname, d in result.per_table.items():
            print(f"  {tname:<20} {d['rows']:>6,}  {d['raw_kb']:>7.1f}  "
                  f"{d['nosql_kb']:>8.1f}  {d['access_freq']:>9,}  ${d['est_cost_usd']:>8.4f}")

        print(f"\n  Notes:")
        for note in result.notes:
            print(f"    • {note}")

        print("\n" + "=" * 60)


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from workload_analyzer import WorkloadAnalyzer
    from schema_profiler import SchemaProfiler

    ws = WorkloadAnalyzer(5000).analyze()
    sp = SchemaProfiler("../data/ecommerce_dataset").load().profile()
    est = CostEstimator(sp, ws)
    result = est.estimate()
    est.print_report(result)
