# SQL-to-NoSQL Schema Transformation & Cost Estimation Prototype

**Master's Thesis Prototype — Data Engineering & AI, ESILV Paris**

A research prototype that demonstrates automated migration from a relational e-commerce
database to a MongoDB document schema using workload-aware analysis and simplified
cloud cost estimation.

---

## System Architecture
┌─────────────────────────────────────────────────────────┐
│              SQL-to-NoSQL Migration Pipeline            │
│                                                         │
│  CSV Dataset (6 tables)                                 │
│       │                                                 │
│       ▼                                                 │
│  ┌─────────────────┐                                    │
│  │  Schema Profiler│  → Table stats, FK detection,      │
│  │  (profiler.py)  │    cardinality, null ratios        │
│  └────────┬────────┘                                    │
│           │                                             │
│           ▼                                             │
│  ┌─────────────────┐                                    │
│  │ Workload Analyzer│ → Query simulation (10k queries), │
│  │ (analyzer.py)   │   join freq, read/write ratio,     │
│  └────────┬────────┘   hot paths, access patterns       │
│           │                                             │
│           ▼                                             │
│  ┌─────────────────┐                                    │
│  │  Mapping Engine │  → Rule-based: embed/reference/    │
│  │  (mapping.py)   │    snapshot decisions              │
│  └────────┬────────┘                                    │
│           │                                             │
│           ▼                                             │
│  ┌─────────────────┐                                    │
│  │ NoSQL Generator │  → MongoDB $jsonSchema, example    │
│  │ (generator.py)  │    documents, index specs          │
│  └────────┬────────┘                                    │
│           │                                             │
│           ▼                                             │
│  ┌─────────────────┐                                    │
│  │ Cost Estimator  │  → Storage, IO, compute estimates  │
│  │ (estimator.py)  │    vs RDS baseline comparison      │
│  └────────┬────────┘                                    │
│           │                                             │
│           ▼                                             │
│  ┌──────────────────────┐                                │
│  │ Cloud Cost Comparator│ → Compare AWS, Azure, GCP,    │
│  │ (cloud_cost_comparator) │ DigitalOcean infrastructure │
│  └──────────────────────┘                                │
│                                                         │
│ Output: migration_summary.json + mongodb_schemas.json   │
└─────────────────────────────────────────────────────────┘---



## Folder Structure

```
sql_to_nosql_migration/
│
├── data/
│   ├── generate_dataset.py        # Synthetic CSV generator
│   └── ecommerce_dataset/         # Auto-created on first run
│       ├── users.csv
│       ├── products.csv
│       ├── orders.csv
│       ├── order_items.csv
│       ├── reviews.csv
│       └── events.csv
│
├── src/
│   ├── schema_profiler.py         # Table/column statistics + FK detection
│   ├── workload_analyzer.py       # Query simulation + access patterns
│   ├── mapping_engine.py          # Rule-based embed/reference decisions
│   ├── nosql_generator.py         # MongoDB schema + example documents
│   └── cost_estimator.py          # NoSQL cost estimation model
│
├── output/                        # Auto-created on run
│   ├── mongodb_schemas.json       # Full schema definitions
│   └── migration_summary.json     # Pipeline summary
│
├── cloud_cost_comparator.py       # Multi-cloud cost comparison
├── main.py                        # Pipeline runner
├── requirements.txt
└── README.md

---

## Dataset Schema

| Table         | Rows  | Description                       |
|---------------|-------|-----------------------------------|
| users         | 200   | Customer accounts                 |
| products      | 100   | Product catalog                   |
| orders        | 500   | Purchase orders                   |
| order_items   | ~1475 | Line items per order              |
| reviews       | 400   | Product ratings and feedback      |
| events        | 2000  | User activity log (clickstream)   |

**Foreign Key Relationships:**
```
orders.user_id         → users.id
order_items.order_id   → orders.id
order_items.product_id → products.id
reviews.user_id        → users.id
reviews.product_id     → products.id
events.user_id         → users.id
events.product_id      → products.id
```

---

## Module Descriptions

### 1. Schema Profiler (`src/schema_profiler.py`)
Loads all 6 CSV files and computes per-table and per-column statistics:
- Row count, column count, in-memory size
- Data type inference
- Null ratio per column
- Cardinality ratio (unique values / total rows)
- Primary key detection (cardinality > 95%)
- Foreign key detection (heuristic: `_id` suffix + known FK map)

### 2. Workload Analyzer (`src/workload_analyzer.py`)
Simulates 10,000 realistic SQL queries drawn from weighted templates:
- 19 query templates (SELECT, INSERT, UPDATE, aggregation, JOIN)
- Weighted by realistic frequency (event inserts are most common)
- Computes: table access frequency, join pair frequency, read/write ratio
- Derives 4 NoSQL access patterns from the workload

### 3. Mapping Engine (`src/mapping_engine.py`)
Rule-based decision system with 8 rules:

| Rule | Condition                        | Action                          |
|------|----------------------------------|---------------------------------|
| R1   | join_ratio > 20%                 | Recommend Document DB (MongoDB) |
| R2   | join_ratio < 10%                 | Recommend Key-Value             |
| R3   | write_ratio > 60%                | Consider Wide-Column            |
| R4   | Time-series table detected       | Time-Series collection          |
| R5   | Lookup table (small, read-heavy) | Key-Value cache                 |
| R6   | Parent:Child avg < 50            | EMBED child into parent         |
| R7   | Child collection unbounded       | REFERENCE + summary snapshot    |
| R8   | High-freq join for display       | DENORMALIZE snapshot field      |

### 4. NoSQL Schema Generator (`src/nosql_generator.py`)
Generates for each MongoDB collection:
- MongoDB `$jsonSchema` validator
- Example document (realistic data)
- Index specifications (with TTL for events)
- Human-readable notes and migration guidance

### 5. Cost Estimator (`src/cost_estimator.py`)
Simplified cost formula:
```
Cost = α·StorageCost + β·ReadCost + γ·WriteCost + δ·IndexCost + ε·NetworkCost
```

Pricing constants (Atlas M10-equivalent):
- Storage: $0.25/GB/month
- Read ops: $0.30/million
- Write ops: $1.00/million
- Network egress: $0.09/GB

Also estimates RDS baseline for comparison.

---

## Quick Start

### Prerequisites
- Python 3.8+
- pip

### Install dependencies
```bash
pip install -r requirements.txt
```

6. Multi-Cloud Cost Comparator (cloud_cost_comparator.py)

This module compares estimated infrastructure costs across multiple cloud providers.

Providers included:

Provider	Database Service
AWS	DynamoDB / MongoDB Atlas
Azure	Cosmos DB
Google Cloud	Firestore
DigitalOcean	Managed MongoDB

The comparator calculates approximate monthly costs using:

Storage usage

Read operations

Write operations

Network traffic

Example simplified formula:

Total Cost =
(Storage × StoragePrice)
+ (Reads × ReadPrice)
+ (Writes × WritePrice)
+ (Network × NetworkPrice)

This comparison helps evaluate which cloud platform provides the most cost-efficient infrastructure for the transformed NoSQL workload.


### Run the full pipeline
```bash
python main.py
```

The first run will automatically generate the synthetic e-commerce dataset.

### Run individual modules
```bash
# Schema profiler only
python src/schema_profiler.py data/ecommerce_dataset

# Workload analyzer only
python src/workload_analyzer.py

# Cost estimator only
python src/cost_estimator.py
```

### Regenerate dataset
```bash
python data/generate_dataset.py
```

---

## Expected Output

```
╔══════════════════════════════════════════════════════════╗
║   SQL-to-NoSQL Schema Transformation Prototype           ║
║   Master's Thesis — Data Engineering & AI, ESILV Paris   ║
╚══════════════════════════════════════════════════════════╝

STEP 0: DATASET PREPARATION
Found 6 CSV files

STEP 1: SCHEMA PROFILER
Tables analyzed

STEP 2: WORKLOAD ANALYZER
10,000 queries simulated

STEP 3: MAPPING ENGINE
Recommended model: MongoDB

STEP 4: NOSQL SCHEMA GENERATOR
5 collections created

STEP 5: COST ESTIMATOR
MongoDB cost ≈ $25/month

STEP 6: CLOUD COST COMPARISON

AWS           : $0.0296 / month
Azure         : $0.0294 / month
GCP           : $0.0280 / month
DigitalOcean  : $0.0030 / month

Best provider → DigitalOcean
```

---

## MongoDB Collection Summary

| Collection              | Strategy         | Key Feature                            |
|-------------------------|------------------|----------------------------------------|
| `users`                 | Root document    | Embedded orders + items (2-level)      |
| `products`              | Document         | Cached review_summary snapshot         |
| `reviews`               | Document         | Denormalized product + user snapshots  |
| `events`                | Time-series      | TTL auto-expiry (90 days)              |
| `order_items_analytics` | Flat document    | Analytics queries only                 |

---

## Thesis Context

This prototype supports research in:
- **Schema mapping automation** — converting ER models to document schemas
- **Workload-driven design** — embedding decisions based on query frequency
- **NoSQL cost modeling** — cloud pricing estimation for migration planning
- **Trade-off analysis** — embed vs reference vs denormalize

### Extending the prototype
- Add a Streamlit UI: `pip install streamlit` then `streamlit run app.py`
- Connect to real MongoDB: `pip install pymongo` and add a migration executor
- Add more workload patterns in `workload_analyzer.py` `QUERY_TEMPLATES`
- Adjust cost constants in `cost_estimator.py` `PRICING` dict for your cloud region

---

## License
Research prototype — for academic use.
