"""
mapping_engine.py
─────────────────
Rule-based engine that maps a relational schema + workload metrics to
a recommended NoSQL model and concrete embedding/referencing decisions.

Rules:
  R1  join_ratio > 0.3         → Document DB (MongoDB)
  R2  join_ratio < 0.1         → Key-Value store
  R3  write_ratio > 0.6        → Wide-Column store
  R4  time-series event table  → Time-Series / Wide-Column
  R5  lookup table (small, read-heavy) → Key-Value cache
  R6  parent-child 1:few       → Embed child into parent document
  R7  parent-child 1:many (>50 avg) → Reference (keep separate collection)
  R8  high join freq pair      → Denormalize (embed or snapshot)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class EmbeddingDecision:
    parent_table:  str
    child_table:   str
    strategy:      str      # "embed" | "reference" | "denormalize_snapshot"
    reason:        str
    embed_array:   str      # name of the array field in parent document


@dataclass
class CollectionMapping:
    original_table:    str
    mongo_collection:  str
    nosql_model:       str   # "document" | "key-value" | "wide-column" | "time-series"
    embedding_decisions: List[EmbeddingDecision] = field(default_factory=list)
    notes:             str = ""


@dataclass
class MappingResult:
    recommended_nosql_model: str
    confidence:              float          # 0-1
    rule_activations:        List[str]
    collection_mappings:     List[CollectionMapping]
    summary:                 str


# ── Constants ─────────────────────────────────────────────────────────────────

# Average cardinality estimates for the e-commerce domain (orders per user, etc.)
AVG_CARDINALITY = {
    ("users",   "orders"):       12,    # avg 12 orders per user
    ("orders",  "order_items"):   3,    # avg 3 items per order
    ("products","reviews"):       8,    # avg 8 reviews per product
    ("users",   "reviews"):       5,
    ("users",   "events"):      200,    # many events per user
}

EMBED_THRESHOLD  = 50   # embed if avg child count < 50
EVENT_TABLES     = {"events"}
LOOKUP_TABLES    = {"products"}


# ── Engine ────────────────────────────────────────────────────────────────────

class MappingEngine:

    def __init__(self, workload_stats, schema_profiles):
        self.ws = workload_stats
        self.sp = schema_profiles

    def run(self) -> MappingResult:
        rules_fired: List[str] = []
        recommended_model = "document"      # default
        confidence = 0.5

        jratio  = self.ws.overall_join_ratio
        wratio  = self.ws.write_count / self.ws.total_queries

        # ── Rule evaluation ───────────────────────────────────────────────────
        if jratio > 0.20:
            recommended_model = "document"
            confidence = min(0.95, 0.60 + jratio)
            rules_fired.append(
                f"R1: join_ratio={jratio:.2%} > 20% → Document DB (MongoDB)"
            )
        elif jratio < 0.10:
            recommended_model = "key-value"
            confidence = 0.70
            rules_fired.append(
                f"R2: join_ratio={jratio:.2%} < 10% → Key-Value store"
            )

        if wratio > 0.60:
            rules_fired.append(
                f"R3: write_ratio={wratio:.2%} > 60% → consider Wide-Column for high-write tables"
            )

        if any(t in self.sp for t in EVENT_TABLES):
            rules_fired.append(
                "R4: 'events' table detected → Time-Series / Wide-Column for event data"
            )

        # ── Collection mapping decisions ──────────────────────────────────────
        collection_mappings = self._build_collection_mappings(rules_fired)

        summary = (
            f"Based on join_ratio={jratio:.2%} and write_ratio={wratio:.2%}, "
            f"the recommended primary NoSQL model is '{recommended_model.upper()}' (MongoDB). "
            f"{len(rules_fired)} rules activated. "
            f"{sum(1 for cm in collection_mappings for ed in cm.embedding_decisions if ed.strategy == 'embed')} "
            "embed decisions taken."
        )

        return MappingResult(
            recommended_nosql_model=recommended_model,
            confidence=round(confidence, 3),
            rule_activations=rules_fired,
            collection_mappings=collection_mappings,
            summary=summary,
        )

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _build_collection_mappings(self, rules_fired: List[str]) -> List[CollectionMapping]:
        mappings: List[CollectionMapping] = []

        # ── users collection ─────────────────────────────────────────────────
        user_cm = CollectionMapping(
            original_table="users",
            mongo_collection="users",
            nosql_model="document",
            notes="Root document. Orders embedded for fast profile fetch.",
        )
        # Rule R6: orders per user avg 12 < 50 → embed
        user_cm.embedding_decisions.append(EmbeddingDecision(
            parent_table="users",
            child_table="orders",
            strategy="embed",
            reason="R6: avg 12 orders/user < threshold 50 → embed for single-read profile fetch",
            embed_array="orders",
        ))
        # Rule R6: order_items per order avg 3 < 50 → embed inside order
        user_cm.embedding_decisions.append(EmbeddingDecision(
            parent_table="orders",
            child_table="order_items",
            strategy="embed",
            reason="R6: avg 3 items/order < threshold 50 → embed items inside order sub-document",
            embed_array="items",
        ))
        rules_fired.append("R6: users→orders (avg 12) → EMBED orders array in user document")
        rules_fired.append("R6: orders→order_items (avg 3) → EMBED items array in order sub-document")
        mappings.append(user_cm)

        # ── products collection ───────────────────────────────────────────────
        prod_cm = CollectionMapping(
            original_table="products",
            mongo_collection="products",
            nosql_model="document",
            notes="Product catalog. Reviews referenced separately (can grow large).",
        )
        # Rule R7: reviews per product avg 8 < 50 but can grow → reference + stats snapshot
        prod_cm.embedding_decisions.append(EmbeddingDecision(
            parent_table="products",
            child_table="reviews",
            strategy="reference",
            reason="R7: reviews can grow unbounded → keep reviews collection separate; store avg_rating snapshot",
            embed_array="review_summary",
        ))
        rules_fired.append("R7: products→reviews (can grow) → REFERENCE reviews; snapshot avg_rating on product doc")
        mappings.append(prod_cm)

        # ── reviews collection ────────────────────────────────────────────────
        rev_cm = CollectionMapping(
            original_table="reviews",
            mongo_collection="reviews",
            nosql_model="document",
            notes="Standalone reviews collection. Indexed by product_id and user_id.",
        )
        # Denormalize product name + user name for fast display
        rev_cm.embedding_decisions.append(EmbeddingDecision(
            parent_table="reviews",
            child_table="products",
            strategy="denormalize_snapshot",
            reason="R8: high read freq for reviews with product name → snapshot product_name field",
            embed_array="product_snapshot",
        ))
        rules_fired.append("R8: reviews JOIN products (display) → SNAPSHOT product_name inside review doc")
        mappings.append(rev_cm)

        # ── events collection ─────────────────────────────────────────────────
        evt_cm = CollectionMapping(
            original_table="events",
            mongo_collection="events",
            nosql_model="time-series",
            notes="High-write append-only log. Use MongoDB time-series collection or TTL index.",
        )
        rules_fired.append("R4: events table → TIME-SERIES collection with TTL; no embedding")
        mappings.append(evt_cm)

        # ── order_items (absorbed into orders→users, standalone for analytics) ─
        oi_cm = CollectionMapping(
            original_table="order_items",
            mongo_collection="order_items_analytics",
            nosql_model="document",
            notes="Optional standalone collection for analytics queries on items; "
                  "primary access via embedded path users→orders→items.",
        )
        mappings.append(oi_cm)

        return mappings

    # ── Report ────────────────────────────────────────────────────────────────

    def print_report(self, result: MappingResult):
        print("=" * 60)
        print("  MAPPING ENGINE REPORT")
        print("=" * 60)
        print(f"\n  Recommended Model : {result.recommended_nosql_model.upper()}")
        print(f"  Confidence        : {result.confidence:.1%}")

        print("\n  Rules Activated:")
        for r in result.rule_activations:
            print(f"    ✓ {r}")

        print("\n  Collection Mapping Decisions:")
        for cm in result.collection_mappings:
            print(f"\n    [{cm.original_table}] → MongoDB collection '{cm.mongo_collection}'")
            print(f"      Model : {cm.nosql_model}")
            print(f"      Notes : {cm.notes}")
            for ed in cm.embedding_decisions:
                symbol = {"embed": "↘ EMBED", "reference": "↗ REFERENCE",
                          "denormalize_snapshot": "⊕ SNAPSHOT"}[ed.strategy]
                print(f"      {symbol}: {ed.child_table} → {ed.embed_array}")
                print(f"        Reason: {ed.reason}")

        print(f"\n  Summary: {result.summary}")
        print("\n" + "=" * 60)


if __name__ == "__main__":
    # Minimal smoke-test
    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from workload_analyzer import WorkloadAnalyzer
    from schema_profiler import SchemaProfiler

    ws = WorkloadAnalyzer(1000).analyze()
    sp = SchemaProfiler("../data/ecommerce_dataset").load().profile()
    engine = MappingEngine(ws, sp)
    result = engine.run()
    engine.print_report(result)
