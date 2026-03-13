"""
nosql_generator.py
──────────────────
Generates MongoDB document schema definitions from:
  - The relational schema profiles
  - The mapping engine decisions

Outputs:
  1. JSON Schema definitions for each MongoDB collection
  2. Python dict "template documents" (example records)
  3. Index recommendations
  4. Written human-readable schema explanation
"""

import json
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class IndexSpec:
    collection:  str
    fields:      List[str]
    unique:      bool = False
    sparse:      bool = False
    ttl_seconds: Optional[int] = None
    purpose:     str = ""


@dataclass
class CollectionSchema:
    name:            str
    description:     str
    json_schema:     dict        # MongoDB $jsonSchema validator
    example_document: dict
    indexes:         List[IndexSpec] = field(default_factory=list)
    notes:           List[str]  = field(default_factory=list)


@dataclass
class GeneratedSchemas:
    collections: List[CollectionSchema]
    migration_notes: List[str]


# ── Generator ─────────────────────────────────────────────────────────────────

class NoSQLGenerator:

    def __init__(self, schema_profiles, mapping_result):
        self.sp = schema_profiles
        self.mr = mapping_result

    def generate(self) -> GeneratedSchemas:
        collections = [
            self._build_users_schema(),
            self._build_products_schema(),
            self._build_reviews_schema(),
            self._build_events_schema(),
            self._build_order_items_analytics_schema(),
        ]

        migration_notes = [
            "Run data migration scripts to populate MongoDB from CSV source.",
            "Create indexes BEFORE bulk-loading data for faster build times.",
            "Use MongoDB Compass or mongoimport for initial data load.",
            "Enable MongoDB time-series collection for 'events' for better compression.",
            "Consider MongoDB Change Streams for real-time sync during migration cutover.",
            "Validate with count checks: SQL row counts should match embedded sub-document totals.",
        ]

        return GeneratedSchemas(collections=collections, migration_notes=migration_notes)

    # ── Collection builders ───────────────────────────────────────────────────

    def _build_users_schema(self) -> CollectionSchema:
        json_schema = {
            "bsonType": "object",
            "required": ["user_id", "name", "email"],
            "properties": {
                "user_id":    {"bsonType": "int",    "description": "Primary key from users.id"},
                "name":       {"bsonType": "string"},
                "email":      {"bsonType": "string"},
                "country":    {"bsonType": "string"},
                "age":        {"bsonType": "int",    "minimum": 0, "maximum": 150},
                "created_at": {"bsonType": "date"},
                "orders": {
                    "bsonType": "array",
                    "description": "Embedded orders (formerly orders table)",
                    "items": {
                        "bsonType": "object",
                        "required": ["order_id", "status"],
                        "properties": {
                            "order_id":     {"bsonType": "int"},
                            "status":       {"bsonType": "string",
                                            "enum": ["pending","shipped","delivered","cancelled"]},
                            "total_amount": {"bsonType": "double"},
                            "created_at":   {"bsonType": "date"},
                            "items": {
                                "bsonType": "array",
                                "description": "Embedded order items",
                                "items": {
                                    "bsonType": "object",
                                    "required": ["product_id", "quantity"],
                                    "properties": {
                                        "item_id":    {"bsonType": "int"},
                                        "product_id": {"bsonType": "int"},
                                        "quantity":   {"bsonType": "int", "minimum": 1},
                                        "unit_price": {"bsonType": "double"},
                                        "subtotal":   {"bsonType": "double"},
                                        "product_name": {"bsonType": "string",
                                                         "description": "Denormalized snapshot"},
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        example = {
            "_id": "ObjectId('...')",
            "user_id": 42,
            "name": "Alice Smith",
            "email": "alice.smith42@example.com",
            "country": "US",
            "age": 29,
            "created_at": "ISODate('2022-03-15')",
            "orders": [
                {
                    "order_id": 101,
                    "status": "delivered",
                    "total_amount": 149.97,
                    "created_at": "ISODate('2023-01-10')",
                    "items": [
                        {"item_id": 1, "product_id": 7,  "quantity": 2,
                         "unit_price": 49.99, "subtotal": 99.98,
                         "product_name": "Ultra Widget 7"},
                        {"item_id": 2, "product_id": 15, "quantity": 1,
                         "unit_price": 49.99, "subtotal": 49.99,
                         "product_name": "Elite Gadget 15"},
                    ]
                },
                {
                    "order_id": 204,
                    "status": "shipped",
                    "total_amount": 79.99,
                    "created_at": "ISODate('2023-06-02')",
                    "items": [
                        {"item_id": 9, "product_id": 3, "quantity": 1,
                         "unit_price": 79.99, "subtotal": 79.99,
                         "product_name": "Pro Device 3"},
                    ]
                }
            ]
        }

        indexes = [
            IndexSpec("users", ["user_id"],   unique=True, purpose="Primary lookup"),
            IndexSpec("users", ["email"],      unique=True, purpose="Login / dedup"),
            IndexSpec("users", ["orders.order_id"], purpose="Lookup by order ID inside users"),
            IndexSpec("users", ["orders.items.product_id"], purpose="Find users who bought product X"),
            IndexSpec("users", ["country"],    purpose="Geo-based queries"),
        ]

        notes = [
            "Embedding orders + items eliminates 2 JOIN operations for the most common query.",
            "Average document size: ~4–6 KB (12 orders × 3 items each).",
            "If a user accumulates >100 orders, consider a reference pattern.",
        ]

        return CollectionSchema(
            name="users",
            description="Root user document with embedded orders and order items.",
            json_schema=json_schema,
            example_document=example,
            indexes=indexes,
            notes=notes,
        )

    def _build_products_schema(self) -> CollectionSchema:
        json_schema = {
            "bsonType": "object",
            "required": ["product_id", "name", "price"],
            "properties": {
                "product_id":  {"bsonType": "int"},
                "name":        {"bsonType": "string"},
                "category":    {"bsonType": "string"},
                "price":       {"bsonType": "double", "minimum": 0},
                "stock":       {"bsonType": "int",    "minimum": 0},
                "created_at":  {"bsonType": "date"},
                "review_summary": {
                    "bsonType": "object",
                    "description": "Cached aggregation to avoid costly review joins",
                    "properties": {
                        "avg_rating":   {"bsonType": "double"},
                        "total_reviews":{"bsonType": "int"},
                        "last_updated": {"bsonType": "date"},
                    }
                }
            }
        }

        example = {
            "_id": "ObjectId('...')",
            "product_id": 7,
            "name": "Ultra Widget 7",
            "category": "Electronics",
            "price": 49.99,
            "stock": 142,
            "created_at": "ISODate('2021-06-01')",
            "review_summary": {
                "avg_rating": 4.2,
                "total_reviews": 38,
                "last_updated": "ISODate('2024-01-01')"
            }
        }

        indexes = [
            IndexSpec("products", ["product_id"], unique=True, purpose="Primary lookup"),
            IndexSpec("products", ["category"],   purpose="Browse by category"),
            IndexSpec("products", ["price"],       purpose="Price range queries"),
            IndexSpec("products", ["review_summary.avg_rating"], purpose="Sort by rating"),
        ]

        return CollectionSchema(
            name="products",
            description="Product catalog with embedded review aggregation snapshot.",
            json_schema=json_schema,
            example_document=example,
            indexes=indexes,
            notes=["review_summary is updated asynchronously when new reviews arrive."],
        )

    def _build_reviews_schema(self) -> CollectionSchema:
        json_schema = {
            "bsonType": "object",
            "required": ["user_id", "product_id", "rating"],
            "properties": {
                "review_id":   {"bsonType": "int"},
                "user_id":     {"bsonType": "int"},
                "product_id":  {"bsonType": "int"},
                "rating":      {"bsonType": "int", "minimum": 1, "maximum": 5},
                "review_text": {"bsonType": "string"},
                "created_at":  {"bsonType": "date"},
                "product_snapshot": {
                    "bsonType": "object",
                    "description": "Denormalized for display without product JOIN",
                    "properties": {
                        "name":     {"bsonType": "string"},
                        "category": {"bsonType": "string"},
                    }
                },
                "user_snapshot": {
                    "bsonType": "object",
                    "properties": {"name": {"bsonType": "string"}},
                }
            }
        }

        example = {
            "_id": "ObjectId('...')",
            "review_id": 55,
            "user_id": 42,
            "product_id": 7,
            "rating": 5,
            "review_text": "Absolutely love it!",
            "created_at": "ISODate('2023-08-20')",
            "product_snapshot": {"name": "Ultra Widget 7", "category": "Electronics"},
            "user_snapshot":    {"name": "Alice Smith"},
        }

        indexes = [
            IndexSpec("reviews", ["product_id"], purpose="All reviews for a product"),
            IndexSpec("reviews", ["user_id"],     purpose="All reviews by a user"),
            IndexSpec("reviews", ["product_id", "rating"], purpose="Filtered reviews"),
        ]

        return CollectionSchema(
            name="reviews",
            description="Product reviews with denormalized product and user snapshots.",
            json_schema=json_schema,
            example_document=example,
            indexes=indexes,
        )

    def _build_events_schema(self) -> CollectionSchema:
        json_schema = {
            "bsonType": "object",
            "required": ["user_id", "event_type", "timestamp"],
            "properties": {
                "event_id":   {"bsonType": "int"},
                "user_id":    {"bsonType": "int"},
                "event_type": {"bsonType": "string",
                               "enum": ["page_view","add_to_cart","purchase",
                                        "search","login","logout"]},
                "product_id": {"bsonType": ["int", "null"]},
                "session_id": {"bsonType": "string"},
                "timestamp":  {"bsonType": "date"},
                "metadata":   {"bsonType": "object"},
            }
        }

        example = {
            "_id": "ObjectId('...')",
            "event_id": 1204,
            "user_id": 42,
            "event_type": "add_to_cart",
            "product_id": 7,
            "session_id": "sess_7821",
            "timestamp": "ISODate('2023-09-01T14:22:00Z')",
            "metadata": {"ip": "192.168.1.45", "browser": "Chrome"}
        }

        indexes = [
            IndexSpec("events", ["user_id", "timestamp"],
                      purpose="User activity feed (most common query)"),
            IndexSpec("events", ["timestamp"],
                      ttl_seconds=60*60*24*90,
                      purpose="Auto-expire events older than 90 days"),
            IndexSpec("events", ["event_type"],
                      purpose="Filter by event type for analytics"),
        ]

        return CollectionSchema(
            name="events",
            description="Append-only user event stream. Use MongoDB time-series collection.",
            json_schema=json_schema,
            example_document=example,
            indexes=indexes,
            notes=[
                "Create as time-series collection: timeseries={timeField:'timestamp', metaField:'user_id'}",
                "Set expireAfterSeconds=7776000 (90 days) for automatic TTL pruning.",
            ],
        )

    def _build_order_items_analytics_schema(self) -> CollectionSchema:
        json_schema = {
            "bsonType": "object",
            "required": ["order_id", "product_id"],
            "properties": {
                "item_id":    {"bsonType": "int"},
                "order_id":   {"bsonType": "int"},
                "product_id": {"bsonType": "int"},
                "quantity":   {"bsonType": "int"},
                "unit_price": {"bsonType": "double"},
                "subtotal":   {"bsonType": "double"},
            }
        }

        example = {
            "_id": "ObjectId('...')",
            "item_id": 9,
            "order_id": 204,
            "product_id": 3,
            "quantity": 1,
            "unit_price": 79.99,
            "subtotal": 79.99,
        }

        indexes = [
            IndexSpec("order_items_analytics", ["product_id"], purpose="Sales per product analytics"),
            IndexSpec("order_items_analytics", ["order_id"],   purpose="Items in an order"),
        ]

        return CollectionSchema(
            name="order_items_analytics",
            description="Flat order items for analytics queries. Primary access is via users→orders→items.",
            json_schema=json_schema,
            example_document=example,
            indexes=indexes,
            notes=["Optional collection. Primary path is embedded in users collection."],
        )

    # ── Report ────────────────────────────────────────────────────────────────

    def print_report(self, result: GeneratedSchemas):
        print("=" * 60)
        print("  NOSQL SCHEMA GENERATOR REPORT")
        print("=" * 60)
        print(f"\n  Generated {len(result.collections)} MongoDB collections\n")

        for cs in result.collections:
            print(f"  ┌─ Collection: {cs.name.upper()}")
            print(f"  │  {cs.description}")

            print(f"\n  │  Example Document:")
            doc_str = json.dumps(cs.example_document, indent=4)
            for line in doc_str.splitlines():
                print(f"  │  {line}")

            print(f"\n  │  Indexes ({len(cs.indexes)}):")
            for idx in cs.indexes:
                ttl_str = f"  [TTL {idx.ttl_seconds}s]" if idx.ttl_seconds else ""
                unique_str = " [UNIQUE]" if idx.unique else ""
                print(f"  │    • {idx.fields}{unique_str}{ttl_str}  ← {idx.purpose}")

            if cs.notes:
                print(f"\n  │  Notes:")
                for n in cs.notes:
                    print(f"  │    ⚑ {n}")

            print()

        print("  Migration Notes:")
        for note in result.migration_notes:
            print(f"    → {note}")

        print("\n" + "=" * 60)

    def export_schemas_json(self, result: GeneratedSchemas, output_path: str):
        """Write all schemas to a JSON file for documentation."""
        output = []
        for cs in result.collections:
            output.append({
                "collection": cs.name,
                "description": cs.description,
                "json_schema": cs.json_schema,
                "example_document": cs.example_document,
                "indexes": [
                    {
                        "fields": idx.fields,
                        "unique": idx.unique,
                        "ttl_seconds": idx.ttl_seconds,
                        "purpose": idx.purpose,
                    }
                    for idx in cs.indexes
                ],
                "notes": cs.notes,
            })
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Schemas exported → {output_path}")


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from workload_analyzer import WorkloadAnalyzer
    from schema_profiler import SchemaProfiler
    from mapping_engine import MappingEngine

    ws = WorkloadAnalyzer(1000).analyze()
    sp = SchemaProfiler("../data/ecommerce_dataset").load().profile()
    mr = MappingEngine(ws, sp).run()
    gen = NoSQLGenerator(sp, mr)
    result = gen.generate()
    gen.print_report(result)
