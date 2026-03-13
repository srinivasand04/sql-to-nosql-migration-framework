"""
workload_analyzer.py
────────────────────
Simulates a realistic SQL workload for the e-commerce schema and computes:
  - Table access frequency
  - Join frequency per table pair
  - Overall read / write ratio
  - Hot-path query patterns
  - Derived NoSQL access patterns
"""

import random
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from collections import defaultdict


# ── Workload definition ───────────────────────────────────────────────────────

# Each entry: (sql_template, tables_touched, is_join, is_write, weight)
QUERY_TEMPLATES = [
    # User lookups
    ("SELECT * FROM users WHERE id = ?",                           ["users"],                            False, False, 15),
    ("SELECT * FROM users WHERE email = ?",                        ["users"],                            False, False, 10),
    # Order queries
    ("SELECT * FROM orders WHERE user_id = ?",                     ["orders"],                           False, False, 25),
    ("SELECT * FROM orders WHERE id = ?",                          ["orders"],                           False, False, 18),
    # Order + items join (most common)
    ("SELECT o.*, oi.* FROM orders o JOIN order_items oi ON o.id = oi.order_id WHERE o.user_id = ?",
                                                                   ["orders", "order_items"],            True,  False, 30),
    ("SELECT oi.*, p.* FROM order_items oi JOIN products p ON oi.product_id = p.id WHERE oi.order_id = ?",
                                                                   ["order_items", "products"],          True,  False, 28),
    # Product queries
    ("SELECT * FROM products WHERE id = ?",                        ["products"],                         False, False, 20),
    ("SELECT * FROM products WHERE category = ?",                  ["products"],                         False, False, 12),
    # Review queries
    ("SELECT * FROM reviews WHERE product_id = ?",                 ["reviews"],                          False, False, 16),
    ("SELECT * FROM reviews WHERE user_id = ?",                    ["reviews"],                          False, False, 8),
    ("SELECT r.*, u.name FROM reviews r JOIN users u ON r.user_id = u.id WHERE r.product_id = ?",
                                                                   ["reviews", "users"],                 True,  False, 14),
    # Event queries (write-heavy)
    ("INSERT INTO events (user_id, event_type, ...) VALUES (...)", ["events"],                           False, True,  40),
    ("SELECT * FROM events WHERE user_id = ? ORDER BY timestamp DESC LIMIT 50",
                                                                   ["events"],                           False, False, 10),
    # Order mutations
    ("INSERT INTO orders (user_id, ...) VALUES (...)",             ["orders"],                           False, True,  20),
    ("UPDATE orders SET status = ? WHERE id = ?",                  ["orders"],                           False, True,  15),
    ("INSERT INTO order_items (order_id, product_id, ...) VALUES (...)",
                                                                   ["order_items"],                      False, True,  20),
    # Review mutations
    ("INSERT INTO reviews (user_id, product_id, ...) VALUES (...)",["reviews"],                          False, True,  12),
    # Aggregate
    ("SELECT product_id, AVG(rating) FROM reviews GROUP BY product_id",
                                                                   ["reviews"],                          False, False, 5),
    ("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",      ["orders"],                           False, False, 4),
    # Full user profile join
    ("SELECT u.*, o.*, oi.*, p.* FROM users u "
     "JOIN orders o ON u.id = o.user_id "
     "JOIN order_items oi ON o.id = oi.order_id "
     "JOIN products p ON oi.product_id = p.id WHERE u.id = ?",
                                                                   ["users", "orders", "order_items", "products"],
                                                                                                         True,  False, 8),
]


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class WorkloadStats:
    total_queries: int
    table_access_frequency: Dict[str, int]       = field(default_factory=dict)
    table_access_ratio: Dict[str, float]         = field(default_factory=dict)
    join_frequency: Dict[Tuple[str,str], int]    = field(default_factory=dict)
    join_pairs_ratio: Dict[Tuple[str,str], float]= field(default_factory=dict)
    overall_join_ratio: float                    = 0.0
    read_count: int                              = 0
    write_count: int                             = 0
    read_write_ratio: float                      = 0.0
    hot_queries: List[dict]                      = field(default_factory=list)
    access_patterns: List[str]                   = field(default_factory=list)


# ── Analyzer ─────────────────────────────────────────────────────────────────

class WorkloadAnalyzer:
    """Simulates query workload and derives access pattern metrics."""

    def __init__(self, n_simulated_queries: int = 10_000):
        self.n = n_simulated_queries
        self.stats: WorkloadStats = None

    def analyze(self) -> WorkloadStats:
        """Run simulation and compute all metrics."""
        random.seed(42)

        table_hits    = defaultdict(int)
        join_hits     = defaultdict(int)
        read_count    = 0
        write_count   = 0
        query_counts  = defaultdict(int)

        # Build weighted template list
        templates = []
        for tmpl in QUERY_TEMPLATES:
            templates.extend([tmpl] * tmpl[4])  # weight = repetitions

        for _ in range(self.n):
            sql, tables, is_join, is_write, _ = random.choice(templates)
            query_counts[sql] += 1

            for t in tables:
                table_hits[t] += 1

            if is_join:
                # Record all pairs in this join
                for i in range(len(tables)):
                    for j in range(i + 1, len(tables)):
                        pair = tuple(sorted([tables[i], tables[j]]))
                        join_hits[pair] += 1

            if is_write:
                write_count += 1
            else:
                read_count += 1

        total = self.n
        join_total = sum(join_hits.values())

        # Access ratios (relative to most accessed table)
        max_hits = max(table_hits.values()) if table_hits else 1
        access_ratio = {t: round(c / max_hits, 4) for t, c in table_hits.items()}

        # Join pair ratios (relative to total queries)
        join_pairs_ratio = {pair: round(c / total, 4) for pair, c in join_hits.items()}

        # Overall join ratio = queries that involved ≥1 join / total
        join_query_count = sum(
            query_counts[sql]
            for sql, tables, is_join, is_write, _ in QUERY_TEMPLATES
            if is_join
        )
        overall_join_ratio = round(join_query_count / total, 4)

        # Hot queries (top 5 by count)
        hot_queries = sorted(
            [{"sql": sql, "count": cnt} for sql, cnt in query_counts.items()],
            key=lambda x: -x["count"]
        )[:5]

        # Derive NoSQL access patterns
        access_patterns = self._derive_access_patterns(
            table_hits, join_hits, read_count, write_count
        )

        self.stats = WorkloadStats(
            total_queries=total,
            table_access_frequency=dict(table_hits),
            table_access_ratio=access_ratio,
            join_frequency=dict(join_hits),
            join_pairs_ratio=join_pairs_ratio,
            overall_join_ratio=overall_join_ratio,
            read_count=read_count,
            write_count=write_count,
            read_write_ratio=round(read_count / write_count, 2) if write_count else float("inf"),
            hot_queries=hot_queries,
            access_patterns=access_patterns,
        )
        return self.stats

    def _derive_access_patterns(
        self,
        table_hits: dict,
        join_hits: dict,
        reads: int,
        writes: int
    ) -> List[str]:
        patterns = []

        # Pattern 1: Fetch user + all orders
        if join_hits.get(("orders", "users"), 0) > 0 or \
           join_hits.get(("orders", "order_items"), 0) > 0:
            patterns.append("PATTERN-1: Fetch user with embedded orders and order items")

        # Pattern 2: Product + reviews
        if join_hits.get(("products", "reviews"), 0) > 0 or \
           table_hits.get("reviews", 0) > 100:
            patterns.append("PATTERN-2: Fetch product with aggregated reviews")

        # Pattern 3: Event stream (insert-heavy)
        if table_hits.get("events", 0) > 0:
            patterns.append("PATTERN-3: Append-only event log per user")

        # Pattern 4: Order fulfillment join
        pair = tuple(sorted(["order_items", "products"]))
        if join_hits.get(pair, 0) > 0:
            patterns.append("PATTERN-4: Order items with denormalized product snapshot")

        return patterns

    # ── Report ────────────────────────────────────────────────────────────────

    def print_report(self):
        if not self.stats:
            self.analyze()
        s = self.stats

        print("=" * 60)
        print("  WORKLOAD ANALYZER REPORT")
        print("=" * 60)
        print(f"\n  Simulated queries : {s.total_queries:,}")
        print(f"  Reads             : {s.read_count:,}")
        print(f"  Writes            : {s.write_count:,}")
        print(f"  Read/Write ratio  : {s.read_write_ratio:.2f}:1")
        print(f"  Overall join ratio: {s.overall_join_ratio:.2%}")

        print("\n  Table Access Frequency:")
        for t, cnt in sorted(s.table_access_frequency.items(), key=lambda x: -x[1]):
            bar = "█" * int(s.table_access_ratio[t] * 20)
            print(f"    {t:<15} {cnt:>6,}  {bar}")

        print("\n  Top Join Pairs:")
        for pair, cnt in sorted(s.join_frequency.items(), key=lambda x: -x[1])[:6]:
            print(f"    {pair[0]} ⟷ {pair[1]:<20} {cnt:>6,} queries")

        print("\n  Hot Query Patterns (top 5):")
        for i, q in enumerate(s.hot_queries, 1):
            short = q["sql"][:70] + ("..." if len(q["sql"]) > 70 else "")
            print(f"    {i}. [{q['count']:,}×] {short}")

        print("\n  Derived NoSQL Access Patterns:")
        for p in s.access_patterns:
            print(f"    → {p}")

        print("\n" + "=" * 60)


if __name__ == "__main__":
    wa = WorkloadAnalyzer(n_simulated_queries=10_000)
    wa.analyze()
    wa.print_report()
