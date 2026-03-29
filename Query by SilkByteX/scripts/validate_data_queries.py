#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from yelp_text_to_sql.database import execute_sql

CHECKS = [
    ("business_table", "SELECT COUNT(*) AS n FROM business"),
    ("review_or_rating_table", "SELECT COUNT(*) AS n FROM review"),
    ("users_table", "SELECT COUNT(*) AS n FROM users"),
    ("checkin_table", "SELECT COUNT(*) AS n FROM checkin"),
    ("top_cities", "SELECT city, COUNT(*) AS c FROM business GROUP BY city ORDER BY c DESC LIMIT 10"),
    ("top_reviewers", "SELECT user_id, COUNT(*) AS c FROM review GROUP BY user_id ORDER BY c DESC LIMIT 10"),
    ("rating_distribution", "SELECT stars, COUNT(*) AS c FROM review GROUP BY stars ORDER BY stars"),
    ("checkin_rank", "SELECT business_id, LENGTH(date) AS raw_len FROM checkin ORDER BY raw_len DESC LIMIT 10"),
]


def main() -> int:
    failed = []
    print("Running data query validation checks...")
    for name, sql in CHECKS:
        result = execute_sql(sql)
        ok = bool(result.executed)
        rows = len(result.rows)
        print(f"[{ 'PASS' if ok else 'FAIL' }] {name}: rows={rows} message={result.message}")
        if not ok:
            print(f"   error: {result.error}")
            failed.append(name)

    if failed:
        print("\nValidation failed for:")
        for name in failed:
            print(f"- {name}")
        return 1

    print("\nAll core data query checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
