#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo "[1/7] Python + package check"
python - <<'PY'
import importlib
mods = ["sqlalchemy", "pyhive", "thrift", "thrift_sasl", "pyspark"]
for m in mods:
    try:
        importlib.import_module(m)
        print(f"OK: {m}")
    except Exception as e:
        print(f"MISSING: {m} ({e})")
PY

echo "[2/7] Environment snapshot"
python - <<'PY'
import os
keys = [
    "YELP_SQL_ENGINE","HIVE_HOST","HIVE_PORT","HIVE_DATABASE","HIVE_AUTH",
    "HIVE_USERNAME","SPARK_MASTER","SPARK_APP_NAME","SPARK_WAREHOUSE_DIR","HIVE_METASTORE_URI",
    "DEEPSEEK_MODEL"
]
for k in keys:
    v = os.getenv(k, "")
    if k in {"HIVE_USERNAME","DEEPSEEK_MODEL"} and v:
        print(f"{k}=<set>")
    else:
        print(f"{k}={v}")
PY

echo "[3/7] Backend smoke (run_test_query)"
python - <<'PY'
from yelp_text_to_sql.database import run_test_query
res = run_test_query()
print("executed:", res.executed)
print("message:", res.message)
print("error:", res.error)
print("rows:", len(res.rows))
if not res.executed:
    raise SystemExit(2)
PY

echo "[4/7] Required table checks"
python - <<'PY'
from yelp_text_to_sql.database import execute_sql
for t in ["business","review","users","checkin"]:
    r = execute_sql(f"SELECT COUNT(*) as c FROM {t}")
    print(f"{t}: executed={r.executed}, error={r.error}, rows={r.rows[:1]}")
    if not r.executed:
        raise SystemExit(3)
PY

echo "[5/7] Text-to-SQL pipeline smoke"
python - <<'PY'
from yelp_text_to_sql.pipeline import run_natural_language_query
res = run_natural_language_query("Show me total number of reviews", use_demo_mode=False)
print("status:", res.status)
print("success:", res.success)
print("retry:", res.retry_status)
print("sql:", res.final_sql[:180])
print("rows:", len(res.rows))
if not res.final_sql:
    raise SystemExit(4)
PY

echo "[6/7] Core data-query validation pack"
python scripts/validate_data_queries.py

echo "[7/7] Streamlit import smoke"
python - <<'PY'
import yelp_text_to_sql.ui
print("ui import OK")
PY

echo "Preflight passed: backend is deployment-ready."
