#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PYTHONUNBUFFERED=1

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo "[run] Installing dependencies (if needed)"
pip install -r requirements.txt

echo "[run] Running preflight checks"
bash scripts/preflight_node_master.sh

echo "[run] Starting FastAPI on :8000"
python -m uvicorn yelp_text_to_sql.api:app --host 0.0.0.0 --port 8000 --log-level info &
API_PID=$!

echo "[run] Starting Streamlit on :8501"
streamlit run app.py --server.address 0.0.0.0 --server.port 8501 &
ST_PID=$!

cleanup() {
  echo "[run] Shutting down services..."
  kill "$API_PID" "$ST_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

HOST_ADDR="$(hostname -I 2>/dev/null | awk '{print $1}')"
if [[ -z "${HOST_ADDR}" ]]; then
  HOST_ADDR="127.0.0.1"
fi

echo "[run] Services are up"
echo "  - API:       http://${HOST_ADDR}:8000/docs"
echo "  - Streamlit: http://${HOST_ADDR}:8501"

echo "Press Ctrl+C to stop both services"
wait "$API_PID" "$ST_PID"
