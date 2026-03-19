#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR="${SCRIPT_DIR}/dbt"

export DBT_PROFILES_DIR="${PROJECT_DIR}"
export DEFAULT_POSTGRES_HOST=$([ -n "${KUBERNETES_SERVICE_HOST:-}" ] && echo "postgres" || echo "localhost")

DBT_VARS=$(printf '{
  "audience_window_days": %s,
  "high_intent_score_threshold": %s,
  "high_intent_meaningful_threshold": %s,
  "newly_engaged_window_days": %s,
  "incremental_backfill_days": %s
}' \
  "${DBT_AUDIENCE_WINDOW_DAYS:-30}" \
  "${DBT_HIGH_INTENT_SCORE_THRESHOLD:-10}" \
  "${DBT_HIGH_INTENT_MEANINGFUL_THRESHOLD:-5}" \
  "${DBT_NEWLY_ENGAGED_WINDOW_DAYS:-14}" \
  "${DBT_INCREMENTAL_BACKFILL_DAYS:-1}"
)

run_dbt() {
    echo "Running: dbt $*"
    dbt "$@"
}

cd "${PROJECT_DIR}"

echo "Starting data-modeling (dbt)"
run_dbt debug --vars "${DBT_VARS}"
run_dbt seed --vars "${DBT_VARS}"
run_dbt build --vars "${DBT_VARS}"
echo "data-modeling finished successfully"