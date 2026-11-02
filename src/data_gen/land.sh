#!/usr/bin/env bash
# Land generated source-system exports into a target's UC volumes.
# Usage: src/data_gen/land.sh <schema> [catalog]   e.g. land.sh mkr_claims
set -euo pipefail
SCHEMA=${1:?schema required}
CATALOG=${2:-serverless_stable_mkr_catalog}
SRC=${SRC:-data/raw}
V="dbfs:/Volumes/$CATALOG/$SCHEMA"

for d in policy_admin vendors weather claims_fnol siu; do
  databricks fs cp -r --overwrite "$SRC/$d" "$V/landing/$d"
done
databricks fs cp --overwrite "$SRC/_simulation_truth/fraud_truth.csv" "$V/simulation/fraud_truth.csv"

for d in policy_admin vendors weather claims_fnol siu; do
  echo "landing/$d: $(databricks fs ls "$V/landing/$d" | wc -l | tr -d ' ') files"
done
echo "simulation: $(databricks fs ls "$V/simulation" | tr '\n' ' ')"
