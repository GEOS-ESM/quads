#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Inputs
# -----------------------------
YEAR=2021
MONTH=2
MODEL="GEOSFP"

# -----------------------------
# Pipeline submission
# -----------------------------
jid3=$(sbatch --parsable --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL copy_from_pickle_to_database.sh)
echo "Submitted step3 (sqlite populate): $jid3"

echo "Done. Jobs:"
echo "  step3: $jid3"
