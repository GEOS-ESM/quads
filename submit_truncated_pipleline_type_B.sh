#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# -----------------------------
YEAR=2022
MONTH=11
MODEL="MERRA2"

# -----------------------------
# Pipeline submission
# -----------------------------
jid3=$(sbatch --parsable --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL copy_from_pickle_to_database.sh)
echo "Submitted step3 (sqlite populate): $jid3"

echo "Done. Job:"
echo "  step3: $jid3"
