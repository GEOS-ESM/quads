#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Inputs, for geosfp and geoscf
# -----------------------------
YEAR=2021
MONTH=2
MODEL="GEOSFP"

# -----------------------------
# Pipeline submission
# -----------------------------
jid2=$(sbatch --parsable --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL daily_to_monthly_pkl.sh)
echo "Submitted step2 (merge monthly): $jid2"

jid3=$(sbatch --parsable --dependency=afterok:$jid2 --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL copy_from_pickle_to_database.sh)
echo "Submitted step3 (sqlite populate): $jid3"

echo "Done. Jobs:"
echo "  step2: $jid2"
echo "  step3: $jid3"
