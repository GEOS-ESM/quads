#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Inputs for: geosfp, geoscf, and g (with daily file directories)
# -----------------------------
YEAR=2021
MONTH=2
MODEL="GEOSFP"

# -----------------------------
# Pipeline submission
# -----------------------------
jid1=$(sbatch --parsable --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL submit_month_type_A.sh)
echo "Submitted step1 (daily digests): $jid1"

jid2=$(sbatch --parsable --dependency=afterok:$jid1 --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL daily_to_monthly_pkl.sh)
echo "Submitted step2 (merge monthly): $jid2"

jid3=$(sbatch --parsable --dependency=afterok:$jid2 --export=ALL,YEAR=$YEAR,MONTH=$MONTH,MODEL=$MODEL copy_from_pickle_to_database.sh)
echo "Submitted step3 (sqlite populate): $jid3"

echo "Done. Jobs:"
echo "  step1: $jid1"
echo "  step2: $jid2"
echo "  step3: $jid3"

