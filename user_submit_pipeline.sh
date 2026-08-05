#!/usr/bin/env bash
#SBATCH --job-name=quads_user_result
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/%x.%A_%a.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/%x.%A_%a.err 
#SBATCH --account=s2441
#SBATCH --time=6:00:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --array=1-31 
# original: #SBATCH --array=1-31

set -euo pipefail

cd /home/sadhika8/JupyterLinks/nobackup/quads_dev

module purge 2>/dev/null || true
module load python/GEOSpyD

# Avoid picking up ~/.local installs (forces venv+GEOSpyD only)
export PYTHONNOUSERSITE=1

source /home/sadhika8/JupyterLinks/nobackup/quads_dev/.venv/bin/activate # activates the virtual environment

# -----------------------------

# user inputs

MODEL="GEOSFP"
DATE="2024-12"

mkdir -p submission_records

LOGFILE="submission_records/user_results_submission.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') Compute results jobs submitted for MODEL=$MODEL MONTH=$DATE" >> "$LOGFILE"

LOG_BASE="/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/quads_user_${MODEL}_${DATE}.${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
exec > "${LOG_BASE}.out" 2> "${LOG_BASE}.err"
#
DAY=$(printf "%02d" "${SLURM_ARRAY_TASK_ID}")

days_in_month=$(python - <<PY
from calendar import monthrange
y, m = map(int, "${DATE}".split("-"))
print(monthrange(y, m)[1])
PY
)

if (( 10#$DAY > days_in_month )); then
    echo "Skipping invalid date ${DATE}-${DAY}"
    exit 0
fi

DATE="${DATE}-${DAY}"

echo "Submitting QUADS user job"
echo "MODEL=$MODEL"
echo "DATE=$DATE"

python -u -m quads.for_users \
	--model "$MODEL" \
	--date "$DATE"
