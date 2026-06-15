#!/usr/bin/env bash
#SBATCH --job-name=quads_typeA
#SBATCH --account=s2441
#SBATCH --time=6:00:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%A_%a.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%A_%a.err
#SBATCH --array=1-31

set -euo pipefail

cd /home/sadhika8/JupyterLinks/nobackup/quads_dev # needed only if there are relative paths
module purge 2>/dev/null || true
module load python/GEOSpyD

# Avoid picking up ~/.local installs (forces venv+GEOSpyD only)
export PYTHONNOUSERSITE=1

source /home/sadhika8/JupyterLinks/nobackup/quads_dev/.venv/bin/activate # activates the virtual environment

# Year/month/model for this run (allow sbatch --export to override)
YEAR=${YEAR:-2024}
MONTH=${MONTH:-4}
MODEL=${MODEL:-GEOSFP}

MONTH_PADDED=$(printf "%02d" "${MONTH}")
DAY_PADDED=$(printf "%02d" "${SLURM_ARRAY_TASK_ID}")
DATE_STR="${YEAR}-${MONTH_PADDED}-${DAY_PADDED}"

# ignore invalid days
CHECK_MONTH=$(date -d "${DATE_STR}" +%m 2>/dev/null || echo "XX")
if [ "${CHECK_MONTH}" != "${MONTH_PADDED}" ]; then
    echo "Skipping invalid date: ${DATE_STR}"
    exit 0
fi

echo "Running for DATE=${DATE_STR}, MODEL=${MODEL}"

python -u -m quads.compute_and_save_daily_digests \
    --date "${DATE_STR}" \
    --model "${MODEL}" # no need of .py, since it is installed, u - for real time buffering

