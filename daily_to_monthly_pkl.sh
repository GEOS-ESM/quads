#!/usr/bin/env bash
#SBATCH --job-name=merge_month
#SBATCH --account=s2441
#SBATCH --time=0:40:00
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%j.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%j.err

set -euo pipefail


cd /home/sadhika8/JupyterLinks/nobackup/quads_dev

module purge 2>/dev/null || true
module load python/GEOSpyD

# Avoid picking up ~/.local installs (forces venv+GEOSpyD only)
export PYTHONNOUSERSITE=1

source /home/sadhika8/JupyterLinks/nobackup/quads_dev/.venv/bin/activate # activates the virtual environment

YEAR=${YEAR:-2024}
MONTH=${MONTH:-6}
MODEL=${MODEL:-GEOSFP}

python -u -m quads.merge_digests_and_write_pickle \
  --model "$MODEL" \
  --year "$YEAR" \
  --month "$MONTH"

