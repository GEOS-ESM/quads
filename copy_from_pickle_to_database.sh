#!/usr/bin/env bash
#SBATCH --job-name=copy_month_to_sqlite
#SBATCH --account=s2441
#SBATCH --time=0:20:00
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%j.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/file.%j.err

set -euo pipefail

cd /home/sadhika8/JupyterLinks/nobackup/quads_dev # needed only if there are relative paths
module purge 2>/dev/null || true
module load python/GEOSpyD

# Avoid picking up ~/.local installs (forces venv+GEOSpyD only)
export PYTHONNOUSERSITE=1

source /home/sadhika8/JupyterLinks/nobackup/quads_dev/.venv/bin/activate # activates the virtual environment

# Year/month/model for this run (allow sbatch --export to override)
YEAR=${YEAR:-2023}
MONTH=${MONTH:-2}
MODEL=${MODEL:-MERRA2}

echo "Copying monthly digests to SQLite for MODEL=${MODEL}, YEAR=${YEAR}, MONTH=${MONTH}"

python -u -m quads.copy_from_monthly_pickle_to_sqlitedb \
	--model "$MODEL" \
	--year "$YEAR" \
	--month "$MONTH"
