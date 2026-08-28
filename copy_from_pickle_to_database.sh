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
YEAR=${YEAR:?}
MONTH=${MONTH:?}
MODEL=${MODEL:?}

echo "Copying monthly digests to SQLite for MODEL=${MODEL}, YEAR=${YEAR}, MONTH=${MONTH}"

# go to /home/sadhika8/JupyterLinks/nobackup
 
python -u -m quads.copy_from_monthly_pickle_to_sqlitedb \
	--model "$MODEL" \
	--year "$YEAR" \
	--month "$MONTH"

# now delete all daily .pkl files -- careful here!. Runs for merra2 as well, but does not find anything to delete
MONTH_PADDED=$(printf "%02d" "$MONTH")
MONTH_DIR="/home/sadhika8/JupyterLinks/nobackup/quads_data/${MODEL}/${YEAR}/${MONTH_PADDED}"

if [[ -d "$MONTH_DIR" ]]; then
    echo "Deleting daily pickle files in $MONTH_DIR"
    rm -f "${MONTH_DIR}/${YEAR}"*.pkl
else
    echo "Directory not found: $MONTH_DIR"
fi

