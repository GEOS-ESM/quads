#!/usr/bin/env bash
#SBATCH --job-name=quads_user
#SBATCH --account=s2441
#SBATCH --time=3:00:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/user.%j.out
#SBATCH --error=/home/sadhika8/JupyterLinks/nobackup/quads_dev/log_files/user.%j.err

set -euo pipefail

cd /home/sadhika8/JupyterLinks/nobackup/quads_dev

module purge 2>/dev/null || true
module load python/GEOSpyD

# Avoid picking up ~/.local installs (forces venv+GEOSpyD only)
export PYTHONNOUSERSITE=1

source /home/sadhika8/JupyterLinks/nobackup/quads_dev/.venv/bin/activate # activates the virtual environment

# -----------------------------
# User inputs (edit these only)
# -----------------------------
MODEL="GEOSFP"      # GEOSFP / GEOSCF / MERRA2 / GEOSIT
DATE="2024-02-10"   # YYYY-MM-DD for GEOSFP/GEOSCF
                    #  MERRA2/GEOSIT only needs YYYY-MM. But, the code expects the full format YYYY-MM-DD and ignores the DD part.

# Export to Python
export MODEL DATE

echo "Submitting QUADS user job"
echo "MODEL=$MODEL"
echo "DATE=$DATE"

python -u -m quads.for_users

