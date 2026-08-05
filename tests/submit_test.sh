#!/usr/bin/env bash
#SBATCH --job-name=test_digest
#SBATCH --account=s2441
#SBATCH --time=01:00:00
#SBATCH --output=job.%j.out
#SBATCH --error=job.%j.err

module load python/GEOSpyD
source .venv/bin/activate
python -u test_compute_and_save_daily_digests.py --date "2023-01-01" --model "GEOSFP"
