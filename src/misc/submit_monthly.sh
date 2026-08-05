#!/usr/bin/env bash
#SBATCH --job-name=quads
#SBATCH --account=s2441
#SBATCH --partition=compute          # needed for 12h limit
#SBATCH --qos=allnccs               # matches your account on 'compute'
#SBATCH --time=12:00:00
#SBATCH --array=1-5%2                 # 1..4 = weeks, 5 = 29–end
#SBATCH --output=logs/%x.%A_%a.out  # unique logs per array task
#SBATCH --error=logs/%x.%A_%a.err
#SBATCH --export=ALL

mkdir -p logs

module purge
module load python/GEOSpyD

python run_monthly_batch.py

