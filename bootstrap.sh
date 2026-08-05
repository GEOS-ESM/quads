#!/usr/bin/env bash
set -euo pipefail

module load python/GEOSpyD
export PYTHONNOUSERSITE=1

if [[ ! -d ".venv" ]]; then
  python -m venv --system-site-packages .venv
fi
source .venv/bin/activate

python -m pip install -U pip
python -m pip install -r requirements.txt
python -m pip install -e .
