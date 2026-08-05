# run_month_batch.py
import calendar, os
from pathlib import Path
from datetime import datetime

from lowm_para_serial import compute_and_save_results, zip_dir, cleanup_after_zip

# ---- set month here ----
OUT_DIR_ROOT = "/discover/nobackup/ashiklom/thamzey/HOME/quadsql/quads/data_from_newquads"
MODEL = "GEOSFP"
YEAR, MONTH = 2023, 6
DATA_YAML = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
STRATA_YAML = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"
# ------------------------

monthly_out = Path(OUT_DIR_ROOT) / f"{MODEL}_{YEAR}_{MONTH:02d}"
monthly_out.mkdir(parents=True, exist_ok=True)

# Determine which week-chunk to run:
# Use SLURM_ARRAY_TASK_ID if present (1..5), else fall back to whole month.
_, ndays = calendar.monthrange(YEAR, MONTH)
chunks = [(1, 7), (8, 14), (15, 21), (22, 28), (29, ndays)]

task_id = os.getenv("SLURM_ARRAY_TASK_ID")
if task_id:
    i = max(1, min(5, int(task_id))) - 1
    start_day, end_day = chunks[i]
else:
    start_day, end_day = 1, ndays  # not an array job: process whole month

# Clamp end_day to the month length (handles Feb, 30/31):
end_day = min(end_day, ndays)

for day in range(start_day, end_day + 1):
    date = datetime(YEAR, MONTH, day)
    print(f"[{date.date()}] Starting…")

    compute_and_save_results(
        model=MODEL,
        date=date,
        data_yaml_file=DATA_YAML,
        strata_file=STRATA_YAML,
        out_dir=str(monthly_out),
    )

    date_dir = monthly_out / date.strftime("%Y-%m-%d")
    zip_path = date_dir / "results.zip"
    print(f"[{date.date()}] Zipping {date_dir} → {zip_path} …")
    zipped = zip_dir(date_dir, zip_path)
    print(f"[{date.date()}] ✔ Zipped to: {zipped}")

    cleanup_after_zip(date_dir, zip_name="results.zip")
    print(f"[{date.date()}] ✔ Cleaned up loose files.\n")

