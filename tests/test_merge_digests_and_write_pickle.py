# merge daily pickle files into monthly pickles based on id_key

from __future__ import annotations

from pathlib import Path
import calendar
import pickle
import os
import argparse
import numpy as np

from pytdigest import TDigest
from quads.make_tdigest import get_quantiles_from_tdigest


def load_daily_results(daily_file: Path):
    """Load one daily pickle file (list of payload dicts)."""
    with open(daily_file, "rb") as fh:
        return pickle.load(fh)


def merge_month_for_model(
    base_out_dir: str,
    model: str,
    year: int,
    month: int,
    compression: int = 300,
):
    """
    Read all daily pickle files for (model, year, month),
    merge centroids per id_key using TDigest, and write a single pickle:

        monthly_merged_digest_{model}_{YYYY-MM}.pkl

    Output structure (pickled):

        [
                {"id_key": id_key'
                "centroids": merged_centroids,
                "quantiles": (quantiles,qlist)
            },
            ...
        ]
    """
    base = Path(base_out_dir)
    year_dir = base / model / f"{year:04d}"
    month_dir = year_dir / f"{month:02d}"

    if not month_dir.exists():
        raise FileNotFoundError(f"Month directory does not exist: {month_dir}")

    # How many days in this month?
    _, last_day = calendar.monthrange(year, month)

    # Bucket: id_key -> TDigest
    digests_by_key: dict[str, TDigest] = {}

    # check only a few days
    for day in range(1, 3):
        day_str = f"{year:04d}-{month:02d}-{day:02d}"
        daily_file = month_dir / f"{day_str}.pkl"


        if not daily_file.exists():
            print(f"WARNING!!! Skipping missing daily file: {daily_file}")
            continue

        print(f"Loading {daily_file}")
        daily_payloads = load_daily_results(daily_file)
        daily_payloads = [payload for payload in daily_payloads if payload is not None]

        for payload in daily_payloads:
            id_key = payload["id_key"]
            centroids = payload["centroids"]  # list of (mean, count)
            
            td_total = digests_by_key.get(id_key)
            td_day = TDigest.of_centroids(np.asarray(centroids), compression=compression)
            
            digests_by_key[id_key] = td_day if td_total is None else TDigest.combine(td_total, td_day)

    # Build final results: one entry per id_key
    #results = {}
    results = []
    for id_key, td in digests_by_key.items():
        merged_centroids = td.get_centroids()
        #quantiles, qlist = get_quantiles_from_tdigest(td)
        quantiles_and_qlist = get_quantiles_from_tdigest(td)
        
        results.append({
            "id_key": id_key,
            "centroids": merged_centroids,
            "quantiles": quantiles_and_qlist,
            })

    # "quantiles"- quantiles and levels
    # Write monthly merged pickle atomically
    month_str = f"{year:04d}-{month:02d}"
    out_name = f"two_merged_digest_{model}_{month_str}.pkl"
    final = month_dir / out_name
    tmp = month_dir / f".{out_name}.tmp"

    with open(tmp, "wb") as fh:
        pickle.dump(results, fh, protocol=4)

    os.replace(tmp, final)
    print(f"✅ Wrote monthly merged file: {final}")
    print(f"   Total keys: {len(results)}")


if __name__ == "__main__":

    # Adjust as needed - will stay same for all models
    BASE_OUT_DIR = "/home/sadhika8/JupyterLinks/nobackup/quads_data"
    merge_month_for_model(
        base_out_dir=BASE_OUT_DIR,
        model="GEOSFP",
        year=2024,
        month=1,
        compression=300,
    )

