# This is the main python script for quads

from pathlib import Path
from datetime import datetime
from typing import Dict
import pickle
import os

import yaml
import xarray as xr
import dask
from dask import delayed
from dask.diagnostics import Profiler, ResourceProfiler

from get_collections_and_files import list_files_and_excluded_vars
from make_tdigest import create_digest, get_quantiles_from_tdigest

# Coordinate name preferences (in order)
LEV_NAMES = ["lev", "level", "pressure"]
LAT_NAMES = ["lat", "latitude", "y"]


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------
def load_strata(strata_yaml_file: str | Path) -> Dict[str, Dict]:
    """Load the 'STRATA' section from a YAML file."""
    with open(strata_yaml_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]


# ------------------------------------------------------------------
# main driver
# ------------------------------------------------------------------
def compute_and_save_results(
    model: str,
    date: datetime,
    data_yaml_file: str,
    strata_file: str,
    out_dir: str,
):
    """
    Computes t-digests and returns all results for this day as a list
    of payload dicts. Writing to disk is handled in __main__.
    """
    # 1) Resolve files and collections from YAML.
    _, collection_dict, excluded = list_files_and_excluded_vars(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
    )

    # 2) Load strata definitions.
    strata = load_strata(strata_file)

    # 3) Ensure base output dir exists (model/year/month handled in __main__).
    OUT_DIR = Path(out_dir)
    OUT_DIR.mkdir(exist_ok=True)

    # Collect all payloads for this date in memory (on driver).
    all_payloads_for_day = []

    # 4) For each collection, open datasets and build jobs.
    for coll_name, files in list(collection_dict.items()):
        print(coll_name, files)
        # 4.1) Open multi-file dataset lazily with xarray/dask.
        start = datetime.now()
        ds = xr.open_mfdataset(
            files,
            combine="by_coords",
            drop_variables=excluded,
            data_vars="minimal",
            coords="minimal",
            compat="no_conflicts",
            engine="h5netcdf",
            chunks="auto",
            parallel=True,
        )
        print(ds.dims)
        end = datetime.now()
        print(f"time to open metadataset: {(end - start).total_seconds()}")

        # 4.2) Identify coordinate names (lat, optional level).
        lat_name = next(c for c in LAT_NAMES if c in ds.coords)
        lev_dim = next((d for d in LEV_NAMES if d in ds.coords), None)

        delayed_jobs = []

        # 4.3) For each variable...
        for var in ds.data_vars:
            da = ds[var]

            # 4.4) For each level (or None if no level coord)...
            levels = (
                ds[lev_dim].values
                if lev_dim and (lev_dim in da.coords)
                else [None]
            )

            for lev_val in levels:
                da_lev = da.sel({lev_dim: lev_val}) if lev_val is not None else da

                # 4.5) For each stratum (lat band)...
                for sname, sdef in strata.items():
                    lat_slice = slice(sdef["lat"]["min"], sdef["lat"]["max"])
                    dims2stack = [d for d in da_lev.dims]

                    # 4.6) Build a flat sample array (lazy dask array).
                    flat = (
                        da_lev.sel({lat_name: lat_slice})
                        .stack(sample=dims2stack)
                        .data.reshape(-1)  # lazy
                    )

                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"

                    # 4.7) Define delayed analytics pipeline for this slice.
                    @delayed
                    def analyse(arr, key=id_key):
                        digest = create_digest(arr, compression=300)
                        quantiles = get_quantiles_from_tdigest(digest)
                        payload = {
                            "id_key": key,
                            "centroids": digest.get_centroids(),
                            "quantiles": quantiles,  # same structure you used before
                        }
                        return payload

                    # 4.8) Queue the job.
                    delayed_jobs.append(analyse(flat))

        # 5) Execute all delayed jobs (reads ➜ computes ➜ returns payloads).
        if delayed_jobs:
            with Profiler() as prof, ResourceProfiler(dt=0.1) as rprof:
                start = datetime.now()
                finished = dask.compute(*delayed_jobs, scheduler="threads")
                end = datetime.now()
                print(
                    f"✔ {len(finished)} results computed for collection {coll_name} "
                    f"in {(end - start).total_seconds()} secs"
                )

            # finished is a tuple/list of payload dicts
            all_payloads_for_day.extend(finished)

        del ds  # help GC

    return all_payloads_for_day


# ------------------------------------------------------------------
if __name__ == "__main__":
    out_dir = "/home/sadhika8/JupyterLinks/nobackup/quads_data"
    model = "GEOSCF"
    date = datetime(2024, 12, 20)
    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"

    results = compute_and_save_results(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
        strata_file=strata_file,
        out_dir=out_dir,
    )

    # One daily file under out_dir/model/YYYY/MM/YYYY-MM-DD.pkl
    base = Path(out_dir)
    year_dir = base / model / f"{date.year:04d}"
    month_dir = year_dir / f"{date.month:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)

    day_str = date.strftime("%Y-%m-%d")
    final = month_dir / f"{day_str}.pkl"
    tmp = month_dir / f".{day_str}.pkl.tmp"

    with open(tmp, "wb") as fh:
        pickle.dump(results, fh, protocol=pickle.HIGHEST_PROTOCOL)

    os.replace(tmp, final)
    print(f"✅ Saved {len(results)} payloads to {final}")

