# This is the main python script for quads
''' Computes daily digests and saves them on a .pkl file. For MERRA2 and GEOSIT, there will be a montly digest as the source data are stored in montly subdirectories'''

from pathlib import Path
from datetime import datetime
from typing import Dict
import pickle
import os

import yaml
import xarray as xr
import numpy as np
import dask
#from dask import delayed
#from dask.diagnostics import Profiler, ResourceProfiler

from quads.get_collections_and_files import list_files_and_excluded_vars
from quads.make_tdigest import create_digest, get_quantiles_from_tdigest

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


@dask.delayed
def analyse(da_sel, key):
    """ processes one chunk of data generating results"""
    arr_np = np.asarray(da_sel) # becomes numpy array
    flat = arr_np.reshape(-1)
        
    flat = flat[~np.isnan(flat)]
    if flat.size > 0:
        digest = create_digest(flat, compression=300)
        quantiles = get_quantiles_from_tdigest(digest)
        payload = {
            "id_key": key,
            "centroids": digest.get_centroids(),
            "quantiles": quantiles,
        }
        #print("done")
        return payload
    else:
        return None
# ------------------------------------------------------------------
# main driver
# ------------------------------------------------------------------
def compute_results(
    model: str,
    date: datetime,
    data_yaml_file: str,
    strata_file: str,
):
    """
    Computes t-digests and returns all results for this day (or month, depending on the model) as a list
    of payload dicts. Writing to disk is handled in __main__.
    <date> - day of interest
    <data_yaml_file> - path of config file with input data address
    <strata_file> - path of config file listing lat/lon strata
    """
    # 1) Resolve get collections, files, and variables to exclude
    _, collection_dict, excluded = list_files_and_excluded_vars(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
    )

    # 2) Load strata definitions.
    strata = load_strata(strata_file)

    # Collect all payloads for this date in memory (on driver).
    all_payloads_for_day = []

    # 3) For each collection, open datasets and build jobs.
    for coll_name, files in collection_dict.items():
        print(coll_name)
        if not files:
            print(f"{coll_name} has no files to process")
            continue
        # 3.1) Open multi-file dataset lazily with xarray/dask.

        if coll_name != "tavg3_3d_mst_Nv":
            continue

        print(f"Found {len(files)} files from collection: {coll_name}")
        print(files)

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
        # print(ds.dims)
        end = datetime.now()
        print(f"time to open metadataset: {(end - start).total_seconds()}")

        # 3.2) Identify coordinate names (lat, optional level).
        lat_name = next((c for c in LAT_NAMES if c in ds.coords), None)
        lev_dim = next((d for d in LEV_NAMES if d in ds.coords), None)

        delayed_jobs = []

        lat_arr = ds[lat_name].values
        is_strictly_ascending  = np.all(np.diff(lat_arr) > 0) 
        is_strictly_descending = np.all(np.diff(lat_arr) < 0) 


        # 3.3) For each variable...
        for var in ds.data_vars:
            da = ds[var]

            #print(var)
            # 3.4) For each level (or None if no level coord)...
            levels = (
                ds[lev_dim].values
                if lev_dim and (lev_dim in da.coords)
                else [None]
            )
            # collection, variable, level - data
            for lev_val in levels:
                da_lev = da.sel({lev_dim: lev_val}) if lev_val is not None else da

                # 3.5) For each stratum (lat band) -- process stratum level data
                for sname, sdef in strata.items():
                    lat_min, lat_max = sdef["lat"]["min"], sdef["lat"]["max"]
                    if is_strictly_ascending:
                        lat_slice = slice(lat_min, lat_max)
                    elif is_strictly_descending:
                        lat_slice = slice(lat_max, lat_min)
                    else:
                        raise ValueError(f"Latitude coordinate {lat_name} is not monotonic: {lat_arr}")


                    da_sel = da_lev.sel({lat_name: lat_slice})

                    #print(f"""data for collection {coll_name}, variable {var}, level {lev_val},
                    #lat_slice {lat_min}: {lat_max}:""")

                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"

                    da_sel.to_netcdf(f"da_sel_{id_key}.nc4")
                    # 3.6) Queue the job.
                    delayed_jobs.append(analyse(da_sel, id_key))

            print(var, len(delayed_jobs))
        # 4) Execute all delayed jobs (reads ➜ computes ➜ returns payloads).
        if delayed_jobs:
            start = datetime.now()
            finished = dask.compute(*delayed_jobs, scheduler="threads", num_workers=40)
            if finished is None:
                print(None)
            finished = [elem for elem in finished if elem is not None]
            end = datetime.now()
            print(
                f"✔ {len(finished)} results computed for collection {coll_name} "
                 f"in {(end - start).total_seconds()} secs"
            )

            # finished is a tuple/list of payload dicts
            all_payloads_for_day.extend(finished)

        ds.close()

    return all_payloads_for_day


# ------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date in YYYY-MM-DD format")
    parser.add_argument("--model", default="GEOSFP", help="Model name")
    args = parser.parse_args()

    out_dir = "/home/sadhika8/JupyterLinks/nobackup/quads_data"

    # model: from CLI if given, else default "GEOSFP"
    model = args.model

    # date: 
    date = datetime.strptime(args.date, "%Y-%m-%d")

    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"

    results = compute_results(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
        strata_file=strata_file,
    )

    # One daily file under out_dir/model/YYYY/MM/YYYY-MM-DD.pkl, one monthly for MERRA2 and GEOSIT
    base = Path(out_dir)
    year_dir = base / model / f"{date.year:04d}"
    month_dir = year_dir / f"{date.month:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)

    day_str = date.strftime("%Y-%m-%d")
    month_str = date.strftime("%Y-%m")

    if model in ["GEOSFP", "GEOSCF"]:
        final = month_dir / f"test_{day_str}.pkl"
        tmp = month_dir / f".{day_str}.pkl.tmp"
    else: # directly writes a monthly aggregated .pkl in case of GEOSIT and MERRA2
        aggregated_month_str = f"monthly_merged_digest_{model}_{month_str}"
        final = month_dir / f"test_{aggregated_month_str}.pkl"
        tmp = month_dir / f".{aggregated_month_str}.pkl.tmp"

    with open(tmp, "wb") as fh:
        pickle.dump(results, fh, protocol=pickle.HIGHEST_PROTOCOL)

    os.replace(tmp, final)
    print(f"Saved {len(results)} payloads to {final}")

