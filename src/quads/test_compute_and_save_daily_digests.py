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
import pandas as pd

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
# single-process analyse
# ------------------------------------------------------------------
def analyse(da_sel, key):
    """processes one chunk of data generating results (single-process, no dask)"""

    arr_np = np.asarray(da_sel)  # becomes numpy array (forces compute)
    flat = arr_np.reshape(-1)
    #print(flat)
    flat = flat[~np.isnan(flat)]

    if flat.size > 0:
        digest = create_digest(flat, compression=300)
        q_vals, q_list = get_quantiles_from_tdigest(digest)
        
        #print(key, flat, q_vals)
        return {
            "id_key": key,
            "flat": flat,      # <-- store original flattened data
            "q_vals": q_vals,
            "q_list": q_list,
                }
    else:
        print(key)
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
    #c = 0
    for coll_name, files in collection_dict.items():
        if coll_name != "tavg1_2d_lfo_Nx":
            continue
        print(f"Processing collection: {coll_name}")
        if not files:
            print(f"{coll_name} has no files to process")
            continue

        start = datetime.now()
        ds = xr.open_mfdataset(
            files,
            combine="by_coords",
            drop_variables=excluded,
            data_vars="minimal",
            coords="minimal",
            compat="no_conflicts",
            engine="h5netcdf",
            chunks=None,     # single-process: no dask chunking
            parallel=False,  # single-process
        )
        end = datetime.now()
        print(f"time to open metadataset: {(end - start).total_seconds()}")

        # 3.2) Identify coordinate names (lat, optional level).
        lat_name = next((c for c in LAT_NAMES if c in ds.coords), None)
        lev_dim = next((d for d in LEV_NAMES if d in ds.coords), None)

        lat_arr = ds[lat_name].values
        is_strictly_ascending = np.all(np.diff(lat_arr) > 0)
        is_strictly_descending = np.all(np.diff(lat_arr) < 0)

        # 3.3) For each variable...
        for var in ds.data_vars:
            da = ds[var]

            # 3.4) For each level (or None if no level coord)...
            levels = (
                ds[lev_dim].values
                if lev_dim and (lev_dim in da.coords)
                else [None]
            )

            for lev_val in levels:
                da_lev = da.sel({lev_dim: lev_val}) if lev_val is not None else da

                # 3.5) For each stratum (lat band)
                for sname, sdef in strata.items():
                    lat_min, lat_max = sdef["lat"]["min"], sdef["lat"]["max"]
                    if is_strictly_ascending:
                        lat_slice = slice(lat_min, lat_max)
                    elif is_strictly_descending:
                        lat_slice = slice(lat_max, lat_min)
                    else:
                        raise ValueError(f"Latitude coordinate {lat_name} is not monotonic: {lat_arr}")

                    da_sel = da_lev.sel({lat_name: lat_slice})
                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"

                    # single-process: run immediately and store result
                    all_payloads_for_day.append(analyse(da_sel, id_key))

        ds.close()

    return all_payloads_for_day


# ------------------------------------------------------------------
if __name__ == "__main__":

    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"

    results = compute_results(
        model="GEOSFP",
        date=datetime(2024, 4, 10),
        data_yaml_file=data_yaml_file,
        strata_file=strata_file,
    )

    # Save everything (including 'flat') for later inspection/plotting
    out_pkl = "debug_results.pkl"
    with open(out_pkl, "wb") as f:
        pickle.dump(results, f)

    # Optional: convenience DataFrames for quantiles (ragged lengths OK via Series/concat)
    df_q_vals = pd.concat([pd.Series(r["q_vals"], name=r["id_key"]) for r in results if r is not None], axis=1)
    df_q_list = pd.concat([pd.Series(r["q_list"], name=r["id_key"]) for r in results if r is not None], axis=1)
    df_q_vals.to_pickle("debug_q_vals.pkl")
    df_q_list.to_pickle("debug_q_list.pkl")

    # Optional: store flat as a dict of arrays (cleaner than forcing into a DataFrame)
    flats = {r["id_key"]: r["flat"] for r in results if r is not None}
    with open("debug_flat.pkl", "wb") as f:
        pickle.dump(flats, f)

    print(f"Saved: {out_pkl}, debug_flat.pkl, debug_q_vals.pkl, debug_q_list.pkl")

