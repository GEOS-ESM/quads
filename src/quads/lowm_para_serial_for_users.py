# lowm_para_serial_for_users.py
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict
import pickle
import sqlite3
import yaml
import xarray as xr
import numpy as np
import pandas as pd
import dask
from dask import delayed

from quads.get_collections_and_files import list_files_and_excluded_vars
from quads.sanity_check_v2 import edge_check

# Coordinate name preferences (in order)
LEV_NAMES = ["lev", "level", "pressure"]
LAT_NAMES = ["lat", "latitude", "y"]


# -----------------------------
# helpers
# -----------------------------
def load_strata(strata_yaml_file: str | Path) -> Dict[str, Dict]:
    """Load the 'STRATA' section from the provided YAML file."""
    with open(strata_yaml_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]


def load_quantiles_from_db(db_path: Path, model: str, year: int, month: int, id_string: str):
    """
    Fetch (quantiles, quantile_list) for a given key/month from SQLite.
    Print msg if not found.
    """
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            f"SELECT quantiles, quantile_list FROM {model} "
            "WHERE model=? AND year=? AND month=? AND id_string=?",
            (model, year, month, id_string),
        ).fetchone()

    if row is None:
        print(
            f"No reference quantiles in DB for "
            f"(model={model}, year={year}, month={month}, id_string='{id_string}') "
            f"at {db_path}"
        )
        return None

    return pickle.loads(row[0]), pickle.loads(row[1])

@delayed
def analyse(da_sel,
            key,
            model,
            reference_year,
            reference_month,
            database_path,
            ):
    arr_np = np.asarray(da_sel) # becomes numpy array
    data = arr_np.reshape(-1)

    ref = load_quantiles_from_db(database_path, model, reference_year, reference_month, key)

    if ref is None:
        return None

    quantiles, qlist = ref
    n_low, n_high, fence_low, fence_high = edge_check(data, (quantiles, qlist))
    #no_viol = int(n_low + n_high)
    return key, n_low, n_high, fence_low, fence_high,  quantiles, qlist
# -----------------------------
# main driver
# -----------------------------
def compute_and_save_results(
    model: str,
    date: datetime,
    data_yaml_file: str,
    strata_file: str,
    historical_reference_date: datetime,
    db_path: str | Path,
) -> pd.DataFrame:
    """
    For each (collection,var,level,stratum) slice:
      - flatten data
      - fetch reference quantiles from SQLite (for this model/year/month/id)
      - run sanity check (edge_check)
      - return row: id_string, no_of_violations, quantiles, quantile_list
    """
    # Resolve files and exclusions from YAML
    _, collection_dict, excluded = list_files_and_excluded_vars(
        model=model, date=date, data_yaml_file=data_yaml_file
    )

    # Load strata definitions
    strata = load_strata(strata_file)

    df = pd.DataFrame(columns=["id_string", "no_of_violations_left", "no_of_violations_right","fence_low","fence_high", "quantile_values", "q_list"])

    for coll_name, files in list(collection_dict.items()):
        #if coll_name != "inst3_2d_asm_Nx":
        #   continue

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

                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"

                    # 3.6) Queue the job.
                    delayed_jobs.append(analyse(da_sel, id_key, model, historical_reference_date.year, historical_reference_date.month, Path(db_path)))

        print(len(delayed_jobs))
        finished = dask.compute(*delayed_jobs, scheduler="threads")
        finished = [x for x in finished if x is not None]
        if finished:
            new_df = pd.DataFrame(finished, columns=df.columns)
            if df.empty:
                df = new_df
            else:
                df = pd.concat([df, new_df], ignore_index=True)
    
    return df


# -----------------------------
# __main__
# -----------------------------
if __name__ == "__main__":

    model = "GEOSFP"
    model_lower = model.lower()
    date = datetime(2025, 5, 10)             
    historical_reference_dates = [
            date - relativedelta(years=1, months=1),
            date - relativedelta(years=1), 
            date - relativedelta(years=1, months=-1),
    ]
    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"
    db_path = Path(f"/home/sadhika8/JupyterLinks/nobackup/quads_database/{model_lower}_monthly_aggregated_centroids_and_quantiles.db")

    for historical_reference_date in historical_reference_dates:
        df = compute_and_save_results(
            model=model,
            date=date,
            data_yaml_file=data_yaml_file,
            strata_file=strata_file,
            historical_reference_date=historical_reference_date,
            db_path=db_path,
    )

        ref_str = historical_reference_date.strftime("%Y-%m")
        df_var_name = f"quads_test_{model}_{date.strftime('%Y_%m_%d')}_reference_date_{ref_str}"
        out_file = f"{df_var_name}.pkl"
        df.to_pickle(out_file)

        print(f"Created DataFrame '{df_var_name}' with {len(df)} rows.")
        print(f"✔ Saved DataFrame to ./{out_file}")
        print(df.head())

# for a comprehensive data analysis the users should be able to access the chunk of data they want to analyze -- I can write a script for that

