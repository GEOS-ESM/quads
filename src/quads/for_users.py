# compute and store results with comparing daily data with historical
# SQLite database

from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
#from typing import Dict
import pickle
import sqlite3
import yaml
import xarray as xr
import numpy as np
import pandas as pd
import dask
from dask import delayed
import argparse
from pytdigest import TDigest

from quads.get_collections_and_files import list_files_and_excluded_vars
from quads.sanity_check_v2 import fence
from quads.make_tdigest import get_quantiles_from_tdigest  # returns (quantiles, quantile_list)

# Coordinate name preferences (in order)
LEV_NAMES = ["lev", "level", "pressure"]
LAT_NAMES = ["lat", "latitude", "y"]

# -----------------------------
# helper functions
# -----------------------------
def load_strata(strata_yaml_file: str | Path) -> dict[str, dict]:
    """Load the 'STRATA' section from the provided YAML file."""
    with open(strata_yaml_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]


def load_tdigest_from_db(db_path: Path, model: str, year: int, month: int, id_string: str) -> dict | None:
    """
    Fetch (compression, centroids, quantiles, quantile_list) for a given set of unique identifiers from SQLite.
    Returns None if not found.
    """
    table_name = model.lower()
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            f"SELECT compression, centroids, quantiles, quantile_list FROM {table_name} "
            "WHERE model=? AND year=? AND month=? AND id_string=?",
            (model, year, month, id_string),
        ).fetchone()
    #print(row)

    if row is None:
        print(
            f"No reference quantiles in DB for "
            f"(model={model}, year={year}, month={month}, id_string='{id_string}') "
            f"at {db_path}"
        )
        return None

    compression, centroids, quantiles, qlist =  row[0], pickle.loads(row[1]), pickle.loads(row[2]), pickle.loads(row[3]) 
    return {
            "year": year,
            "month": month,
            "compression": compression,
            "centroids": centroids,
            "quantiles": quantiles,
            "quantile_list": qlist,
            }


def combine_tdigest_refs(refs: list[dict]) -> TDigest | None:
    """
    combine multiple DB-loaded tdigest references.
    """
    if not refs:
        return None

    compression = refs[0]["compression"]

    td_merged = None

    for ref in refs:
        centroids = ref["centroids"]
        #compression = ref["compression"]
        td = TDigest.of_centroids(np.asarray(centroids), compression=compression)

        td_merged = td if td_merged is None else TDigest.combine(td_merged, td)
    
    return td_merged

def check_physical_constraints(constraints_dict: dict[str, dict[str, bool]],
                               variable: str,) -> tuple[bool, bool, bool]:
    """ read the yaml file to figure out the physical constraints """
    values = constraints_dict.get(variable, {})
    return (values.get("is_positive", False), 
            values.get("is_fractional", False),
            values.get("is_priority", False)
            )

@delayed
def analyse(da_sel,
            key,
            model,
            historical_reference_dates,
            database_path,
            is_positive,
            is_fractional,
            is_high_priority,
            ) -> tuple[list, pd.DataFrame] | None:
    """
    Compute results for the given data slice and collect violated data points.
    Reference is built by combining TDigests from 3 relevant months of previous 
    3 years.
    is_positive, is_fractional, and is_high_priority are read from physical constraint yaml file 
    and are used to modify the t-digest fence later
    """

    refs = []
    
    ref_dates = historical_reference_dates # it is yyyy-mm-dd format, but dd is not needed below
    for ref_date in ref_dates:
        for delta in (-1, 0, 1):
            d = ref_date + relativedelta(months=delta)
            y, m = d.year, d.month
        
            ref = load_tdigest_from_db(db_path=database_path, model=model, year=y, month=m, id_string=key)
            if ref is not None:
                refs.append(ref)
            else:
                print(f"Warning: no tdigest found for year: {y}, month: {m} and id_string: {key}")
    
    if not refs:
        print("No reference tdigest data  available")
        return None
    if len(refs) < 9:

        print(f"Warning: does not have enough number of historical references. Only {len(refs)} found.")

    td_ref = combine_tdigest_refs(refs)

    if td_ref is None:
        return None

    ref_quantiles = get_quantiles_from_tdigest(td_ref)

    quantiles, qlist = ref_quantiles
    fence_low, fence_high = fence(ref_quantiles)

    if is_positive:
        if fence_low < 0:
            fence_low = 0

    if is_fractional:
        if fence_low < 0:
            fence_low = 0
        if fence_high > 1:
            fence_high = 1
 
    mask_low = da_sel < fence_low
    mask_high = da_sel > fence_high
    mask_bad = mask_low | mask_high 

    n_low, n_high = dask.compute(mask_low.sum(), mask_high.sum())
    n_low = int(n_low)
    n_high = int(n_high)
    n_tot = n_low + n_high


    summary_row = (key, n_low, n_high, n_tot, fence_low, fence_high,  quantiles, qlist, is_positive, is_fractional, is_high_priority)

    violations_df = None

    if n_tot > 0:
        da_bad = da_sel.where(mask_bad, drop=True)

        violations_df = (da_bad.to_dataframe(name="value")
                        .reset_index()
                        .dropna(subset=["value"])
                        )

        violations_df["id_string"] = key
        violations_df["fence_low"] = fence_low
        violations_df["fence_high"] = fence_high
        violations_df["is_positive"] = is_positive
        violations_df["is_fractional"] = is_fractional
        violations_df["is_high_priority"] = is_high_priority

    return summary_row, violations_df
# -----------------------------
# main driver
# -----------------------------
def compute_and_save_results(
    model: str,
    date: datetime,
    data_yaml_file: str,
    strata_file: str,
    db_path: str | Path,
    physical_constraints_yaml_path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each (collection,var,level,stratum) slice, computes:
      - fetch reference quantiles from SQLite (for this model/year/month/id)
      - return: a datafame including  id_string, no_of_violations, quantiles, quantile_list for slices with violations
                a dataframe that includes all outliers (a subset of entire day worth of data)
    model: model name
    date: the day you want to analyze
    data_yaml_file: path of the file having the netcd file address and collection lists for a given model
    strata_file: file for latitude stratum list
    db_path: SQLite databse path for historical tdigest results (monthly results)
    """
    # Resolve files and exclusions from YAML
    _, collection_dict, excluded = list_files_and_excluded_vars(
        model=model, date=date, data_yaml_file=data_yaml_file
    )
    
    year_list = [1, 2, 3]
    historical_reference_dates = [date - relativedelta(years=year, months=0) for year in year_list] 

    # Load strata definitions
    strata = load_strata(strata_file)

    # load the physical constraint data
    with open(physical_constraints_yaml_path, "r") as f:
        data = yaml.safe_load(f)

    constraints_dict = data.get("PHYSICAL_CONSTRAINTS", {})

    df = pd.DataFrame(columns=["id_string", "no_of_violations_left", "no_of_violations_right", "no_of_total_violations", "fence_low","fence_high", "quantile_values", "q_list", "is_positive", "is_fractional", "is_high_priority"])

    all_violations_tables = []

    for coll_name, files in collection_dict.items():
        #if coll_name != "inst3_2d_asm_Nx":
        #   continue

        if model in ["MERRA2"]:
            day = date.day
            tag = f"{day:02d}.nc4" # note the assumption here
            files = [fi for fi in files if fi.endswith(tag)] # filtering daily files
            print(day, files)
            #print(files)
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

        # 3.2) Identify coordinate names (lat, optional level).
        lat_name = next((c for c in LAT_NAMES if c in ds.coords), None) # first matching candidate
        lev_dim = next((d for d in LEV_NAMES if d in ds.coords), None)

        delayed_jobs = []

        lat_arr = ds[lat_name].values
        is_strictly_ascending  = np.all(np.diff(lat_arr) > 0) 
        is_strictly_descending = np.all(np.diff(lat_arr) < 0) 


        # 3.3) For each variable...
        for var in ds.data_vars:
            # check physical bounds
            is_positive, is_fractional, is_high_priority = check_physical_constraints(constraints_dict, var)

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
                    delayed_jobs.append(analyse(da_sel, id_key, model, historical_reference_dates,
                                                Path(db_path),is_positive, is_fractional, is_high_priority))
                

        print(len(delayed_jobs))
        finished = dask.compute(*delayed_jobs, scheduler="threads", num_workers=32)
        finished = [x for x in finished if x is not None]
        if finished:
            summary_rows = []
            violation_tables = []

            for summary_row, violations_df in finished:
                if summary_row[3] > 0:
                    summary_rows.append(summary_row)

                if violations_df is not None and not violations_df.empty:
                    violation_tables.append(violations_df)

            new_df = pd.DataFrame(summary_rows, columns=df.columns)
            if df.empty:
                df = new_df
            else:
                df = pd.concat([df, new_df], ignore_index=True)
            all_violations_tables.extend(violation_tables)

    if all_violations_tables:
        violations_all = pd.concat(all_violations_tables, ignore_index=True)
    else:
        violations_all = pd.DataFrame()

    return df, violations_all


# -----------------------------
# __main__
# -----------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    model = args.model
    date_str = args.date

    date = datetime.strptime(date_str, "%Y-%m-%d")

    model_lower = model.lower()

    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads_dev/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads_dev/conf/strata.yaml"
    physical_constraints_yaml_path = "/home/sadhika8/JupyterLinks/nobackup/quads_dev/conf/GEOS_PHYSICAL_CONSTRAINTS.yaml"
    db_path = Path(f"/home/sadhika8/JupyterLinks/nobackup/quads_database/{model_lower}_monthly_aggregated_centroids_and_quantiles.db")
    results_base_path = Path(f"/home/sadhika8/JupyterLinks/nobackup/quads_results")
    results_base_path.mkdir(parents=True, exist_ok=True)

    print(f"Running QUADS user job for MODEL={model}, DATE={date_str}")

    df, violations_df = compute_and_save_results(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
        strata_file=strata_file,
        db_path=db_path,
        physical_constraints_yaml_path = physical_constraints_yaml_path,
    )
    df = df.sort_values(by=["is_high_priority", "no_of_total_violations"], ascending=[False, False])

     # One daily file under out_dir/model/YYYY/MM/YYYY-MM-DD.pkl, one monthly for GEOSIT
    base = results_base_path
    year_dir = base / model / f"{date.year:04d}"
    month_dir = year_dir / f"{date.month:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)
    day_dir = month_dir / f"{date.day:02d}"
    day_dir.mkdir(parents=True, exist_ok=True)

    #ref_str = historical_reference_date.strftime("%Y-%m")
    df_var_name = f"quads_results_{model_lower}_{date.strftime('%Y_%m_%d')}"
    out_file = f"{df_var_name}_summary.pkl"
    out_path = day_dir / out_file
    df.to_pickle(out_path)

    print(f"Created DataFrame '{df_var_name}' with {len(df)} rows.")
    print(f"✔ Saved DataFrame to {out_path}")

    violations_out_path = day_dir / f"{df_var_name}_violations_raw_data.parquet"

    if not violations_df.empty:
        violations_df = violations_df.sort_values("id_string")
        violations_df.to_parquet(
                violations_out_path,
                engine="pyarrow",
                index=False,
                compression="zstd",
                row_group_size=100_000,
                )

        print(f"Saved violations to {violations_out_path}")
    else:
        print("No violations found; no parquet written")

