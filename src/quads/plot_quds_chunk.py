from datetime import datetime
from pathlib import Path
import yaml
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt

from get_collections_and_files import list_files_and_excluded_vars

LEV_NAMES = ["lev", "level", "pressure"]
LAT_NAMES = ["lat", "latitude", "y"]

def load_strata(strata_file: str | Path):
    with open(strata_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]

def pick_name(ds, names):
    return next((n for n in names if n in ds.coords), None)

def plot_id_key_slice(model: str, date: str, id_key: str, data_yaml_file: str, strata_file: str):
    # id_key = "collection|var|lev_val|stratum"
    coll, var, lev_str, sname = id_key.split("|", 3)
    lev_val = None if lev_str == "None" else lev_str  # keep as string; xarray can match numbers too

    dt = datetime.strptime(date, "%Y-%m-%d")
    _, collection_dict, excluded = list_files_and_excluded_vars(model=model, date=dt, data_yaml_file=data_yaml_file)
    strata = load_strata(strata_file)

    ds = xr.open_mfdataset(
        collection_dict[coll],
        combine="by_coords",
        drop_variables=excluded,
        data_vars="minimal",
        coords="minimal",
        compat="no_conflicts",
        engine="h5netcdf",
        chunks="auto",
        parallel=True,
    )

    lat = pick_name(ds, LAT_NAMES)
    lev = pick_name(ds, LEV_NAMES)

    da = ds[var]
    if lev_val is not None and lev and lev in da.coords:
        da = da.sel({lev: lev_val})

    lat_min, lat_max = strata[sname]["lat"]["min"], strata[sname]["lat"]["max"]
    lat_arr = ds[lat].values
    lat_slice = slice(lat_min, lat_max) if np.all(np.diff(lat_arr) > 0) else slice(lat_max, lat_min)
    da = da.sel({lat: lat_slice})

    if "time" in da.dims:
        da = da.isel(time=0)

    arr = da.squeeze()

    plt.figure()
    if arr.ndim == 1:
        x = arr[arr.dims[0]].values if arr.dims[0] in arr.coords else np.arange(arr.size)
        plt.plot(x, arr.values)
        plt.xlabel(arr.dims[0])
        plt.ylabel(var)
    else:
        plt.pcolormesh(arr.values, shading="auto")
        plt.colorbar(label=var)
    plt.title(f"{model} {date}\n{id_key}")
    plt.tight_layout()
    plt.show()

# Example:
plot_id_key_slice(
     model="GEOSFP",
     date="2025-05-10",
     id_key="inst1_2d_hwl_Nx|TOTSCATAU|None|Strat4",
     data_yaml_file="/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml",
     strata_file="/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml",
)
print("done")
