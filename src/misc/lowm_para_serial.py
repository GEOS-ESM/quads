# This is the main python scripts for quads

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

from zipfile import ZipFile, is_zipfile, ZIP_DEFLATED
import shutil

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

def save_one_result(key, digest, quantiles, out_dir: Path) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "id_key": key,
        "centroids": digest.get_centroids(),
        "quantiles": quantiles,
    }

    final = out_dir / f"{key}.pkl"
    tmp = out_dir / f".{key}.pkl.tmp"

    with open(tmp, "wb") as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)

    os.replace(tmp, final)  # atomic finalize

def zip_dir(src_dir: Path, zip_path: Path) -> str:
    """
    Zip all files under src_dir into zip_path with DEFLATE compression.
    Uses a temp file and os.replace for atomic finalization.
    """
    src_dir = Path(src_dir)
    tmp = zip_path.with_suffix(zip_path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(tmp, mode="w", compression=ZIP_DEFLATED, allowZip64=True) as z:
        base = src_dir
        for root, _, files in os.walk(base):
            for name in files:
                fp = Path(root) / name
                # don't include the output zip itself (if re-running)
                if fp == zip_path or fp == tmp:
                    continue
                z.write(fp, arcname=str(fp.relative_to(base)))

    os.replace(tmp, zip_path)
    return str(zip_path)



def cleanup_after_zip(date_dir: Path, zip_name: str = "results.zip") -> None:
    """
    Keep <date_dir>/<zip_name>; remove everything else under <date_dir>.
    Only runs if <zip_name> exists and passes ZipFile().testzip().
    """
    date_dir = Path(date_dir)
    zip_path = date_dir / zip_name

    # Safety checks
    if not zip_path.exists() or not is_zipfile(zip_path):
        raise RuntimeError(f"{zip_path} missing or not a valid zip")

    # Ensure the zip is readable (returns None on success)
    with ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad is not None:
            raise RuntimeError(f"Zip integrity check failed on member: {bad}")

    # Remove everything at the top-level except the zip
    for entry in date_dir.iterdir():
        if entry == zip_path:
            continue
        if entry.is_dir():
            shutil.rmtree(entry)   # removes directory and all contents
        else:
            try:
                entry.unlink()
            except FileNotFoundError:
                pass  # in case it was removed between listing and unlink

# will later save the results in a sqlite database

# ------------------------------------------------------------------
# main driver
# ------------------------------------------------------------------
def compute_and_save_results(
    model: str,
    date: datetime,
    data_yaml_file: str,
    strata_file: str,
    out_dir: str,
) -> None:
    """Computes t-digests and saves results."""
    # 1) Resolve files and collections from YAML.
    _, collection_dict, excluded = list_files_and_excluded_vars(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
    )

    # 2) Load strata definitions.
    strata = load_strata(strata_file)

    # 3) Ensure output directories exist.
    OUT_DIR = Path(out_dir)
    OUT_DIR.mkdir(exist_ok=True)

    DATE_DIR = OUT_DIR / date.strftime("%Y-%m-%d")
    DATE_DIR.mkdir(parents=True, exist_ok=True)

    # 4) For each collection, open datasets and build jobs.
    for coll_name, files in list(collection_dict.items()):
        #if coll_name != "inst3_2d_asm_Nx":
            #break
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
        stored_results = []

        # 4.3) For each variable...
        for var in ds.data_vars:
            da = ds[var]
            #print(var)
            # 4.4) For each level (or None if no level coord)...
            levels = (
                ds[lev_dim].values
                if lev_dim and (lev_dim in da.coords)
                else [None]
            )

            for lev_val in levels:
                da_lev = da.sel({lev_dim: lev_val}) if lev_val is not None else da

                # 4.5) For each stratum (lat band)...
                for sname, sdef in strata.items(): # strata name, defination
                    #print("sname", "sdef", sname, sdef)
                    lat_slice = slice(sdef["lat"]["min"], sdef["lat"]["max"])
                    #print(lat_slice)# gives slice(20, 45, None/deault step)
                    dims2stack = [d for d in da_lev.dims] 
                    #print(dims2stack) # gives [lon, lat, time]
                    # 4.6) Build a flat sample array (lazy dask array).
                    flat = (
                        da_lev.sel({lat_name: lat_slice})
                        .stack(sample=dims2stack)
                        .data.reshape(-1)  # lazy
                    )

                    #print(flat.shape) # multiple time dimensions from multiple files

                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"
                    # find idstring and uuid here
                    

                    # 4.7) Define delayed analytics pipeline for this slice.
                    @delayed
                    def analyse(arr, key=id_key):
                        digest = create_digest(arr, compression=300)
                        quantiles = get_quantiles_from_tdigest(digest)
                        #limits = check_limit(arr)
                        save_one_result(key, digest, quantiles, DATE_DIR) 
                        #stored_results.append([key, digest, quantiles, limits])
                        return key  # small confirmation

                    # 4.8) Queue the job.
                    delayed_jobs.append(analyse(flat))
                    #print(f"var: {var}, level: {lev_val}, strata: {sname}, total chunks analyzed: {len(delayed_jobs)}")

        # 5) Execute all delayed jobs (reads ➜ computes ➜ saves ➜ frees RAM).
        timeline_html = f"dask_timeline_{coll_name}.html"
        with Profiler() as prof, ResourceProfiler(dt=0.1) as rprof:
            start = datetime.now()
            finished = dask.compute(*delayed_jobs, scheduler="threads")
            #finished = dask.compute(*delayed_jobs, scheduler="processes", num_workers=40)
            end = datetime.now()
            print(
                f"✔ {len(finished)} results written for collection {coll_name} "
                f"in {(end - start).total_seconds()} secs"
            )


# ------------------------------------------------------------------
if __name__ == "__main__":
    out_dir = "/discover/nobackup/ashiklom/thamzey/HOME/quadsql/quads/data_from_newquads" #"/home/sadhika8/JupyterLinks/nobackup/quads_v1_results/geosfp"
    model = "GEOSFP"
    date = datetime(2024, 5, 20)
    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"

    results = compute_and_save_results(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
        strata_file=strata_file,
        out_dir=out_dir,
    )

    # Zip the entire date directory at the end
    date_dir = Path(out_dir) / date.strftime("%Y-%m-%d")  # <-- define it here
    zip_path = date_dir / "results.zip"
    print("Zipping date directory...")
    zipped = zip_dir(date_dir, zip_path)
    print(f"✔ Zipped to: {zipped}")
    # Now remove the tiny files
    cleanup_after_zip(date_dir, zip_name="results.zip")
    print("✔ Cleaned up loose files.")
