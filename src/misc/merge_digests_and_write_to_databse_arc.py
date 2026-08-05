from pathlib import Path
from zipfile import ZipFile
from collections import defaultdict
import pickle
import sqlite3
from datetime import datetime

from pytdigest import TDigest
from make_tdigest import get_quantiles_from_tdigest  # returns (quantiles, quantile_list)

def merge_tdigests_with_percentiles(root_dir: str | Path,
                                    year: int,
                                    month: int,
                                    daily_zip_name: str = "results.zip",
                                    compression: int = 300):
    """
    Merge t-digests across daily zips (ONLY for the given year/month) by filename (key).
    Assumes each zip entry is a plain pickle with a dict containing 'centroids'.

    Returns:
      dict[key_without_ext] = {
          "centroids": merged_centroids,         # list[(mean, count)]
          "quantiles": quantiles,                # list[float]
          "quantile_list": quantile_list         # list[float]
      }
    """
    root = Path(root_dir)

    # only YYYY-MM-DD folders in this month
    def _is_target_day(p: Path) -> bool:
        try:
            d = datetime.strptime(p.name, "%Y-%m-%d").date()
            return d.year == year and d.month == month
        except ValueError:
            return False

    date_dirs = sorted(p for p in root.iterdir() if p.is_dir() and _is_target_day(p))
    zips = [d / daily_zip_name for d in date_dirs if (d / daily_zip_name).exists()]

    # group members by base filename
    buckets: dict[str, list[tuple[Path, str]]] = defaultdict(list)
    for zpath in zips:
        with ZipFile(zpath, "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                fname = Path(info.filename).name  # ignore any internal folders
                buckets[fname].append((zpath, info.filename))

    results = {}
    for fname, entries in buckets.items():
        td = TDigest(compression=compression)
        for zpath, member in entries:
            with ZipFile(zpath, "r") as zf:
                payload = pickle.loads(zf.read(member))  # plain pickle with 'centroids'
            for mean, count in payload["centroids"]:
                td.update(float(mean), float(count))

        merged_centroids = td.get_centroids()
        quantiles, qlist = get_quantiles_from_tdigest(td)

        key_no_ext = fname[:-4] if fname.endswith(".pkl") else fname
        results[key_no_ext] = {
            "centroids": merged_centroids,
            "quantiles": quantiles,
            "quantile_list": qlist,
        }

    return results

def save_results_to_sqlite(results: dict,
                           db_path: str | Path,
                           model: str,
                           year: int,
                           month: int,
                           compression: int = 300) -> int:
    """
    Upsert merged results into SQLite table 'geosfp' created earlier.
    Returns number of keys written.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    upsert_sql = """
    INSERT INTO geosfp(model, year, month, id_string, compression, centroids, quantiles, quantile_list)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(model, year, month, id_string) DO UPDATE SET
        compression=excluded.compression,
        centroids=excluded.centroids,
        quantiles=excluded.quantiles,
        quantile_list=excluded.quantile_list
    """
    n = 0
    try:
        with conn:
            for key, rec in results.items():
                conn.execute(
                    upsert_sql,
                    (
                        model, year, month, key, compression,
                        pickle.dumps(rec["centroids"],    protocol=pickle.HIGHEST_PROTOCOL),
                        pickle.dumps(rec["quantiles"],    protocol=pickle.HIGHEST_PROTOCOL),
                        pickle.dumps(rec["quantile_list"],protocol=pickle.HIGHEST_PROTOCOL),
                    ),
                )
                n += 1
    finally:
        conn.close()
    return n

if __name__ == "__main__":
    # inputs
    MODEL   = "GEOSFP"
    YEAR    = 2024
    MONTH   = 6
    ROOT    = "/home/sadhika8/JupyterLinks/nobackup/quads_v1_results/geosfp"  # parent containing YYYY-MM-DD dirs
    DB_PATH = "/home/sadhika8/JupyterLinks/nobackup/quads_database/geosfp_monthly_aggregated_digests_and_quantiles.db"

    out = merge_tdigests_with_percentiles(ROOT, year=YEAR, month=MONTH, daily_zip_name="results.zip", compression=300)
    print(f"Merged {len(out)} keys for {YEAR}-{MONTH:02d}")

    written = save_results_to_sqlite(out, DB_PATH, model=MODEL, year=YEAR, month=MONTH, compression=300)
    print(f"Upserted {written} keys into SQLite {DB_PATH}")

