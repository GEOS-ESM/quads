#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import pickle

from merge_digests import merge_percentiles_by_name  # returns {id_string: (quantiles, qlist)}

def write_merged_percentiles_to_db(
    input_root: str | Path,
    model: str,
    year: int,
    month: int,
    db_path: str | Path = "/home/sadhika8/JupyterLinks/nobackup/quads_database/geosfp_monthly_aggregated_digests_percentiles.db",
) -> int:
    """
    Merge percentiles by filename under input_root and upsert into SQLite 'geosfp'
    with UNIQUE (year, month, id_string). Stores only the percentile blob.
    Returns number of rows written/updated.
    """
    input_root = Path(input_root)
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) Merge (reuses your working function)
    merged = merge_percentiles_by_name(input_root)  # {id_string: (quantiles, qlist)}

    # 2) Ensure DB/table
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS geosfp (
        model      TEXT    NOT NULL,
        year       INTEGER NOT NULL,
        month      INTEGER NOT NULL,
        id_string  TEXT    NOT NULL,
        percentile BLOB    NOT NULL,
        UNIQUE (year, month, id_string)
    );
    """)
    conn.commit()

    # 3) Write rows
    rows = 0
    for id_string, (quantiles, qlist) in merged.items():
        pct_blob = pickle.dumps((quantiles, qlist), protocol=pickle.HIGHEST_PROTOCOL)
        cur.execute("""
        INSERT INTO geosfp (model, year, month, id_string, percentile)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(year, month, id_string) DO UPDATE SET
          model=excluded.model,
          percentile=excluded.percentile;
        """, (model, int(year), int(month), id_string, pct_blob))
        rows += 1

    conn.commit()
    conn.close()
    return rows

if __name__ == "__main__":
    INPUT_ROOT = "/home/sadhika8/JupyterLinks/nobackup/quads_v1_results/geosfp"
    n = write_merged_percentiles_to_db(
        input_root=INPUT_ROOT,
        model="GEOSFP",
        year=2024,
        month=6,
    )
    print(f"inserted {n} rows into database.")

