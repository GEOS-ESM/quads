# create_db.py
from pathlib import Path
import sqlite3

DB_PATH = Path("/home/sadhika8/JupyterLinks/nobackup/quads_database/geosit_monthly_aggregated_centroids_and_quantiles.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

with sqlite3.connect(str(DB_PATH)) as conn:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS geosit (
        model          TEXT    NOT NULL,
        year           INTEGER NOT NULL,
        month          INTEGER NOT NULL,
        id_string      TEXT    NOT NULL,
        compression    INTEGER NOT NULL,
        centroids      BLOB    NOT NULL,   -- pickled list[(mean, count)]
        quantiles      BLOB    NOT NULL,   -- pickled list[float]
        quantile_list  BLOB    NOT NULL,   -- pickled list[float]
        PRIMARY KEY (model, year, month, id_string)
    )
    """)
print(f"Created (or verified) SQLite DB at: {DB_PATH}")

