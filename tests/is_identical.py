import sqlite3

DB_PATH = (
    "/home/sadhika8/JupyterLinks/nobackup/quads_database/"
    "merra2_monthly_aggregated_centroids_and_quantiles.db"
)

with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as conn:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM merra2 WHERE model = ? AND year = ? AND month = ? LIMIT 1",
        ("MERRA2", 2024, 1),
    ).fetchone()

if row is None:
    print("no rows found")
else:
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            print(f"{key}: <BLOB, {len(value):,} bytes>")
        else:
            print(f"{key}: {value}")
