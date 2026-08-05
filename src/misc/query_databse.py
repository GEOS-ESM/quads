from pathlib import Path
import sqlite3
import pickle

model = "GEOSFP"
model_lower = model.lower()

db_path = Path(
    "/home/sadhika8/JupyterLinks/nobackup/quads_database/"
    f"{model_lower}_monthly_aggregated_centroids_and_quantiles.db"
)

id_string = "tavg1_2d_lfo_Nx|SWLAND|None|Strat1"
year, month = 2024, 4
table = model_lower

with sqlite3.connect(str(db_path)) as conn:
    row = conn.execute(
        f'''SELECT model, year, month, id_string, compression, centroids, quantiles, quantile_list
            FROM "{table}"
            WHERE model=? AND year=? AND month=? AND id_string=?''',
        (model, year, month, id_string),
    ).fetchone()

if row is None:
    raise RuntimeError(f"Row not found for {model} {year}-{month:02d} id_string={id_string}")

# Unpack + unpickle blobs
(model_out, y_out, m_out, id_out, compression,
 centroids_blob, quantiles_blob, qlist_blob) = row

centroids = pickle.loads(centroids_blob)
quantiles = pickle.loads(quantiles_blob)
qlist = pickle.loads(qlist_blob)

print("Found row:")
print("  model:", model_out)
print("  year-month:", f"{y_out:04d}-{m_out:02d}")
print("  id_string:", id_out)
print("  compression:", compression)
print("  centroids len:", centroids)
print("  quantiles len:", quantiles)
print("  qlist len:", len(qlist))

