import pickle
import sqlite3

DB_PATH = (
    "/home/sadhika8/JupyterLinks/nobackup/quads_database/"
    "merra2_monthly_aggregated_centroids_and_quantiles.db"
)
ID_STRING = "inst3_3d_asm_Nv|RH|22.0|Strat7"
MONTH = 1
YEAR_A = 2024
YEAR_B = 2025

with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as conn:
    def get(year):
        row = conn.execute(
            "SELECT quantiles FROM merra2 "
            "WHERE model = ? AND year = ? AND month = ? AND id_string = ?",
            ("MERRA2", year, MONTH, ID_STRING),
        ).fetchone()
        return row[0] if row else None

    a, b = get(YEAR_A), get(YEAR_B)

if a is None or b is None:
    print(f"missing row: {YEAR_A}={a is not None}, {YEAR_B}={b is not None}")
else:
    print(f"raw bytes identical: {a == b}")
    qa, qb = pickle.loads(a), pickle.loads(b)
    print(f"unpickled equal:     {qa == qb}")
    print(f"\n{YEAR_A}: {qa}")
    print(f"{YEAR_B}: {qb}")
