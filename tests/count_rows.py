import sqlite3
from datetime import datetime

DB_PATH = (
    "/home/sadhika8/JupyterLinks/nobackup/quads_database/"
    "merra2_monthly_aggregated_centroids_and_quantiles.db"
)

MODEL = "MERRA2"
START = "2022/7"
END = "2026/06"


def months_between(start, end):
    current = datetime.strptime(start, "%Y/%m")
    final = datetime.strptime(end, "%Y/%m")

    if current > final:
        raise ValueError("START must not be later than END")

    while current <= final:
        yield current.year, current.month

        if current.month == 12:
            current = current.replace(
                year=current.year + 1,
                month=1,
            )
        else:
            current = current.replace(month=current.month + 1)


with sqlite3.connect(DB_PATH) as conn:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()

    matching_tables = []

    for (table,) in tables:
        columns = {
            row[1]
            for row in conn.execute(f'PRAGMA table_info("{table}")')
        }

        if {"model", "year", "month"} <= columns:
            matching_tables.append(table)

    if not matching_tables:
        raise RuntimeError(
            "No table containing model, year, and month columns was found"
        )

    for table in matching_tables:
        print(f"\nTable: {table}")
        print("Month    Rows")
        print("-------  ----")

        for year, month in months_between(START, END):
            count = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM "{table}"
                WHERE model = ?
                  AND year = ?
                  AND month = ?
                """,
                (MODEL, year, month),
            ).fetchone()[0]

            print(f"{year}/{month:02d}  {count}")
