from __future__ import annotations

from pathlib import Path
import sqlite3
import pickle
import os
import argparse

def load_monthly_pickle(base_out_dir: str, model: str, year: int, month: int) -> list:
    base = Path(base_out_dir)
    month_str = f"{year:04d}-{month:02d}"
    monthly_file = (
        base
        / model
        / f"{year:04d}"
        / f"{month:02d}"
        / f"monthly_merged_digest_{model}_{month_str}.pkl"
    )

    if not monthly_file.exists():
        raise FileNotFoundError(f"Monthly pickle not found: {monthly_file}")

    with open(monthly_file, "rb") as fh:
        data = pickle.load(fh)

    data = [p for p in data if p is not None]

    return data


def insert_month_into_db(
    db_path: Path,
    model: str,
    year: int,
    month: int,
    monthly_data: list[dict],
    compression: int,
):
    if not db_path.exists():
        print(f"Database does not exist: {db_path}")
        return

    with sqlite3.connect(str(db_path), timeout=300.0, isolation_level="IMMEDIATE") as conn:
        cur = conn.cursor()
        #cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=FULL;")

        #cur.execute("BEGIN IMMEDIATE;")
        try:
            rows = []
            for payload in monthly_data:
                id_string = payload["id_key"]
                centroids = payload["centroids"]
                quantiles, qlist = payload["quantiles"]
                #qlist = payload["quantile_list"]

                rows.append(
                    (
                        model,
                        year,
                        month,
                        id_string,
                        compression,
                        pickle.dumps(
                            centroids, protocol=4
                        ),
                        pickle.dumps(
                            quantiles, protocol=4
                        ),
                        pickle.dumps(
                            qlist, protocol=4
                        ),
                    )
                )
            
            table_name = model.lower()

            sql = f"""
                INSERT INTO {table_name} (
                    model, year, month, id_string,
                    compression, centroids, quantiles, quantile_list
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(model, year, month, id_string)
                DO UPDATE SET
                    compression   = excluded.compression,
                    centroids     = excluded.centroids,
                    quantiles     = excluded.quantiles,
                    quantile_list = excluded.quantile_list
                """

            cur.executemany(sql, rows)

            conn.commit()
            print(f"Inserted/updated {len(rows)} rows for {model} {year}-{month:02d}")
        except Exception:
            conn.rollback()
            raise
# all or nothing writing -- either writes the entire month worth of rows or none -- conn.rollback() achieves that

def main():
    base_out_dir = "/home/sadhika8/JupyterLinks/nobackup/quads_data"

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    args = parser.parse_args()
    
    model = args.model
    year = args.year
    month = args.month

    model_lower = model.lower()
    print(f"[INFO] model: {model}, year: {year}, month: {month}", flush = True)

    compression = 300

    db_path = Path(
        "/home/sadhika8/JupyterLinks/nobackup/quads_database/"
        f"{model_lower}_monthly_aggregated_centroids_and_quantiles.db"
    )

    monthly_data = load_monthly_pickle(
        base_out_dir=base_out_dir,
        model=model,
        year=year,
        month=month,
    )

    insert_month_into_db(
        db_path=db_path,
        model=model,
        year=year,
        month=month,
        monthly_data=monthly_data,
        compression=compression,
    )


if __name__ == "__main__":
    main()

