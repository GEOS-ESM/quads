from pathlib import Path
import sqlite3


def delete_rows_with_nulls(db_path: Path, table_name: str):
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.cursor()

        # Delete any row where any required column is NULL
        sql = f"""
        DELETE FROM "{table_name}"
        WHERE
            model IS NULL OR
            year IS NULL OR
            month IS NULL OR
            id_string IS NULL OR
            compression IS NULL OR
            centroids IS NULL OR
            quantiles IS NULL OR
            quantile_list IS NULL
        """

        cur.execute(sql)
        deleted = cur.rowcount
        conn.commit()

    print(f"Deleted {deleted} rows with NULL values from {table_name}")

def main():
    model = "GEOSFP"
    model_lower = model.lower()

    db_path = Path(
        "/home/sadhika8/JupyterLinks/nobackup/quads_database/"
        f"{model_lower}_monthly_aggregated_centroids_and_quantiles.db"
    )

    delete_rows_with_nulls(db_path=db_path, table_name=model_lower)


if __name__ == "__main__":
    main()

