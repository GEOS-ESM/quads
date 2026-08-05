#quads=> select tdigest from geosfp.slidingwindow where uuid = 'c1ad8f7a-9606-3c32-89f0-97502495c192';
#quads=> select tdigest from geosfp.longterm where uuid = 'c1ad8f7a-9606-3c32-89f0-97502495c192';
#select tdigest from geosfp.slidingwindow where uuid = 'c1ad8f7a-9606-3c32-89f0-97502495c192' and month = 1 and year = 2024;

# select_tdigest.py
import pickle
import gzip
import psycopg2

def select_tdigest(uuid_value, month, year, conn=None):
    """
    Return the t-digest object (or None if not found) for (uuid, month, year)
    from geosfp.slidingwindow.
    If conn is None, a temporary connection is opened using libpq defaults.
    """
    opened_here = False
    if conn is None:
        conn = psycopg2.connect("host=edb1.nccs.nasa.gov dbname=quads user=sadhika8 sslmode=require")   # uses env / .pgpass / defaults
        opened_here = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tdigest
                FROM geosfp.slidingwindow
                WHERE uuid = %s AND month = %s AND year = %s
                LIMIT 1
                """,
                (uuid_value, month, year),
            )
            row = cur.fetchone()
    finally:
        if opened_here:
            conn.close()

    if not row:
        return None

    blob = row[0]
    import binascii

    print("First 20 raw bytes:", binascii.hexlify(blob[:20]))
    
    if isinstance(blob, memoryview):  # psycopg2 returns BYTEA as memoryview
        blob = blob.tobytes()

    # Try plain pickle first; fall back to gzipped pickle.
    try:
        return pickle.loads(blob)
    except Exception:
        try:
            return pickle.loads(gzip.decompress(blob))
        except Exception:
            # Not a pickle? Return raw bytes so caller can inspect.
            return blob


if __name__ == "__main__":
    uuid_value = "c1ad8f7a-9606-3c32-89f0-97502495c192"
    month = 1
    year = 2024

    td = select_tdigest(uuid_value, month, year)
    if td is None:
        print("No tdigest found for that (uuid, month, year).")
    else:
        print(f"Fetched tdigest: {type(td)}")
        print(td.inverse_cdf(0.5))
        # Optional: try to show some detail if it's a TDigest-like object
        for attr in ("size", "get_centroids", "compression"):
            if hasattr(td, attr):
                try:
                    val = getattr(td, attr)
                    val = val() if callable(val) else val
                    print(f"  {attr}: {val if not hasattr(val, '__len__') else len(val)}")
                except Exception:
                    pass

