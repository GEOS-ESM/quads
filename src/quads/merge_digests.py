#!/usr/bin/env python3
from pathlib import Path
import gzip
import pickle
from collections import defaultdict

from pytdigest import TDigest
from quads.make_tdigest import get_quantiles_from_tdigest  # returns (quantiles, quantile_list)

def _safe_load_payload(p: Path):
    try:
        if p.stat().st_size == 0:
            print(f"[skip: empty] {p}")
            return None
        with gzip.open(p, "rb") as fh:
            return pickle.load(fh)
    except Exception as e:
        print(f"[skip: unreadable] {p} :: {e}")
        return None

def merge_percentiles_by_name(root_dir: str | Path, compression: int = 300):
    """
    Walk all subdirs, group *.pkl.gz by basename, rebuild+merge from 'centroids',
    and return only percentiles.

    Returns: dict[id_string] = (quantiles, quantile_list)
    """
    root = Path(root_dir)
    groups = defaultdict(list)
    for f in root.rglob("*.pkl.gz"):
        groups[f.name].append(f)

    results = {}
    for fname, paths in groups.items():
        merged = TDigest(compression=compression)
        valid_parts = 0

        for p in paths:
            payload = _safe_load_payload(p)
            if not payload:
                continue

            # Support both formats just in case:
            # 1) {"centroids": [(mean, count), ...], ...}
            # 2) {"digest": <TDigest>, ...}  (if you ever switch to saving the object)
            if "centroids" in payload:
                cents = payload["centroids"]
                for mean, count in cents:
                    merged.update(float(mean), float(count))
                valid_parts += 1
            elif "digest" in payload and isinstance(payload["digest"], TDigest):
                d = payload["digest"]
                # No direct add API guaranteed across versions; pour centroids:
                for mean, count in d.get_centroids():
                    merged.update(float(mean), float(count))
                valid_parts += 1
            else:
                print(f"[skip: no centroids/digest] {p}")

        if valid_parts == 0:
            continue  # nothing usable for this name

        id_string = fname[:-7] if fname.endswith(".pkl.gz") else fname
        quantiles, qlist = get_quantiles_from_tdigest(merged)
        results[id_string] = (quantiles, qlist)

    return results

if __name__ == "__main__":
    INPUT_ROOT = "/home/sadhika8/JupyterLinks/nobackup/quads_v1_results/geosfp"
    out = merge_percentiles_by_name(INPUT_ROOT)
    # small summary
    for k, (qs, ql) in sorted(out.items()):
        print(k)
        if ql:
            print(f"  q={ql[0]} -> {qs[0]}")
            print(f"  q={ql[-1]} -> {qs[-1]}")

