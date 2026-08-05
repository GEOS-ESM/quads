import uuid
import numpy as np

LON_NAMES = ("lon", "longitude", "x")

def _norm(v):
    v = float(v)
    return "0" if v == 0.0 else str(np.round(v, 6))

def make_id_uuid(da, model, collection, field, lat_name, lev_dim, nlev_total=0):
    # units
    units = (da.attrs.get("units") if "units" in da.attrs else getattr(da, "units", "")) or ""
    units = units.replace("/", ":")

    # ---- minimal fix: guard for missing lev on this DataArray ----
    has_lev = bool(lev_dim) and (lev_dim in da.coords)
    # vartype + lev string
    if (nlev_total == 0) or (not has_lev):
        vartype = 0
        lev_str = "-1"
    else:
        vartype = 2 if 72 <= nlev_total < 180 else (3 if nlev_total >= 180 else 1)
        lev_val = np.asarray(da.coords[lev_dim].values).ravel()[0]
        lev_str = str(np.round(float(lev_val), 6))
    # --------------------------------------------------------------

    # lat/lon bounds from the stratified DataArray
    lat_vals = np.asarray(da[lat_name].values, dtype=float)
    latmin, latmax = _norm(lat_vals.min()), _norm(lat_vals.max())
    lon_name = next((n for n in LON_NAMES if n in da.coords), "lon")
    lon_vals = np.asarray(da[lon_name].values, dtype=float)
    lonmin, lonmax = _norm(lon_vals.min()), _norm(lon_vals.max())

    parts = [str(model), str(collection), str(field), str(vartype),
             units, lev_str, latmin, latmax, lonmin, lonmax]
    idstring = "|".join(parts)
    uid = str(uuid.uuid3(uuid.NAMESPACE_DNS, idstring))
    return idstring, uid

