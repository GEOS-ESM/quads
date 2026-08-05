# inspect the given chunk of data with basic statistical properties and compare it with those
# for the original data

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt

file = "da_sel_tavg3_3d_mst_Nv|REEVAPLSAN|5.0|Strat5.nc4"
var = "REEVAPLSAN"

da = xr.open_dataarray(file, engine="netcdf4", decode_times=False) if var is None else xr.open_dataset(file, engine="netcdf4", decode_times=False)[var]

x = np.asarray(da).ravel()
x = x[np.isfinite(x)]  # drop NaN/Inf

print("N:", x.size)
print("mean:", float(x.mean()))
print("std:", float(x.std()))
print("min:", float(x.min()))
print("max:", float(x.max()))

plt.figure()
plt.hist(x, bins=100)
plt.title(f"Histogram: {var}")
plt.xlabel(var)
plt.ylabel("count")
plt.tight_layout()
plt.show()
