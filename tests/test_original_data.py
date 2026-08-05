# inspect original data from a single file and compare it with a chunk (that comes from aggregates)

import xarray as xr
import numpy as np

#file = "/discover/nobackup/projects/gmao/gmao_ops/pub/fp/das/Y2023/M01/D01/GEOS.fp.asm.tavg3_3d_mst_Nv.20230101_0430.V01.nc4"
file =  "/discover/nobackup/projects/gmao/gmao_ops/pub/fp/das/Y2024/M02/D01/f5295_fp.inst1_2d_hwl_Nx.20240201_0000z.nc4"

#file = "/discover/nobackup/projects/gmao/gmao_ops/pub/fp/das/Y2023/M02/D01/f5294_fp.inst1_2d_hwl_Nx.20230201_0000z.nc4"
ds = xr.open_dataset(file, engine="netcdf4", decode_times=False)

print("METADATA:", dict(ds.attrs))
print("N_VARS:", len(ds.data_vars))

# print variable names and (if present) their vertical/level dimension name + size
print("\nVAR -> LEVEL DIM (if any)")
for name, da in ds.data_vars.items():
    lev_dim = next((d for d in da.dims if d in ("lev", "plev", "level", "eta", "isobaric", "isobaricInhPa")), None)
    #if lev_dim is None:
    #    print(f"{name}: None")
    #else:
    #    print(f"{name}: {lev_dim} (size={da.sizes[lev_dim]})")

# existing chunk/stat code
var = "CO2CL"
lat_name = "lat"
lev_name = "lev"
lat_min, lat_max = -90, -70
level = "NONE"

da = ds[var]

lat_chunk = slice(lat_min, lat_max)
chunk = da.sel({lat_name: lat_chunk})#.isel({lev_name: level})

x = np.asarray(chunk).ravel()
x = x[np.isfinite(x)]

print("\nCHUNK INFO")
print("file:", file)
print("var:", var)
print("lat_slice:", lat_min, "to", lat_max)
print("level index:", level)
print("chunk shape:", chunk.shape)
print("N (finite):", x.size)
print("mean:", float(x.mean()))
print("std:", float(x.std()))
print("min:", float(x.min()))
print("max:", float(x.max()))

ds.close()
