import xarray as xr

file_path = (
    "/discover/nobackup/projects/gmao/gmao_ops/pub/fp/das/Y2025/M06/D01/GEOS.fp.asm.inst3_3d_aer_Nv.20250601_0600.V01.nc4"
)

with xr.open_dataset(file_path) as dataset:
    for variable in dataset.variables:
        print(variable)

with xr.open_dataset(file_path) as ds:
    swland = ds["BCPHILIC"]

    print(swland)
    print("Shape:", swland.shape)
    print("Dimensions:", swland.dims)
    print("Min:", swland.min().item())
    print("Max:", swland.max().item())
    print("Mean:", swland.mean().item())
    print("Missing:", swland.isnull().sum().item())
