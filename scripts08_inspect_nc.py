import xarray as xr

path = "data/raw_era5/era5land_t2m_2006-01_real.nc"
ds = xr.open_dataset(path, engine="netcdf4")

print("DATA_VARS:", list(ds.data_vars))
print("COORDS:", list(ds.coords))
for c in ds.coords:
    try:
        print(c, ds.coords[c].dims, ds.coords[c].shape)
    except Exception:
        print(c)

# Si existe t2m, muestra dims
if "t2m" in ds:
    print("\nt2m dims:", ds["t2m"].dims)
