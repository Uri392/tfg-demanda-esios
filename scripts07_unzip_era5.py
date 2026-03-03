from pathlib import Path
import zipfile

zip_path = Path("data/raw_era5/era5land_t2m_2006-01.nc")  # ahora mismo es ZIP
out_dir = zip_path.parent / "unzipped_era5"
out_dir.mkdir(parents=True, exist_ok=True)

with zipfile.ZipFile(zip_path, "r") as z:
    names = z.namelist()
    print("ZIP contains:")
    for n in names:
        print(" -", n)

    z.extractall(out_dir)

print("\nExtracted to:", out_dir)

# Busca el primer .nc real dentro
nc_files = list(out_dir.rglob("*.nc"))
if not nc_files:
    raise SystemExit("No se encontró ningún .nc dentro del ZIP.")

real_nc = nc_files[0]
print("Real NetCDF:", real_nc)

# Copia/renombra a una ruta final limpia
final_nc = zip_path.parent / "era5land_t2m_2006-01_real.nc"
final_nc.write_bytes(real_nc.read_bytes())
print("Saved as:", final_nc)
