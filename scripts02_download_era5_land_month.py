import cdsapi
from pathlib import Path

OUT_DIR = Path("data/raw_era5")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def download_month(year: int, month: int) -> Path:
    c = cdsapi.Client()

    out_file = OUT_DIR / f"era5land_t2m_{year}-{month:02d}.nc"

    c.retrieve(
        "reanalysis-era5-land",
        {
            "variable": "2m_temperature",
            "year": str(year),
            "month": f"{month:02d}",
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": [44.5, -10.5, 35.5, 5.5],  # N, W, S, E
            "format": "netcdf",
        },
        str(out_file),
    )

    return out_file

if __name__ == "__main__":
    path = download_month(2006, 1)
    print("OK descargado:", path)
