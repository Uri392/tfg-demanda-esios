import calendar
import zipfile
from pathlib import Path
from datetime import datetime, timezone

import cdsapi
import numpy as np
import pandas as pd
import sqlite3
import xarray as xr

DB_PATH = Path("data/db/master_energy.sqlite")
RAW_DIR = Path("data/raw_era5")
RAW_DIR.mkdir(parents=True, exist_ok=True)

AREA = [44.5, -10.5, 35.5, 5.5]  # N, W, S, E


def month_range(year: int, month: int):
    start = f"{year}-{month:02d}-01 00:00:00"
    if month == 12:
        end = f"{year+1}-01-01 00:00:00"
    else:
        end = f"{year}-{month+1:02d}-01 00:00:00"
    expected = calendar.monthrange(year, month)[1] * 24
    return start, end, expected


def db_rows_in_month(year: int, month: int) -> int:
    start, end, _ = month_range(year, month)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM temperature_peninsula_hourly
            WHERE timestamp_utc >= ? AND timestamp_utc < ?
            """,
            (start, end),
        )
        return cur.fetchone()[0]


def download_month_zip(year: int, month: int) -> Path:
    c = cdsapi.Client()

    last_day = calendar.monthrange(year, month)[1]
    days = [f"{d:02d}" for d in range(1, last_day + 1)]
    hours = [f"{h:02d}:00" for h in range(24)]

    out_zip = RAW_DIR / f"era5land_t2m_{year}-{month:02d}.zip"

    c.retrieve(
        "reanalysis-era5-land",
        {
            "variable": "2m_temperature",
            "year": str(year),
            "month": f"{month:02d}",
            "day": days,
            "time": hours,
            "area": AREA,
            "format": "netcdf",
        },
        str(out_zip),
    )
    return out_zip


def unzip_to_real_nc(zip_path: Path) -> Path:
    out_nc = zip_path.with_suffix("").with_name(zip_path.stem + "_real.nc")

    with zipfile.ZipFile(zip_path, "r") as z:
        nc_names = [n for n in z.namelist() if n.lower().endswith(".nc")]
        if not nc_names:
            raise RuntimeError(f"No hay .nc dentro de {zip_path}. Contiene: {z.namelist()}")

        name = nc_names[0]
        tmp_dir = zip_path.parent / "unzipped_era5"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        z.extract(name, tmp_dir)

        extracted = tmp_dir / name
        out_nc.write_bytes(extracted.read_bytes())

    return out_nc


def build_hourly_temp_df(nc_path: Path) -> pd.DataFrame:
    ds = xr.open_dataset(nc_path, engine="netcdf4")

    da = ds["t2m"]
    lat = da["latitude"]
    weights = np.cos(np.deg2rad(lat))

    t_mean = da.weighted(weights).mean(dim=["latitude", "longitude"])
    s = (t_mean - 273.15).to_series()
    s.name = "temp_c"
    df = s.reset_index()

    for candidate in ["valid_time", "time"]:
        if candidate in df.columns:
            time_col = candidate
            break
    else:
        raise KeyError(f"No encuentro time/valid_time. Columnas: {list(df.columns)}")

    df["timestamp_utc"] = pd.to_datetime(df[time_col], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[["timestamp_utc", "temp_c"]].sort_values("timestamp_utc").reset_index(drop=True)
    return df


def upsert_temperature(df: pd.DataFrame, source: str) -> int:
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = [(ts, float(temp), source, ingested_at) for ts, temp in zip(df["timestamp_utc"], df["temp_c"])]

    sql = """
    INSERT INTO temperature_peninsula_hourly(timestamp_utc, temp_c, source, ingested_at_utc)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(timestamp_utc) DO UPDATE SET
        temp_c=excluded.temp_c,
        source=excluded.source,
        ingested_at_utc=excluded.ingested_at_utc
    """

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("BEGIN")
        conn.executemany(sql, rows)
        conn.commit()

    return len(rows)


def process_month(year: int, month: int, cleanup: bool = True):
    start, end, expected = month_range(year, month)

    already = db_rows_in_month(year, month)
    if already == expected:
        print(f"SKIP {year}-{month:02d}: ya está completo ({already}/{expected})")
        return

    print(f"\n=== {year}-{month:02d} ===")
    print(f"DB tiene {already}/{expected} -> procesando...")

    zip_path = download_month_zip(year, month)
    print("Downloaded:", zip_path.name)

    real_nc = unzip_to_real_nc(zip_path)
    print("Real NC:", real_nc.name)

    df = build_hourly_temp_df(real_nc)
    print("rows_df =", len(df), "min=", df["timestamp_utc"].min(), "max=", df["timestamp_utc"].max())

    n = upsert_temperature(df, source="ERA5-Land t2m (area mean)")
    print("inserted/updated =", n)

    got = db_rows_in_month(year, month)
    print("DB rows in month =", got, "(expected", expected, ")")
    if got != expected:
        raise RuntimeError(f"Faltan horas en {year}-{month:02d}: got={got}, expected={expected}")

    if cleanup:
        # borra zip y nc real para ahorrar espacio
        try:
            zip_path.unlink(missing_ok=True)
            real_nc.unlink(missing_ok=True)
        except Exception as e:
            print("WARN cleanup:", e)


def run_years(year_from: int, year_to: int, cleanup: bool = True):
    for y in range(year_from, year_to + 1):
        for m in range(1, 13):
            process_month(y, m, cleanup=cleanup)
        print(f"✅ Año {y} completado.")


if __name__ == "__main__":
    # Cambia el rango aquí:
    run_years(2007, 2025, cleanup=True)
