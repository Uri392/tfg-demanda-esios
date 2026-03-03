import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import xarray as xr

DB_PATH = Path("data/db/master_energy.sqlite")
NC_PATH = Path("data/raw_era5/era5land_t2m_2006-01_real.nc")


def build_hourly_temp_df(nc_path: Path) -> pd.DataFrame:
    ds = xr.open_dataset(nc_path, engine="netcdf4")

    if "t2m" not in ds.data_vars:
        raise ValueError(f"No encuentro 't2m'. Variables: {list(ds.data_vars)}")

    da = ds["t2m"]  # dims: (valid_time, latitude, longitude) en tu caso

    # pesos por latitud
    lat = da["latitude"]
    weights = np.cos(np.deg2rad(lat))

    # media espacial ponderada
    t_mean = da.weighted(weights).mean(dim=["latitude", "longitude"])

    # Kelvin -> Celsius
    s = (t_mean - 273.15).to_series()
    s.name = "temp_c"
    df = s.reset_index()

    # tu fichero usa valid_time
    if "valid_time" in df.columns:
        time_col = "valid_time"
    elif "time" in df.columns:
        time_col = "time"
    else:
        raise KeyError(f"No encuentro columna de tiempo. Columnas: {list(df.columns)}")

    df["timestamp_utc"] = pd.to_datetime(df[time_col], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[["timestamp_utc", "temp_c"]].sort_values("timestamp_utc").reset_index(drop=True)

    return df


def upsert_to_sqlite(df: pd.DataFrame) -> int:
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    source = "ERA5-Land t2m (area mean)"

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


def main():
    print("CWD:", Path.cwd())
    print("DB:", DB_PATH.resolve())
    print("NC:", NC_PATH.resolve())
    print("DB exists:", DB_PATH.exists())
    print("NC exists:", NC_PATH.exists())

    df = build_hourly_temp_df(NC_PATH)

    print("\nDF checks:")
    print("rows_df =", len(df))
    print("min_ts  =", df["timestamp_utc"].min())
    print("max_ts  =", df["timestamp_utc"].max())
    print("temp_min_c =", float(df["temp_c"].min()))
    print("temp_max_c =", float(df["temp_c"].max()))
    print("\nHead:")
    print(df.head(3).to_string(index=False))
    print("\nTail:")
    print(df.tail(3).to_string(index=False))

    n = upsert_to_sqlite(df)
    print(f"\nInserted/updated rows = {n}")

    # comprobar en DB
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM temperature_peninsula_hourly;")
        total = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM temperature_peninsula_hourly
            WHERE timestamp_utc >= '2006-01-01 00:00:00'
              AND timestamp_utc <  '2006-02-01 00:00:00'
        """)
        jan = cur.fetchone()[0]

    print("DB total rows =", total)
    print("DB rows Jan 2006 =", jan)


if __name__ == "__main__":
    main()
