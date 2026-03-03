import sqlite3
import pandas as pd
import requests
import time

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"
INDICATOR_ID = 1293

DB_PATH = "data/db/entsoe_load_es.sqlite"
ESIOS_TABLE = "esios_peninsula_hourly"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN,
}

def init_db(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {ESIOS_TABLE} (
            timestamp TEXT PRIMARY KEY,
            load_mw REAL NOT NULL,
            source TEXT NOT NULL
        );
    """)
    con.commit()

def fetch_month(start_iso, end_iso):
    url = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"
    params = {
        "start_date": start_iso,
        "end_date": end_iso,
        "time_trunc": "hour",
        "time_agg": "avg",
    }

    for attempt in range(1, 4):
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code in (500, 502, 503, 504):
            print(f"  ⚠️ {r.status_code} reintento {attempt}/3")
            time.sleep(2 * attempt)
            continue
        r.raise_for_status()
        data = r.json()
        return data.get("indicator", {}).get("values", [])

    return []

def values_to_df(values):
    df = pd.DataFrame(values)
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "load_mw", "source"])

    # buscar columna datetime_utc
    ts_col = None
    for c in ["datetime_utc", "datetime", "date", "timestamp"]:
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        raise RuntimeError(f"No encuentro columna fecha. Columnas: {df.columns.tolist()}")

    df["timestamp"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["load_mw"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = "ESIOS_1293"

    df = df.dropna(subset=["timestamp", "load_mw"]).copy()
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    return df[["timestamp", "load_mw", "source"]]

def upsert(con, df):
    rows = list(df.itertuples(index=False, name=None))
    con.executemany(
        f"INSERT OR REPLACE INTO {ESIOS_TABLE} (timestamp, load_mw, source) VALUES (?, ?, ?)",
        rows
    )
    con.commit()
    return len(rows)

def main():
    ranges = [
        ("2025-10-01T00:00:00Z", "2025-11-01T00:00:00Z"),
        ("2025-11-01T00:00:00Z", "2025-12-01T00:00:00Z"),
        ("2025-12-01T00:00:00Z", "2026-01-01T00:00:00Z"),
    ]

    with sqlite3.connect(DB_PATH) as con:
        init_db(con)
        total = 0

        for start_iso, end_iso in ranges:
            print("\nPidiendo:", start_iso, "->", end_iso)
            values = fetch_month(start_iso, end_iso)
            df = values_to_df(values)
            n = upsert(con, df)
            total += n
            print("  ✅ filas guardadas:", n)

    print("\n✅ Terminado. Total filas:", total)
    print("Tabla:", ESIOS_TABLE)

if __name__ == "__main__":
    main()
