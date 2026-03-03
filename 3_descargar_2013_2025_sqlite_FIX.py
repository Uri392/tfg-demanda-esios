import time
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"
INDICATOR_ID = 1293
DB_PATH = "demanda.sqlite"

URL = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"

HEADERS = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN,
}

def month_ranges(start, end):
    cur = start
    while cur < end:
        nxt = cur + relativedelta(months=1)
        yield cur, min(nxt, end)
        cur = nxt

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS electric_demand_hourly (
            timestamp_utc TEXT NOT NULL,
            geo_id INTEGER NOT NULL,
            geo_name TEXT,
            demand_mw REAL NOT NULL,
            source TEXT NOT NULL,
            PRIMARY KEY (timestamp_utc, geo_id)
        );
    """)
    con.commit()
    con.close()

def month_is_complete(con, start_dt, end_dt, geo_id=8741, threshold=650):
    # Cuenta filas del mes. Si supera threshold, lo consideramos "completo".
    q = """
    SELECT COUNT(*) AS n
    FROM electric_demand_hourly
    WHERE geo_id = ?
      AND timestamp_utc >= ?
      AND timestamp_utc < ?;
    """
    n = con.execute(q, (geo_id, start_dt.isoformat(), end_dt.isoformat())).fetchone()[0]
    return n >= threshold, n

def fetch_chunk(start_dt, end_dt):
    params = {
        "start_date": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date":   end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "time_trunc": "hour",
        "time_agg":   "avg",
    }

    for attempt in range(8):
        r = requests.get(URL, headers=HEADERS, params=params, timeout=60)

        if r.status_code == 429:
            wait = 2 + attempt * 2
            print(f"429 rate limit. Espero {wait}s...")
            time.sleep(wait)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError("Demasiados 429.")

def extract_df(data):
    values = data["indicator"]["values"]
    if not values:
        return pd.DataFrame()

    df = pd.DataFrame(values)

    # Usamos datetime_utc directamente (ya viene en UTC)
    df["timestamp_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df["demand_mw"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["timestamp_utc", "demand_mw", "geo_id"]).copy()
    df["geo_id"] = df["geo_id"].astype(int)
    df["geo_name"] = df["geo_name"].astype(str)
    df["source"] = f"esios_indicator_{INDICATOR_ID}"

    df = df[["timestamp_utc", "geo_id", "geo_name", "demand_mw", "source"]]
    df = df.drop_duplicates(subset=["timestamp_utc", "geo_id"]).sort_values("timestamp_utc")
    return df

def insert_df(con, df):
    if df.empty:
        return 0
    con.executemany(
        "INSERT OR REPLACE INTO electric_demand_hourly (timestamp_utc, geo_id, geo_name, demand_mw, source) VALUES (?, ?, ?, ?, ?)",
        [
            (t.isoformat(), int(gid), gname, float(v), src)
            for t, gid, gname, v, src in zip(
                df["timestamp_utc"], df["geo_id"], df["geo_name"], df["demand_mw"], df["source"]
            )
        ]
    )
    return len(df)

def main():
    init_db()
    con = sqlite3.connect(DB_PATH)

    # Ajusta aquí el rango donde tú dices que sí hay datos
    start = datetime(2013, 1, 1, 0, 0)
    end   = datetime(2026, 1, 1, 0, 0)

    total = 0
    for s, e in month_ranges(start, end):
        complete, n_existing = month_is_complete(con, s, e, geo_id=8741, threshold=650)
        if complete:
            print(f"⏭️  {s.date()} ya completo (n={n_existing})")
            continue

        print(f"\n📥 Pidiendo {s.date()} -> {e.date()} (tenías n={n_existing})")
        data = fetch_chunk(s, e)
        df = extract_df(data)

        inserted = insert_df(con, df)
        con.commit()
        total += inserted
        print(f"   ✅ insertadas: {inserted} | total insertadas ahora: {total}")

        time.sleep(0.4)

    con.close()
    print("\n✅ Terminado (FIX). DB:", DB_PATH)

if __name__ == "__main__":
    main()
