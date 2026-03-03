import os
import glob
import sqlite3
import pandas as pd

RAW_DIR = os.path.join("data", "raw", "entsoe")
DB_PATH = os.path.join("data", "db", "entsoe_load_es.sqlite")
TABLE = "load_hourly_es"
COUNTRY_CODE = "ES"

def try_read_excel(path: str) -> pd.DataFrame:
    # Probamos varios "header=" porque ENTSO-E cambia el formato entre ficheros
    for header in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        try:
            df = pd.read_excel(path, sheet_name=0, header=header)
            if df is None or df.empty:
                continue
            cols = [str(c).strip() for c in df.columns]
            if "Country" in cols and "Year" in cols and "Month" in cols and "Day" in cols:
                df.columns = cols
                return df
        except Exception:
            continue
    return pd.DataFrame()

def detect_hour_cols(df: pd.DataFrame):
    # Algunas veces vienen como int (0..23), otras como strings ("0".."23")
    hour_cols = []
    for c in df.columns:
        if isinstance(c, int) and 0 <= c <= 23:
            hour_cols.append(c)
        else:
            cs = str(c).strip()
            if cs.isdigit():
                v = int(cs)
                if 0 <= v <= 23:
                    hour_cols.append(c)
    # Devolvemos en orden 0..23 (por valor numérico)
    def hour_key(col):
        if isinstance(col, int):
            return col
        return int(str(col).strip())
    hour_cols = sorted(set(hour_cols), key=hour_key)
    return hour_cols

def get_coverage_col(df: pd.DataFrame):
    # suele ser "Coverage ratio" pero puede variar
    for c in df.columns:
        if "coverage" in str(c).strip().lower():
            return c
    return None

def to_long_hourly(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["Country"] == COUNTRY_CODE].copy()
    if df.empty:
        return df

    hour_cols = detect_hour_cols(df)
    if len(hour_cols) != 24:
        raise RuntimeError(f"No encuentro 24 columnas de horas (0..23). Encontradas: {hour_cols}")

    cov_col = get_coverage_col(df)

    id_vars = ["Country", "Year", "Month", "Day"]
    if cov_col is not None:
        id_vars.append(cov_col)

    long_df = df.melt(
        id_vars=id_vars,
        value_vars=hour_cols,
        var_name="hour",
        value_name="load_mw",
    )

    # hour a número 0..23
    long_df["hour"] = long_df["hour"].apply(lambda x: int(x) if isinstance(x, int) else int(str(x).strip()))

    # timestamp (naive, sin timezone)
    long_df["timestamp"] = pd.to_datetime(long_df[["Year", "Month", "Day"]]) + pd.to_timedelta(long_df["hour"], unit="h")

    # coverage_ratio (si existe)
    if cov_col is not None:
        long_df["coverage_ratio"] = pd.to_numeric(long_df[cov_col], errors="coerce")
    else:
        long_df["coverage_ratio"] = None

    # limpieza
    long_df["load_mw"] = pd.to_numeric(long_df["load_mw"], errors="coerce")
    long_df = long_df.dropna(subset=["timestamp", "load_mw"]).copy()

    return long_df[["timestamp", "load_mw", "coverage_ratio"]].sort_values("timestamp")

def init_db(con: sqlite3.Connection):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            timestamp TEXT PRIMARY KEY,
            load_mw REAL NOT NULL,
            coverage_ratio REAL
        );
    """)
    con.commit()

def upsert(con: sqlite3.Connection, long_df: pd.DataFrame) -> int:
    rows = list(zip(
        long_df["timestamp"].astype(str),
        long_df["load_mw"].astype(float),
        long_df["coverage_ratio"].astype(float) if long_df["coverage_ratio"].notna().any() else [None]*len(long_df),
    ))

    con.executemany(
        f"INSERT OR REPLACE INTO {TABLE} (timestamp, load_mw, coverage_ratio) VALUES (?, ?, ?)",
        rows
    )
    con.commit()
    return len(rows)

def main():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.xlsx")))
    if not files:
        raise SystemExit(f"No veo .xlsx en {RAW_DIR}")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with sqlite3.connect(DB_PATH) as con:
        init_db(con)

        total_inserted = 0
        for f in files:
            print("\nLeyendo:", os.path.basename(f))
            df = try_read_excel(f)
            if df.empty:
                print("  ⚠️ No pude leer este archivo con el formato esperado (Country/Year/Month/Day).")
                continue

            part = to_long_hourly(df)
            if part.empty:
                print("  ⚠️ No hay filas de ES en este archivo.")
                continue

            n = upsert(con, part)
            total_inserted += n
            print(f"  ✅ Upsert filas: {n} | rango: {part['timestamp'].min()} -> {part['timestamp'].max()}")

    print("\n✅ Terminado. DB:", DB_PATH)
    print("Total filas insertadas/actualizadas (suma de upserts):", total_inserted)

if __name__ == "__main__":
    main()

