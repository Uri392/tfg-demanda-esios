import os
import glob
import re
import sqlite3
import pandas as pd

RAW_DIR = os.path.join("data", "raw", "entsoe")
DB_PATH = os.path.join("data", "db", "entsoe_load_es.sqlite")
TABLE = "load_hourly_es"
COUNTRY_CODE = "ES"


def init_db(con: sqlite3.Connection):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            timestamp TEXT PRIMARY KEY,
            load_mw REAL NOT NULL,
            coverage_ratio REAL
        );
    """)
    con.commit()


def upsert(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = list(zip(
        df["timestamp"].astype(str),
        df["load_mw"].astype(float),
        df["coverage_ratio"].astype(float),
    ))
    con.executemany(
        f"INSERT OR REPLACE INTO {TABLE} (timestamp, load_mw, coverage_ratio) VALUES (?, ?, ?)",
        rows
    )
    con.commit()
    return len(rows)


def import_long_format_xlsx(path: str) -> pd.DataFrame:
    """
    Formato esperado (2020+ y a veces 2019):
      - CountryCode
      - DateUTC
      - Value
      - Cov_ratio
    """
    # Leemos solo las columnas necesarias (mucho más rápido)
    df = pd.read_excel(
        path,
        sheet_name=0,
        usecols=["DateUTC", "CountryCode", "Cov_ratio", "Value"],
    )

    df = df[df["CountryCode"] == COUNTRY_CODE].copy()
    if df.empty:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["DateUTC"], errors="coerce")
    df["load_mw"] = pd.to_numeric(df["Value"], errors="coerce")
    df["coverage_ratio"] = pd.to_numeric(df["Cov_ratio"], errors="coerce")

    df = df.dropna(subset=["timestamp", "load_mw"]).copy()
    df = df[["timestamp", "load_mw", "coverage_ratio"]].sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"])

    return df


def find_files_2019_2025() -> list[str]:
    """
    Busca automáticamente monthly_hourly_load_values_YYYY.xlsx dentro de RAW_DIR
    y se queda con YYYY en [2019..2025].
    """
    pattern = os.path.join(RAW_DIR, "monthly_hourly_load_values_*.xlsx")
    candidates = sorted(glob.glob(pattern))

    files = []
    for f in candidates:
        name = os.path.basename(f)
        m = re.search(r"monthly_hourly_load_values_(\d{4})\.xlsx$", name)
        if not m:
            continue
        year = int(m.group(1))
        if 2019 <= year <= 2025:
            files.append(f)

    return sorted(files)


def main():
    print("✅ Script 15 arrancó")
    print("RAW_DIR:", os.path.abspath(RAW_DIR))

    if not os.path.exists(RAW_DIR):
        raise SystemExit(f"❌ No existe la carpeta: {RAW_DIR}")

    print("Archivos en RAW_DIR:", len(os.listdir(RAW_DIR)))

    files = find_files_2019_2025()
    if not files:
        raise SystemExit("❌ No encuentro archivos monthly_hourly_load_values_YYYY.xlsx (2019..2025) en data/raw/entsoe")

    print("✅ Archivos detectados (2019..2025):")
    for f in files:
        print(" -", os.path.basename(f))

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with sqlite3.connect(DB_PATH) as con:
        init_db(con)

        total_upserts = 0
        for f in files:
            print("\nLeyendo:", os.path.basename(f))
            part = import_long_format_xlsx(f)

            if part.empty:
                print("  ⚠️ No hay filas ES (o formato no compatible).")
                continue

            n = upsert(con, part)
            total_upserts += n
            print(f"  ✅ upsert: {n} filas | rango: {part['timestamp'].min()} -> {part['timestamp'].max()}")

    print("\n✅ Terminado. Total upserts (suma):", total_upserts)
    print("DB:", os.path.abspath(DB_PATH))


if __name__ == "__main__":
    main()

