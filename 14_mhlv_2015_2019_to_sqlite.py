import os
import sqlite3
import pandas as pd

FILE = os.path.join("data", "raw", "entsoe", "MHLV_data-2015-2019.xlsx")
DB_PATH = os.path.join("data", "db", "entsoe_load_es.sqlite")
TABLE = "load_hourly_es"

def init_db(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            timestamp TEXT PRIMARY KEY,
            load_mw REAL NOT NULL,
            coverage_ratio REAL
        );
    """)
    con.commit()

def read_sheet(sheet_name):
    df = pd.read_excel(FILE, sheet_name=sheet_name)
    # Filtrar España
    df = df[df["CountryCode"] == "ES"].copy()

    # Nos quedamos con lo importante
    df["timestamp"] = pd.to_datetime(df["DateUTC"], errors="coerce")
    df["load_mw"] = pd.to_numeric(df["Value"], errors="coerce")
    df["coverage_ratio"] = pd.to_numeric(df["Cov_ratio"], errors="coerce")

    df = df.dropna(subset=["timestamp", "load_mw"]).copy()
    df = df[["timestamp", "load_mw", "coverage_ratio"]].sort_values("timestamp")
    return df

def main():
    print("Leyendo:", FILE)

    parts = []
    for sh in ["2015-2017", "2018-2019"]:
        df = read_sheet(sh)
        print(f"  Hoja {sh} -> filas ES:", len(df))
        if not df.empty:
            print("   rango:", df["timestamp"].min(), "->", df["timestamp"].max())
            parts.append(df)

    if not parts:
        raise SystemExit("No he encontrado filas de ES en el fichero MHLV.")

    full = pd.concat(parts, ignore_index=True)
    full = full.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with sqlite3.connect(DB_PATH) as con:
        init_db(con)

        rows = list(zip(
            full["timestamp"].astype(str),
            full["load_mw"].astype(float),
            full["coverage_ratio"].astype(float),
        ))

        con.executemany(
            f"INSERT OR REPLACE INTO {TABLE} (timestamp, load_mw, coverage_ratio) VALUES (?, ?, ?)",
            rows
        )
        con.commit()

    print("✅ Importado MHLV a:", DB_PATH)
    print("Filas insertadas/actualizadas:", len(full))
    print("Rango final:", full["timestamp"].min(), "->", full["timestamp"].max())

if __name__ == "__main__":
    main()
