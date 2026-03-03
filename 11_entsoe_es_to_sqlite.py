import os
import sqlite3
import pandas as pd

INPUT_XLSX = os.path.join("data", "raw", "entsoe", "Monthly-hourly-load-values_2006-2015.xlsx")
DB_PATH = os.path.join("data", "db", "entsoe_load_es.sqlite")
TABLE = "load_hourly_es"

def main():
    # 1) Leer excel
    df = pd.read_excel(INPUT_XLSX, sheet_name=0, header=3)  # header=3 suele funcionar en este fichero
    print("Columnas:", df.columns.tolist()[:15], "...")
    print("Filas totales (todos países):", len(df))

    # 2) Filtrar España
    es = df[df["Country"] == "ES"].copy()
    print("Filas España (días):", len(es))
    print("Años España:", sorted(es["Year"].unique()))

    # 3) Detectar columnas de horas 0..23
    hour_cols = [c for c in es.columns if isinstance(c, int) and 0 <= c <= 23]
    hour_cols = sorted(hour_cols)
    if len(hour_cols) != 24:
        raise RuntimeError(f"No encuentro 24 columnas horarias 0..23. Encontradas: {hour_cols}")

    # 4) Pasar de ancho (0..23) a largo (fila por hora)
    long_df = es.melt(
        id_vars=["Country", "Year", "Month", "Day", "Coverage ratio"],
        value_vars=hour_cols,
        var_name="hour",
        value_name="load_mw",
    )

    # 5) Crear timestamp (naive, sin timezone por ahora)
    long_df["timestamp"] = pd.to_datetime(long_df[["Year", "Month", "Day"]]) + pd.to_timedelta(long_df["hour"], unit="h")

    # 6) Limpiar
    long_df["load_mw"] = pd.to_numeric(long_df["load_mw"], errors="coerce")
    long_df = long_df.dropna(subset=["timestamp", "load_mw"]).copy()

    # 7) Guardar en SQLite
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with sqlite3.connect(DB_PATH) as con:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                timestamp TEXT PRIMARY KEY,
                load_mw REAL NOT NULL,
                coverage_ratio REAL
            );
        """)
        con.commit()

        rows = list(zip(
            long_df["timestamp"].astype(str),
            long_df["load_mw"].astype(float),
            long_df["Coverage ratio"].astype(float),
        ))

        con.executemany(
            f"INSERT OR REPLACE INTO {TABLE} (timestamp, load_mw, coverage_ratio) VALUES (?, ?, ?)",
            rows
        )
        con.commit()

    print("✅ Guardado DB:", DB_PATH)
    print("Filas horarias guardadas:", len(long_df))
    print("Rango:", long_df["timestamp"].min(), "->", long_df["timestamp"].max())
    print(long_df.head(3))
    print(long_df.tail(3))

if __name__ == "__main__":
    main()
