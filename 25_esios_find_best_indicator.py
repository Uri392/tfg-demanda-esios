import sqlite3
import pandas as pd
import requests
import time

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"

DB_PATH = "data/db/entsoe_load_es.sqlite"
ENTSOE_TABLE = "load_hourly_es"

# Candidatos (según tu lista)
CANDIDATES = {
    1293: "Demanda real (te da Península)",
    2037: "Demanda real nacional",
    1740: "Demanda Real SNP (no peninsulares)",
    10004: "Demanda real suma de generación",
}

START = "2024-01-01T00:00:00Z"
END   = "2024-01-03T00:00:00Z"  # 48 horas -> suficiente para comparar

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN,
}

def fetch_esios(indicator_id: int):
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    params = {
        "start_date": START,
        "end_date": END,
        "time_trunc": "hour",
        "time_agg": "avg",
    }

    # Reintentos por si hay 502/500 puntuales
    for attempt in range(1, 4):
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code in (500, 502, 503, 504):
            print(f"  ⚠️ {indicator_id} -> {r.status_code}, reintento {attempt}/3")
            time.sleep(2 * attempt)
            continue
        r.raise_for_status()
        data = r.json()
        values = data.get("indicator", {}).get("values", [])
        return values

    raise RuntimeError(f"No pude obtener datos de {indicator_id} tras reintentos.")

def values_to_df(values):
    if not values:
        return pd.DataFrame(columns=["timestamp", "load_mw"])

    df = pd.DataFrame(values)

    # Columna de tiempo (robusto)
    ts_col = None
    for c in ["datetime_utc", "datetime", "DateUTC", "date", "timestamp"]:
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        raise RuntimeError(f"No encuentro columna de fecha. Columnas: {df.columns.tolist()}")

    df["timestamp"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df["load_mw"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["timestamp", "load_mw"]).copy()

    # Convertimos a string sin zona para poder empatar con ENTSO-E (que tienes como texto naive)
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Si viniera desordenado:
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])

    return df[["timestamp", "load_mw"]]

def load_entsoe_window():
    con = sqlite3.connect(DB_PATH)
    q = f"""
    SELECT timestamp, load_mw
    FROM {ENTSOE_TABLE}
    WHERE timestamp >= '2024-01-01 00:00:00'
      AND timestamp <  '2024-01-03 00:00:00'
    ORDER BY timestamp;
    """
    df = pd.read_sql(q, con)
    con.close()
    return df

def score(entsoe, esios):
    merged = entsoe.merge(esios, on="timestamp", how="inner", suffixes=("_entsoe", "_esios"))
    n = len(merged)
    if n == 0:
        return {"n": 0}

    # Métricas
    diff = (merged["load_mw_esios"] - merged["load_mw_entsoe"]).abs()
    mae = float(diff.mean())
    ratio = float((merged["load_mw_esios"].mean() / merged["load_mw_entsoe"].mean()))

    # correlación (si hay varianza)
    corr = float(merged["load_mw_esios"].corr(merged["load_mw_entsoe"])) if n > 2 else None

    return {"n": n, "mae": mae, "ratio_mean": ratio, "corr": corr}

def main():
    entsoe = load_entsoe_window()
    print("ENTSO-E ventanas filas:", len(entsoe))

    results = []
    for ind_id, name in CANDIDATES.items():
        print(f"\nProbando {ind_id} - {name}")
        values = fetch_esios(ind_id)
        print("  values:", len(values))
        esios = values_to_df(values)
        print("  esios filas:", len(esios))

        s = score(entsoe, esios)
        s["id"] = ind_id
        s["name"] = name
        results.append(s)

        print("  overlap n:", s.get("n"))
        if s.get("n", 0) > 0:
            print("  corr:", s.get("corr"))
            print("  mae :", round(s.get("mae", 0), 3))
            print("  ratio_mean (esios/entsoe):", round(s.get("ratio_mean", 0), 4))

    df = pd.DataFrame(results).sort_values(by=["n", "mae"], ascending=[False, True])
    print("\n=== RESUMEN ORDENADO (mejor arriba) ===")
    print(df)

if __name__ == "__main__":
    main()
