import requests
import pandas as pd

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"
INDICATOR_ID = 1293

url = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN,
}

# 👇 Cambia estas fechas para probar rangos
params = {
	"start_date": "2024-01-01T00:00:00Z",
	"end_date":   "2024-02-01T00:00:00Z",
    "time_trunc": "hour",
    "time_agg":   "avg",
}

r = requests.get(url, headers=headers, params=params, timeout=60)
print("STATUS:", r.status_code)
r.raise_for_status()

data = r.json()
values = data["indicator"]["values"]

print("NUMERO DE PUNTOS (values):", len(values))

# Si no hay datos, no seguimos
if len(values) == 0:
    print("⚠️ No hay datos en ese rango. Prueba con un año más reciente.")
    # información útil
    print("Nombre indicador:", data["indicator"].get("name"))
    print("Última actualización:", data["indicator"].get("values_updated_at"))
    print("Geos:", data["indicator"].get("geos"))
    raise SystemExit

df = pd.DataFrame(values)

print("COLUMNAS:", list(df.columns))
print("EJEMPLO 1ª FILA:", df.iloc[0].to_dict())

# Columna de tiempo
if "datetime" in df.columns:
    time_col = "datetime"
elif "date" in df.columns:
    time_col = "date"
else:
    raise KeyError(f"No encuentro columna de tiempo. Columnas: {list(df.columns)}")

# Columna de valor
if "value" in df.columns:
    value_col = "value"
else:
    raise KeyError(f"No encuentro columna 'value'. Columnas: {list(df.columns)}")

df["timestamp_utc"] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
df["demand_mw"] = pd.to_numeric(df[value_col], errors="coerce")

df = df.dropna(subset=["timestamp_utc", "demand_mw"]).copy()
df = df[["timestamp_utc", "demand_mw"]].sort_values("timestamp_utc")

out = "demanda_prueba.csv"
df.to_csv(out, index=False)

print("✅ Guardado:", out)
print("Filas:", len(df))
print(df.head())
print(df.tail())
