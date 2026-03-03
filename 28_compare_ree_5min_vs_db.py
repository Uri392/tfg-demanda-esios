import sqlite3
import pandas as pd

# ========= CONFIG =========
CSV_PATH = r"Custom-Report-2025-01-23-Seguimiento de la demanda de energía eléctrica (MW).csv"

DB_PATH = r"data\db\entsoe_load_es.sqlite"
DB_TABLE = "load_hourly_es_final"   # si tu tabla final se llama distinto, cámbiala aquí

TARGET_DATE = "2025-01-23"          # pon aquí el día que quieres comprobar (YYYY-MM-DD)
REE_TIMEZONE = "Europe/Madrid"      # REE suele venir en hora local España

# ========= 1) LEER CSV REE (5 min) =========
# El CSV tiene 2 líneas iniciales (título + vacío) y una coma final en cada fila,
# por eso le damos una columna extra "_" para que no se descuadre.
df = pd.read_csv(
    CSV_PATH,
    encoding="cp1252",
    skiprows=2,
    header=0,
    names=["Hora", "Real", "Prevista", "Programada", "_"]
)

# Limpiar
df = df.drop(columns=["_"])
df["Hora"] = pd.to_datetime(df["Hora"], errors="coerce")
for c in ["Real", "Prevista", "Programada"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df = df.dropna(subset=["Hora", "Real"]).copy()

# ========= 2) Pasar Hora local -> UTC =========
# IMPORTANTE: si tu DB está en UTC (ENTSO-E suele ser DateUTC), hay que comparar en UTC.
df["dt_local"] = df["Hora"].dt.tz_localize(REE_TIMEZONE, nonexistent="shift_forward", ambiguous="NaT")
df = df.dropna(subset=["dt_local"]).copy()

df["dt_utc"] = df["dt_local"].dt.tz_convert("UTC")
df["hour_utc"] = df["dt_utc"].dt.floor("h")

# ========= 3) Agregar a hora (media de 12 valores) =========
hourly = (
    df.groupby("hour_utc", as_index=False)["Real"]
      .mean()
      .rename(columns={"Real": "ree_load_mw"})
)

# Convertir a string UTC naive (igual que tu DB)
hourly["timestamp"] = hourly["hour_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
hourly = hourly[["timestamp", "ree_load_mw"]].sort_values("timestamp")

# Filtrar el día objetivo (en UTC)
# OJO: si eliges TARGET_DATE, esto interpreta ese "día" en UTC. Para julio en España,
# el día local puede desplazarse 2h. Si quieres el "día local", dímelo y lo ajustamos.
hourly = hourly[hourly["timestamp"].str.startswith(TARGET_DATE)].copy()

print("REE hourly rows:", len(hourly))
if len(hourly) == 0:
    print("⚠️ No hay filas REE para TARGET_DATE en UTC. Prueba a cambiar TARGET_DATE o dime si quieres filtrar por día local.")
    raise SystemExit

print("REE range:", hourly["timestamp"].min(), "->", hourly["timestamp"].max())
print(hourly.head(3))
print(hourly.tail(3))

# ========= 4) Leer DB en el mismo rango =========
con = sqlite3.connect(DB_PATH)

min_ts = hourly["timestamp"].min()
max_ts = hourly["timestamp"].max()

q = f"""
SELECT timestamp, load_mw
FROM {DB_TABLE}
WHERE timestamp >= ? AND timestamp <= ?
ORDER BY timestamp
"""
db = pd.read_sql(q, con, params=[min_ts, max_ts])
con.close()

print("\nDB rows:", len(db))
print("DB range:", db["timestamp"].min(), "->", db["timestamp"].max())

# ========= 5) Comparar =========
m = db.merge(hourly, on="timestamp", how="inner")
print("\nOVERLAP rows:", len(m))

if len(m) == 0:
    print("⚠️ No hay solape. Posible problema de zona horaria o nombre de tabla DB.")
    raise SystemExit

diff = (m["ree_load_mw"] - m["load_mw"]).abs()
mae = diff.mean()
ratio = m["ree_load_mw"].mean() / m["load_mw"].mean()
corr = m["ree_load_mw"].corr(m["load_mw"])

print("\n=== METRICAS ===")
print("corr:", corr)
print("MAE (MW):", mae)
print("ratio mean (REE/DB):", ratio)

print("\nPrimeras 5 comparaciones:")
print(m.head(5))
