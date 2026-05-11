import sqlite3
import pandas as pd

DB_PATH = r"data\db\master_energy.sqlite"

q = """
SELECT d.timestamp_utc, d.demand_mw, t.temp_c
FROM demand_peninsula_hourly d
JOIN temperature_peninsula_hourly t
  ON d.timestamp_utc = t.timestamp_utc
"""

con = sqlite3.connect(DB_PATH)
df = pd.read_sql_query(q, con)
con.close()

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
df["hour"] = df["timestamp_utc"].dt.hour

# ✅ Esto es EXACTAMENTE: "cojo todas las filas con hour=1, saco promedio", etc.
dia_tipo_24 = df.groupby("hour")[["demand_mw", "temp_c"]].mean().sort_index()

print("\n=== DÍA TIPO (promedio por hora, 24 filas) ===")
print(dia_tipo_24.round({"demand_mw": 0, "temp_c": 2}))

# Promedio global del "día tipo" (media de las 24 horas)
prom_global = dia_tipo_24.mean()
print("\n=== PROMEDIO GLOBAL (media de las 24 horas) ===")
print(f"Demanda media: {prom_global['demand_mw']:.0f} MW")
print(f"Temperatura media: {prom_global['temp_c']:.2f} °C")

dia_tipo_24.to_csv("eda_dia_tipo_24.csv")