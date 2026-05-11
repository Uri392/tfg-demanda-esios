import os
import sqlite3
import pandas as pd

# =========================
# CONFIG
# =========================
DB_PATH = r"data\db\master_energy.sqlite"

# Carpeta de salida
OUT_DIR = "eda_outputs"
os.makedirs(OUT_DIR, exist_ok=True)

# =========================
# CARGA DATOS
# =========================
q = """
SELECT d.timestamp_utc, d.demand_mw, t.temp_c
FROM demand_peninsula_hourly d
JOIN temperature_peninsula_hourly t
  ON d.timestamp_utc = t.timestamp_utc
ORDER BY d.timestamp_utc
"""

con = sqlite3.connect(DB_PATH)
df = pd.read_sql_query(q, con)
con.close()

# Parseo timestamp (UTC)
df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

# Features
df["hour"] = df["timestamp_utc"].dt.hour
df["month"] = df["timestamp_utc"].dt.month
df["year"] = df["timestamp_utc"].dt.year

# =========================
# DÍA TIPO POR MES (month, hour) -> media de demanda y temperatura
# =========================
dia_tipo_mes = (
    df.groupby(["month", "hour"])[["demand_mw", "temp_c"]]
      .mean()
      .reset_index()
      .sort_values(["month", "hour"])
)

# =========================
# CHECKS DE CALIDAD (importante)
# =========================
print("\n=== DIAGNÓSTICO RÁPIDO ===")
print("Working dir:", os.getcwd())
print("DB abs path:", os.path.abspath(DB_PATH))
print("DB size (bytes):", os.path.getsize(DB_PATH))
print("Filas cargadas:", len(df))

# Meses presentes
meses = sorted(dia_tipo_mes["month"].unique())
print("\nMeses presentes:", meses)

# Horas por mes (debería ser 24)
hours_per_month = dia_tipo_mes.groupby("month")["hour"].nunique()
print("\n=== Nº horas por mes (debería ser 24) ===")
print(hours_per_month.to_string())

bad = hours_per_month[hours_per_month != 24]
if len(bad) == 0:
    print("\n✅ OK: todos los meses tienen 24 horas.")
else:
    print("\n❌ OJO: meses con horas != 24:")
    print(bad.to_string())

# Duplicados en la clave (month, hour) (debería ser 0)
dups = dia_tipo_mes.duplicated(subset=["month", "hour"]).sum()
print("\nDuplicados (month, hour):", dups)

# =========================
# IMPRESIONES ÚTILES (sin scroll infinito)
# =========================
print("\n=== ENERO COMPLETO (24 horas) ===")
print(dia_tipo_mes[dia_tipo_mes["month"] == 1].to_string(
    index=False,
    formatters={"demand_mw": "{:,.0f}".format, "temp_c": "{:.2f}".format}
))

print("\n=== AGOSTO COMPLETO (24 horas) ===")
print(dia_tipo_mes[dia_tipo_mes["month"] == 8].to_string(
    index=False,
    formatters={"demand_mw": "{:,.0f}".format, "temp_c": "{:.2f}".format}
))

print("\n=== ÚLTIMAS 10 FILAS (para comprobar que llega a diciembre hora 23) ===")
print(dia_tipo_mes.tail(10).to_string(
    index=False,
    formatters={"demand_mw": "{:,.0f}".format, "temp_c": "{:.2f}".format}
))

# =========================
# EXPORTS (para Excel / memoria)
# =========================
# 1) Formato largo: 288 filas (month, hour)
path_long = os.path.join(OUT_DIR, "dia_tipo_mes_12x24_long.csv")
dia_tipo_mes.to_csv(path_long, index=False)

# 2) Matrices 12x24 (más fáciles de leer)
pivot_demand = dia_tipo_mes.pivot(index="month", columns="hour", values="demand_mw")
pivot_temp = dia_tipo_mes.pivot(index="month", columns="hour", values="temp_c")

path_dem = os.path.join(OUT_DIR, "dia_tipo_mes_demand_12x24.csv")
path_tmp = os.path.join(OUT_DIR, "dia_tipo_mes_temp_12x24.csv")

pivot_demand.to_csv(path_dem)
pivot_temp.to_csv(path_tmp)

print("\n✅ CSVs guardados en carpeta:", OUT_DIR)
print(" -", path_long)
print(" -", path_dem)
print(" -", path_tmp)

# =========================
# EXTRA: resumen por mes (1 valor por mes)
# (promedio de las 24 horas del día tipo de cada mes)
# =========================
resumen_mes = (
    dia_tipo_mes.groupby("month")[["demand_mw", "temp_c"]]
      .mean()
      .reset_index()
      .sort_values("month")
)

path_res = os.path.join(OUT_DIR, "resumen_mes_media_diaria.csv")
resumen_mes.to_csv(path_res, index=False)

print("\n=== RESUMEN POR MES (media de las 24 horas del día tipo del mes) ===")
print(resumen_mes.to_string(index=False, formatters={
    "demand_mw": "{:,.0f}".format,
    "temp_c": "{:.2f}".format
}))
print("\n -", path_res)