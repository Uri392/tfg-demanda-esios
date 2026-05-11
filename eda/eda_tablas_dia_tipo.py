import sqlite3
import pandas as pd

DB_PATH = r"data\db\master_energy.sqlite"

q = """
SELECT
  d.timestamp_utc,
  d.demand_mw,
  t.temp_c
FROM demand_peninsula_hourly d
JOIN temperature_peninsula_hourly t
  ON d.timestamp_utc = t.timestamp_utc
ORDER BY d.timestamp_utc
"""

con = sqlite3.connect(DB_PATH)
df = pd.read_sql_query(q, con)
con.close()

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
df["hour"] = df["timestamp_utc"].dt.hour
df["month"] = df["timestamp_utc"].dt.month
df["year"] = df["timestamp_utc"].dt.year

# Trimestre por meses completos
def quarter_from_month(m):
    if m in (1, 2, 3): return "Q1 (Jan-Mar)"
    if m in (4, 5, 6): return "Q2 (Apr-Jun)"
    if m in (7, 8, 9): return "Q3 (Jul-Sep)"
    return "Q4 (Oct-Dec)"

df["quarter"] = df["month"].apply(quarter_from_month)

# =========================
# A) DÍA TIPO ANUAL (24 filas)
# =========================
dia_tipo_24 = (
    df.groupby("hour")[["demand_mw", "temp_c"]]
    .mean()
    .reset_index()
    .sort_values("hour")
)

print("\n=== DÍA TIPO ANUAL (media por hora) ===")
print(dia_tipo_24.to_string(index=False, formatters={
    "demand_mw": "{:,.0f}".format,
    "temp_c": "{:.2f}".format
}))

# Resumen del "día tipo" en 1 número (media diaria de las 24 horas)
dia_tipo_resumen = dia_tipo_24[["demand_mw", "temp_c"]].mean()

print("\n=== RESUMEN DEL DÍA TIPO (media de las 24 horas) ===")
print(f"Demanda media del día tipo: {dia_tipo_resumen['demand_mw']:.0f} MW")
print(f"Temperatura media del día tipo: {dia_tipo_resumen['temp_c']:.2f} °C")

# =========================
# B) DÍA TIPO POR MES (12x24) -> imprimimos 24 filas por mes (tabla larga)
# =========================
dia_tipo_mes = (
    df.groupby(["month", "hour"])[["demand_mw", "temp_c"]]
    .mean()
    .reset_index()
    .sort_values(["month", "hour"])
)

print("\n=== DÍA TIPO POR MES (primeros 30 registros para ver formato) ===")
print(dia_tipo_mes.head(30).to_string(index=False, formatters={
    "demand_mw": "{:,.0f}".format,
    "temp_c": "{:.2f}".format
}))

# =========================
# C) DÍA TIPO POR TRIMESTRE (4x24)
# =========================
dia_tipo_trim = (
    df.groupby(["quarter", "hour"])[["demand_mw", "temp_c"]]
    .mean()
    .reset_index()
    .sort_values(["quarter", "hour"])
)

print("\n=== DÍA TIPO POR TRIMESTRE (primeros 30 registros para ver formato) ===")
print(dia_tipo_trim.head(30).to_string(index=False, formatters={
    "demand_mw": "{:,.0f}".format,
    "temp_c": "{:.2f}".format
}))

# =========================
# Guardar CSVs (para trabajar luego sin recalcular)
# =========================
dia_tipo_24.to_csv("eda_dia_tipo_24.csv", index=False)
dia_tipo_mes.to_csv("eda_dia_tipo_mes_12x24_long.csv", index=False)
dia_tipo_trim.to_csv("eda_dia_tipo_trimestre_4x24_long.csv", index=False)

print("\n✅ CSVs guardados: eda_dia_tipo_24.csv, eda_dia_tipo_mes_12x24_long.csv, eda_dia_tipo_trimestre_4x24_long.csv")
