import os
import sqlite3
import pandas as pd

# =========================
# CONFIG
# =========================
DB_PATH = r"data\db\master_energy.sqlite"
OUT_DIR = "eda_outputs_first5_last5"
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

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
df["year"] = df["timestamp_utc"].dt.year
df["month"] = df["timestamp_utc"].dt.month
df["hour"] = df["timestamp_utc"].dt.hour

# =========================
# DEFINIR FIRST5 vs LAST5
# =========================
years_sorted = sorted(df["year"].unique())
first5_years = years_sorted[:5]
last5_years = years_sorted[-5:]

def period_label(y):
    if y in first5_years:
        return f"first5 ({first5_years[0]}-{first5_years[-1]})"
    if y in last5_years:
        return f"last5 ({last5_years[0]}-{last5_years[-1]})"
    return None

df["period_5y"] = df["year"].apply(period_label)
df_5y = df.dropna(subset=["period_5y"]).copy()

print("\n=== PERIODOS ===")
print("First5:", first5_years)
print("Last5 :", last5_years)
print("Filas first+last:", len(df_5y))

# =========================
# A) DÍA TIPO (24 filas) por periodo
# =========================
dia_tipo_24 = (
    df_5y.groupby(["period_5y", "hour"])[["demand_mw", "temp_c"]]
        .mean()
        .reset_index()
        .sort_values(["period_5y", "hour"])
)

# Guardar formato largo
path_daytype_long = os.path.join(OUT_DIR, "dia_tipo_24_first5_last5_long.csv")
dia_tipo_24.to_csv(path_daytype_long, index=False)

# Guardar matrices 24x2 (una por periodo) en un solo CSV pivotado
pivot_day_demand = dia_tipo_24.pivot(index="hour", columns="period_5y", values="demand_mw")
pivot_day_temp = dia_tipo_24.pivot(index="hour", columns="period_5y", values="temp_c")

pivot_day_demand.to_csv(os.path.join(OUT_DIR, "dia_tipo_24_demand_pivot.csv"))
pivot_day_temp.to_csv(os.path.join(OUT_DIR, "dia_tipo_24_temp_pivot.csv"))

print("\n✅ Día tipo 24h guardado:")
print(" -", path_daytype_long)

# Mostrar las 24 horas completas (para verificar)
print("\n=== DÍA TIPO 24h (DEMANDA) ===")
print(pivot_day_demand.round(0).to_string())
print("\n=== DÍA TIPO 24h (TEMP) ===")
print(pivot_day_temp.round(2).to_string())

# Resumen global por periodo (media de las 24 horas)
resumen_dia = (
    dia_tipo_24.groupby("period_5y")[["demand_mw", "temp_c"]].mean()
)
resumen_dia.to_csv(os.path.join(OUT_DIR, "resumen_dia_media_24h.csv"))

print("\n=== RESUMEN DÍA (media de las 24 horas) ===")
print(resumen_dia.round({"demand_mw": 0, "temp_c": 2}).to_string())

# =========================
# B) DÍA TIPO POR MES (12x24) por periodo
# =========================
dia_tipo_mes = (
    df_5y.groupby(["period_5y", "month", "hour"])[["demand_mw", "temp_c"]]
        .mean()
        .reset_index()
        .sort_values(["period_5y", "month", "hour"])
)

path_month_long = os.path.join(OUT_DIR, "dia_tipo_mes_12x24_first5_last5_long.csv")
dia_tipo_mes.to_csv(path_month_long, index=False)

print("\n✅ Día tipo por mes guardado:")
print(" -", path_month_long)

# Checks: 24 horas por mes y periodo
hours_check = dia_tipo_mes.groupby(["period_5y", "month"])["hour"].nunique()
bad = hours_check[hours_check != 24]
print("\n=== CHECK horas por (periodo, mes): debería ser 24 ===")
if len(bad) == 0:
    print("✅ OK: todos los (periodo, mes) tienen 24 horas.")
else:
    print("❌ OJO: algunos (periodo, mes) no tienen 24 horas:")
    print(bad.to_string())

# Exportar matrices 12x24 por periodo (demand y temp)
for period in dia_tipo_mes["period_5y"].unique():
    sub = dia_tipo_mes[dia_tipo_mes["period_5y"] == period]

    dem_12x24 = sub.pivot(index="month", columns="hour", values="demand_mw")
    tmp_12x24 = sub.pivot(index="month", columns="hour", values="temp_c")

    safe = period.replace(" ", "_").replace("(", "").replace(")", "")
    dem_12x24.to_csv(os.path.join(OUT_DIR, f"dia_tipo_mes_demand_12x24_{safe}.csv"))
    tmp_12x24.to_csv(os.path.join(OUT_DIR, f"dia_tipo_mes_temp_12x24_{safe}.csv"))

print("\n✅ Matrices 12x24 por periodo guardadas en:", OUT_DIR)

# Extra: resumen por mes (media de las 24 horas) por periodo
resumen_mes = (
    dia_tipo_mes.groupby(["period_5y", "month"])[["demand_mw", "temp_c"]]
        .mean()
        .reset_index()
        .sort_values(["period_5y", "month"])
)
resumen_mes.to_csv(os.path.join(OUT_DIR, "resumen_mes_media_24h_por_periodo.csv"), index=False)

print("\n✅ Resumen por mes (media 24h) guardado.")
