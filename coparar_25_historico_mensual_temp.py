import sqlite3
import pandas as pd

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 300)

DB_PATH = r"data\db\master_energy.sqlite"

con = sqlite3.connect(DB_PATH)

# 1) Media mensual por año
df = pd.read_sql("""
SELECT
    substr(timestamp_utc, 1, 4) AS year,
    substr(timestamp_utc, 6, 2) AS month,
    AVG(temp_c) AS mean_temp_c
FROM temperature_peninsula_hourly
GROUP BY year, month
ORDER BY year, month;
""", con)

con.close()

df["year"] = df["year"].astype(int)
df["month"] = df["month"].astype(int)
df["mean_temp_c"] = df["mean_temp_c"].round(2)

# 2) Tabla pivote: filas = año, columnas = mes
pivot = df.pivot(index="year", columns="month", values="mean_temp_c")

print("=== MEDIA MENSUAL POR AÑO (°C) ===")
print(pivot.round(2).to_string())

# 3) Media histórica por mes (sin 2025, para comparar 2025 contra el pasado)
hist = (
    df[df["year"] < 2025]
    .groupby("month", as_index=False)["mean_temp_c"]
    .mean()
    .rename(columns={"mean_temp_c": "historical_mean_2006_2024"})
)

hist["historical_mean_2006_2024"] = hist["historical_mean_2006_2024"].round(2)

# 4) Valores de 2025
y2025 = (
    df[df["year"] == 2025][["month", "mean_temp_c"]]
    .rename(columns={"mean_temp_c": "temp_2025"})
    .copy()
)

# 5) Comparación
comp = hist.merge(y2025, on="month", how="outer")
comp["diff_2025_minus_hist"] = (comp["temp_2025"] - comp["historical_mean_2006_2024"]).round(2)

# nombres de mes opcionales
month_names = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
}
comp["month_name"] = comp["month"].map(month_names)

comp = comp[["month", "month_name", "historical_mean_2006_2024", "temp_2025", "diff_2025_minus_hist"]]

print("\n=== COMPARACIÓN 2025 vs MEDIA HISTÓRICA 2006-2024 ===")
print(comp.to_string(index=False))
