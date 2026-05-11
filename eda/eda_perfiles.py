import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB_PATH = r"data\db\master_energy.sqlite"

# --- 1) Cargar y unir demanda + temperatura por timestamp_utc ---
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

# Parseo del timestamp (UTC)
df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

# Features de calendario
df["year"] = df["timestamp_utc"].dt.year
df["month"] = df["timestamp_utc"].dt.month
df["hour"] = df["timestamp_utc"].dt.hour

# Trimestres (meses completos): Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec
def quarter_from_month(m):
    if m in (1, 2, 3): return "Q1 (Jan-Mar)"
    if m in (4, 5, 6): return "Q2 (Apr-Jun)"
    if m in (7, 8, 9): return "Q3 (Jul-Sep)"
    return "Q4 (Oct-Dec)"

df["quarter"] = df["month"].apply(quarter_from_month)

# --- 2) Día tipo anual (24 valores) ---
daytype_annual = df.groupby("hour")[["demand_mw", "temp_c"]].mean().reset_index()

# --- 3) Día tipo por trimestre (4 curvas) ---
daytype_quarter = df.groupby(["quarter", "hour"])[["demand_mw", "temp_c"]].mean().reset_index()

# --- 4) Mes tipo: matriz (mes x hora) para heatmap/tabla ---
month_hour = df.groupby(["month", "hour"])[["demand_mw", "temp_c"]].mean().reset_index()
pivot_demand = month_hour.pivot(index="month", columns="hour", values="demand_mw")
pivot_temp = month_hour.pivot(index="month", columns="hour", values="temp_c")

# --- 5) Primeros 5 años vs últimos 5 años ---
years_sorted = sorted(df["year"].unique())
first5 = years_sorted[:5]
last5 = years_sorted[-5:]

df["period_5y"] = "other"
df.loc[df["year"].isin(first5), "period_5y"] = f"first5 ({first5[0]}-{first5[-1]})"
df.loc[df["year"].isin(last5), "period_5y"] = f"last5 ({last5[0]}-{last5[-1]})"

daytype_5y = (
    df[df["period_5y"] != "other"]
    .groupby(["period_5y", "hour"])[["demand_mw", "temp_c"]]
    .mean()
    .reset_index()
)

# --- 6) COVID 2020 vs resto (día tipo) ---
df["covid_flag"] = df["year"].apply(lambda y: "COVID (2020)" if y == 2020 else "No-COVID (rest)")
daytype_covid = df.groupby(["covid_flag", "hour"])[["demand_mw", "temp_c"]].mean().reset_index()

# =========================
# PLOTS (matplotlib simple)
# =========================

# A) Día tipo anual
plt.figure()
plt.plot(daytype_annual["hour"], daytype_annual["demand_mw"])
plt.title("Día tipo anual - Demanda (MW) vs hora (UTC)")
plt.xlabel("Hora (0-23)")
plt.ylabel("Demanda (MW)")
plt.grid(True)
plt.show()

plt.figure()
plt.plot(daytype_annual["hour"], daytype_annual["temp_c"])
plt.title("Día tipo anual - Temperatura (°C) vs hora (UTC)")
plt.xlabel("Hora (0-23)")
plt.ylabel("Temperatura (°C)")
plt.grid(True)
plt.show()

# B) Día tipo por trimestres (Demanda)
plt.figure()
for qname in ["Q1 (Jan-Mar)", "Q2 (Apr-Jun)", "Q3 (Jul-Sep)", "Q4 (Oct-Dec)"]:
    sub = daytype_quarter[daytype_quarter["quarter"] == qname]
    plt.plot(sub["hour"], sub["demand_mw"], label=qname)
plt.title("Día tipo por trimestre - Demanda (MW)")
plt.xlabel("Hora (0-23)")
plt.ylabel("Demanda (MW)")
plt.grid(True)
plt.legend()
plt.show()

# C) Comparación first5 vs last5 (Demanda)
plt.figure()
for pname in daytype_5y["period_5y"].unique():
    sub = daytype_5y[daytype_5y["period_5y"] == pname]
    plt.plot(sub["hour"], sub["demand_mw"], label=pname)
plt.title("Día tipo - Primeros 5 años vs últimos 5 años (Demanda MW)")
plt.xlabel("Hora (0-23)")
plt.ylabel("Demanda (MW)")
plt.grid(True)
plt.legend()
plt.show()

# D) COVID 2020 vs resto (Demanda)
plt.figure()
for cname in ["No-COVID (rest)", "COVID (2020)"]:
    sub = daytype_covid[daytype_covid["covid_flag"] == cname]
    plt.plot(sub["hour"], sub["demand_mw"], label=cname)
plt.title("Día tipo - COVID 2020 vs resto (Demanda MW)")
plt.xlabel("Hora (0-23)")
plt.ylabel("Demanda (MW)")
plt.grid(True)
plt.legend()
plt.show()

# --- Guardar tablas (para memoria/defensa) ---
daytype_annual.to_csv("eda_daytype_annual.csv", index=False)
daytype_quarter.to_csv("eda_daytype_quarter.csv", index=False)
pivot_demand.to_csv("eda_month_hour_demand.csv")
pivot_temp.to_csv("eda_month_hour_temp.csv")
daytype_5y.to_csv("eda_daytype_first5_last5.csv", index=False)
daytype_covid.to_csv("eda_daytype_covid.csv", index=False)

print("OK: generados CSVs y gráficos.")
