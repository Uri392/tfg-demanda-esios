import sqlite3
import pandas as pd
from pathlib import Path

DB = Path("data/db/master_energy.sqlite")

query = """
SELECT d.timestamp_utc, d.demand_mw, t.temp_c
FROM demand_peninsula_hourly d
JOIN temperature_peninsula_hourly t
  ON d.timestamp_utc = t.timestamp_utc
WHERE d.timestamp_utc >= '2019-01-01 00:00:00'
  AND d.timestamp_utc <  '2020-01-01 00:00:00'
ORDER BY d.timestamp_utc;
"""

with sqlite3.connect(DB) as conn:
    df = pd.read_sql_query(query, conn)

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
df["month"] = df["timestamp_utc"].dt.month

print("rows 2019 join:", len(df))
print("temp min/max  :", df["temp_c"].min(), df["temp_c"].max())
print("demand min/max:", df["demand_mw"].min(), df["demand_mw"].max())

# medias mensuales: deberían mostrar estacionalidad clara
m = df.groupby("month")[["temp_c", "demand_mw"]].mean()
print("\nMonthly means (2019):")
print(m.round(2))
