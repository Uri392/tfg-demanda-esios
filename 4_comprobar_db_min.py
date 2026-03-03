import sqlite3
import pandas as pd

con = sqlite3.connect("demanda.sqlite")

print("Total filas:")
print(pd.read_sql("SELECT COUNT(*) AS n FROM electric_demand_hourly", con))

print("\nRango fechas:")
print(pd.read_sql("SELECT MIN(timestamp_utc) AS min_dt, MAX(timestamp_utc) AS max_dt FROM electric_demand_hourly", con))

print("\nFilas por año:")
print(pd.read_sql("""
SELECT substr(timestamp_utc,1,4) AS year, COUNT(*) AS n
FROM electric_demand_hourly
GROUP BY year
ORDER BY year;
""", con))

con.close()
