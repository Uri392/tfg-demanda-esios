import sqlite3
import pandas as pd

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

DB_PATH = r"data\db\master_energy.sqlite"   # si la ruta real es otra, la cambias aquí

con = sqlite3.connect(DB_PATH)

print("=== TABLAS ===")
tables = pd.read_sql("""
SELECT name
FROM sqlite_master
WHERE type='table'
ORDER BY name;
""", con)
print(tables)

print("\n=== TOTAL FILAS DEMANDA ===")
print(pd.read_sql("""
SELECT COUNT(*) AS total_filas
FROM demand_peninsula_hourly;
""", con))

print("\n=== RANGO TEMPORAL ===")
print(pd.read_sql("""
SELECT MIN(timestamp_utc) AS min_ts,
       MAX(timestamp_utc) AS max_ts
FROM demand_peninsula_hourly;
""", con))

print("\n=== FILAS POR AÑO ===")
print(pd.read_sql("""
SELECT substr(timestamp_utc,1,4) AS year,
       COUNT(*) AS n
FROM demand_peninsula_hourly
GROUP BY year
ORDER BY year;
""", con))

print("\n=== PRIMERAS 10 FILAS ===")
print(pd.read_sql("""
SELECT *
FROM demand_peninsula_hourly
ORDER BY timestamp_utc
LIMIT 10;
""", con))

con.close()
