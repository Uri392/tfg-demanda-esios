import sqlite3
import pandas as pd

DB_PATH = "data/db/entsoe_load_es.sqlite"
TABLE = "load_hourly_es"

con = sqlite3.connect(DB_PATH)

print("Total filas:")
print(pd.read_sql(f"SELECT COUNT(*) AS n FROM {TABLE}", con))

print("\nRango fechas:")
print(pd.read_sql(f"SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM {TABLE}", con))

print("\nFilas por año:")
print(pd.read_sql(f"""
SELECT substr(timestamp,1,4) AS year, COUNT(*) AS n
FROM {TABLE}
GROUP BY year
ORDER BY year;
""", con))

con.close()
