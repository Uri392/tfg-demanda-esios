import sqlite3
import pandas as pd

DB_PATH = "data/db/entsoe_load_es.sqlite"
TABLE = "load_hourly_es"

con = sqlite3.connect(DB_PATH)

print("Rango total:")
print(pd.read_sql(f"SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts, COUNT(*) AS n FROM {TABLE}", con))

print("\nRango por año:")
print(pd.read_sql(f"""
SELECT substr(timestamp,1,4) AS year,
       MIN(timestamp) AS min_ts,
       MAX(timestamp) AS max_ts,
       COUNT(*) AS n
FROM {TABLE}
GROUP BY year
ORDER BY year;
""", con))

con.close()
