import sqlite3
import pandas as pd

DB_MASTER = "data/db/master_energy.sqlite"

con = sqlite3.connect(DB_MASTER)

df = pd.read_sql("""
SELECT substr(timestamp_utc,1,4) AS year, COUNT(*) AS n
FROM demand_peninsula_hourly
GROUP BY year
ORDER BY year;
""", con)

print(df)

minmax = pd.read_sql("""
SELECT MIN(timestamp_utc) AS min_ts, MAX(timestamp_utc) AS max_ts, COUNT(*) AS total
FROM demand_peninsula_hourly;
""", con)

print("\n", minmax)

con.close()
