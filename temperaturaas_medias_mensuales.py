import sqlite3
import pandas as pd

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

DB_PATH = r"data\db\master_energy.sqlite"

con = sqlite3.connect(DB_PATH)

# Sacamos media mensual por año
df = pd.read_sql("""
SELECT
    substr(timestamp_utc, 1, 4) AS year,
    substr(timestamp_utc, 6, 2) AS month,
    ROUND(AVG(temp_c), 2) AS mean_temp_c
FROM temperature_peninsula_hourly
GROUP BY year, month
ORDER BY year, month;
""", con)

con.close()

print("=== TEMPERATURA MEDIA MENSUAL POR AÑO ===")
print(df.to_string(index=False))
