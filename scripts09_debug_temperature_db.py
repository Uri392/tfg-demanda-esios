import os
import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

print("CWD:", os.getcwd())
print("DB exists:", DB_PATH.exists())
print("DB absolute:", DB_PATH.resolve())

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()

    print("\nTables:")
    for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"):
        print(" -", name)

    print("\nTemperature rows (total):")
    cur.execute("SELECT COUNT(*) FROM temperature_peninsula_hourly;")
    print(cur.fetchone()[0])

    print("\nTemperature min/max timestamp:")
    cur.execute("SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM temperature_peninsula_hourly;")
    print(cur.fetchone())

    print("\nSample 5 rows:")
    cur.execute("SELECT timestamp_utc, temp_c, source FROM temperature_peninsula_hourly ORDER BY timestamp_utc LIMIT 5;")
    for row in cur.fetchall():
        print(row)

    print("\nRows Jan 2006 (range query):")
    cur.execute("""
        SELECT COUNT(*) FROM temperature_peninsula_hourly
        WHERE timestamp_utc >= '2006-01-01 00:00:00'
          AND timestamp_utc <  '2006-02-01 00:00:00'
    """)
    print(cur.fetchone()[0])
