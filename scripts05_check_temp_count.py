import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM temperature_peninsula_hourly
        WHERE timestamp_utc >= '2006-01-01 00:00:00'
          AND timestamp_utc <  '2006-02-01 00:00:00'
    """)
    print("rows_jan_2006 =", cur.fetchone()[0])
