import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()

    print("Tables:")
    for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"):
        print(" -", name)

    print("\nSchema (temperature table):")
    for row in cur.execute("PRAGMA table_info(temperature_peninsula_hourly);"):
        print(row)
