import sqlite3
from pathlib import Path

DB = Path("data/db/master_energy.sqlite")

with sqlite3.connect(DB) as conn:
    cur = conn.cursor()

    cur.execute("SELECT MIN(temp_c), MAX(temp_c), AVG(temp_c) FROM temperature_peninsula_hourly;")
    tmin, tmax, tavg = cur.fetchone()

    # valores extremos (top 10 fríos/calor)
    cur.execute("""
        SELECT timestamp_utc, temp_c
        FROM temperature_peninsula_hourly
        ORDER BY temp_c ASC
        LIMIT 10
    """)
    cold = cur.fetchall()

    cur.execute("""
        SELECT timestamp_utc, temp_c
        FROM temperature_peninsula_hourly
        ORDER BY temp_c DESC
        LIMIT 10
    """)
    hot = cur.fetchall()

print("min/avg/max temp_c:", tmin, tavg, tmax)
print("\n10 coldest:")
for r in cold: print(r)
print("\n10 hottest:")
for r in hot: print(r)

# checks sencillos (ajusta si quieres)
print("\nSanity checks:")
print("min > -30 ?", tmin > -30)
print("max <  45 ?", tmax < 45)
