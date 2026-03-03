import sqlite3

con = sqlite3.connect("demanda.sqlite")
cur = con.cursor()

# borra todo antes de 2014-01-01 UTC
cur.execute("""
DELETE FROM electric_demand_hourly
WHERE timestamp_utc < '2014-01-01T00:00:00+00:00'
""")

# borra todo desde 2026-01-01 UTC en adelante (por si hubiera)
cur.execute("""
DELETE FROM electric_demand_hourly
WHERE timestamp_utc >= '2026-01-01T00:00:00+00:00'
""")

con.commit()
con.close()
print("✅ Bordes limpiados.")
