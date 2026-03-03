import sqlite3
from pathlib import Path

DB = Path("data/db/master_energy.sqlite")

START = "2006-01-01 00:00:00"
END   = "2026-01-01 00:00:00"   # exclusivo (hasta fin de 2025)

with sqlite3.connect(DB) as conn:
    cur = conn.cursor()

    # total rows en rango
    cur.execute("""
        SELECT COUNT(*)
        FROM temperature_peninsula_hourly
        WHERE timestamp_utc >= ? AND timestamp_utc < ?
    """, (START, END))
    n = cur.fetchone()[0]

    # horas esperadas (incluye bisiestos)
    cur.execute("""
        WITH RECURSIVE hours(ts) AS (
          SELECT ? 
          UNION ALL
          SELECT datetime(ts, '+1 hour') FROM hours WHERE ts < datetime(?, '-1 hour')
        )
        SELECT COUNT(*) FROM hours
    """, (START, END))
    expected = cur.fetchone()[0]

    # duplicados (no debería haber)
    cur.execute("""
        SELECT COUNT(*) FROM (
          SELECT timestamp_utc
          FROM temperature_peninsula_hourly
          WHERE timestamp_utc >= ? AND timestamp_utc < ?
          GROUP BY timestamp_utc
          HAVING COUNT(*) > 1
        )
    """, (START, END))
    dups = cur.fetchone()[0]

    # horas faltantes (top 20)
    cur.execute("""
        WITH RECURSIVE hours(ts) AS (
          SELECT ? 
          UNION ALL
          SELECT datetime(ts, '+1 hour') FROM hours WHERE ts < datetime(?, '-1 hour')
        )
        SELECT ts
        FROM hours
        LEFT JOIN temperature_peninsula_hourly t
          ON t.timestamp_utc = ts
        WHERE t.timestamp_utc IS NULL
        LIMIT 20
    """, (START, END))
    missing = cur.fetchall()

print("TEMP rows in range:", n)
print("Expected hours     :", expected)
print("Duplicates         :", dups)
print("Missing examples   :", [m[0] for m in missing])
print("OK completeness?   :", (n == expected and dups == 0 and len(missing) == 0))
