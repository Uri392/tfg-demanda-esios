import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        n = cur.execute("""
            SELECT COUNT(*)
            FROM demand_peninsula_hourly
            WHERE source = 'filled_linear_1h'
        """).fetchone()[0]

        print("Filled rows (source=filled_linear_1h):", n)

        rows = cur.execute("""
            SELECT timestamp_utc, demand_mw
            FROM demand_peninsula_hourly
            WHERE source = 'filled_linear_1h'
            ORDER BY timestamp_utc
            LIMIT 25
        """).fetchall()

        print("\nFirst filled rows:")
        for r in rows:
            print(r)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
