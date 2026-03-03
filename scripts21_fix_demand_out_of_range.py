import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")
END_OK = "2025-12-31 23:00:00"   # último timestamp válido
START_OK = "2006-01-01 00:00:00"

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()

        # cuenta antes
        n_before = cur.execute("SELECT COUNT(*) FROM demand_peninsula_hourly").fetchone()[0]
        print("demand rows before:", n_before)

        # lista out-of-range
        bad = cur.execute("""
            SELECT timestamp_utc, demand_mw, source
            FROM demand_peninsula_hourly
            WHERE timestamp_utc < ? OR timestamp_utc > ?
            ORDER BY timestamp_utc
        """, (START_OK, END_OK)).fetchall()

        print("out-of-range rows found:", len(bad))
        for r in bad[:10]:
            print("  ", r)

        # borra out-of-range
        cur.execute("""
            DELETE FROM demand_peninsula_hourly
            WHERE timestamp_utc < ? OR timestamp_utc > ?
        """, (START_OK, END_OK))
        conn.commit()

        n_after = cur.execute("SELECT COUNT(*) FROM demand_peninsula_hourly").fetchone()[0]
        print("demand rows after :", n_after)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
