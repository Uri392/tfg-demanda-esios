import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/db/master_energy.sqlite")

def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()

        # lista de timestamps que están en temperatura pero no en demanda
        missing = [r[0] for r in cur.execute("""
            SELECT t.timestamp_utc
            FROM temperature_peninsula_hourly t
            LEFT JOIN demand_peninsula_hourly d
              ON d.timestamp_utc = t.timestamp_utc
            WHERE d.timestamp_utc IS NULL
            ORDER BY t.timestamp_utc
        """).fetchall()]

        print("Missing demand timestamps:", len(missing))
        if not missing:
            print("Nothing to fill.")
            return

        filled = 0
        now = utc_now_str()

        for ts in missing:
            # valor anterior (ts - 1h) y siguiente (ts + 1h)
            prev_row = cur.execute("""
                SELECT demand_mw
                FROM demand_peninsula_hourly
                WHERE timestamp_utc = datetime(?, '-1 hour')
            """, (ts,)).fetchone()

            next_row = cur.execute("""
                SELECT demand_mw
                FROM demand_peninsula_hourly
                WHERE timestamp_utc = datetime(?, '+1 hour')
            """, (ts,)).fetchone()

            if prev_row is None or next_row is None:
                print("SKIP (missing neighbor):", ts, "prev=", prev_row, "next=", next_row)
                continue

            demand_fill = (float(prev_row[0]) + float(next_row[0])) / 2.0

            cur.execute("""
                INSERT OR REPLACE INTO demand_peninsula_hourly
                (timestamp_utc, demand_mw, source, ingested_at_utc)
                VALUES (?, ?, ?, ?)
            """, (ts, demand_fill, "filled_linear_1h", now))

            filled += 1

        conn.commit()
        print("Filled rows:", filled)

        # quick check
        n_d = cur.execute("SELECT COUNT(*) FROM demand_peninsula_hourly").fetchone()[0]
        n_t = cur.execute("SELECT COUNT(*) FROM temperature_peninsula_hourly").fetchone()[0]
        n_j = cur.execute("""
            SELECT COUNT(*)
            FROM demand_peninsula_hourly d
            JOIN temperature_peninsula_hourly t
              ON t.timestamp_utc = d.timestamp_utc
        """).fetchone()[0]

        print("demand rows now:", n_d)
        print("temp rows      :", n_t)
        print("join rows now  :", n_j)
        print("missing vs temp:", n_t - n_j)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
