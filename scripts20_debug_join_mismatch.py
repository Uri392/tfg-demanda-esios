import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

def one(conn, q):
    return conn.execute(q).fetchone()[0]

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        n_d = one(conn, "SELECT COUNT(*) FROM demand_peninsula_hourly;")
        n_t = one(conn, "SELECT COUNT(*) FROM temperature_peninsula_hourly;")
        n_j = one(conn, """
            SELECT COUNT(*)
            FROM demand_peninsula_hourly d
            JOIN temperature_peninsula_hourly t
              ON t.timestamp_utc = d.timestamp_utc;
        """)
        print("demand rows:", n_d)
        print("temp rows  :", n_t)
        print("join rows  :", n_j)
        print("missing vs temp:", n_t - n_j)

        # quick sanity: demand min/max
        print("demand min/max:", conn.execute(
            "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM demand_peninsula_hourly"
        ).fetchone())
        print("temp   min/max:", conn.execute(
            "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM temperature_peninsula_hourly"
        ).fetchone())
    finally:
        conn.close()

if __name__ == "__main__":
    main()
