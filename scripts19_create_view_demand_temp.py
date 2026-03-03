import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

SQL = """
DROP VIEW IF EXISTS v_demand_temp_hourly;

CREATE VIEW v_demand_temp_hourly AS
SELECT
  d.timestamp_utc,
  d.demand_mw,
  t.temp_c
FROM demand_peninsula_hourly d
JOIN temperature_peninsula_hourly t
  ON t.timestamp_utc = d.timestamp_utc;
"""

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH.resolve()}")

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SQL)
        conn.commit()

        # quick check
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM v_demand_temp_hourly;")
        n = cur.fetchone()[0]
        print("OK view created. Rows in join view:", n)

        cur.execute("SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM v_demand_temp_hourly;")
        print("Min/Max ts:", cur.fetchone())
    finally:
        conn.close()

if __name__ == "__main__":
    main()
