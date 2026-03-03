import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()

        # timestamps que existen en temperatura pero no en demanda
        rows = cur.execute("""
            SELECT t.timestamp_utc
            FROM temperature_peninsula_hourly t
            LEFT JOIN demand_peninsula_hourly d
              ON d.timestamp_utc = t.timestamp_utc
            WHERE d.timestamp_utc IS NULL
            ORDER BY t.timestamp_utc
        """).fetchall()

        missing = [r[0] for r in rows]

        print("Missing demand timestamps:", len(missing))
        for ts in missing:
            print(ts)

        # Agrupar por "bloques" consecutivos para entender si son cortes puntuales o tramos
        def to_int(ts):
            # ts: 'YYYY-MM-DD HH:MM:SS'
            import datetime as dt
            return int(dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp() // 3600)

        blocks = []
        if missing:
            start = prev = missing[0]
            prev_i = to_int(prev)
            for ts in missing[1:]:
                i = to_int(ts)
                if i == prev_i + 1:
                    prev = ts
                    prev_i = i
                else:
                    blocks.append((start, prev))
                    start = prev = ts
                    prev_i = i
            blocks.append((start, prev))

        print("\nBlocks (consecutive gaps):", len(blocks))
        for a, b in blocks:
            print(f"{a}  ->  {b}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
