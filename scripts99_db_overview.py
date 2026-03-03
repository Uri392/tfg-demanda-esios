import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

def q(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()

def one(conn, sql, params=()):
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None

def main():
    print("DB path:", DB_PATH.resolve())
    print("DB exists:", DB_PATH.exists())
    if not DB_PATH.exists():
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        tables = q(conn, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        print("\nTables:")
        for (t,) in tables:
            print(f" - {t}")

        for (t,) in tables:
            cnt = one(conn, f"SELECT COUNT(*) FROM {t};")
            print(f"\n=== {t} ===")
            print("rows:", cnt)

            # if it has timestamp_utc, show min/max
            cols = [r[1] for r in q(conn, f"PRAGMA table_info({t});")]
            if "timestamp_utc" in cols and cnt and cnt > 0:
                mn = one(conn, f"SELECT MIN(timestamp_utc) FROM {t};")
                mx = one(conn, f"SELECT MAX(timestamp_utc) FROM {t};")
                print("min_ts:", mn)
                print("max_ts:", mx)

            # show 3 sample rows
            sample = q(conn, f"SELECT * FROM {t} LIMIT 3;")
            if sample:
                print("sample (first 3):")
                for r in sample:
                    print(" ", r)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
