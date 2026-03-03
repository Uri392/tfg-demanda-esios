import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/master_energy.sqlite")

ddl = """
CREATE TABLE IF NOT EXISTS temperature_peninsula_hourly (
    timestamp_utc TEXT PRIMARY KEY,          -- 'YYYY-MM-DD HH:MM:SS' (UTC)
    temp_c REAL NOT NULL,
    source TEXT NOT NULL,
    ingested_at_utc TEXT NOT NULL
);

-- Útil si luego creas otras tablas relacionadas
CREATE INDEX IF NOT EXISTS idx_temp_ts ON temperature_peninsula_hourly(timestamp_utc);
"""

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(ddl)
        conn.commit()
        print("OK: tabla temperature_peninsula_hourly creada/verificada")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
