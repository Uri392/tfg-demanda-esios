import sqlite3
from datetime import datetime, timezone

DB_MASTER = "data/db/master_energy.sqlite"

con = sqlite3.connect(DB_MASTER)
cur = con.cursor()

# Tabla principal: demanda horaria peninsular (UTC)
cur.execute("""
CREATE TABLE IF NOT EXISTS demand_peninsula_hourly (
    timestamp_utc TEXT PRIMARY KEY,   -- 'YYYY-MM-DD HH:MM:SS' en UTC
    demand_mw REAL NOT NULL,
    source TEXT NOT NULL,             -- 'ENTSOE' o 'ESIOS_1293' o 'REE_5min_avg' etc
    ingested_at_utc TEXT NOT NULL     -- cuándo lo metiste en la DB
);
""")

# Índice (normalmente ya basta con el PK, pero si luego haces rangos grandes ayuda)
cur.execute("""
CREATE INDEX IF NOT EXISTS idx_demand_ts
ON demand_peninsula_hourly(timestamp_utc);
""")

# Tabla opcional de metadatos (útil para dejar trazabilidad)
cur.execute("""
CREATE TABLE IF NOT EXISTS dataset_log (
    dataset TEXT,
    note TEXT,
    created_at_utc TEXT
);
""")

cur.execute(
    "INSERT INTO dataset_log(dataset, note, created_at_utc) VALUES (?, ?, ?)",
    ("master_energy", "DB maestra creada", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
)

con.commit()
con.close()

print("✅ Creada DB:", DB_MASTER)
print("✅ Tabla: demand_peninsula_hourly")
