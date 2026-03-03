import sqlite3
from datetime import datetime, timezone

DB_SOURCE = "data/db/entsoe_load_es.sqlite"
SOURCE_TABLE = "load_hourly_es_final"     # <-- si tu tabla final se llama diferente, cámbialo aquí

DB_MASTER = "data/db/master_energy.sqlite"

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

con_src = sqlite3.connect(DB_SOURCE)
con_dst = sqlite3.connect(DB_MASTER)

# 1) leer todo lo que ya tienes limpio
rows = con_src.execute(f"""
SELECT timestamp, load_mw, source
FROM {SOURCE_TABLE}
ORDER BY timestamp;
""").fetchall()

print("Filas leídas de source:", len(rows))

# 2) upsert a la DB maestra
con_dst.executemany("""
INSERT OR REPLACE INTO demand_peninsula_hourly
(timestamp_utc, demand_mw, source, ingested_at_utc)
VALUES (?, ?, ?, ?);
""", [(ts, mw, src, now) for (ts, mw, src) in rows])

con_dst.commit()

con_src.close()
con_dst.close()

print("✅ Copiadas a DB maestra:", DB_MASTER)
