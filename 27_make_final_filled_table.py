import sqlite3
import pandas as pd

DB_PATH = "data/db/entsoe_load_es.sqlite"

ENTSOE_TABLE = "load_hourly_es"
ESIOS_TABLE = "esios_peninsula_hourly"
FINAL_TABLE = "load_hourly_es_final"

con = sqlite3.connect(DB_PATH)

con.execute(f"""
CREATE TABLE IF NOT EXISTS {FINAL_TABLE} (
  timestamp TEXT PRIMARY KEY,
  load_mw REAL NOT NULL,
  coverage_ratio REAL,
  source TEXT NOT NULL
);
""")
con.commit()

# 1) meter todo ENTSO-E
con.execute(f"""
INSERT OR REPLACE INTO {FINAL_TABLE} (timestamp, load_mw, coverage_ratio, source)
SELECT timestamp, load_mw, coverage_ratio, 'ENTSOE'
FROM {ENTSOE_TABLE};
""")
con.commit()

# 2) meter ESIOS solo si no existe ya el timestamp
con.execute(f"""
INSERT OR IGNORE INTO {FINAL_TABLE} (timestamp, load_mw, coverage_ratio, source)
SELECT timestamp, load_mw, NULL, 'ESIOS_1293'
FROM {ESIOS_TABLE};
""")
con.commit()

# Check por año
df = pd.read_sql(f"""
SELECT substr(timestamp,1,4) AS year, COUNT(*) AS n
FROM {FINAL_TABLE}
GROUP BY year
ORDER BY year;
""", con)

print(df)

con.close()
print("\n✅ Tabla final:", FINAL_TABLE)
