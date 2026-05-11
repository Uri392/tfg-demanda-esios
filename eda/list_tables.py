import sqlite3

DB_PATH = r"data\db\master_energy.sqlite"
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

print("TABLAS:")
for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"):
    print(" -", name)

con.close()