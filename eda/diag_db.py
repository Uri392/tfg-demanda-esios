import os
import sqlite3

DB_PATH = "master_energy.sqlite"  # ruta relativa

print("=== DIAGNÓSTICO DB ===")
print("Working dir:", os.getcwd())
print("DB_PATH:", DB_PATH)
print("DB abs path:", os.path.abspath(DB_PATH))

if not os.path.exists(DB_PATH):
    print("❌ El archivo NO existe en esta ruta.")
    raise SystemExit

print("Tamaño (bytes):", os.path.getsize(DB_PATH))

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()
print("Nº tablas:", len(tables))
for t in tables:
    print(" -", t[0])

# Además, listamos views por si acaso
views = cur.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;").fetchall()
print("Nº views:", len(views))
for v in views:
    print(" (view)", v[0])

con.close()
print("✅ Fin diagnóstico")