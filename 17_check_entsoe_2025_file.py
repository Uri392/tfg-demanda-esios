import pandas as pd

FILE = r"data\raw\entsoe\monthly_hourly_load_values_2025.xlsx"

# Leemos solo las columnas necesarias (más rápido)
df = pd.read_excel(FILE, usecols=["DateUTC", "CountryCode", "Value", "Cov_ratio"])

# Filtramos España
es = df[df["CountryCode"] == "ES"].copy()

# Convertimos fecha y valor
es["DateUTC"] = pd.to_datetime(es["DateUTC"], errors="coerce")
es["Value"] = pd.to_numeric(es["Value"], errors="coerce")

es = es.dropna(subset=["DateUTC", "Value"]).copy()

print("Filas ES (brutas):", len(es))
print("Rango ES:", es["DateUTC"].min(), "->", es["DateUTC"].max())

# ¿Viene ordenado?
is_sorted = es["DateUTC"].is_monotonic_increasing
print("¿Está ordenado por DateUTC?:", is_sorted)

# Duplicados de timestamp
dup = es["DateUTC"].duplicated().sum()
print("Duplicados de DateUTC:", dup)

# Horas únicas
unique_hours = es["DateUTC"].nunique()
print("Horas únicas:", unique_hours)

# Si quieres ver las primeras/últimas fechas ORDENADAS:
es_sorted = es.sort_values("DateUTC")
print("\nPrimeras 3 (ordenadas):")
print(es_sorted.head(3)[["DateUTC","Value"]])

print("\nÚltimas 3 (ordenadas):")
print(es_sorted.tail(3)[["DateUTC","Value"]])
