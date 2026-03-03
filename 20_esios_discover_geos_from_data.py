import requests

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"
INDICATOR_ID = 1293

url = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"

params = {
    "start_date": "2024-01-01T00:00:00Z",
    "end_date":   "2024-01-02T00:00:00Z",
    "time_trunc": "hour",
    "time_agg":   "avg",
}

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN,
}

r = requests.get(url, headers=headers, params=params, timeout=60)
print("STATUS:", r.status_code)
r.raise_for_status()

data = r.json()

# Los puntos suelen venir en indicator["values"]
values = data.get("indicator", {}).get("values", [])
print("Num values:", len(values))

# Extraer geos únicos
geos = {}
for v in values:
    geo_id = v.get("geo_id")
    geo_name = v.get("geo_name")
    if geo_id is not None and geo_name is not None:
        geos[geo_id] = geo_name

print("\nGEOS encontrados en los datos (unicos):")
for gid in sorted(geos.keys()):
    print(gid, "-", geos[gid])
