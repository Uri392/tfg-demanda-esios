import requests

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"   # entre comillas
INDICATOR_ID = 1293

url = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "x-api-key": TOKEN,
}

r = requests.get(url, headers=headers, timeout=60)

print("STATUS:", r.status_code)
r.raise_for_status()

data = r.json()

print("\nGEOS DISPONIBLES PARA ESTE INDICADOR:")
for g in data["indicator"]["geos"]:
    print(g["geo_id"], "-", g["geo_name"])
