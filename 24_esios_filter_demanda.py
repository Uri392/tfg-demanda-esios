import requests

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"

url = "https://api.esios.ree.es/indicators"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN,
}

params = {"search": "demanda"}

r = requests.get(url, headers=headers, params=params, timeout=60)
print("STATUS:", r.status_code)
r.raise_for_status()

data = r.json()
inds = data.get("indicators", [])

# Filtrar por nombres que contengan "demanda"
hits = []
for it in inds:
    name = (it.get("name") or "")
    if "demanda" in name.lower():
        hits.append((it.get("id"), name))

print("Total con 'demanda' en el nombre:", len(hits))

# Imprimir todos los hits
for i, (id_, name) in enumerate(hits, 1):
    print(f"{i:03d} | {id_} | {name}")
