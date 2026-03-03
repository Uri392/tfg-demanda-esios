import requests

TOKEN = "635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f"

url = "https://api.esios.ree.es/indicators"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "x-api-key": TOKEN,
}

params = {
    "search": "demanda",   # palabra clave
}

r = requests.get(url, headers=headers, params=params, timeout=60)
print("STATUS:", r.status_code)
r.raise_for_status()

data = r.json()

# La respuesta suele traer una lista con "indicators"
inds = data.get("indicators", [])
print("Num indicadores:", len(inds))

# Mostrar los 30 primeros (id y nombre)
for it in inds[:30]:
    print(it.get("id"), "-", it.get("name"))
