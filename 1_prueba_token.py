import requests

TOKEN = '635def0e7cbc18f3773071d9c6d8e8c8d1c25081e1d1d906905b5a7ace46fd4f'
INDICATOR_ID = 1293  # demanda real (según documentación ESIOS)

url = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"

params = {
    "start_date": "2024-01-01T00:00:00Z",
    "end_date":   "2024-01-03T00:00:00Z",
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
print("PRIMEROS 500 CARACTERES DE RESPUESTA:")
print(r.text[:500])
