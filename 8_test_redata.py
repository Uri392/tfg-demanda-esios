import requests
import pandas as pd

def prueba_rango(start, end, geo_limit, geo_id):
    url = "https://apidatos.ree.es/es/datos/demanda/demanda-tiempo-real"
    params = {
        "start_date": start,   # "YYYY-MM-DDTHH:MM"
        "end_date": end,
        "time_trunc": "hour",
        "geo_trunc": "electric_system",
        "geo_limit": geo_limit,
        "geo_ids": str(geo_id)
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    # Suele venir en data['included'][...]['attributes']['values']
    total = 0
    for item in data.get("included", []):
        values = item.get("attributes", {}).get("values", [])
        total += len(values)
    return total

# Península: según documentación, geo_id habitual 8741 (como en sus ejemplos)
for year in [2024, 2020, 2016, 2014]:
    start = f"{year}-01-01T00:00"
    end   = f"{year}-01-07T23:50"  # una semana
    n = prueba_rango(start, end, "peninsular", 8741)
    print(year, "-> puntos devueltos:", n)
