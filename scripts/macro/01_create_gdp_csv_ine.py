from pathlib import Path
import pandas as pd


OUT_DIR = Path("data/external/macro")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PATH = OUT_DIR / "gdp_spain_ine_2006_2025.csv"


def main():
    data = [
        {
            "year": 2006,
            "gdp_current_million_eur": 1004976,
            "gdp_volume_index_2020_100": 100.557,
            "gdp_volume_growth_pct": 4.1,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2007,
            "gdp_current_million_eur": 1077541,
            "gdp_volume_index_2020_100": 104.110,
            "gdp_volume_growth_pct": 3.5,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2008,
            "gdp_current_million_eur": 1112432,
            "gdp_volume_index_2020_100": 104.909,
            "gdp_volume_growth_pct": 0.8,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2009,
            "gdp_current_million_eur": 1072990,
            "gdp_volume_index_2020_100": 100.956,
            "gdp_volume_growth_pct": -3.8,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2010,
            "gdp_current_million_eur": 1077145,
            "gdp_volume_index_2020_100": 101.051,
            "gdp_volume_growth_pct": 0.1,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2011,
            "gdp_current_million_eur": 1068690,
            "gdp_volume_index_2020_100": 100.404,
            "gdp_volume_growth_pct": -0.6,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2012,
            "gdp_current_million_eur": 1035964,
            "gdp_volume_index_2020_100": 97.527,
            "gdp_volume_growth_pct": -2.9,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2013,
            "gdp_current_million_eur": 1025652,
            "gdp_volume_index_2020_100": 96.135,
            "gdp_volume_growth_pct": -1.4,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2014,
            "gdp_current_million_eur": 1038949,
            "gdp_volume_index_2020_100": 97.597,
            "gdp_volume_growth_pct": 1.5,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2015,
            "gdp_current_million_eur": 1087112,
            "gdp_volume_index_2020_100": 101.560,
            "gdp_volume_growth_pct": 4.1,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2016,
            "gdp_current_million_eur": 1122967,
            "gdp_volume_index_2020_100": 104.521,
            "gdp_volume_growth_pct": 2.9,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2017,
            "gdp_current_million_eur": 1170024,
            "gdp_volume_index_2020_100": 107.548,
            "gdp_volume_growth_pct": 2.9,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2018,
            "gdp_current_million_eur": 1212276,
            "gdp_volume_index_2020_100": 110.124,
            "gdp_volume_growth_pct": 2.4,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2019,
            "gdp_current_million_eur": 1253710,
            "gdp_volume_index_2020_100": 112.284,
            "gdp_volume_growth_pct": 2.0,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2020,
            "gdp_current_million_eur": 1129214,
            "gdp_volume_index_2020_100": 100.000,
            "gdp_volume_growth_pct": -10.9,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "Año COVID",
        },
        {
            "year": 2021,
            "gdp_current_million_eur": 1235474,
            "gdp_volume_index_2020_100": 106.683,
            "gdp_volume_growth_pct": 6.7,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2022,
            "gdp_current_million_eur": 1375863,
            "gdp_volume_index_2020_100": 113.479,
            "gdp_volume_growth_pct": 6.4,
            "status_ine": "revisado",
            "is_provisional": 0,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2023,
            "gdp_current_million_eur": 1497761,
            "gdp_volume_index_2020_100": 116.272,
            "gdp_volume_growth_pct": 2.5,
            "status_ine": "provisional",
            "is_provisional": 1,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "",
        },
        {
            "year": 2024,
            "gdp_current_million_eur": 1594330,
            "gdp_volume_index_2020_100": 120.289,
            "gdp_volume_growth_pct": 3.5,
            "status_ine": "avance",
            "is_provisional": 1,
            "source": "INE - Contabilidad Nacional Anual",
            "notes": "Dato avance",
        },
        {
            "year": 2025,
            "gdp_current_million_eur": 1687152,
            "gdp_volume_index_2020_100": 123.657,
            "gdp_volume_growth_pct": 2.8,
            "status_ine": "estimacion_cntr",
            "is_provisional": 1,
            "source": "INE - Contabilidad Nacional Trimestral",
            "notes": "2025 estimado a partir de CNTR. Índice de volumen aproximado como 2024 * 1.028",
        },
    ]

    df = pd.DataFrame(data)

    df.to_csv(OUT_PATH, index=False, encoding="utf-8")

    print("CSV creado correctamente:")
    print(OUT_PATH)
    print()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()