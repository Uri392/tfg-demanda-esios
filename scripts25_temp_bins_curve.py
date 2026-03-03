import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = Path("data/db/master_energy.sqlite")

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("""
            SELECT temp_c, demand_mw
            FROM v_demand_temp_hourly
            WHERE temp_c IS NOT NULL AND demand_mw IS NOT NULL
        """, conn)

        # bins de 1°C (puedes cambiar a 0.5 o 2°C)
        df["temp_bin"] = df["temp_c"].round(0).astype(int)

        out = (
            df.groupby("temp_bin")
              .agg(hours=("demand_mw", "size"),
                   demand_mean=("demand_mw", "mean"),
                   demand_p10=("demand_mw", lambda s: s.quantile(0.10)),
                   demand_p90=("demand_mw", lambda s: s.quantile(0.90)))
              .reset_index()
              .sort_values("temp_bin")
        )

        # filtra bins con pocas horas (ruido)
        out2 = out[out["hours"] >= 200]  # ajusta si quieres

        print("Bins total:", len(out), " | bins used (hours>=200):", len(out2))
        print("\nSample (first 20 rows):")
        print(out2.head(20).to_string(index=False))

        # guarda csv para meterlo en memoria / excel / informe
        out2.to_csv("data/outputs/temp_bins_curve.csv", index=False)
        print("\nSaved: data/outputs/temp_bins_curve.csv")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
