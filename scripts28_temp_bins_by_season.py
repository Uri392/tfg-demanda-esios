import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = Path("data/db/master_energy.sqlite")

def season_from_month(m: int) -> str:
    # meteorological seasons
    if m in (12, 1, 2):
        return "DJF"  # winter
    if m in (3, 4, 5):
        return "MAM"  # spring
    if m in (6, 7, 8):
        return "JJA"  # summer
    return "SON"      # autumn

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("""
            SELECT timestamp_utc, temp_c, demand_mw
            FROM v_demand_temp_hourly
            WHERE temp_c IS NOT NULL AND demand_mw IS NOT NULL
        """, conn)

        df["ts"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        df["month"] = df["ts"].dt.month
        df["season"] = df["month"].apply(season_from_month)

        df["temp_bin"] = df["temp_c"].round(0).astype(int)

        out = (
            df.groupby(["season", "temp_bin"])
              .agg(hours=("demand_mw", "size"),
                   demand_mean=("demand_mw", "mean"))
              .reset_index()
        )

        # quita bins muy raros por estación (pocas horas)
        out = out[out["hours"] >= 100].sort_values(["season", "temp_bin"])

        Path("data/outputs").mkdir(parents=True, exist_ok=True)
        out.to_csv("data/outputs/temp_bins_by_season.csv", index=False)
        print("Saved: data/outputs/temp_bins_by_season.csv")
        print(out.head(20).to_string(index=False))

    finally:
        conn.close()

if __name__ == "__main__":
    main()
