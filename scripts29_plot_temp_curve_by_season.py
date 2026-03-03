import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

CSV_PATH = Path("data/outputs/temp_bins_by_season.csv")
OUT_PATH = Path("data/outputs/temp_demand_curve_by_season.png")

def main():
    df = pd.read_csv(CSV_PATH)

    plt.figure(figsize=(10, 5))

    for season in ["DJF", "MAM", "JJA", "SON"]:
        d = df[df["season"] == season].sort_values("temp_bin")
        if len(d) == 0:
            continue
        plt.plot(d["temp_bin"], d["demand_mean"], marker="o", linewidth=1, label=season)

    plt.xlabel("Temperature bin (°C, rounded)")
    plt.ylabel("Demand (MW)")
    plt.title("Spain Peninsula: Demand vs Temperature by season (2006–2025)")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_PATH, dpi=150)
    print("Saved plot:", OUT_PATH)

if __name__ == "__main__":
    main()
