import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

CSV_PATH = Path("data/outputs/temp_bins_curve.csv")
OUT_PATH = Path("data/outputs/temp_demand_curve.png")

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing CSV: {CSV_PATH.resolve()}")

    df = pd.read_csv(CSV_PATH)

    # Por si quedó algún bin raro, ordena
    df = df.sort_values("temp_bin")

    # Plot
    plt.figure(figsize=(10, 5))
    plt.plot(df["temp_bin"], df["demand_mean"], marker="o", linewidth=1)

    # Banda p10-p90
    plt.fill_between(df["temp_bin"], df["demand_p10"], df["demand_p90"], alpha=0.2)

    plt.xlabel("Temperature bin (°C, rounded)")
    plt.ylabel("Demand (MW)")
    plt.title("Spain Peninsula: Demand vs Temperature (hourly, 2006–2025)")
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(OUT_PATH, dpi=150)
    print("Saved plot:", OUT_PATH)

if __name__ == "__main__":
    main()
