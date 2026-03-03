import pandas as pd
from pathlib import Path

CSV_PATH = Path("data/outputs/temp_bins_curve.csv")

def main():
    df = pd.read_csv(CSV_PATH).sort_values("temp_bin")

    # mínimo global de la demanda media
    idx_min = df["demand_mean"].idxmin()
    row_min = df.loc[idx_min]

    print("Global minimum demand_mean:")
    print(f"  temp_bin = {int(row_min['temp_bin'])} °C")
    print(f"  demand_mean = {row_min['demand_mean']:.2f} MW")
    print(f"  hours = {int(row_min['hours'])}")

    # detectar “cambio de tendencia”: buscar el primer tramo donde sube de forma sostenida
    # (heurística simple: 3 incrementos seguidos)
    df2 = df.reset_index(drop=True)
    diffs = df2["demand_mean"].diff()

    start_up = None
    for i in range(3, len(df2)):
        if diffs.iloc[i] > 0 and diffs.iloc[i-1] > 0 and diffs.iloc[i-2] > 0:
            start_up = i-2
            break

    if start_up is not None:
        print("\nFirst sustained increase (3 bins in a row):")
        print(f"  starts around temp_bin = {int(df2.loc[start_up,'temp_bin'])} °C")
        print(f"  demand_mean there = {df2.loc[start_up,'demand_mean']:.2f} MW")
    else:
        print("\nNo clear sustained increase detected (maybe monotone or noisy at hot end).")

if __name__ == "__main__":
    main()
