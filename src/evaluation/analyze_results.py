import pandas as pd
from pathlib import Path


def summarize(path: str):
    df = pd.read_csv(path)
    print("Rows:", len(df))
    for col in ["energy_joules", "duration_ms"]:
        if col in df:
            print(col, "mean=", df[col].mean())


if __name__ == "__main__":
    files = sorted(Path("data/results").glob("experiments_*.csv"))
    if not files:
        raise SystemExit("No results found")
    summarize(str(files[-1]))
