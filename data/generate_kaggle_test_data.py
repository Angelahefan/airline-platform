"""
Quick-and-dirty generator for KAGGLE-FORMAT test data (for testing
kaggle_adapter.py only -- NOT part of the main synthetic pipeline).

Unlike data/generate_data.py (which produces data already in this project's
internal raw-layer column names), this script produces data in the RAW
Kaggle column format (FL_DATE, OP_CARRIER, ...) so you can actually exercise
kaggle_adapter.py's extract/transform logic end-to-end, the same way a real
downloaded Kaggle CSV would look before ingestion touches it.

Usage:
    python -m data.generate_kaggle_test_data
    (writes to data/kaggle/test_flights.csv by default)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from config import settings
from data.reference import AIRPORTS, CARRIERS

N_ROWS = 200  # small on purpose -- this is for adapter testing, not load testing


def main() -> None:
    rng = np.random.default_rng(7)

    codes = [a[0] for a in AIRPORTS]
    carrier_codes = [c[0] for c in CARRIERS]

    origin_idx = rng.integers(0, len(codes), N_ROWS)
    dest_idx = rng.integers(0, len(codes), N_ROWS)
    clash = origin_idx == dest_idx
    while clash.any():
        dest_idx[clash] = rng.integers(0, len(codes), clash.sum())
        clash = origin_idx == dest_idx

    dates = pd.date_range("2024-01-01", periods=30, freq="D")
    flight_date = rng.choice(dates, N_ROWS)

    dep_delay = rng.integers(-10, 60, N_ROWS)
    arr_delay = dep_delay + rng.integers(-10, 10, N_ROWS)
    cancelled = (rng.random(N_ROWS) < 0.02).astype(int)
    diverted = (rng.random(N_ROWS) < 0.005).astype(int)

    df = pd.DataFrame({
        "FL_DATE": pd.to_datetime(flight_date).strftime("%Y-%m-%d"),
        "OP_CARRIER": rng.choice(carrier_codes, N_ROWS),
        "OP_CARRIER_FL_NUM": rng.integers(1, 6500, N_ROWS),
        "ORIGIN": np.array(codes)[origin_idx],
        "DEST": np.array(codes)[dest_idx],
        "CRS_DEP_TIME": rng.integers(0, 24, N_ROWS) * 100,  # e.g. 800, 1430
        "DEP_DELAY": dep_delay,
        "ARR_DELAY": arr_delay,
        "CANCELLED": cancelled,
        "DIVERTED": diverted,
        "DISTANCE": rng.integers(200, 3000, N_ROWS),
    })

    out_dir = settings.kaggle_data_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "test_flights.csv"
    df.to_csv(out_path, index=False)
    print(f"[generate_kaggle_test_data] wrote {len(df):,} rows -> {out_path}")


if __name__ == "__main__":
    main()
