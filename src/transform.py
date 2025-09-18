import pandas as pd
import numpy as np

def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values(["symbol", "timestamp"], inplace=True)

    # Returns
    df["return"] = df.groupby("symbol")["close"].pct_change()

    # 20-day SMA and std on returns for anomaly flag
    df["sma20"] = df.groupby("symbol")["close"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df["ret_z"] = df.groupby("symbol")["return"].transform(
        lambda s: (s - s.rolling(20, min_periods=5).mean()) / (s.rolling(20, min_periods=5).std(ddof=0))
    )

    # Simple anomaly rule: |z| > 3 on return
    df["is_anomaly"] = df["ret_z"].abs() > 3

    # Deduplicate
    df.drop_duplicates(subset=["symbol", "timestamp"], keep="last", inplace=True)
    return df
