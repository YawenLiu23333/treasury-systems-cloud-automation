import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def _randwalk(n, start=100.0, vol=0.02, seed=None):
    rng = np.random.default_rng(seed)
    rets = rng.normal(0, vol, n)
    prices = start * np.exp(np.cumsum(rets))
    return prices

def generate_prices(symbols, days=60):
    # Generate synthetic daily prices for given symbols.
    end = datetime.utcnow().date()
    idx = pd.date_range(end - timedelta(days=days-1), end, freq="D")
    frames = []
    for i, sym in enumerate(symbols):
        prices = _randwalk(len(idx), start=100 + 5*i, vol=0.015 + 0.003*i, seed=42+i)
        frames.append(pd.DataFrame({
            "timestamp": idx,
            "symbol": sym,
            "close": prices.round(4)
        }))
    return pd.concat(frames, ignore_index=True)
