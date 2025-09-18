import pandas as pd
from src.transform import transform_prices

def test_transform_has_anomaly_flag():
    raw = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=30, freq="D"),
        "symbol": ["TEST"]*30,
        "close": [100 + i for i in range(30)]
    })
    out = transform_prices(raw)
    assert "is_anomaly" in out.columns
    assert out["symbol"].nunique() == 1
    assert len(out) == 30
