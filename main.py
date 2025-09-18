from src.config import settings
from src.ingest import generate_prices
from src.transform import transform_prices
from src.load import create_schema, upsert_prices
from src.monitor import send_alert
import argparse
import traceback

def run(symbols, days):
    print(f"[{settings.ENV}] Starting pipeline for symbols={symbols} days={days}")
    create_schema()
    raw = generate_prices(symbols, days=days)
    df = transform_prices(raw)
    upsert_prices(df)
    print(f"[{settings.ENV}] Done. Rows loaded: {len(df)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", type=str, default="AAPL,MSFT,NG")
    parser.add_argument("--days", type=int, default=60)
    args = parser.parse_args()

    try:
        run([s.strip() for s in args.symbols.split(",") if s.strip()], args.days)
    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        send_alert(f"Pipeline failure: {e}")
        raise
