from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime, Float, Boolean, UniqueConstraint
from sqlalchemy.engine import Engine
import pandas as pd
from src.config import settings

metadata = MetaData()
prices_table = Table(
    "prices",
    metadata,
    Column("symbol", String(32), nullable=False),
    Column("timestamp", DateTime, nullable=False),
    Column("close", Float, nullable=False),
    Column("return", Float),
    Column("sma20", Float),
    Column("ret_z", Float),
    Column("is_anomaly", Boolean, default=False),
    UniqueConstraint("symbol", "timestamp", name="uq_symbol_ts"),
)

def _engine() -> Engine:
    return create_engine(settings.DATABASE_URL, future=True)

def create_schema():
    engine = _engine()
    metadata.create_all(engine)

def upsert_prices(df: pd.DataFrame):
    engine = _engine()
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    with engine.begin() as conn:
        tmp_table = "__prices_stage"
        df.to_sql(tmp_table, conn, if_exists="replace", index=False)
        dialect = conn.dialect.name
        if dialect in ("mssql",):
            conn.execute(
                f"""
                MERGE prices AS tgt
                USING (SELECT symbol, timestamp, close, "return", sma20, ret_z, is_anomaly FROM {tmp_table}) AS src
                ON (tgt.symbol = src.symbol AND tgt.timestamp = src.timestamp)
                WHEN MATCHED THEN UPDATE SET
                    close = src.close, "return" = src."return", sma20 = src.sma20, ret_z = src.ret_z, is_anomaly = src.is_anomaly
                WHEN NOT MATCHED THEN
                    INSERT (symbol, timestamp, close, "return", sma20, ret_z, is_anomaly)
                    VALUES (src.symbol, src.timestamp, src.close, src."return", src.sma20, src.ret_z, src.is_anomaly);
                """
            )
            conn.execute(f"DROP TABLE {tmp_table}")
        else:
            conn.execute(
                f"""
                INSERT INTO prices (symbol, timestamp, close, "return", sma20, ret_z, is_anomaly)
                SELECT symbol, timestamp, close, "return", sma20, ret_z, is_anomaly FROM {tmp_table}
                ON CONFLICT(symbol, timestamp) DO UPDATE SET
                    close=excluded.close,
                    "return"=excluded."return",
                    sma20=excluded.sma20,
                    ret_z=excluded.ret_z,
                    is_anomaly=excluded.is_anomaly
                """
            )
            conn.execute(f"DROP TABLE {tmp_table}")
