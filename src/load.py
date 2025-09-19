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

        if dialect == "mssql":
            # Azure SQL / SQL Server: MERGE
            conn.exec_driver_sql(
                f"""
                MERGE prices AS tgt
                USING (SELECT symbol, timestamp, close, "return", sma20, ret_z, is_anomaly FROM {tmp_table}) AS src
                ON (tgt.symbol = src.symbol AND tgt.timestamp = src.timestamp)
                WHEN MATCHED THEN UPDATE SET
                    close = src.close,
                    "return" = src."return",
                    sma20 = src.sma20,
                    ret_z = src.ret_z,
                    is_anomaly = src.is_anomaly
                WHEN NOT MATCHED THEN
                    INSERT (symbol, timestamp, close, "return", sma20, ret_z, is_anomaly)
                    VALUES (src.symbol, src.timestamp, src.close, src."return", src.sma20, src.ret_z, src.is_anomaly);
                """
            )
        elif dialect == "sqlite":
            # SQLite: two-step upsert (UPDATE existing, then INSERT new)
            conn.exec_driver_sql(
                f"""
                UPDATE prices
                SET
                  close      = (SELECT s.close      FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  "return"   = (SELECT s."return"   FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  sma20      = (SELECT s.sma20      FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  ret_z      = (SELECT s.ret_z      FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  is_anomaly = (SELECT s.is_anomaly FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp)
                WHERE EXISTS (
                  SELECT 1 FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp
                );
                """
            )
            conn.exec_driver_sql(
                f"""
                INSERT INTO prices (symbol, timestamp, close, "return", sma20, ret_z, is_anomaly)
                SELECT s.symbol, s.timestamp, s.close, s."return", s.sma20, s.ret_z, s.is_anomaly
                FROM {tmp_table} s
                WHERE NOT EXISTS (
                  SELECT 1 FROM prices p WHERE p.symbol = s.symbol AND p.timestamp = s.timestamp
                );
                """
            )
        else:
            # Generic fallback: do the same two-step logic (portable on many dialects)
            conn.exec_driver_sql(
                f"""
                UPDATE prices
                SET
                  close      = (SELECT s.close      FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  "return"   = (SELECT s."return"   FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  sma20      = (SELECT s.sma20      FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  ret_z      = (SELECT s.ret_z      FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp),
                  is_anomaly = (SELECT s.is_anomaly FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp)
                WHERE EXISTS (
                  SELECT 1 FROM {tmp_table} s WHERE s.symbol = prices.symbol AND s.timestamp = prices.timestamp
                );
                """
            )
            conn.exec_driver_sql(
                f"""
                INSERT INTO prices (symbol, timestamp, close, "return", sma20, ret_z, is_anomaly)
                SELECT s.symbol, s.timestamp, s.close, s."return", s.sma20, s.ret_z, s.is_anomaly
                FROM {tmp_table} s
                WHERE NOT EXISTS (
                  SELECT 1 FROM prices p WHERE p.symbol = s.symbol AND p.timestamp = s.timestamp
                );
                """
            )

        # cleanup
        conn.exec_driver_sql(f"DROP TABLE {tmp_table}")
