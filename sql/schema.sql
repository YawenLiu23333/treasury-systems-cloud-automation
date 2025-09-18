CREATE TABLE IF NOT EXISTS prices (
  symbol TEXT NOT NULL,
  timestamp DATETIME NOT NULL,
  close REAL NOT NULL,
  "return" REAL,
  sma20 REAL,
  ret_z REAL,
  is_anomaly BOOLEAN DEFAULT 0,
  CONSTRAINT uq_symbol_ts UNIQUE (symbol, timestamp)
);
