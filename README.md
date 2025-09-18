# Treasury Systems – Cloud Automation Pipeline (Python + SQL + Azure-ready)

A compact data pipeline that:
1) **Ingests** synthetic time-series “market” data for symbols
2) **Transforms** it (returns, moving averages, anomaly flags)
3) **Loads** it into a SQL database (SQLite by default; switch to **Azure SQL** via `DATABASE_URL`)
4) **Alerts** (optional) to a webhook (Power Automate/Logic Apps) on errors

## Quick Start (Local)

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python main.py --symbols AAPL,NG,MSFT --days 60
```

This creates/updates a local SQLite DB at `./local.db` and prints a summary.

## Environment Variables

Create a `.env` (see `.env.example`):
- `DATABASE_URL` – defaults to SQLite (`sqlite:///local.db`).  
  - Example **Azure SQL**:  
    `mssql+pyodbc://USER:PASSWORD@YOURSERVER.database.windows.net:1433/YOURDB?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no`
- `ALERT_WEBHOOK_URL` – (optional) HTTP endpoint for alerts (Power Automate/Logic Apps).
- `ENV` – `local`/`dev`/`prod` (for logging tag).

## Azure SQL (Swap in minutes)
1. Create an Azure SQL Database + user.
2. Ensure your local IP is allowed (or run from an Azure runner).
3. Set `DATABASE_URL` to your Azure connection string (example above).
4. Run the schema (optional; the app creates tables if missing):
   ```bash
   python -c "from src.load import create_schema; create_schema()"
   ```

## Power Automate / Logic Apps Alerts (Optional)
See `power_automate/README.md` for creating an HTTP-triggered flow. Set `ALERT_WEBHOOK_URL` in `.env`.

## Tests
```bash
pytest -q
```

## CI (GitHub Actions)
- On push, CI installs deps and runs tests (`.github/workflows/ci.yml`).

## What This Demonstrates
- Python data engineering (pandas, anomaly detection)
- SQL persistence (SQLite locally, Azure SQL ready)
- Cloud awareness (swap DB driver via env)
- Observability (webhook alerts)
- Dev hygiene (tests, CI, README, Dockerfile)

---

## How to Publish on GitHub

```bash
git init
git add .
git commit -m "Initial commit: Cloud Automation pipeline (Python + SQL + Azure-ready)"
# Create a new empty repo on GitHub named treasury-systems-cloud-automation
git branch -M main
git remote add origin https://github.com/<your-username>/treasury-systems-cloud-automation.git
git push -u origin main
```
