# Event-Driven Inventory Management System (Windows)

## Requirements
- Windows 10/11, Python 3.10-3.12 (tick "Add to PATH"), Docker Desktop (WSL2 backend, running)
- 8 GB RAM recommended; free ports 9092, 9870, 9864, 9000, 8501
- Optional: Online Retail II .xlsx from UCI in `data\` (filename starts with `online_retail`); else synthetic data

## Setup (PowerShell, in project folder)
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
Set-ExecutionPolicy -Scope Process Bypass

## Run everything
.\start_all.ps1
Dashboard: http://localhost:8501   HDFS UI: http://localhost:9870

## Run manually (separate PowerShell windows, venv active)
docker compose up -d
python prepare_data.py
python consumer.py c1
python consumer.py c2
python forecast_service.py --loop 90 --auto
streamlit run dashboard.py
python producer.py 40

If prophet fails to install, delete it from requirements.txt (ARIMA-only fallback works).

## Use the REAL Online Retail II dataset (instead of synthetic)
1. Download from https://archive.ics.uci.edu/dataset/502/online+retail+ii (zip -> online_retail_II.xlsx)
2. Put it in `data\` (filename must start with `online_retail`)
3. Reset old synthetic state, then run again (PowerShell):
   docker compose down -v
   Remove-Item data\inventory.db*, data\stream_events.csv, data\hdfs_fallback -Recurse -Force -ErrorAction SilentlyContinue
   .\start_all.ps1
prepare_data.py prints "Reading data/online_retail_II.xlsx" (takes 1-3 min). Dashboard badge shows "Online Retail II (UCI)".
Real data needs no live feed: the last 20% of the dataset is replayed as the live Kafka stream.

## What the planner does (v3)
- prepare_data.py clips bulk-order/cancellation outliers per SKU (99th pct) and writes data\init_stock.csv (~3 weeks of demand per warehouse).
- forecast_service.py plans PER WAREHOUSE: demand split by each warehouse's share, reorder point + order-up-to level,
  surplus warehouses TRANSFER stock to short ones first, the rest is a SUPPLIER restock. Review period uses the replay's simulated days.
- Simulated test (5 SKUs, no restock vs auto): blocked oversells 890 -> 1, stock stays balanced across warehouses.
