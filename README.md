# S&P 500 Real-Time Data Pipeline

**Automated daily ingestion of 500+ S&P 500 stocks into Snowflake using Airflow, yfinance, S3, and pandas.**
sp500-pipeline/
├── airflow/
│   ├── dags/
│   │   └── SP500.py
│   └── docker-compose.yaml
├── data/
│   └── (empty - .gitkeep)
├── sql/
│   └── create_table.sql
├── requirements.txt
├── .gitignore
├── README.md
└── LICENSE
---

## What It Does
- **Fetches** OHLCV + Adjusted Close for all S&P 500 tickers from `yfinance`
- **Calculates** `close_change` and `close_pct_change` per symbol
- **Stores** raw & transformed CSVs in **S3**
- **Loads** into **Snowflake** with **exact schema match**
- Runs **fully in Dockerized Airflow**

---

## Tech Stack
- **Apache Airflow** (Docker) – Orchestration
- **yfinance** – Data source
- **pandas** – Transformations
- **AWS S3** – Staging
- **Snowflake** – Data warehouse
- **Python 3.12**

---

## Key Features
- **Idempotent** & **restartable**
- **XCom path passing** between tasks
- **Case-sensitive column handling** (`"close_change"`)
- **Robust error logging**
- **Production-ready structure**

---

## Status
**Live & Running Daily**

![Airflow](https://img.shields.io/badge/Airflow-2.9.3-blue)  
![Snowflake](https://img.shields.io/badge/Snowflake-Live-green)  
![Python](https://img.shields.io/badge/Python-3.12-yellow)

---
