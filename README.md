# S&P 500 Real-Time Data Pipeline

**Automated daily ingestion of 500+ S&P 500 stocks into Snowflake using Airflow, yfinance, S3, and pandas.**

[![Airflow](https://img.shields.io/badgeyczaj

![Airflow](https://img.shields.io/badge/Airflow-2.9.3-blue)
![Snowflake](https://img.shields.io/badge/Snowflake-Live-green)
![Python](https://img.shields.io/badge/Python-3.12-yellow)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

---

## What It Does

- **Fetches** OHLCV + Adjusted Close for all S&P 500 tickers from `yfinance`
- **Calculates** `close_change` and `close_pct_change` per symbol
- **Stores** raw & transformed CSVs in **S3**
- **Loads** into **Snowflake** with **exact schema match**
- Runs **fully in Dockerized Airflow**

---

## ETL Process (Futuristic View)

![ETL Process](./docs/etl-process.png)

> *Data flows from yfinance → Airflow → S3 → Snowflake with real-time transformation*

---

## System Architecture

![Architecture Diagram](./docs/architecture-diagram.png)

> *Orchestrated by Airflow, staged in S3, landed in Snowflake with case-sensitive columns*

---

## Tech Stack

| Component       | Tool                     |
|----------------|--------------------------|
| Orchestration  | Apache Airflow (Docker)  |
| Data Source    | `yfinance`               |
| Transformation | `pandas`                 |
| Storage        | AWS S3                   |
| Data Warehouse | Snowflake                |
| Language       | Python 3.12              |

---

## Key Features

- **Idempotent** & **restartable**
- **XCom path passing** between tasks
- **Case-sensitive column handling** (`"close_change"`)
- **Robust error logging**
- **Production-ready structure**
- **Daily scheduled**

---

## Status

**Live & Running Daily**

---

## Project Structure

S-P500-airflow-pipeline/
├── airflow/
│   ├── dags/
│   │   └── SP500.py
│   └── docker-compose.yaml
├── docs/
│   ├── etl-process.png
│   └── architecture-diagram.png
├── sql/
│   └── create_table.sql
├── requirements.txt
├── .gitignore
├── README.md
└── LICENSE

---

## Setup Instructions

### 1. Clone the Repository

git clone https://github.com/Hamza-khan10/S-P500-airflow-pipeline.git
cd S-P500-airflow-pipeline/airflow

### 2. Start Airflow in Docker
docker-compose up -d

### 3. Configure Airflow Connections (UI: Admin → Connections)

aws_default → AWS Access Key + Secret
snowflake_default → Snowflake Account, User, Password, Role, Warehouse

### 4. Trigger the DAG
→ Go to Airflow UI: http://localhost:8080
→ Trigger sp500_yfinance_to_s3_to_snowflake

### Snowflake Table Schema
CREATE OR REPLACE TABLE MARKET_DB.PUBLIC.SP500_DATA (
    "DATE"             TIMESTAMP_NTZ,
    "SYMBOL"           STRING,
    "OPEN"             FLOAT,
    "HIGH"             FLOAT,
    "LOW"              FLOAT,
    "CLOSE"            FLOAT,
    "ADJ_CLOSE"        FLOAT,
    "VOLUME"           BIGINT,
    "close_change"     FLOAT,
    "close_pct_change" FLOAT
);
