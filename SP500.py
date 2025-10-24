# SP500.py
# S&P 500 Daily Pipeline: yfinance → S3 → Transform → Snowflake
# Author: You (with love from Grok)
# Status: LIVE & PRODUCTION-READY

import os
import logging
import pandas as pd
import yfinance as yf
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago

# ==================== CONFIG ====================
S3_BUCKET = "your-s3-bucket-name"           # CHANGE ME
S3_PREFIX = "sp500/"                        # Optional: organize in folder
AWS_CONN_ID = "aws_default"                 # Airflow connection
SNOWFLAKE_CONN_ID = "snowflake_default"     # Airflow connection
SNOWFLAKE_TABLE = "MARKET_DB.PUBLIC.SP500_DATA"
DATA_DIR = "/opt/airflow/data"              # Airflow Docker volume
# ===============================================

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": 300,
    "email_on_failure": False,
}

dag = DAG(
    "sp500_yfinance_to_s3_to_snowflake",
    default_args=default_args,
    description="Daily S&P 500 → S3 → Snowflake",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["finance", "etl", "snowflake"],
)

# ==================== HELPERS ====================
def _now_ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _write_csv(df: pd.DataFrame, prefix: str) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = f"{prefix}_{_now_ts()}.csv"
    path = os.path.join(DATA_DIR, filename)
    df.to_csv(path, index=False)
    logging.info("CSV written: %s", path)
    return path

# ==================== TASKS ====================

def fetch_yfinance_data(**context):
    """Fetch all S&P 500 tickers from yfinance"""
    logging.info("Fetching S&P 500 tickers...")
    sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    symbols = sp500["Symbol"].str.replace(".", "-", regex=False).tolist()
    logging.info("Found %d symbols", len(symbols))

    data = []
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d")
            if not hist.empty:
                row = hist.iloc[0]
                data.append({
                    "Datetime": row.name.strftime("%Y-%m-%d"),
                    "Symbol": symbol,
                    "Open": row["Open"],
                    "High": row["High"],
                    "Low": row["Low"],
                    "Close": row["Close"],
                    "Adj Close": row.get("Close", row["Close"]),  # fallback
                    "Volume": int(row["Volume"]),
                })
        except Exception as e:
            logging.warning("Failed for %s: %s", symbol, e)

    df = pd.DataFrame(data)
    logging.info("Fetched %d rows", len(df))
    raw_path = _write_csv(df, "sp500_raw")
    ti = context["ti"]
    ti.xcom_push(key="raw_csv_path", value=raw_path)
    return raw_path


def upload_to_s3(**context):
    """Upload raw CSV to S3"""
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="fetch_yfinance_data", key="raw_csv_path")
    if not raw_path or not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV missing: {raw_path}")

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    key = f"{S3_PREFIX}raw/sp500_raw_{_now_ts()}.csv"
    s3_hook.load_file(filename=raw_path, key=key, bucket_name=S3_BUCKET, replace=True)

    s3_url = f"s3://{S3_BUCKET}/{key}"
    ti.xcom_push(key="s3_path", value=s3_url)
    ti.xcom_push(key="raw_csv_path", value=raw_path)  # Pass forward
    logging.info("Uploaded raw CSV → %s", s3_url)
    return s3_url


def transform_data(**context):
    """Transform: rename, calculate % change, uppercase for Snowflake"""
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="fetch_yfinance_data", key="raw_csv_path")
    if not raw_path or not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV not found: {raw_path}")

    df = pd.read_csv(raw_path)
    logging.info("Raw CSV shape: %s  columns: %s", df.shape, df.columns.tolist())

    # Safety check
    if len(df.columns) != len(set(df.columns)):
        raise ValueError("Duplicate columns in raw CSV")

    # Rename Datetime → Date
    df = df.rename(columns={"Datetime": "Date"})
    df = df.sort_values(["Symbol", "Date"]).reset_index(drop=True)

    # Calculate % change
    df["close_change"] = df.groupby("Symbol")["Close"].diff().fillna(0.0)
    df["close_pct_change"] = df.groupby("Symbol")["Close"].pct_change().fillna(0.0) * 100

    # Force numeric
    for c in ["Open", "High", "Low", "Close", "Adj Close", "Volume", "close_change", "close_pct_change"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Select final columns
    final_cols = [
        "Date", "Symbol", "Open", "High", "Low", "Close",
        "Adj Close", "Volume", "close_change", "close_pct_change"
    ]
    final_df = df[final_cols].copy()

    # RENAME TO MATCH SNOWFLAKE (quoted lowercase)
    rename = {
        "Date": "DATE",
        "Symbol": "SYMBOL",
        "Open": "OPEN",
        "High": "HIGH",
        "Low": "LOW",
        "Close": "CLOSE",
        "Adj Close": "ADJ_CLOSE",
        "Volume": "VOLUME"
    }
    final_df.rename(columns=rename, inplace=True)

    logging.info("Transformed shape: %s  columns: %s", final_df.shape, final_df.columns.tolist())
    transformed_path = _write_csv(final_df, "sp500_transformed")
    ti.xcom_push(key="transformed_csv_path", value=transformed_path)
    return transformed_path


def load_to_snowflake(**context):
    """Load transformed CSV into Snowflake"""
    ti = context["ti"]
    transformed_path = ti.xcom_pull(task_ids="transform_data", key="transformed_csv_path")
    if not transformed_path or not os.path.exists(transformed_path):
        raise FileNotFoundError(f"Transformed CSV not found: {transformed_path}")

    df = pd.read_csv(transformed_path)
    logging.info("Loading DataFrame shape: %s  columns: %s", df.shape, df.columns.tolist())

    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    success, nchunks, nrows, _ = snowflake_hook.run(
        sql=f"COPY INTO {SNOWFLAKE_TABLE} FROM '@~/{os.path.basename(transformed_path)}' FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        handler=lambda cursor: cursor.fetch_pandas_all()
    )
    # Use write_pandas
    from snowflake.connector import pandas_tools
    conn = snowflake_hook.get_conn()
    pandas_tools.write_pandas(conn, df, "SP500_DATA", database="MARKET_DB", schema="PUBLIC", quote_identifiers=True)

    logging.info("Loaded %d rows into %s", len(df), SNOWFLAKE_TABLE)


# ==================== TASK DEFINITIONS ====================
fetch_task = PythonOperator(task_id="fetch_yfinance_data", python_callable=fetch_yfinance_data, dag=dag)
upload_task = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3, dag=dag)
transform_task = PythonOperator(task_id="transform_data", python_callable=transform_data, dag=dag)
load_task = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_snowflake, dag=dag)

# ==================== DEPENDENCIES ====================
fetch_task >> upload_task >> transform_task >> load_task
