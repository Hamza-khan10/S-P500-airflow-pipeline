-- Final Snowflake Table Schema
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
