## Data Pipeline with Airflow and AWS

# Overview
This project implements a data pipeline using Apache Airflow for orchestration and AWS services for data processing. The pipeline includes ETL (Extract, Transform, Load) processes for financial data from Yahoo Finance.


**Yahoo Finance ETL:**
- Fetches historical stock prices for specified stock symbols from Yahoo Finance using the yfinance library.
- Refines the data and stores it in separate CSV files for each stock.

The pipeline is orchestrated using Apache Airflow, and the data is stored in AWS S3.

## Prerequisites

- Python (3.6 or later)
- Apache Airflow
- AWS Account with access to S3
- Twitter Developer Account (for Twitter ETL)
- `yfinance` library (for Yahoo Finance ETL)
