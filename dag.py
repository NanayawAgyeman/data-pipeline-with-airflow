from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from yahoo_finance_etl import run_yahoo_finance_etl

default_args = {
    'owner': 'Nana Yaw Agyeman',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['nyagyeman002@st.ug.edu.gh'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'yahoo_finance_dag',
    default_args=default_args,
    description='Yahoo Finance ETL DAG!',
    schedule_interval=timedelta(days=1),
)

# List of stock symbols to fetch data for
stock_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'FB', 'NVDA', 'V', 'PYPL', 'NFLX']

run_yahoo_finance_tasks = []
for symbol in stock_symbols:
    task_id = f'run_yahoo_finance_etl_{symbol}'
    run_yahoo_finance_task = PythonOperator(
        task_id=task_id,
        python_callable=run_yahoo_finance_etl,
        op_args=[symbol],
        dag=dag,
    )
    run_yahoo_finance_tasks.append(run_yahoo_finance_task)

# Set up task dependencies
run_yahoo_finance_tasks

if __name__ == "__main__":
    dag.cli()
