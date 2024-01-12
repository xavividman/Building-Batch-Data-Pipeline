import pandas as pd
import yfinance as yf
import numpy as np
import sqlalchemy as sa
import configparser
import redshift_connector

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.incremental_etl import check_date
from common.incremental_etl import extract
from common.incremental_etl import transform
from common.incremental_etl import load


#----------------------IMPORT CREDENTIALS-------------------------
def read_config():
    config = configparser.ConfigParser()
    config.read('/opt/airflow/secrets/config.ini')
    return config

conn_data = read_config()
conn_data = conn_data["Redshift"]

host = conn_data["HOST"]
port = conn_data["PORT"]
database = conn_data["DATABASE"]
user = conn_data["USER"]
password = conn_data["PASSWORD"]

schema = f"{user}_schema"

#----------------------PARAMETERS-------------------------
company_symbol = ['AAPL', 'AMZN', 'MSFT']
company_names = ['Apple', 'Amazon', 'Microsoft']

#------------TASK FUNCTIONS---------------------------------
def check_date_func(**kwargs):
    start_end_days = check_date()
    kwargs['ti'].xcom_push(key='check_date', value=start_end_days)


def extract_func(**kwargs):
    ti = kwargs['ti']
    start_end_days = ti.xcom_pull(key='check_date', task_ids='check_date')

    df = extract(company_symbol, start_end_days[0], start_end_days[1])
    kwargs['ti'].xcom_push(key='extract', value=df.to_json())


def transform_func(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='extract', task_ids='extract')
    df = pd.read_json(df_json)

    df = transform(df)
    kwargs['ti'].xcom_push(key='transform', value=df.to_json())

def load_func(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='transform', task_ids='transform')

    df = pd.read_json(df_json)
    load(company_symbol, company_names, df, table_name='stock_prices', if_exists='append')


default_args={
    'owner': 'Xavier Vidman',
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'provide_context': True
}


with DAG(
    default_args = default_args,
    dag_id = 'id_dag_many_tasks',
    description = 'Dag para el ETL de AAPL',
    start_date = datetime.utcnow(),
    schedule_interval = '@hourly'
    ) as dag:

    check_date_task = PythonOperator(
        task_id = 'check_date',
        python_callable = check_date_func,
        provide_context = True
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable = extract_func,
        provide_context = True
    )

    transform_task = PythonOperator(
        task_id = 'transform',
        python_callable = transform_func,
        provide_context = True
    )

    load_task = PythonOperator(
        task_id = 'load',
        python_callable = load_func,
        provide_context = True
    )

    check_date_task >> extract_task >> transform_task >> load_task
