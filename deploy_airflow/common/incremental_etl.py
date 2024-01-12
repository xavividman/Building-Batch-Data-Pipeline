import pandas as pd
import yfinance as yf
import numpy as np
import sqlalchemy as sa
import configparser
import psycopg2
from datetime import timedelta

#----------------------IMPORT CREDENTIALS-------------------------
def read_config():
    config = configparser.ConfigParser()
    config.read('/opt/airflow/secrets/config.ini')
    return config

conn_data = read_config()
conn_data = conn_data["PostgreSQL"]

host = conn_data["HOST"]
port = conn_data["PORT"]
database = conn_data["DATABASE"]
user = conn_data["USER"]
password = conn_data["PASSWORD"]
schema = conn_data["SCHEMA"]

#----------------------CHECK DATE-------------------------
def check_date():
    with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f'SELECT datetime from {schema}.stock_prices ORDER BY datetime DESC LIMIT 1')
            last_day_in_stock_prices = cursor.fetchone()

    start_date = last_day_in_stock_prices[0] + timedelta(days=1)
    end_date = last_day_in_stock_prices[0] + timedelta(days=2)
    return [start_date, end_date]

#-------------------------ETL--------------------------
def extract(company_symbol: list, start_date: str, end_date: str):
    df = yf.download(tickers=company_symbol, start=start_date, end=end_date, interval='1h')
    df = df.stack(level=1).reset_index().rename(columns={'level_1':'company'})
    return df

def transform(df: pd.DataFrame):
    df.columns = [i.replace(' ', '_').lower() for i in df.columns]

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].round(2)
    return df

def load(company_symbol: list, company_names: list, df: pd.DataFrame, table_name: str, if_exists: str='append'):

    #------------DATABASE CONNECTION---------------------------------
    from sqlalchemy.engine.url import URL
    # build the sqlalchemy URL
    url = URL.create(
    drivername='postgresql',
    host=host,
    port=port,
    database=database,
    username=user,
    password=password,
    )

    engine = sa.create_engine(url)

    #------------LOAD STOCK PRICES DATA---------------------------------
    df.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False, method='multi')

    #------------LOAD COMPANIES DATA---------------------------------
    with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
        with conn.cursor() as cursor:
            for symbol,name in zip(company_symbol, company_names):
                # Verificar si la compañía ya existe en la tabla
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.companies WHERE symbol = %s", [symbol])

                if cursor.fetchone()[0] == 0:
                    # Insertar la compañía si no existe
                    cursor.execute(f"INSERT INTO {schema}.companies (symbol, name) VALUES (%s, %s);", [symbol, name])
                    conn.commit()


def main():
    company_symbol = ['AAPL', 'AMZN', 'GOOGL', 'MSFT']
    company_names = ['Apple', 'Amazon', 'Google', 'Microsoft'] 
    start_end_days = check_date()

    df = extract(company_symbol, start_end_days[0], start_end_days[1])
    df = transform(df=df)
    load(company_symbol, company_names, df, table_name='stock_prices', if_exists='append')

if __name__=="__main__":
    main()





