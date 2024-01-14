import psycopg2
import configparser


def read_config():
    config = configparser.ConfigParser()
    config.read('../config.ini')
    return config

conn_data = read_config()
conn_data = conn_data["PostgreSQL"]

host = conn_data["HOST"]
port = conn_data["PORT"]
database = conn_data["DATABASE"]
user = conn_data["USER"]
password = conn_data["PASSWORD"]
schema = conn_data["SCHEMA"]

#----------------------CREATE SCHEMA-----------------------
def create_schema():
    with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS market_schema;")


def create_companies_table():
    query = f"""
    CREATE TABLE {schema}.companies(
    id_company      SERIAL NOT NULL PRIMARY KEY,
    symbol          VARCHAR(10),
    name            VARCHAR(100)
    );
    """

    with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.companies;")
            cursor.execute(query)


def create_stock_prices_table():
    query = f"""
    CREATE TABLE {schema}.stock_prices(
    id_stock_price  SERIAL NOT NULL PRIMARY KEY,
    datetime        TIMESTAMP,
    company         VARCHAR(10),
    adj_close       DECIMAL(10,2),
    close           DECIMAL(10,2),
    high            DECIMAL(10,2),
    low             DECIMAL(10,2),
    "open"          DECIMAL(10,2),
    volume          INT
    );
    """

    with psycopg2.connect(host=host, port=port, user=user, password=password, database=database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.stock_prices;")
            cursor.execute(query)


def main():
    create_schema()
    create_companies_table()
    create_stock_prices_table()

if __name__=="__main__":
    main()