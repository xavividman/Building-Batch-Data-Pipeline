from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.incremental_etl import main
from common.incremental_etl import read_config
import smtplib


def send(context):
    read_data = read_config()
    credential = read_data["SMTP"]
    gmail_secret = credential["GMAIL_SECRET"]
    email = 'xavividman96@gmail.com'
    print(gmail_secret)
    try:
        x = smtplib.SMTP('smtp.gmail.com', 587)
        x.starttls()

        x.login(email, gmail_secret)
        print(gmail_secret)
        subject = f"Airflow reporte {context['dag']} {context['ds']}"
        body = f"Tarea {context['task_instance_key_str']} ejecutada"
        message = "Subject: {}\n\n{}".format(subject, body)

        x.sendmail(email, email, message)
        print('Exito')
    except Exception as e:
        print(e)
        print("Failure")


default_args={
    'owner': 'Xavier Vidman',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}


with DAG(
    default_args = default_args,
    dag_id = 'id-01-incremental-etl',
    description = 'incremental etl in one task',
    start_date = datetime(2024,1,1,0),
    schedule_interval = '@hourly'
    ) as dag:

    etl_task = PythonOperator(
        task_id = 'task-id-01',
        python_callable = main
    )

    etl_task