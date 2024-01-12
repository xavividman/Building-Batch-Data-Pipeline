#FROM apache/airflow:2.7.0
#
#COPY requirements.txt /opt/airflow
#
#RUN pip install -r /opt/airflow/requirements.txt
#
#COPY --chown=airflow:root ./dags/etl.py /opt/airflow/dags
#
#RUN airflow db init && \
#airflow users create --username admin --password admin \
#--firstname xavi --lastname vidman \
#--role Admin --email admin@example.org


#ENTRYPOINT airflow scheduler & airflow webserver


FROM apache/airflow:2.7.0

USER root
RUN apt-get update

USER airflow

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .