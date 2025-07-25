FROM apache/airflow:2.9.1-python3.12

USER root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags /opt/airflow/dags
COPY . .

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

USER airflow
