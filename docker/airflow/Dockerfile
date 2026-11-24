# Airflow image: orchestration only. It carries the same `accident_risk`
# package as the processing container so DAG tasks and CLI runs cannot drift.
FROM apache/airflow:3.4.0-python3.11

USER airflow

# Constraint file pins transitive dependencies to versions tested with this
# Airflow release, so installing pandas/matplotlib cannot break the scheduler.
ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.11

COPY requirements/processing.txt /tmp/processing.txt
RUN pip install --no-cache-dir --constraint \
        "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
        -r /tmp/processing.txt \
    && pip install --no-cache-dir "apache-airflow-providers-postgres"

ENV PYTHONPATH=/src \
    MPLBACKEND=Agg \
    PYTHONUNBUFFERED=1

USER airflow
