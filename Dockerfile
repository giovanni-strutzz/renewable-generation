FROM apache/airflow:3.2.1-python3.12

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install pytest pytest-mock pymongo requests
RUN pip install --no-cache-dir pyspark==3.5.1
RUN pip install apache-airflow-providers-standard