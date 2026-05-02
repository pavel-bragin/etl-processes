FROM apache/airflow:2.10.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt