FROM apache/airflow:3.3.1-python3.11

USER root
ARG SPARK_VERSION=3.5.9
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark \
    && rm /tmp/spark.tgz
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.1" \
    "apache-airflow-providers-apache-spark==6.3.4" \
    "confluent-kafka>=2.6,<3"

