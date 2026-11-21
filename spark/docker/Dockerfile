FROM apache/spark:3.5.9-python3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends software-properties-common ca-certificates \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3.11 \
    && python3.11 --version \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=/usr/bin/python3.11

USER 185
