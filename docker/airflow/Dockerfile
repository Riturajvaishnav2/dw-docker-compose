FROM apache/airflow:2.9.3-python3.11
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps curl unzip \
    && curl https://rclone.org/install.sh | bash \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/airflow/dag_generated_configs \
    && chown -R 50000:0 /opt/airflow/dag_generated_configs \
    && chmod -R 775 /opt/airflow/dag_generated_configs
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 pandas openpyxl xlrd boto3 \
    "openmetadata-ingestion[trino]==1.4.0" \
    "openmetadata-managed-apis==1.4.0" \
    authlib