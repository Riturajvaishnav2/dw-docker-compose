FROM apache/superset:4.0.2
USER root
RUN pip install --no-cache-dir \
    trino==0.330.0 \
    psycopg2-binary==2.9.9 \
    "cryptography>=42.0.4,<43.0.0" \
    authlib
USER superset
