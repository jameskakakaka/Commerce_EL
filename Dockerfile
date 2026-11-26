# Astro Runtime 11.3.0 = Apache Airflow 2.9 on Python 3.11
FROM quay.io/astronomer/astro-runtime:11.3.0

# dbt gets its own virtualenv so its dependencies never conflict with Airflow's.
# duckdb is pinned to the same version as requirements.txt: DuckDB files are not
# forward-compatible across versions.
RUN python -m venv dbt_venv \
    && dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-duckdb==1.11.1 duckdb==1.5.2

ENV DBT_EXECUTABLE=/usr/local/airflow/dbt_venv/bin/dbt \
    DO_NOT_TRACK=1
