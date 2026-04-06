FROM apache/airflow:2.8.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends git libpq-dev \
  && apt-get clean   \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir dbt-postgres==1.7.0

# Chỉ cần 1 lệnh COPY này là mang được cả code Airflow và dbt vào trong container
COPY ./dags /opt/airflow/dags

# Thiết lập đường dẫn trỏ vào đúng thư mục dbt nằm trong dags
ENV DBT_PROFILES_DIR=/opt/airflow/dags/banking_dwh