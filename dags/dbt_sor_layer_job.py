from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Cấu hình mặc định
default_args = {
    'owner': 'minh_anh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_sor_layer_only',  # ID này phải là duy nhất để xuất hiện riêng biệt trên UI
    default_args=default_args,
    description='Chạy riêng biệt tầng SOR: Run và Test lưu lỗi',
    schedule_interval='15 2 * * *', # Lịch chạy riêng của sếp
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['sor', 'dbt_banking'],
) as dag:

    # 1. Tác vụ Chạy các model có tag "sor"
    run_sor_tag = BashOperator(
        task_id='run_dbt_sor_models',
        bash_command=(
            "cp -r /opt/airflow/dags/repo/dags/banking_dwh /tmp/ && "
            "cd /tmp/banking_dwh && "
            "dbt run --select tag:sor --profiles-dir ."
        )
    )

    # 2. Tác vụ Test các model có tag "sor" và lưu kết quả lỗi
    test_sor_tag = BashOperator(
        task_id='test_dbt_sor_models',
        bash_command=(
            "cp -r /opt/airflow/dags/repo/dags/banking_dwh /tmp/ && "
            "cd /tmp/banking_dwh && "
            "dbt test --select tag:sor --store-failures --profiles-dir ."
        )
    )

    # Thứ tự thực thi
    run_sor_tag >> test_sor_tag