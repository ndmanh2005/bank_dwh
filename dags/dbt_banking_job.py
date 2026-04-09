from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'minh_anh',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_banking_dwh_job',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=['dbt', 'postgres', 'banking_dwh'],
) as dag:

    check_dbt_ready = BashOperator(
        task_id='check_dbt_ready',
        bash_command='dbt --version',
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command="cd /opt/airflow/dags/repo/dags/banking_dwh && dbt run --profiles-dir .", 
    )

    check_dbt_ready >> run_dbt_models