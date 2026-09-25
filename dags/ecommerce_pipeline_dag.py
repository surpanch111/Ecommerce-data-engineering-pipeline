from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_daily_pipeline',
    default_args=default_args,
    description='A simple ELT pipeline orchestrating dlt and dbt sequentially',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ecommerce', 'dlt', 'dbt'],
) as dag:

    # Task 1: Run Ingestion using dlt
    # We navigate to the root directory where the python script exists
    run_ingestion = BashOperator(
        task_id='dlt_extract_and_load',
        bash_command='source /Users/mac/de_projects/ecommerce-pipeline/venv/bin/activate && cd /Users/mac/de_projects/ecommerce-pipeline && python ingestion/pipeline.py',
    )

    # Task 2: Run Transformation using dbt
    # We navigate into the transform/ folder where the dbt configurations live
    run_transform = BashOperator(
        task_id='dbt_build_medallion',
        bash_command='source /Users/mac/de_projects/ecommerce-pipeline/venv/bin/activate && cd /Users/mac/de_projects/ecommerce-pipeline/transform && dbt build',
    )

    # Set standard sequential dependencies: Extract/Load MUST finish before Transformations start
    run_ingestion >> run_transform
