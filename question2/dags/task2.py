from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta, datetime
from includes.clickhouse import transfer_clickhouse_to_sqlite
from notification.email_alert import task_fail_alert


default_args = {
    'owner': 'adewunmi',
    'depends_on_past': False,
    'on_failure_callback': task_fail_alert,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="clickhouse_to_sqlite",
    start_date=datetime(2025, 9, 13),
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    params={"dag_owner": "DE Team"},
) as dag:
    
    create_sqlite_table = SqliteOperator(
        task_id="create_table",
        sqlite_conn_id="sqlite_default",
        sql="sql_script/create_table.sql"
    )

    transfer_task = PythonOperator(
        task_id="transfer_clickhouse_to_sqlite",
        python_callable=transfer_clickhouse_to_sqlite
    )

    create_sqlite_table >> transfer_task
