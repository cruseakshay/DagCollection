from datetime import datetime
from airflow import DAG
from airflow.models import Param
from airflow.operators.bash import BashOperator

with DAG('debugging', schedule=None, start_date=datetime(2025, 10, 1), params={"cmd": Param(default="echo hello")}) as dag:
    BashOperator(
        task_id='task',
        bash_command="{{ params.cmd }}"
    )
