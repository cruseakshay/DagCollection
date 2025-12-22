from airflow.decorators import dag, task
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def create_dag(dag_id, schedule, default_args):
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Dynamically generated DAG {dag_id}.',
        schedule=schedule,
        start_date=datetime(2025, 11, 13),
        catchup=True,
        tags=['example'],
    )
    def example_dag():
        @task
        def example_task(task_number):
            print(f"Hello World from {dag_id} - example_task_{task_number}")

        previous_task = None
        for task_no in range(1, 6):
            current_task = example_task.override(task_id=f"example_task_{task_no}")(task_no)
            if previous_task:
                previous_task >> current_task
            previous_task = current_task

    return example_dag()

for i in range(1, 21):
    dag_id = f'example_dag_{i}'
    schedule = '@daily'
    globals()[dag_id] = create_dag(dag_id, schedule, default_args)
