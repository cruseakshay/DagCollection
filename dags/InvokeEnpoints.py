from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from datetime import datetime, timedelta
import requests
import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "invoke_endpoints_dag",
    default_args=default_args,
    description="DAG to invoke a list of endpoints",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

endpoints = {
    "task1": "https://some-hash.mockapi.io/api/vi/https_test/dummy",
    "task2": "https://some-hash.mockapi.io/api/vi/https_test/dummy",
    "task3": "https://some-hash.mockapi.io/api/vi/https_test/dummy",
}


def invoke_endpoint(endpoint):
    hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
    conn = hook.get_conn()
    access_token = hook._get_access_token()
    header = {"Authorization": "Bearer " + access_token}
    payload = {}
    try:
        response = requests.get(endpoint, headers=header, data=payload)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        logging.info(f"Endpoint: {endpoint}, Status Code: {response.status_code}")
        print(response.text)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error invoking endpoint: {endpoint}")
        logging.error(str(e))
        raise


def create_endpoint_tasks():
    tasks = []
    for key, endpoint in endpoints.items():
        task = PythonOperator(
            task_id=f"invoke_{key}",
            python_callable=invoke_endpoint,
            op_kwargs={"endpoint": endpoint},
            dag=dag,
        )
        tasks.append(task)
    return tasks


invoke_tasks = create_endpoint_tasks()

# Set task dependencies using a for loop
for i in range(len(invoke_tasks) - 1):
    invoke_tasks[i] >> invoke_tasks[i + 1]
