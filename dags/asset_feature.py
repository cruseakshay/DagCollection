from airflow.sdk import DAG, Asset, task
from pendulum import datetime

# Hourly assets
hourly_asset_1 = Asset("s3://bucket/hourly/data1.csv")

# Daily assets
daily_asset_1 = Asset("s3://bucket/daily/report.csv")

with DAG(
    dag_id="hourly_producer",
    schedule="0 * * * *",  # Hourly schedule
    start_date=datetime(2025, 10, 12),
    max_active_runs=1,
    catchup=True,
):

    @task(outlets=[hourly_asset_1])
    def hello_world(**context):
        import time
        time.sleep(5)

        print("Hello World!")
        context["outlet_events"][hourly_asset_1].extra = {    
            "execution_time": str(context["ts"]),
        }

    hello_world()


with DAG(
    dag_id="daily_producer",
    schedule="0 0 * * *",  # Daily schedule
    start_date=datetime(2025, 10, 12),
    max_active_runs=1,
    catchup=True,
):

    @task(outlets=[daily_asset_1])
    def hello_world(**context):
        import time
        time.sleep(5)

        print("Hello World!")
        context["outlet_events"][daily_asset_1].extra = {
            "execution_time": str(context["ts"]),
        }

    hello_world()

# Expectation: This dag runs when either hourly or daily assets are updated meaning 25 runs are possible in a day
with DAG(
    dag_id="mixed_frequency_consumer",
    # hourly and daily assets in the schedule
    schedule=(hourly_asset_1 | daily_asset_1),  # Run when either asset is updated
    catchup=False,
    max_active_runs=1,
):

    @task(inlets=[hourly_asset_1, daily_asset_1])
    def process_data(**context):
        
        import time
        time.sleep(5)

        dag_run = context["dag_run"]
        print(f"Type: {dag_run.run_type}")
        
        if str(dag_run.run_type) == "DagRunType.ASSET_TRIGGERED":    
            triggering_events = dag_run.consumed_asset_events
            print(f"Triggering events count: {len(triggering_events)}")
            for event in triggering_events:
                print(f"source_dag: {event.source_dag_id}, event_time: {event.timestamp}, extra: {event.extra}")
        
        print("Processing data...")

    process_data()
