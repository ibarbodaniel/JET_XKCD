import logging
import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from xkcd_pipelines.fetch_insert import XKCDPipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    "xkcd_pipeline",
    default_args=default_args,
    description="A simple XKCD pipeline with polling logic",
    # Run at 8 AM UTC on Mondays, Wednesdays, and Fridays
    schedule_interval="0 8 * * 1,3,5",
)


def poll_for_new_comic():
    pipeline = XKCDPipeline()
    timeout = 6 * 3600  # Poll for up to 6 hours
    polling_interval = 300  # Check every 5 minutes
    start_time = time.time()

    while time.time() - start_time < timeout:
        latest_comic = pipeline.is_database_up_to_date()
        if latest_comic:
            logging.info("No new comic available. Retrying in 5 minutes.")
            time.sleep(polling_interval)
            return True
        else:
            logging.info("New comic available. Proceeding with the pipeline.")
            pipeline.main()

    logging.warning("Polling timed out. No new comic available for the day.")
    return False


# Task to poll for new comic
poll_for_new_comic_task = PythonOperator(
    task_id="poll_for_new_comic",
    python_callable=poll_for_new_comic,
    dag=dag,
)


run_xkcd_pipeline = PythonOperator(
    task_id="run_xkcd_pipeline",
    python_callable=poll_for_new_comic,
    dag=dag,
)

# Task to run dbt transformations
run_dbt = BashOperator(
    task_id="run_dbt",
    bash_command="cd /Users/danielibarbo/JET_XKCD/xkcd_dbt_project/xkcd && dbt run",
)

# Task to run dbt tests
test_dbt = BashOperator(
    task_id="test_dbt",
    bash_command="cd /Users/danielibarbo/JET_XKCD/xkcd_dbt_project/xkcd && dbt test",
)

poll_for_new_comic_task >> run_xkcd_pipeline >> run_dbt >> test_dbt
