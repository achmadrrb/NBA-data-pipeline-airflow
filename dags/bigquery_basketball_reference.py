"""
An operations workflow to load data from basketball-reference website to BigQuery.
"""

from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from basketball_reference import scrape_and_clean, upload_to_bigquery
from config import CONFIG

import warnings

warnings.simplefilter(
    action="ignore", category=FutureWarning
)  # stop getting Pandas FutureWarning's

# 'False' DAG is ready for operation; i.e., 'True' DAG runs using no BigQuery requests
TESTING_DAG = False
# Minutes to sleep on an error
ERROR_SLEEP_MIN = 5
# Max number of searches to perform daily
MAX_SEARCHES = 1500
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = CONFIG.airflow.dag_owner_name
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = CONFIG.airflow.alert_email_addresses
START_DATE = airflow.utils.dates.days_ago(1)

default_args = {
    "owner": DAG_OWNER_NAME,
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ALERT_EMAIL_ADDRESSES,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # removing retries to not call insert duplicates into BigQuery
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "basketball-reference_bigquery",
    description="Call basketball-reference API and insert results into Bigquery",
    default_args=default_args,
    schedule_interval="0 13 * * *",
    catchup=False,
    tags=["data-pipeline-dag"],
    max_active_tasks=3,
)

with dag:
    start = DummyOperator(task_id="start", dag=dag)

    player_daily_results = PythonOperator(
        task_id="player_daily_results",
        provide_context=True,
        python_callable=scrape_and_clean,
    )

    load_data = PythonOperator(
        task_id="player_daily_results",
        provide_context=True,
        python_callable=upload_to_bigquery,
    )

    finish = DummyOperator(task_id="finish", dag=dag)

    start >> player_daily_results >> load_data >> finish
