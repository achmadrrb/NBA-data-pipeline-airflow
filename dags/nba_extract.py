"""
An operations workflow to load data from basketball-reference website to BigQuery.
"""

from datetime import datetime, timedelta, timezone
import pandas as pd
from io import StringIO
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import storage
from basketball_reference import scrape_games
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
START_DATE = datetime(2025, 10, 21, 13, 0, tzinfo=timezone.utc)

BUCKET_NAME = CONFIG.gcs.bucket_name


@task
def extract_games(date: str) -> pd.DataFrame:
    """
    Scrape NBA games for given date, save as CSV locally.
    Returns DataFrame.
    """
    df = scrape_games(date)
    if df.empty:
        raise ValueError(f"No games found for {date}")
    return df


@task
def upload_df_to_gcs(df: pd.DataFrame, date: str) -> str:
    """
    Upload Pandas DataFrame directly to GCS as CSV and return GCS path.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/games/{date}/games.csv")

    # Convert DataFrame to CSV string in-memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    return f"gs://{BUCKET_NAME}/raw/games/{date}/games.csv"


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
    "nba_extract_to_gcs_v1",
    description="Call basketball-reference API and store raw data in GCS",
    default_args=default_args,
    schedule_interval="0 13 * * *",
    catchup=False,
    tags=["data-pipeline-dag"],
)

with dag:
    start = DummyOperator(task_id="start", dag=dag)

    # Bronze -> Scrape data and store in GCS
    games_df = extract_games("{{ ds }}")
    gcs_path = upload_df_to_gcs(games_df, "{{ ds }}")

    finish = DummyOperator(task_id="finish", dag=dag)

    start >> games_df >> gcs_path >> finish
