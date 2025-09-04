"""
An operations workflow to load data from basketball-reference website to BigQuery.
"""

from datetime import datetime, timedelta, timezone
import pandas as pd
from io import StringIO
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSToBigQueryOperator
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
PROJECT_ID = CONFIG.bigquery.project_id
DATASET_ID = CONFIG.bigquery.dataset_id
TABLE_ID = CONFIG.bigquery.table_id


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

    return f"gs://{BUCKET_NAME}/raw/games/{date}/games_{date}.csv"


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
    load_raw_stats = GCSToBigQueryOperator(
        task_id="load_raw_stats",
        bucket="nba-data-pipeline",
        source_objects=["raw/games/{ ds }/games_{ ds }.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        schema_fields=[
            {"name": "rk", "type": "STRING", "mode": "NULLABLE"},
            {"name": "player", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tm", "type": "STRING", "mode": "NULLABLE"},
            {"name": "unnamed_3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "opp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "unnamed_5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "mp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "fg", "type": "STRING", "mode": "NULLABLE"},
            {"name": "fga", "type": "STRING", "mode": "NULLABLE"},
            {"name": "fg_percent", "type": "STRING", "mode": "NULLABLE"},
            {"name": "3p", "type": "STRING", "mode": "NULLABLE"},
            {"name": "3pa", "type": "STRING", "mode": "NULLABLE"},
            {"name": "3p_percent", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ft", "type": "STRING", "mode": "NULLABLE"},
            {"name": "fta", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ft_percent", "type": "STRING", "mode": "NULLABLE"},
            {"name": "orb", "type": "STRING", "mode": "NULLABLE"},
            {"name": "drb", "type": "STRING", "mode": "NULLABLE"},
            {"name": "trb", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ast", "type": "STRING", "mode": "NULLABLE"},
            {"name": "stl", "type": "STRING", "mode": "NULLABLE"},
            {"name": "blk", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tov", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pf", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pts", "type": "STRING", "mode": "NULLABLE"},
            {"name": "+/-", "type": "STRING", "mode": "NULLABLE"},
            {"name": "game_score", "type": "STRING", "mode": "NULLABLE"},
        ],
        skip_leading_rows=1,  # ignore header row
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
    )

    finish = DummyOperator(task_id="finish", dag=dag)

    start >> games_df >> gcs_path >> load_raw_stats >> finish
