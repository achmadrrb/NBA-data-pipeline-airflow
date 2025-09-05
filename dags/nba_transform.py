from airflow.decorators import dag
from datetime import datetime, timezone
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import BigQueryServiceAccountProfileMapping
from config import CONFIG

PROJECT_ID = CONFIG.bigquery.project_id
DBT_PROJECT_PATH = "/opt/airflow/nba_dbt"


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 21, 13, 0, tzinfo=timezone.utc),
    catchup=False,
    tags=["nba", "silver-gold"],
)
def nba_transform_pipeline():
    dbt_tg = DbtTaskGroup(
        group_id="dbt_transforms",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="default",
            target_name="prod",
            profile_mapping=BigQueryServiceAccountProfileMapping(
                conn_id="gcp_conn",
                project_id=PROJECT_ID,
                dataset="nba_dw",
            ),
        ),
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/nba_dbt"),
    )

    dbt_tg


nba_transform_pipeline = nba_transform_pipeline()
