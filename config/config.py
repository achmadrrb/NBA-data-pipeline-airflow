"""
A configuration file
"""

from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GCPSettings(BaseSettings):
    """Configuration for Google Cloud Platform connection."""

    credentials_path: str | None = Field(
        default=None,
        description="Path to the Google Cloud service account key file.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


class BigQuerySettings(BaseSettings):
    """Configuration for BigQuery connection and dataset details."""

    project_id: str | None = Field(
        default=None,
        description="Google Cloud project ID for BigQuery.",
    )

    dataset_id: str | None = Field(
        default=None,
        description="BigQuery dataset ID where tables are stored.",
    )

    table_id: str | None = Field(
        default=None,
        description="BigQuery table ID for storing basketball data.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


class GCSSettings(BaseSettings):
    """Configuration for Google Cloud Storage connection and bucket details."""

    bucket_name: str | None = Field(
        default=None,
        description="Google Cloud Storage bucket name.",
    )

    raw_data_path: str = Field(
        default="raw",
        description="Path in GCS bucket for raw data storage.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


class AirflowSettings(BaseSettings):
    """Configuration for Airflow DAG settings."""

    dag_owner_name: str = Field(
        default="airflow",
        description="Owner name for the Airflow DAG.",
    )

    alert_email_addresses: list[str] = Field(
        default=list(),
        description="List of email addresses for alert notifications.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


class Environment(Enum):
    """Enumeration for different environments."""

    DEV = "dev"
    PRODUCTION = "prod"


class Config(BaseSettings):
    bigquery: BigQuerySettings = Field(
        default_factory=BigQuerySettings, description="BigQuery configuration settings."
    )

    gcp: GCPSettings = Field(
        default_factory=GCPSettings,
        description="Google Cloud Platform configuration settings.",
    )

    gcs: GCSSettings = Field(
        default_factory=GCSSettings,
        description="Google Cloud Storage configuration settings.",
    )

    airflow: AirflowSettings = Field(
        default_factory=AirflowSettings,
        description="Airflow configuration settings.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


CONFIG = Config()
