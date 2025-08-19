"""
A configuration file
"""

from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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


class Environment(Enum):
    """Enumeration for different environments."""

    DEV = "dev"
    PRODUCTION = "prod"


class Config(BaseSettings):
    bigquery: BigQuerySettings = Field(
        default_factory=BigQuerySettings, description="BigQuery configuration settings."
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


CONFIG = Config()
