"""
Pipeline Configuration
======================
Centralized configuration management for the transaction data pipeline.
All environment-specific settings are loaded from environment variables
with sensible defaults for local development.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class PipelineConfig:
    """
    Central configuration for all pipeline components.

    Environment variables override defaults. Use .env file for local dev
    and Docker/Airflow Variables for production.
    """

    # --- Snowflake Connection ---
    SNOWFLAKE_ACCOUNT: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_ACCOUNT", "your_account")
    )
    SNOWFLAKE_USER: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_USER", "your_user")
    )
    SNOWFLAKE_PASSWORD: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_PASSWORD", "your_password")
    )
    SNOWFLAKE_DATABASE: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_DATABASE", "TRANSACTION_DB")
    )
    SNOWFLAKE_WAREHOUSE: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    )
    SNOWFLAKE_ROLE: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
    )

    # --- Schema Names ---
    RAW_SCHEMA: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
    )
    STAGING_SCHEMA: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_STAGING_SCHEMA", "STAGING")
    )
    MART_SCHEMA: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_MART_SCHEMA", "MARTS")
    )
    MONITORING_SCHEMA: str = field(
        default_factory=lambda: os.getenv("SNOWFLAKE_MONITORING_SCHEMA", "MONITORING")
    )

    # --- Pipeline Settings ---
    PIPELINE_ENV: str = field(
        default_factory=lambda: os.getenv("PIPELINE_ENV", "development")
    )
    BATCH_SIZE: int = field(
        default_factory=lambda: int(os.getenv("PIPELINE_BATCH_SIZE", "10000"))
    )
    SLA_SECONDS: int = field(
        default_factory=lambda: int(os.getenv("PIPELINE_SLA_SECONDS", "120"))
    )
    MAX_RETRIES: int = field(
        default_factory=lambda: int(os.getenv("PIPELINE_MAX_RETRIES", "3"))
    )

    # --- Alerting ---
    SLACK_WEBHOOK_URL: str = field(
        default_factory=lambda: os.getenv("SLACK_WEBHOOK_URL", "")
    )
    ALERT_EMAIL: str = field(
        default_factory=lambda: os.getenv("ALERT_EMAIL", "data-team@company.com")
    )

    # --- AWS ---
    S3_BUCKET: str = field(
        default_factory=lambda: os.getenv("S3_BUCKET_NAME", "transaction-pipeline-raw")
    )
    AWS_REGION: str = field(
        default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )

    # --- Monitoring ---
    PROMETHEUS_PUSHGATEWAY: str = field(
        default_factory=lambda: os.getenv(
            "PROMETHEUS_PUSHGATEWAY_URL", "http://localhost:9091"
        )
    )

    # --- Data Quality Thresholds ---
    DQ_NULL_THRESHOLD: float = 0.001  # Max 0.1% nulls allowed
    DQ_DUPLICATE_THRESHOLD: float = 0.0  # Zero duplicates allowed
    DQ_AMOUNT_MIN: float = 0.01
    DQ_AMOUNT_MAX: float = 999999.99
    DQ_FRESHNESS_HOURS: int = 1  # Data must be < 1 hour old
    DQ_VOLUME_VARIANCE_PCT: float = 0.20  # Max 20% variance from expected

    # --- Valid Enums ---
    VALID_CURRENCIES: List[str] = field(
        default_factory=lambda: ["USD", "EUR", "GBP", "CAD", "AUD", "JPY", "CHF", "INR"]
    )
    VALID_STATUSES: List[str] = field(
        default_factory=lambda: ["approved", "declined", "pending", "reversed", "settled"]
    )
    VALID_CHANNELS: List[str] = field(
        default_factory=lambda: ["online", "in_store", "atm", "mobile", "phone"]
    )

    @property
    def is_production(self) -> bool:
        return self.PIPELINE_ENV == "production"

    @property
    def is_test(self) -> bool:
        return self.PIPELINE_ENV == "test"

    @property
    def snowflake_connection_params(self) -> dict:
        """Return Snowflake connection parameters as a dictionary."""
        return {
            "account": self.SNOWFLAKE_ACCOUNT,
            "user": self.SNOWFLAKE_USER,
            "password": self.SNOWFLAKE_PASSWORD,
            "database": self.SNOWFLAKE_DATABASE,
            "warehouse": self.SNOWFLAKE_WAREHOUSE,
            "role": self.SNOWFLAKE_ROLE,
        }
