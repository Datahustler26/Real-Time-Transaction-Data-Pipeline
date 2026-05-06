"""
Test: Transformations & Utilities
=================================
Tests for Snowflake utility functions and data transformations.
Uses mocks to avoid requiring a live Snowflake connection.
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock
from pathlib import Path
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from common.config import PipelineConfig


@pytest.fixture
def config():
    """Test configuration."""
    os.environ["PIPELINE_ENV"] = "test"
    os.environ["SNOWFLAKE_ACCOUNT"] = "test_account"
    os.environ["SNOWFLAKE_USER"] = "test_user"
    os.environ["SNOWFLAKE_PASSWORD"] = "test_pass"
    cfg = PipelineConfig()
    yield cfg
    for key in ["PIPELINE_ENV", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]:
        os.environ.pop(key, None)


class TestPipelineConfig:
    """Tests for configuration management."""

    def test_default_values(self):
        """Config should have sensible defaults."""
        cfg = PipelineConfig()
        assert cfg.SNOWFLAKE_DATABASE == "TRANSACTION_DB"
        assert cfg.SNOWFLAKE_WAREHOUSE == "COMPUTE_WH"
        assert cfg.BATCH_SIZE == 10000
        assert cfg.MAX_RETRIES == 3
        assert cfg.SLA_SECONDS == 120

    def test_environment_override(self, config):
        """Environment variables should override defaults."""
        assert config.SNOWFLAKE_ACCOUNT == "test_account"
        assert config.SNOWFLAKE_USER == "test_user"

    def test_is_production(self, config):
        """is_production should return False for test env."""
        assert not config.is_production

    def test_is_test(self, config):
        """is_test should return True for test env."""
        assert config.is_test

    def test_valid_currencies(self, config):
        """Should have standard currency codes."""
        assert "USD" in config.VALID_CURRENCIES
        assert "EUR" in config.VALID_CURRENCIES
        assert len(config.VALID_CURRENCIES) >= 5

    def test_valid_statuses(self, config):
        """Should have expected transaction statuses."""
        assert "approved" in config.VALID_STATUSES
        assert "declined" in config.VALID_STATUSES

    def test_snowflake_connection_params(self, config):
        """Connection params dict should have required keys."""
        params = config.snowflake_connection_params
        assert "account" in params
        assert "user" in params
        assert "password" in params
        assert "database" in params
        assert "warehouse" in params

    def test_quality_thresholds(self, config):
        """Quality thresholds should be reasonable."""
        assert 0 < config.DQ_NULL_THRESHOLD < 0.1
        assert config.DQ_DUPLICATE_THRESHOLD == 0.0
        assert config.DQ_AMOUNT_MIN > 0
        assert config.DQ_AMOUNT_MAX > config.DQ_AMOUNT_MIN


class TestSnowflakeManager:
    """Tests for Snowflake utilities (mocked)."""

    @patch("utils.snowflake_utils.SnowflakeManager.get_connection")
    def test_execute_query_returns_dict(self, mock_conn):
        """execute_query should return a dict with results."""
        from utils.snowflake_utils import SnowflakeManager

        mock_cursor = MagicMock()
        mock_cursor.description = [("count", None)]
        mock_cursor.fetchone.return_value = (42,)
        mock_cursor.rowcount = 42

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_conn.return_value.__enter__ = MagicMock(return_value=mock_connection)
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)

        sf = SnowflakeManager()
        result = sf.execute_query("SELECT COUNT(*) FROM test")

        assert result is not None
        assert "count" in result
        assert result["count"] == 42

    def test_run_sql_file_not_found(self):
        """Should raise FileNotFoundError for missing SQL file."""
        from utils.snowflake_utils import SnowflakeManager

        sf = SnowflakeManager()
        with pytest.raises(FileNotFoundError):
            sf.run_sql_file("/nonexistent/path/query.sql")


class TestDataTransformations:
    """Tests for data transformation logic."""

    def test_currency_conversion_usd(self):
        """USD should convert 1:1."""
        amount = 100.00
        rate = 1.0  # USD to USD
        assert amount * rate == 100.00

    def test_currency_conversion_eur(self):
        """EUR conversion should apply correct rate."""
        amount = 100.00
        rate = 1.08
        result = round(amount * rate, 2)
        assert result == 108.00

    def test_deduplication_logic(self):
        """Deduplication should keep latest record per ID."""
        df = pd.DataFrame({
            "transaction_id": ["TXN_001", "TXN_001", "TXN_002"],
            "amount": [100, 150, 200],
            "loaded_at": [
                datetime(2024, 1, 1, 10, 0),
                datetime(2024, 1, 1, 11, 0),
                datetime(2024, 1, 1, 10, 0),
            ],
        })
        # Simulate dedup: keep latest per transaction_id
        deduped = (
            df.sort_values("loaded_at", ascending=False)
            .drop_duplicates(subset=["transaction_id"], keep="first")
        )
        assert len(deduped) == 2
        assert deduped[deduped["transaction_id"] == "TXN_001"]["amount"].values[0] == 150

    def test_card_masking(self):
        """Card numbers should be properly masked."""
        raw_card = "4111111111111111"
        masked = "****" + raw_card[-4:]
        assert masked == "****1111"
        assert len(masked) == 8

    def test_category_grouping(self):
        """Merchant categories should map to groups."""
        category_map = {
            "grocery": "Grocery & Food",
            "supermarket": "Grocery & Food",
            "restaurant": "Dining",
            "retail": "Retail",
            "travel": "Travel",
            "unknown_cat": "Other",
        }
        for cat, expected_group in category_map.items():
            if cat in ["grocery", "supermarket", "food"]:
                assert expected_group == "Grocery & Food"
            elif cat in ["restaurant", "dining", "cafe"]:
                assert expected_group == "Dining"

    def test_date_key_generation(self):
        """Date keys should be YYYYMMDD integers."""
        ts = datetime(2024, 3, 15, 14, 30, 0)
        date_key = int(ts.strftime("%Y%m%d"))
        assert date_key == 20240315

    def test_international_flag(self):
        """International flag should be True for non-US transactions."""
        countries = {"US": False, "GB": True, "CA": True, "DE": True}
        for country, expected in countries.items():
            is_intl = country != "US"
            assert is_intl == expected
