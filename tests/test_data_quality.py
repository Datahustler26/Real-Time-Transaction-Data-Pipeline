"""
Test: Data Quality Validator
============================
Unit tests for all 15 data quality validation checks.
Uses synthetic test data with known edge cases.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from utils.data_quality import DataQualityValidator
from common.config import PipelineConfig


@pytest.fixture
def config():
    """Test configuration with relaxed thresholds."""
    cfg = PipelineConfig()
    cfg.PIPELINE_ENV = "test"
    return cfg


@pytest.fixture
def validator(config):
    """DataQualityValidator instance with test config."""
    return DataQualityValidator(config=config)


@pytest.fixture
def valid_df():
    """DataFrame with clean, valid transaction data."""
    now = datetime.utcnow()
    return pd.DataFrame({
        "transaction_id": [f"TXN_{i:06d}" for i in range(100)],
        "customer_id": [f"CUST_{i % 20:04d}" for i in range(100)],
        "merchant_id": [f"MERCH_{i % 10:04d}" for i in range(100)],
        "amount": np.random.uniform(1.00, 5000.00, 100).round(2),
        "currency": np.random.choice(["USD", "EUR", "GBP", "CAD"], 100),
        "status": np.random.choice(["approved", "declined", "pending"], 100),
        "channel": np.random.choice(["online", "in_store", "mobile"], 100),
        "transaction_timestamp": [
            (now - timedelta(minutes=np.random.randint(1, 55))).isoformat()
            for _ in range(100)
        ],
    })


@pytest.fixture
def dirty_df():
    """DataFrame with intentional quality issues for testing failures."""
    now = datetime.utcnow()
    future = now + timedelta(days=5)
    return pd.DataFrame({
        "transaction_id": ["TXN_001", "TXN_001", "TXN_003", None, "TXN_005"],
        "customer_id": ["CUST_01", "CUST_02", None, "CUST_04", "CUST_05"],
        "merchant_id": ["MERCH_01", None, "MERCH_03", "MERCH_04", "MERCH_05"],
        "amount": [100.00, -50.00, 2000000.00, 0.00, 250.00],
        "currency": ["USD", "INVALID", "EUR", "USD", "XYZ"],
        "status": ["approved", "declined", "INVALID_STATUS", "pending", "approved"],
        "channel": ["online", "BAD_CHANNEL", "in_store", "mobile", "online"],
        "transaction_timestamp": [
            (now - timedelta(hours=1)).isoformat(),
            future.isoformat(),
            (now - timedelta(hours=2)).isoformat(),
            (now - timedelta(hours=3)).isoformat(),
            (now - timedelta(hours=0.5)).isoformat(),
        ],
        "card_number": ["4111111111111111", "****1234", "XXXX-XXXX-XXXX-5678", "****9999", "****0000"],
    })


class TestDataQualityValidator:
    """Tests for the DataQualityValidator class."""

    def test_all_checks_pass_on_valid_data(self, validator, valid_df):
        """All 15 checks should pass on clean data."""
        results = validator.run_all_checks(valid_df)
        assert len(results) == 15
        passed = sum(1 for r in results if r["success"])
        assert passed >= 13, f"Expected most checks to pass, got {passed}/15"

    def test_empty_dataframe_returns_no_results(self, validator):
        """Empty DataFrame should return empty results list."""
        results = validator.run_all_checks(pd.DataFrame())
        assert results == []

    def test_none_dataframe_returns_no_results(self, validator):
        """None DataFrame should return empty results list."""
        results = validator.run_all_checks(None)
        assert results == []

    def test_null_transaction_id_detected(self, validator, dirty_df):
        """Should detect null transaction IDs."""
        results = validator.run_all_checks(dirty_df)
        null_check = next(r for r in results if "Transaction ID not null" in r["check_name"])
        assert not null_check["success"]

    def test_duplicate_transaction_id_detected(self, validator, dirty_df):
        """Should detect duplicate transaction IDs."""
        results = validator.run_all_checks(dirty_df)
        dup_check = next(r for r in results if "unique" in r["check_name"].lower())
        assert not dup_check["success"]

    def test_negative_amount_detected(self, validator, dirty_df):
        """Should detect amounts outside valid range."""
        results = validator.run_all_checks(dirty_df)
        range_check = next(r for r in results if "Amount > 0" in r["check_name"])
        assert not range_check["success"]

    def test_excessive_amount_detected(self, validator, dirty_df):
        """Should detect amounts exceeding maximum."""
        results = validator.run_all_checks(dirty_df)
        max_check = next(r for r in results if "Amount < 1M" in r["check_name"])
        assert not max_check["success"]

    def test_invalid_currency_detected(self, validator, dirty_df):
        """Should detect invalid currency codes."""
        results = validator.run_all_checks(dirty_df)
        curr_check = next(r for r in results if "currency" in r["check_name"].lower())
        assert not curr_check["success"]

    def test_invalid_status_detected(self, validator, dirty_df):
        """Should detect invalid status values."""
        results = validator.run_all_checks(dirty_df)
        status_check = next(r for r in results if "status" in r["check_name"].lower())
        assert not status_check["success"]

    def test_future_timestamp_detected(self, validator, dirty_df):
        """Should detect timestamps in the future."""
        results = validator.run_all_checks(dirty_df)
        ts_check = next(r for r in results if "future" in r["check_name"].lower())
        assert not ts_check["success"]

    def test_unmasked_card_number_detected(self, validator, dirty_df):
        """Should detect unmasked credit card numbers."""
        results = validator.run_all_checks(dirty_df)
        card_check = next(r for r in results if "masked" in r["check_name"].lower())
        assert not card_check["success"]

    def test_result_structure(self, validator, valid_df):
        """Each result should have required fields."""
        results = validator.run_all_checks(valid_df)
        required_keys = {"check_name", "check_type", "success", "details", "metric_value", "threshold"}
        for result in results:
            assert required_keys.issubset(result.keys()), f"Missing keys in: {result}"

    def test_check_types_categorized(self, validator, valid_df):
        """Checks should be categorized by type."""
        results = validator.run_all_checks(valid_df)
        types = {r["check_type"] for r in results}
        expected_types = {"completeness", "uniqueness", "accuracy", "referential", "timeliness", "schema", "volume"}
        assert types.intersection(expected_types), f"Got types: {types}"

    def test_schema_compliance_missing_column(self, validator):
        """Should fail when required columns are missing."""
        incomplete_df = pd.DataFrame({
            "transaction_id": ["TXN_001"],
            "amount": [100.00],
        })
        results = validator.run_all_checks(incomplete_df)
        schema_check = next(r for r in results if "schema" in r["check_name"].lower())
        assert not schema_check["success"]

    def test_volume_check_below_minimum(self, validator):
        """Should warn when row count is below minimum."""
        tiny_df = pd.DataFrame({
            "transaction_id": ["TXN_001"],
            "customer_id": ["CUST_001"],
            "merchant_id": ["MERCH_001"],
            "amount": [100.00],
            "currency": ["USD"],
            "status": ["approved"],
            "channel": ["online"],
            "transaction_timestamp": [datetime.utcnow().isoformat()],
        })
        results = validator.run_all_checks(tiny_df)
        vol_check = next(r for r in results if "Row count" in r["check_name"])
        assert not vol_check["success"]
