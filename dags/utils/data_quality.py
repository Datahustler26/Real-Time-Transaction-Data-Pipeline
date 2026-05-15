"""
Data Quality Validator
======================
15+ validation checks for transaction data quality.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from common.config import PipelineConfig

logger = logging.getLogger(__name__)
CONFIG = PipelineConfig()


class DataQualityValidator:
    """Runs data quality checks and returns structured results."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or CONFIG
        self.results: List[Dict[str, Any]] = []

    def run_all_checks(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Execute all 15 data quality checks."""
        self.results = []
        if df is None or df.empty:
            return self.results

        self._check_not_null(df, "transaction_id", "Check 1: Transaction ID not null")
        self._check_unique(df, "transaction_id", "Check 2: Transaction ID unique")
        self._check_range(df, "amount", 0.01, 999999.99, "Check 3: Amount > 0")
        self._check_amount_max(df, "amount", 999999.99, "Check 4: Amount < 1M")
        self._check_values_in_set(df, "currency", self.config.VALID_CURRENCIES, "Check 5: Valid currency")
        self._check_values_in_set(df, "status", self.config.VALID_STATUSES, "Check 6: Valid status")
        self._check_no_future_timestamps(df, "transaction_timestamp", "Check 7: No future timestamps")
        self._check_not_null(df, "customer_id", "Check 8: Customer ID exists")
        self._check_not_null(df, "merchant_id", "Check 9: Merchant ID exists")
        self._check_no_duplicate_rows(df, "Check 10: No duplicate rows")
        self._check_schema_compliance(df, "Check 11: Schema compliance")
        self._check_volume_variance(df, 100, "Check 12: Row count in range")
        self._check_data_freshness(df, "transaction_timestamp", 1, "Check 13: Data freshness")
        self._check_values_in_set(df, "channel", self.config.VALID_CHANNELS, "Check 14: Valid channel")
        self._check_card_number_masked(df, "Check 15: Card number masked")

        passed = sum(1 for r in self.results if r["success"])
        failed = len(self.results) - passed
        logger.info(f"Quality checks: {passed} passed, {failed} failed")
        return self.results

    def _add_result(self, name, check_type, success, details, value, threshold):
        self.results.append({
            "check_name": name, "check_type": check_type,
            "success": success, "details": details,
            "metric_value": value, "threshold": threshold,
        })

    def _check_not_null(self, df, column, name):
        if column not in df.columns:
            self._add_result(name, "completeness", False, f"Column '{column}' missing", 0, 1)
            return
        null_rate = df[column].isnull().sum() / len(df) if len(df) > 0 else 0
        self._add_result(name, "completeness", null_rate <= 0.001,
                         f"Null rate: {null_rate:.4%}", 1 - null_rate, 0.999)

    def _check_unique(self, df, column, name):
        if column not in df.columns:
            self._add_result(name, "uniqueness", False, f"Column '{column}' missing", 0, 1)
            return
        dups = len(df) - df[column].nunique()
        self._add_result(name, "uniqueness", dups == 0, f"{dups} duplicates", 1 - dups / max(len(df), 1), 1.0)

    def _check_no_duplicate_rows(self, df, name):
        dups = df.duplicated().sum()
        self._add_result(name, "uniqueness", dups == 0, f"{dups} duplicate rows", 1 - dups / max(len(df), 1), 1.0)

    def _check_range(self, df, column, min_v, max_v, name):
        if column not in df.columns:
            self._add_result(name, "accuracy", False, f"Column '{column}' missing", 0, 0.999)
            return
        bad = ((df[column] < min_v) | (df[column] > max_v)).sum()
        rate = 1 - bad / max(len(df), 1)
        self._add_result(name, "accuracy", rate >= 0.999, f"{bad} out of range", rate, 0.999)

    def _check_amount_max(self, df, column, max_v, name):
        if column not in df.columns:
            self._add_result(name, "accuracy", False, f"Column '{column}' missing", 0, 0.9999)
            return
        exceeds = (df[column] > max_v).sum()
        rate = 1 - exceeds / max(len(df), 1)
        self._add_result(name, "accuracy", rate >= 0.9999, f"{exceeds} exceed max", rate, 0.9999)

    def _check_values_in_set(self, df, column, valid, name):
        if column not in df.columns:
            self._add_result(name, "referential", False, f"Column '{column}' missing", 0, 1)
            return
        vals = df[column].dropna().astype(str).str.lower()
        valid_l = [v.lower() for v in valid]
        bad = (~vals.isin(valid_l)).sum()
        rate = 1 - bad / max(len(vals), 1)
        self._add_result(name, "referential", rate >= 0.995, f"{bad} invalid values", rate, 0.995)

    def _check_no_future_timestamps(self, df, column, name):
        if column not in df.columns:
            self._add_result(name, "timeliness", False, f"Column '{column}' missing", 0, 1)
            return
        ts = pd.to_datetime(df[column], errors="coerce")
        future = (ts > datetime.utcnow()).sum()
        self._add_result(name, "timeliness", future == 0, f"{future} future timestamps", 1 if future == 0 else 0, 1.0)

    def _check_data_freshness(self, df, column, max_hours, name):
        if column not in df.columns:
            self._add_result(name, "timeliness", False, f"Column '{column}' missing", 0, 0.99)
            return
        ts = pd.to_datetime(df[column], errors="coerce")
        max_ts = ts.max()
        if pd.isna(max_ts):
            self._add_result(name, "timeliness", False, "No valid timestamps", 0, 0.99)
            return
        age_h = (datetime.utcnow() - max_ts).total_seconds() / 3600
        self._add_result(name, "timeliness", age_h <= max_hours, f"Age: {age_h:.2f}h", 1 if age_h <= max_hours else 0, 0.99)

    def _check_schema_compliance(self, df, name):
        required = {"transaction_id", "customer_id", "merchant_id", "amount", "currency", "status", "transaction_timestamp"}
        missing = required - set(df.columns)
        self._add_result(name, "schema", len(missing) == 0,
                         f"Missing: {missing}" if missing else "All columns present",
                         1 if not missing else len(set(df.columns) & required) / len(required), 1.0)

    def _check_volume_variance(self, df, expected_min, name):
        total = len(df)
        self._add_result(name, "volume", total >= expected_min, f"Rows: {total} (min: {expected_min})", float(total), float(expected_min))

    def _check_card_number_masked(self, df, name):
        card_cols = [c for c in df.columns if "card" in c.lower() and "number" in c.lower()]
        if not card_cols:
            self._add_result(name, "security", True, "No card column — N/A", 1, 1)
            return
        for col in card_cols:
            unmasked = df[col].dropna().apply(
                lambda x: len(str(x).replace("-", "").replace(" ", "")) >= 13
                and str(x).replace("-", "").replace(" ", "").isdigit()
            ).sum()
            self._add_result(name, "security", unmasked == 0, f"{unmasked} unmasked in '{col}'", 1 if unmasked == 0 else 0, 1)
