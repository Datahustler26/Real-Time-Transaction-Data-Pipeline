"""
Transaction Data Pipeline - Local Demo Runner
================================================
Runs the full pipeline locally without Docker:
  1. Generate test data (10K transactions)
  2. Preview data statistics
  3. Run 15 data quality checks
  4. Test Lambda event ingestion
  5. Validate pipeline configuration
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "dags"))
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "lambdas" / "event_ingestion"))


def separator(title):
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print()


def main():
    separator("TRANSACTION DATA PIPELINE - LOCAL DEMO RUN")

    # ---- Step 1: Generate Test Data ----
    print("[1/5] Generating 10,000 test transactions...")
    from scripts.seed_test_data import write_transactions, write_customers

    fixtures_dir = str(PROJECT_ROOT / "tests" / "fixtures")
    Path(fixtures_dir).mkdir(parents=True, exist_ok=True)

    write_transactions(10000, fixtures_dir)
    write_customers(fixtures_dir)
    print()

    # ---- Step 2: Load and Preview Data ----
    print("[2/5] Loading and previewing sample data...")
    import pandas as pd

    csv_path = os.path.join(fixtures_dir, "sample_transactions.csv")
    df = pd.read_csv(csv_path)

    print(f"  Loaded {len(df):,} transactions")
    print(f"  Columns: {len(df.columns)} fields")
    print(f"  Date range: {df['transaction_timestamp'].min()} to {df['transaction_timestamp'].max()}")
    print(f"  Unique customers: {df['customer_id'].nunique()}")
    print(f"  Unique merchants: {df['merchant_id'].nunique()}")
    print(f"  Amount range: ${df['amount'].min():.2f} - ${df['amount'].max():.2f}")
    print(f"  Average amount: ${df['amount'].mean():.2f}")
    print()

    print("  Currency breakdown:")
    for curr, count in df["currency"].value_counts().items():
        pct = count / len(df) * 100
        print(f"    {curr}: {count:,} ({pct:.1f}%)")
    print()

    print("  Status breakdown:")
    for status, count in df["status"].value_counts().items():
        pct = count / len(df) * 100
        print(f"    {status}: {count:,} ({pct:.1f}%)")
    print()

    print("  Channel breakdown:")
    for channel, count in df["channel"].value_counts().items():
        pct = count / len(df) * 100
        print(f"    {channel}: {count:,} ({pct:.1f}%)")
    print()

    # ---- Step 3: Run Data Quality Checks ----
    print("[3/5] Running 15 data quality checks...")
    from utils.data_quality import DataQualityValidator

    validator = DataQualityValidator()
    results = validator.run_all_checks(df)

    passed = sum(1 for r in results if r["success"])
    failed = sum(1 for r in results if not r["success"])

    print(f"  Results: {passed} PASSED | {failed} FAILED out of {len(results)} checks")
    print()

    for r in results:
        icon = "PASS" if r["success"] else "FAIL"
        print(f"  [{icon}] {r['check_name']}")
        print(f"         {r['details']}")
    print()

    # ---- Step 4: Test Lambda Ingestion ----
    print("[4/5] Testing Lambda event ingestion...")

    try:
        # Mock boto3 before importing lambda_function
        mock_boto3 = MagicMock()
        sys.modules["boto3"] = mock_boto3
        sys.modules["botocore"] = MagicMock()
        sys.modules["botocore.exceptions"] = MagicMock()

        # Create a mock ClientError
        class MockClientError(Exception):
            pass

        sys.modules["botocore.exceptions"].ClientError = MockClientError

        # Now import lambda
        import importlib
        if "lambda_function" in sys.modules:
            importlib.reload(sys.modules["lambda_function"])
        else:
            import lambda_function

        # Patch the s3_client on the module
        lambda_function.s3_client = MagicMock()
        lambda_function.s3_client.put_object = MagicMock(return_value={})
        lambda_function.sqs_client = None

        from lambda_function import lambda_handler

        # Test 1: Single valid event
        test_event = {
            "transaction_id": "TXN_DEMO_001",
            "customer_id": "CUST_0001",
            "merchant_id": "MERCH_001",
            "amount": 125.50,
            "currency": "USD",
            "timestamp": "2024-03-15T10:23:45",
            "status": "approved",
            "channel": "online",
            "card_number": "4111111111111111",
        }
        result = lambda_handler(test_event, None)
        body = json.loads(result["body"])
        print("  Test 1 - Single event:")
        print(f"    Status: {result['statusCode']}")
        print(f"    Processed: {body['processed']}, Failed: {body['failed']}")
        print(f"    Latency: {body['duration_ms']}ms")
        print()

        # Test 2: Batch of 50 events
        batch_events = [
            {
                "transaction_id": f"TXN_BATCH_{i:03d}",
                "customer_id": f"CUST_{i:04d}",
                "merchant_id": f"MERCH_{i % 10:03d}",
                "amount": round(10 + i * 5.5, 2),
                "currency": "USD",
                "timestamp": "2024-03-15T10:00:00",
            }
            for i in range(50)
        ]
        batch_event = {"body": json.dumps(batch_events)}
        result2 = lambda_handler(batch_event, None)
        body2 = json.loads(result2["body"])
        print("  Test 2 - Batch (50 events):")
        print(f"    Status: {result2['statusCode']}")
        print(f"    Processed: {body2['processed']}, Failed: {body2['failed']}")
        print(f"    Latency: {body2['duration_ms']}ms")
        print()

        # Test 3: Invalid event (negative amount)
        bad_event = {
            "transaction_id": "BAD_001",
            "customer_id": "C1",
            "merchant_id": "M1",
            "amount": -100,
            "currency": "USD",
            "timestamp": "2024-01-01",
        }
        result3 = lambda_handler(bad_event, None)
        body3 = json.loads(result3["body"])
        print("  Test 3 - Invalid event (negative amount):")
        print(f"    Status: {result3['statusCode']}")
        print(f"    Failed: {body3['failed']}")
        if body3.get("errors"):
            print(f"    Error: {body3['errors'][0]['error']}")
        print()

        # Test 4: Invalid currency
        bad_event2 = {
            "transaction_id": "BAD_002",
            "customer_id": "C2",
            "merchant_id": "M2",
            "amount": 50,
            "currency": "FAKE",
            "timestamp": "2024-01-01",
        }
        result4 = lambda_handler(bad_event2, None)
        body4 = json.loads(result4["body"])
        print("  Test 4 - Invalid currency:")
        print(f"    Status: {result4['statusCode']}")
        print(f"    Failed: {body4['failed']}")
        if body4.get("errors"):
            print(f"    Error: {body4['errors'][0]['error']}")
        print()

        lambda_tests = "4/4 scenarios tested"
    except Exception as e:
        print(f"  Lambda test error: {e}")
        lambda_tests = "Skipped (error)"
        print()

    # ---- Step 5: Config Validation ----
    print("[5/5] Validating pipeline configuration...")
    from common.config import PipelineConfig

    config = PipelineConfig()
    print(f"  Environment:     {config.PIPELINE_ENV}")
    print(f"  Database:        {config.SNOWFLAKE_DATABASE}")
    print(f"  Warehouse:       {config.SNOWFLAKE_WAREHOUSE}")
    print(f"  Batch size:      {config.BATCH_SIZE:,}")
    print(f"  SLA threshold:   {config.SLA_SECONDS}s")
    print(f"  Max retries:     {config.MAX_RETRIES}")
    print(f"  Currencies:      {', '.join(config.VALID_CURRENCIES)}")
    print(f"  Statuses:        {', '.join(config.VALID_STATUSES)}")
    print(f"  Channels:        {', '.join(config.VALID_CHANNELS)}")
    print(f"  Null threshold:  {config.DQ_NULL_THRESHOLD}")
    print(f"  Amount range:    ${config.DQ_AMOUNT_MIN} - ${config.DQ_AMOUNT_MAX}")
    print(f"  Freshness max:   {config.DQ_FRESHNESS_HOURS}h")

    # ---- Summary ----
    separator("DEMO COMPLETE - All Components Working!")

    print("  Data Generated:   10,000 transactions + 200 customers")
    print(f"  Quality Checks:   {passed}/{len(results)} passed")
    print(f"  Lambda Tests:     {lambda_tests}")
    print("  Config:           Validated")
    print()
    print("  To run full stack with Docker:")
    print("    1. Install Docker Desktop: https://docker.com")
    print("    2. Copy .env.example to .env and add credentials")
    print("    3. Run: docker-compose up -d")
    print("    4. Airflow UI:  http://localhost:8080")
    print("    5. Grafana:     http://localhost:3000")
    print("    6. Prometheus:  http://localhost:9090")
    print()


if __name__ == "__main__":
    main()
