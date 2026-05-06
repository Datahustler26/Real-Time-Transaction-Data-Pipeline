# Runbook: Transaction Data Pipeline

## Table of Contents
1. [Service Overview](#service-overview)
2. [Common Alerts & Response](#common-alerts--response)
3. [Troubleshooting Guide](#troubleshooting-guide)
4. [Recovery Procedures](#recovery-procedures)
5. [Escalation Matrix](#escalation-matrix)
6. [Maintenance Procedures](#maintenance-procedures)

---

## Service Overview

| Property | Value |
|----------|-------|
| **Service** | Transaction Data Pipeline |
| **DAG ID** | `transaction_pipeline` |
| **Schedule** | Every 15 minutes |
| **SLA** | < 2 minutes end-to-end |
| **Owner** | Data Engineering Team |
| **Slack Channel** | `#data-pipeline-alerts` |
| **Airflow UI** | http://localhost:8080 (local) |
| **Grafana** | http://localhost:3000 (local) |

---

## Common Alerts & Response

### 🚨 ALERT: Task Failed

**Trigger**: Any task in `transaction_pipeline` DAG fails after 3 retries.

**Response Steps**:
1. Check Slack alert for task ID and error message
2. Open Airflow UI → DAGs → `transaction_pipeline` → Task Instances
3. Click failed task → View Log
4. Identify root cause from error logs
5. If transient (network timeout, lock contention): Clear task to retry
6. If data issue: Check raw data quality, fix upstream
7. If code bug: Fix, deploy, then clear task

**Clear a failed task (Airflow CLI)**:
```bash
airflow tasks clear transaction_pipeline -t <task_id> -s <start_date> -e <end_date>
```

### ⚠️ ALERT: SLA Breach

**Trigger**: Pipeline execution exceeds 2-minute SLA.

**Response Steps**:
1. Check Grafana → Task Duration panel for bottleneck task
2. Check Snowflake query history for slow queries
3. Common causes:
   - **Snowflake warehouse suspended**: Resume warehouse
   - **Large data volume spike**: Check row counts
   - **Lock contention**: Check `SHOW LOCKS` in Snowflake
4. If warehouse issue:
   ```sql
   ALTER WAREHOUSE COMPUTE_WH RESUME;
   ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'MEDIUM';
   ```

### 🔴 ALERT: Data Quality Check Failed

**Trigger**: One or more Great Expectations checks fail.

**Response Steps**:
1. Check `run_quality_checks` task log for specific failed checks
2. Query monitoring table for details:
   ```sql
   SELECT * FROM MONITORING.data_quality_metrics
   WHERE status = 'FAIL'
   ORDER BY metric_timestamp DESC
   LIMIT 20;
   ```
3. Common issues and fixes:
   - **Null transaction IDs**: Check source system, may indicate ingestion failure
   - **Duplicate records**: Check deduplication in staging SQL
   - **Invalid currencies**: New currency added? Update `VALID_CURRENCIES` in config
   - **Future timestamps**: Clock skew in source system
4. If false positive: Adjust threshold in `dags/common/config.py`
5. If real issue: Alert source system team

### 🟡 ALERT: Low Data Volume

**Trigger**: Row count drops below expected threshold (> 20% variance).

**Response Steps**:
1. Verify source systems are sending data
2. Check Lambda CloudWatch logs for ingestion errors
3. Check S3 bucket for new files:
   ```bash
   aws s3 ls s3://transaction-pipeline-raw/raw/transactions/$(date +%Y/%m/%d/) --recursive
   ```
4. If no new files: Escalate to source system team

---

## Troubleshooting Guide

### Snowflake Connection Issues
```sql
-- Check warehouse status
SHOW WAREHOUSES;

-- Check active queries
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE EXECUTION_STATUS = 'RUNNING'
ORDER BY START_TIME DESC;

-- Check locks
SHOW LOCKS IN ACCOUNT;
```

### Airflow Issues
```bash
# Check scheduler health
docker-compose logs airflow-scheduler --tail=50

# Restart scheduler
docker-compose restart airflow-scheduler

# Check DAG parsing errors
docker-compose exec airflow-webserver airflow dags list-import-errors

# Test DAG integrity
docker-compose exec airflow-webserver python -c "from dags.transaction_pipeline import dag; print(f'Tasks: {len(dag.tasks)}')"
```

### Lambda Issues
```bash
# Check recent invocations
aws lambda get-function --function-name transaction-event-ingestion

# Check CloudWatch logs
aws logs tail /aws/lambda/transaction-event-ingestion --since 1h

# Test locally
python -c "
from lambdas.event_ingestion.lambda_function import lambda_handler
result = lambda_handler({'transaction_id': 'TEST_001', 'customer_id': 'C001', 'merchant_id': 'M001', 'amount': 100, 'currency': 'USD', 'timestamp': '2024-01-01T00:00:00'}, None)
print(result)
"
```

---

## Recovery Procedures

### Full Pipeline Rerun
```bash
# Clear all tasks for a specific execution date
airflow dags backfill transaction_pipeline \
  -s 2024-03-15 \
  -e 2024-03-15 \
  --reset-dagruns
```

### Partial Rerun (from specific task)
```bash
# Rerun from staging onwards
airflow tasks clear transaction_pipeline \
  -t stage_transactions \
  -s 2024-03-15 \
  -e 2024-03-15 \
  --downstream
```

### Data Reload (Nuclear Option)
```sql
-- WARNING: Only use if data is corrupted
-- 1. Truncate affected tables
TRUNCATE TABLE MARTS.fct_transactions;

-- 2. Reprocess from staging
-- (Run fct_transactions.sql manually or trigger backfill)

-- 3. Verify counts
SELECT COUNT(*) FROM STAGING.stg_raw_transactions;
SELECT COUNT(*) FROM MARTS.fct_transactions;
```

---

## Escalation Matrix

| Severity | Response Time | Escalation Path |
|----------|--------------|-----------------|
| **P1** (Pipeline down) | 15 min | On-call → Team Lead → VP Engineering |
| **P2** (SLA breach) | 30 min | On-call → Team Lead |
| **P3** (Quality warning) | 2 hours | On-call → Data Quality Team |
| **P4** (Monitoring gap) | Next business day | Ticket in backlog |

---

## Maintenance Procedures

### Weekly
- Review Grafana dashboards for trends
- Check Snowflake credit usage
- Review and purge old raw data (> 90 days)

### Monthly
- Update dependencies (`pip install --upgrade`)
- Review and update data quality thresholds
- Capacity planning review

### Quarterly
- Full DR test (failover + recovery)
- Security audit of credentials rotation
- Performance benchmarking
