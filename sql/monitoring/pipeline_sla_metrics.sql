-- ============================================
-- Monitoring: Pipeline SLA Metrics
-- ============================================
-- Tracks pipeline execution times, success rates,
-- and SLA compliance for operational dashboards.
-- ============================================

CREATE TABLE IF NOT EXISTS MONITORING.pipeline_sla_metrics (
    metric_id           BIGINT AUTOINCREMENT PRIMARY KEY,
    metric_timestamp    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    dag_id              VARCHAR(100),
    execution_date      TIMESTAMP_NTZ,
    metric_name         VARCHAR(100),
    metric_value        DECIMAL(18, 6),
    unit                VARCHAR(20),
    status              VARCHAR(20),
    details             VARCHAR(1000)
);

-- Pipeline throughput (records per minute)
INSERT INTO MONITORING.pipeline_sla_metrics
    (dag_id, metric_name, metric_value, unit, status, details)
SELECT
    'transaction_pipeline',
    'throughput_records_per_minute',
    COUNT(*) / GREATEST(DATEDIFF('minute',
        MIN(processed_at), MAX(processed_at)), 1)::DECIMAL,
    'records/min',
    CASE
        WHEN COUNT(*) / GREATEST(DATEDIFF('minute',
            MIN(processed_at), MAX(processed_at)), 1) >= 100
        THEN 'HEALTHY'
        ELSE 'DEGRADED'
    END,
    CONCAT('Processed ', COUNT(*), ' records in last 30 min')
FROM MARTS.fct_transactions
WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP());

-- End-to-end latency
INSERT INTO MONITORING.pipeline_sla_metrics
    (dag_id, metric_name, metric_value, unit, status, details)
SELECT
    'transaction_pipeline',
    'e2e_latency_seconds',
    AVG(DATEDIFF('second', transaction_timestamp, processed_at))::DECIMAL,
    'seconds',
    CASE
        WHEN AVG(DATEDIFF('second', transaction_timestamp, processed_at)) <= 120
        THEN 'WITHIN_SLA'
        ELSE 'SLA_BREACH'
    END,
    CONCAT('Avg latency: ',
        ROUND(AVG(DATEDIFF('second', transaction_timestamp, processed_at)), 1), 's | ',
        'P95: ', APPROX_PERCENTILE(DATEDIFF('second', transaction_timestamp, processed_at), 0.95), 's')
FROM MARTS.fct_transactions
WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP());

-- Daily success rate
INSERT INTO MONITORING.pipeline_sla_metrics
    (dag_id, metric_name, metric_value, unit, status, details)
SELECT
    'transaction_pipeline',
    'daily_volume',
    COUNT(*)::DECIMAL,
    'records',
    CASE WHEN COUNT(*) >= 30000 THEN 'ON_TRACK' ELSE 'BELOW_TARGET' END,
    CONCAT('Today: ', COUNT(*), ' records (target: 500K/day)')
FROM MARTS.fct_transactions
WHERE transaction_timestamp::DATE = CURRENT_DATE();

-- Dimension coverage (% of fact records with valid dimension keys)
INSERT INTO MONITORING.pipeline_sla_metrics
    (dag_id, metric_name, metric_value, unit, status, details)
SELECT
    'transaction_pipeline',
    'dimension_coverage_pct',
    (SUM(CASE WHEN customer_key != -1 AND merchant_key != -1 THEN 1 ELSE 0 END)::DECIMAL
     / NULLIF(COUNT(*), 0)) * 100,
    'percent',
    CASE
        WHEN (SUM(CASE WHEN customer_key != -1 AND merchant_key != -1 THEN 1 ELSE 0 END)::DECIMAL
              / NULLIF(COUNT(*), 0)) >= 0.99
        THEN 'PASS'
        ELSE 'WARNING'
    END,
    CONCAT('Orphan records: ',
        SUM(CASE WHEN customer_key = -1 OR merchant_key = -1 THEN 1 ELSE 0 END))
FROM MARTS.fct_transactions
WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP());
