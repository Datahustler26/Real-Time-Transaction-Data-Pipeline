-- ============================================
-- Monitoring: Data Quality Metrics
-- ============================================
-- Aggregates data quality metrics for Grafana dashboards.
-- Tracks null rates, duplicate rates, freshness, and volume.
-- ============================================

CREATE TABLE IF NOT EXISTS MONITORING.data_quality_metrics (
    metric_id           BIGINT AUTOINCREMENT PRIMARY KEY,
    metric_timestamp    TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    table_name          VARCHAR(100)    NOT NULL,
    metric_name         VARCHAR(100)    NOT NULL,
    metric_value        DECIMAL(18, 6),
    threshold           DECIMAL(18, 6),
    status              VARCHAR(20),    -- PASS, FAIL, WARNING
    details             VARCHAR(1000),
    batch_id            VARCHAR(64)
);

-- Insert latest quality metrics
INSERT INTO MONITORING.data_quality_metrics
    (table_name, metric_name, metric_value, threshold, status, details, batch_id)

-- Row count
SELECT
    'fct_transactions' AS table_name,
    'row_count' AS metric_name,
    COUNT(*)::DECIMAL AS metric_value,
    1000 AS threshold,
    CASE WHEN COUNT(*) >= 1000 THEN 'PASS' ELSE 'WARNING' END AS status,
    CONCAT('Total rows in last 30 min: ', COUNT(*)) AS details,
    MAX(_batch_id) AS batch_id
FROM MARTS.fct_transactions
WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP())

UNION ALL

-- Null rate for amount
SELECT
    'fct_transactions',
    'null_rate_amount',
    (SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*), 0)),
    0.001,
    CASE WHEN (SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*), 0)) <= 0.001
         THEN 'PASS' ELSE 'FAIL' END,
    CONCAT('Null amounts: ', SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END)),
    MAX(_batch_id)
FROM MARTS.fct_transactions
WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP())

UNION ALL

-- Duplicate rate
SELECT
    'fct_transactions',
    'duplicate_rate',
    (COUNT(*) - COUNT(DISTINCT transaction_id))::DECIMAL / NULLIF(COUNT(*), 0),
    0.0,
    CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN 'PASS' ELSE 'FAIL' END,
    CONCAT('Duplicates: ', COUNT(*) - COUNT(DISTINCT transaction_id)),
    MAX(_batch_id)
FROM MARTS.fct_transactions
WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP())

UNION ALL

-- Data freshness (minutes since last record)
SELECT
    'fct_transactions',
    'data_freshness_minutes',
    DATEDIFF('minute', MAX(transaction_timestamp), CURRENT_TIMESTAMP())::DECIMAL,
    60,
    CASE WHEN DATEDIFF('minute', MAX(transaction_timestamp), CURRENT_TIMESTAMP()) <= 60
         THEN 'PASS' ELSE 'WARNING' END,
    CONCAT('Last transaction: ', MAX(transaction_timestamp)),
    MAX(_batch_id)
FROM MARTS.fct_transactions

UNION ALL

-- Average transaction amount
SELECT
    'fct_transactions',
    'avg_amount_usd',
    AVG(amount_usd)::DECIMAL(18, 2),
    NULL,
    'INFO',
    CONCAT('Avg: $', ROUND(AVG(amount_usd), 2), ' | Min: $', MIN(amount_usd), ' | Max: $', MAX(amount_usd)),
    MAX(_batch_id)
FROM MARTS.fct_transactions
WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP());
