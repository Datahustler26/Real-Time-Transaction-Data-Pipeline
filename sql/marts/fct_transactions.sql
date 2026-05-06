-- ============================================
-- Fact Table: Transactions (Star Schema)
-- ============================================
-- Loads fact records with surrogate key lookups from
-- dimension tables. Handles late-arriving facts and
-- orphan records (unknown dimension members).
-- ============================================

CREATE TABLE IF NOT EXISTS MARTS.fct_transactions (
    transaction_key         BIGINT AUTOINCREMENT PRIMARY KEY,
    transaction_id          VARCHAR(64)     NOT NULL UNIQUE,
    customer_key            BIGINT          NOT NULL,
    merchant_key            BIGINT          NOT NULL,
    transaction_date_key    INT             NOT NULL,
    amount                  DECIMAL(18, 2)  NOT NULL,
    currency                VARCHAR(3)      NOT NULL,
    amount_usd              DECIMAL(18, 2),
    status                  VARCHAR(20)     NOT NULL,
    channel                 VARCHAR(20)     NOT NULL,
    is_international        BOOLEAN,
    card_type               VARCHAR(20),
    response_code           VARCHAR(10),
    auth_code               VARCHAR(20),
    transaction_timestamp   TIMESTAMP_NTZ   NOT NULL,
    processed_at            TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _batch_id               VARCHAR(64)
)
CLUSTER BY (transaction_date_key, customer_key);

-- Ensure "unknown" dimension members exist for orphan handling
MERGE INTO MARTS.dim_customer AS tgt
USING (SELECT -1 AS customer_key, 'UNKNOWN' AS customer_id, 'Unknown' AS first_name) AS src
ON tgt.customer_key = src.customer_key
WHEN NOT MATCHED THEN INSERT (customer_key, customer_id, first_name, is_current)
    VALUES (-1, 'UNKNOWN', 'Unknown', TRUE);

MERGE INTO MARTS.dim_merchant AS tgt
USING (SELECT -1 AS merchant_key, 'UNKNOWN' AS merchant_id, 'Unknown' AS merchant_name) AS src
ON tgt.merchant_key = src.merchant_key
WHEN NOT MATCHED THEN INSERT (merchant_key, merchant_id, merchant_name)
    VALUES (-1, 'UNKNOWN', 'Unknown');

-- Load fact records with dimension key lookups
INSERT INTO MARTS.fct_transactions (
    transaction_id, customer_key, merchant_key, transaction_date_key,
    amount, currency, amount_usd, status, channel, is_international,
    card_type, response_code, auth_code, transaction_timestamp,
    processed_at, _batch_id
)
SELECT
    stg.transaction_id,
    -- Customer key lookup (default to -1 "unknown" for orphans)
    COALESCE(dc.customer_key, -1)                       AS customer_key,
    -- Merchant key lookup (default to -1 "unknown" for orphans)
    COALESCE(dm.merchant_key, -1)                       AS merchant_key,
    -- Date key as integer YYYYMMDD
    TO_NUMBER(TO_CHAR(stg.transaction_timestamp, 'YYYYMMDD'))
                                                        AS transaction_date_key,
    stg.amount,
    stg.currency,
    -- Convert to USD (simplified — production would use rate table)
    CASE stg.currency
        WHEN 'USD' THEN stg.amount
        WHEN 'EUR' THEN stg.amount * 1.08
        WHEN 'GBP' THEN stg.amount * 1.26
        WHEN 'CAD' THEN stg.amount * 0.74
        WHEN 'AUD' THEN stg.amount * 0.65
        WHEN 'JPY' THEN stg.amount * 0.0067
        WHEN 'CHF' THEN stg.amount * 1.13
        WHEN 'INR' THEN stg.amount * 0.012
        ELSE stg.amount
    END                                                 AS amount_usd,
    stg.status,
    stg.channel,
    stg.is_international,
    stg.card_type,
    stg.response_code,
    stg.auth_code,
    stg.transaction_timestamp,
    CURRENT_TIMESTAMP()                                 AS processed_at,
    stg._batch_id
FROM STAGING.stg_raw_transactions stg
-- Join to current customer dimension record
LEFT JOIN MARTS.dim_customer dc
    ON stg.customer_id = dc.customer_id
    AND dc.is_current = TRUE
-- Join to merchant dimension
LEFT JOIN MARTS.dim_merchant dm
    ON stg.merchant_id = dm.merchant_id
-- Only load records not already in fact table
WHERE NOT EXISTS (
    SELECT 1 FROM MARTS.fct_transactions f
    WHERE f.transaction_id = stg.transaction_id
)
-- Only process recently staged records
AND stg._staged_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP());
