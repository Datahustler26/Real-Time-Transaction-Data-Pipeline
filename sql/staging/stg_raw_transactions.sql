-- ============================================
-- Staging: Raw Transactions
-- ============================================
-- Transforms raw ingested data into clean staging table.
-- Handles: deduplication, type casting, null handling,
--          timestamp normalization, and data cleansing.
-- ============================================

-- Step 1: Create staging table if not exists
CREATE TABLE IF NOT EXISTS STAGING.stg_raw_transactions (
    transaction_id      VARCHAR(64)     NOT NULL,
    customer_id         VARCHAR(32)     NOT NULL,
    merchant_id         VARCHAR(32)     NOT NULL,
    amount              DECIMAL(18, 2)  NOT NULL,
    currency            VARCHAR(3)      NOT NULL,
    transaction_timestamp TIMESTAMP_NTZ NOT NULL,
    status              VARCHAR(20)     NOT NULL,
    channel             VARCHAR(20)     NOT NULL,
    card_type           VARCHAR(20),
    card_last_four      VARCHAR(4),
    merchant_category   VARCHAR(50),
    merchant_city       VARCHAR(100),
    merchant_country    VARCHAR(3),
    is_international    BOOLEAN,
    response_code       VARCHAR(10),
    auth_code           VARCHAR(20),
    _staged_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _batch_id           VARCHAR(64),
    _source_file        VARCHAR(256),
    PRIMARY KEY (transaction_id)
);

-- Step 2: Merge raw data into staging with deduplication
MERGE INTO STAGING.stg_raw_transactions AS tgt
USING (
    -- Deduplicate raw data: keep latest record per transaction_id
    SELECT * FROM (
        SELECT
            TRIM(transaction_id)                            AS transaction_id,
            TRIM(customer_id)                               AS customer_id,
            TRIM(merchant_id)                               AS merchant_id,
            TRY_CAST(amount AS DECIMAL(18, 2))              AS amount,
            UPPER(TRIM(currency))                           AS currency,
            TRY_TO_TIMESTAMP_NTZ(transaction_timestamp)     AS transaction_timestamp,
            LOWER(TRIM(status))                             AS status,
            LOWER(TRIM(channel))                            AS channel,
            TRIM(card_type)                                 AS card_type,
            RIGHT(TRIM(card_number), 4)                     AS card_last_four,
            TRIM(merchant_category)                         AS merchant_category,
            TRIM(merchant_city)                             AS merchant_city,
            UPPER(TRIM(merchant_country))                   AS merchant_country,
            CASE
                WHEN UPPER(TRIM(merchant_country)) != 'US' THEN TRUE
                ELSE FALSE
            END                                             AS is_international,
            TRIM(response_code)                             AS response_code,
            TRIM(auth_code)                                 AS auth_code,
            _loaded_at,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(transaction_id)
                ORDER BY _loaded_at DESC
            ) AS rn
        FROM RAW.raw_transactions
        WHERE _loaded_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP())
          AND transaction_id IS NOT NULL
          AND amount IS NOT NULL
    )
    WHERE rn = 1
) AS src
ON tgt.transaction_id = src.transaction_id
WHEN NOT MATCHED THEN
    INSERT (
        transaction_id, customer_id, merchant_id, amount, currency,
        transaction_timestamp, status, channel, card_type, card_last_four,
        merchant_category, merchant_city, merchant_country, is_international,
        response_code, auth_code, _staged_at, _batch_id
    )
    VALUES (
        src.transaction_id, src.customer_id, src.merchant_id, src.amount,
        src.currency, src.transaction_timestamp, src.status, src.channel,
        src.card_type, src.card_last_four, src.merchant_category,
        src.merchant_city, src.merchant_country, src.is_international,
        src.response_code, src.auth_code, CURRENT_TIMESTAMP(),
        CONCAT('batch_', TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'))
    )
WHEN MATCHED AND src._loaded_at > tgt._staged_at THEN
    UPDATE SET
        customer_id = src.customer_id,
        merchant_id = src.merchant_id,
        amount = src.amount,
        currency = src.currency,
        transaction_timestamp = src.transaction_timestamp,
        status = src.status,
        channel = src.channel,
        card_type = src.card_type,
        card_last_four = src.card_last_four,
        merchant_category = src.merchant_category,
        merchant_city = src.merchant_city,
        merchant_country = src.merchant_country,
        is_international = src.is_international,
        response_code = src.response_code,
        auth_code = src.auth_code,
        _staged_at = CURRENT_TIMESTAMP();
