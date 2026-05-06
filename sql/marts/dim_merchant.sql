-- ============================================
-- Dimension: Merchant
-- ============================================
-- Merchant dimension with category hierarchy,
-- geographic attributes, and MCC codes.
-- ============================================

CREATE TABLE IF NOT EXISTS MARTS.dim_merchant (
    merchant_key        BIGINT AUTOINCREMENT PRIMARY KEY,
    merchant_id         VARCHAR(32)     NOT NULL UNIQUE,
    merchant_name       VARCHAR(256)    NOT NULL,
    merchant_category   VARCHAR(50),
    category_group      VARCHAR(50),
    mcc_code            VARCHAR(10),
    merchant_city       VARCHAR(100),
    merchant_state      VARCHAR(50),
    merchant_country    VARCHAR(3),
    merchant_region     VARCHAR(50),
    is_online           BOOLEAN         DEFAULT FALSE,
    risk_score          DECIMAL(5, 2)   DEFAULT 0,
    first_seen_date     DATE            DEFAULT CURRENT_DATE(),
    last_transaction_date DATE,
    total_transaction_count BIGINT      DEFAULT 0,
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Merge merchants from staging data
MERGE INTO MARTS.dim_merchant AS tgt
USING (
    SELECT DISTINCT
        merchant_id,
        FIRST_VALUE(merchant_category) OVER (
            PARTITION BY merchant_id ORDER BY _staged_at DESC
        ) AS merchant_category,
        FIRST_VALUE(merchant_city) OVER (
            PARTITION BY merchant_id ORDER BY _staged_at DESC
        ) AS merchant_city,
        FIRST_VALUE(merchant_country) OVER (
            PARTITION BY merchant_id ORDER BY _staged_at DESC
        ) AS merchant_country,
        MIN(transaction_timestamp) OVER (PARTITION BY merchant_id) AS first_seen,
        MAX(transaction_timestamp) OVER (PARTITION BY merchant_id) AS last_seen,
        COUNT(*) OVER (PARTITION BY merchant_id) AS txn_count
    FROM STAGING.stg_raw_transactions
    WHERE merchant_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY _staged_at DESC) = 1
) AS src
ON tgt.merchant_id = src.merchant_id

WHEN MATCHED THEN UPDATE SET
    merchant_category = COALESCE(src.merchant_category, tgt.merchant_category),
    merchant_city = COALESCE(src.merchant_city, tgt.merchant_city),
    merchant_country = COALESCE(src.merchant_country, tgt.merchant_country),
    last_transaction_date = GREATEST(tgt.last_transaction_date, src.last_seen::DATE),
    total_transaction_count = tgt.total_transaction_count + src.txn_count,
    _loaded_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    merchant_id, merchant_name, merchant_category,
    category_group, merchant_city, merchant_country,
    merchant_region, is_online, first_seen_date,
    last_transaction_date, total_transaction_count
)
VALUES (
    src.merchant_id,
    src.merchant_id,  -- Name defaults to ID; enriched later
    src.merchant_category,
    -- Category grouping
    CASE
        WHEN src.merchant_category IN ('grocery', 'supermarket', 'food') THEN 'Grocery & Food'
        WHEN src.merchant_category IN ('gas', 'fuel', 'automotive') THEN 'Transportation'
        WHEN src.merchant_category IN ('restaurant', 'dining', 'cafe') THEN 'Dining'
        WHEN src.merchant_category IN ('retail', 'shopping', 'clothing') THEN 'Retail'
        WHEN src.merchant_category IN ('travel', 'hotel', 'airline') THEN 'Travel'
        WHEN src.merchant_category IN ('entertainment', 'streaming', 'gaming') THEN 'Entertainment'
        WHEN src.merchant_category IN ('healthcare', 'pharmacy', 'medical') THEN 'Healthcare'
        WHEN src.merchant_category IN ('utilities', 'telecom', 'internet') THEN 'Utilities'
        ELSE 'Other'
    END,
    src.merchant_city,
    src.merchant_country,
    CASE src.merchant_country
        WHEN 'US' THEN 'North America'
        WHEN 'CA' THEN 'North America'
        WHEN 'GB' THEN 'Europe'
        WHEN 'DE' THEN 'Europe'
        WHEN 'FR' THEN 'Europe'
        WHEN 'JP' THEN 'Asia Pacific'
        WHEN 'AU' THEN 'Asia Pacific'
        WHEN 'IN' THEN 'Asia Pacific'
        ELSE 'Other'
    END,
    CASE WHEN src.merchant_category IN ('online', 'streaming', 'digital') THEN TRUE ELSE FALSE END,
    src.first_seen::DATE,
    src.last_seen::DATE,
    src.txn_count
);
