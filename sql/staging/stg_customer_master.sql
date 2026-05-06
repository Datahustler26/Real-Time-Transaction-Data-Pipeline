-- ============================================
-- Staging: Customer Master (SCD Type 2)
-- ============================================
-- Merges incoming customer updates into the staging layer
-- with Slowly Changing Dimension Type 2 logic:
-- - New customers → INSERT with is_current = TRUE
-- - Changed customers → Expire old row, INSERT new version
-- - Unchanged customers → No action
-- ============================================

CREATE TABLE IF NOT EXISTS STAGING.stg_customer_master (
    customer_key        BIGINT AUTOINCREMENT PRIMARY KEY,
    customer_id         VARCHAR(32)     NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(256),
    phone               VARCHAR(20),
    address_line_1      VARCHAR(256),
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(3),
    postal_code         VARCHAR(20),
    customer_tier       VARCHAR(20),
    account_open_date   DATE,
    effective_date      DATE            NOT NULL DEFAULT CURRENT_DATE(),
    expiry_date         DATE            NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE,
    _hash_key           VARCHAR(64),
    _updated_at         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Step 1: Identify changed records using hash comparison
CREATE OR REPLACE TEMPORARY TABLE _customer_changes AS
SELECT
    src.customer_id,
    src.first_name,
    src.last_name,
    src.email,
    src.phone,
    src.address_line_1,
    src.city,
    src.state,
    src.country,
    src.postal_code,
    src.customer_tier,
    src.account_open_date,
    MD5(CONCAT_WS('|',
        COALESCE(src.first_name, ''),
        COALESCE(src.last_name, ''),
        COALESCE(src.email, ''),
        COALESCE(src.phone, ''),
        COALESCE(src.address_line_1, ''),
        COALESCE(src.city, ''),
        COALESCE(src.state, ''),
        COALESCE(src.country, ''),
        COALESCE(src.postal_code, ''),
        COALESCE(src.customer_tier, '')
    )) AS new_hash,
    tgt._hash_key AS existing_hash,
    CASE
        WHEN tgt.customer_key IS NULL THEN 'INSERT'
        WHEN tgt._hash_key != MD5(CONCAT_WS('|',
            COALESCE(src.first_name, ''),
            COALESCE(src.last_name, ''),
            COALESCE(src.email, ''),
            COALESCE(src.phone, ''),
            COALESCE(src.address_line_1, ''),
            COALESCE(src.city, ''),
            COALESCE(src.state, ''),
            COALESCE(src.country, ''),
            COALESCE(src.postal_code, ''),
            COALESCE(src.customer_tier, '')
        )) THEN 'UPDATE'
        ELSE 'NO_CHANGE'
    END AS change_type
FROM RAW.raw_customer_master src
LEFT JOIN STAGING.stg_customer_master tgt
    ON src.customer_id = tgt.customer_id
    AND tgt.is_current = TRUE;

-- Step 2: Expire changed records (close old version)
UPDATE STAGING.stg_customer_master tgt
SET
    expiry_date = DATEADD('day', -1, CURRENT_DATE()),
    is_current = FALSE,
    _updated_at = CURRENT_TIMESTAMP()
FROM _customer_changes chg
WHERE tgt.customer_id = chg.customer_id
  AND tgt.is_current = TRUE
  AND chg.change_type = 'UPDATE';

-- Step 3: Insert new versions (new + changed records)
INSERT INTO STAGING.stg_customer_master (
    customer_id, first_name, last_name, email, phone,
    address_line_1, city, state, country, postal_code,
    customer_tier, account_open_date,
    effective_date, expiry_date, is_current, _hash_key
)
SELECT
    customer_id, first_name, last_name, email, phone,
    address_line_1, city, state, country, postal_code,
    customer_tier, account_open_date,
    CURRENT_DATE(),
    '9999-12-31',
    TRUE,
    new_hash
FROM _customer_changes
WHERE change_type IN ('INSERT', 'UPDATE');

-- Cleanup
DROP TABLE IF EXISTS _customer_changes;
