-- ============================================
-- Dimension: Customer (SCD Type 2)
-- ============================================
-- Tracks historical customer attribute changes with
-- effective/expiry dates and is_current flag.
-- ============================================

CREATE TABLE IF NOT EXISTS MARTS.dim_customer (
    customer_key        BIGINT AUTOINCREMENT PRIMARY KEY,
    customer_id         VARCHAR(32)     NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(201),
    email               VARCHAR(256),
    phone               VARCHAR(20),
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(3),
    postal_code         VARCHAR(20),
    customer_tier       VARCHAR(20)     DEFAULT 'standard',
    account_open_date   DATE,
    account_age_days    INT,
    effective_date      DATE            NOT NULL DEFAULT CURRENT_DATE(),
    expiry_date         DATE            NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE,
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- SCD Type 2 Merge from staging
-- Step 1: Expire changed records
UPDATE MARTS.dim_customer tgt
SET
    expiry_date = DATEADD('day', -1, CURRENT_DATE()),
    is_current = FALSE,
    _loaded_at = CURRENT_TIMESTAMP()
FROM STAGING.stg_customer_master src
WHERE tgt.customer_id = src.customer_id
  AND tgt.is_current = TRUE
  AND src.is_current = TRUE
  AND src.effective_date = CURRENT_DATE()
  AND (
      tgt.first_name != src.first_name
      OR tgt.last_name != src.last_name
      OR tgt.email != src.email
      OR tgt.customer_tier != src.customer_tier
      OR tgt.city != src.city
      OR tgt.state != src.state
      OR tgt.country != src.country
  );

-- Step 2: Insert new and changed records
INSERT INTO MARTS.dim_customer (
    customer_id, first_name, last_name, full_name,
    email, phone, city, state, country, postal_code,
    customer_tier, account_open_date, account_age_days,
    effective_date, expiry_date, is_current
)
SELECT
    src.customer_id,
    src.first_name,
    src.last_name,
    CONCAT(COALESCE(src.first_name, ''), ' ', COALESCE(src.last_name, '')) AS full_name,
    src.email,
    src.phone,
    src.city,
    src.state,
    src.country,
    src.postal_code,
    COALESCE(src.customer_tier, 'standard'),
    src.account_open_date,
    DATEDIFF('day', src.account_open_date, CURRENT_DATE()) AS account_age_days,
    src.effective_date,
    src.expiry_date,
    src.is_current
FROM STAGING.stg_customer_master src
WHERE src.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM MARTS.dim_customer d
      WHERE d.customer_id = src.customer_id
        AND d.is_current = TRUE
        AND d.effective_date = src.effective_date
  );
