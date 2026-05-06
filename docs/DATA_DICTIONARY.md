# Data Dictionary

## Overview

This document describes all tables, columns, and relationships in the Transaction Data Pipeline data warehouse.

---

## Schema: RAW

### `raw_transactions`
Raw transaction events as ingested from source systems. Minimal processing — retains original values.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | VARCHAR(64) | NO | Unique transaction identifier from source |
| customer_id | VARCHAR(32) | NO | Customer account identifier |
| merchant_id | VARCHAR(32) | NO | Merchant identifier |
| amount | VARCHAR(50) | YES | Transaction amount (string from source) |
| currency | VARCHAR(10) | YES | ISO 4217 currency code |
| transaction_timestamp | VARCHAR(50) | YES | Transaction time (string from source) |
| status | VARCHAR(50) | YES | Transaction status |
| channel | VARCHAR(50) | YES | Payment channel |
| card_type | VARCHAR(50) | YES | Card brand |
| card_number | VARCHAR(50) | YES | Card number (should be masked) |
| merchant_category | VARCHAR(100) | YES | Merchant category |
| merchant_city | VARCHAR(100) | YES | Merchant city |
| merchant_country | VARCHAR(10) | YES | Merchant country code |
| response_code | VARCHAR(10) | YES | Authorization response code |
| auth_code | VARCHAR(20) | YES | Authorization code |
| _loaded_at | TIMESTAMP_NTZ | NO | Snowflake ingestion timestamp |
| _source_file | VARCHAR(256) | YES | Source file name from S3 |

---

## Schema: STAGING

### `stg_raw_transactions`
Cleaned and deduplicated transaction data with type casting and normalization.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | VARCHAR(64) | NO | **PK** — Unique transaction ID |
| customer_id | VARCHAR(32) | NO | Customer account ID |
| merchant_id | VARCHAR(32) | NO | Merchant ID |
| amount | DECIMAL(18,2) | NO | Transaction amount (cast from raw) |
| currency | VARCHAR(3) | NO | ISO currency code (uppercased) |
| transaction_timestamp | TIMESTAMP_NTZ | NO | Normalized timestamp |
| status | VARCHAR(20) | NO | Lowercased status |
| channel | VARCHAR(20) | NO | Lowercased channel |
| card_type | VARCHAR(20) | YES | Card brand |
| card_last_four | VARCHAR(4) | YES | Last 4 digits of card |
| merchant_category | VARCHAR(50) | YES | Merchant category |
| merchant_city | VARCHAR(100) | YES | City |
| merchant_country | VARCHAR(3) | YES | ISO country code |
| is_international | BOOLEAN | YES | TRUE if non-US merchant |
| response_code | VARCHAR(10) | YES | Auth response code |
| auth_code | VARCHAR(20) | YES | Auth code |
| _staged_at | TIMESTAMP_NTZ | NO | Staging timestamp |
| _batch_id | VARCHAR(64) | YES | Processing batch ID |

### `stg_customer_master`
Customer master data with SCD Type 2 versioning.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_key | BIGINT | NO | **PK** — Surrogate key (auto-increment) |
| customer_id | VARCHAR(32) | NO | Natural key — business customer ID |
| first_name | VARCHAR(100) | YES | Customer first name |
| last_name | VARCHAR(100) | YES | Customer last name |
| email | VARCHAR(256) | YES | Email address |
| phone | VARCHAR(20) | YES | Phone number |
| address_line_1 | VARCHAR(256) | YES | Street address |
| city | VARCHAR(100) | YES | City |
| state | VARCHAR(50) | YES | State/province |
| country | VARCHAR(3) | YES | ISO country code |
| postal_code | VARCHAR(20) | YES | Postal/ZIP code |
| customer_tier | VARCHAR(20) | YES | Account tier (standard/gold/platinum) |
| account_open_date | DATE | YES | Account creation date |
| effective_date | DATE | NO | SCD2 — Row effective date |
| expiry_date | DATE | NO | SCD2 — Row expiry date (9999-12-31 = current) |
| is_current | BOOLEAN | NO | SCD2 — TRUE = active version |
| _hash_key | VARCHAR(64) | YES | MD5 hash for change detection |
| _updated_at | TIMESTAMP_NTZ | NO | Last modification timestamp |

---

## Schema: MARTS

### `fct_transactions` (Fact Table)
Central fact table in star schema. One row per transaction with surrogate key references to dimensions.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_key | BIGINT | NO | **PK** — Surrogate key |
| transaction_id | VARCHAR(64) | NO | **UK** — Natural key |
| customer_key | BIGINT | NO | **FK** → dim_customer.customer_key |
| merchant_key | BIGINT | NO | **FK** → dim_merchant.merchant_key |
| transaction_date_key | INT | NO | Date key (YYYYMMDD format) |
| amount | DECIMAL(18,2) | NO | Original transaction amount |
| currency | VARCHAR(3) | NO | Original currency |
| amount_usd | DECIMAL(18,2) | YES | Amount converted to USD |
| status | VARCHAR(20) | NO | Transaction status |
| channel | VARCHAR(20) | NO | Payment channel |
| is_international | BOOLEAN | YES | International transaction flag |
| card_type | VARCHAR(20) | YES | Card brand |
| response_code | VARCHAR(10) | YES | Authorization response |
| auth_code | VARCHAR(20) | YES | Authorization code |
| transaction_timestamp | TIMESTAMP_NTZ | NO | Actual transaction time |
| processed_at | TIMESTAMP_NTZ | NO | Pipeline processing time |
| _batch_id | VARCHAR(64) | YES | Processing batch ID |

**Clustering**: `(transaction_date_key, customer_key)` — Optimized for date-range and customer queries.

### `dim_customer` (Dimension — SCD Type 2)
Customer dimension with full history tracking.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_key | BIGINT | NO | **PK** — Surrogate key |
| customer_id | VARCHAR(32) | NO | Natural key |
| first_name | VARCHAR(100) | YES | First name |
| last_name | VARCHAR(100) | YES | Last name |
| full_name | VARCHAR(201) | YES | Concatenated full name |
| email | VARCHAR(256) | YES | Email |
| phone | VARCHAR(20) | YES | Phone |
| city | VARCHAR(100) | YES | City |
| state | VARCHAR(50) | YES | State |
| country | VARCHAR(3) | YES | Country code |
| postal_code | VARCHAR(20) | YES | Postal code |
| customer_tier | VARCHAR(20) | YES | Tier (standard/gold/platinum) |
| account_open_date | DATE | YES | Account creation date |
| account_age_days | INT | YES | Days since account opened |
| effective_date | DATE | NO | SCD2 start date |
| expiry_date | DATE | NO | SCD2 end date |
| is_current | BOOLEAN | NO | Current version flag |
| _loaded_at | TIMESTAMP_NTZ | NO | Load timestamp |

### `dim_merchant` (Dimension)
Merchant master with category hierarchy and geographic attributes.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| merchant_key | BIGINT | NO | **PK** — Surrogate key |
| merchant_id | VARCHAR(32) | NO | **UK** — Natural key |
| merchant_name | VARCHAR(256) | NO | Display name |
| merchant_category | VARCHAR(50) | YES | Raw category |
| category_group | VARCHAR(50) | YES | Grouped category |
| mcc_code | VARCHAR(10) | YES | Merchant Category Code |
| merchant_city | VARCHAR(100) | YES | City |
| merchant_state | VARCHAR(50) | YES | State |
| merchant_country | VARCHAR(3) | YES | Country |
| merchant_region | VARCHAR(50) | YES | Geographic region |
| is_online | BOOLEAN | YES | Online-only merchant |
| risk_score | DECIMAL(5,2) | YES | Fraud risk score |
| first_seen_date | DATE | YES | First transaction date |
| last_transaction_date | DATE | YES | Most recent transaction |
| total_transaction_count | BIGINT | YES | Lifetime transaction count |
| _loaded_at | TIMESTAMP_NTZ | NO | Load timestamp |

---

## Schema: MONITORING

### `data_quality_metrics`
Stores results of every data quality check execution.

| Column | Type | Description |
|--------|------|-------------|
| metric_id | BIGINT | PK — Auto-increment |
| metric_timestamp | TIMESTAMP_NTZ | Check execution time |
| table_name | VARCHAR(100) | Target table name |
| metric_name | VARCHAR(100) | Check name |
| metric_value | DECIMAL(18,6) | Measured value |
| threshold | DECIMAL(18,6) | Configured threshold |
| status | VARCHAR(20) | PASS / FAIL / WARNING |
| details | VARCHAR(1000) | Human-readable details |
| batch_id | VARCHAR(64) | Processing batch |

### `pipeline_sla_metrics`
Tracks pipeline performance and SLA compliance.

| Column | Type | Description |
|--------|------|-------------|
| metric_id | BIGINT | PK — Auto-increment |
| metric_timestamp | TIMESTAMP_NTZ | Measurement time |
| dag_id | VARCHAR(100) | Airflow DAG ID |
| execution_date | TIMESTAMP_NTZ | DAG execution date |
| metric_name | VARCHAR(100) | Metric name |
| metric_value | DECIMAL(18,6) | Measured value |
| unit | VARCHAR(20) | Unit of measurement |
| status | VARCHAR(20) | Status assessment |
| details | VARCHAR(1000) | Details |

---

## Key Relationships

```
fct_transactions.customer_key  →  dim_customer.customer_key (where is_current = TRUE)
fct_transactions.merchant_key  →  dim_merchant.merchant_key
fct_transactions.customer_key = -1  →  "Unknown" customer (orphan record)
fct_transactions.merchant_key = -1  →  "Unknown" merchant (orphan record)
```
