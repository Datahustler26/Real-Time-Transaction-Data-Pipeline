#!/bin/bash
# ============================================
# Snowflake Initial Setup Script
# ============================================
# Creates database, schemas, warehouses, roles, and grants
# for the Transaction Data Pipeline.
#
# Prerequisites:
#   - SnowSQL CLI installed (or use Snowflake web UI)
#   - ACCOUNTADMIN or SYSADMIN access
#
# Usage:
#   chmod +x scripts/setup_snowflake.sh
#   ./scripts/setup_snowflake.sh
# ============================================

set -euo pipefail

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

ACCOUNT="${SNOWFLAKE_ACCOUNT:-your_account}"
USER="${SNOWFLAKE_USER:-your_user}"
DATABASE="${SNOWFLAKE_DATABASE:-TRANSACTION_DB}"
WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"

echo "============================================"
echo "Transaction Data Pipeline — Snowflake Setup"
echo "============================================"
echo "Account:    ${ACCOUNT}"
echo "Database:   ${DATABASE}"
echo "Warehouse:  ${WAREHOUSE}"
echo ""

# Generate SQL setup script
SQL_SCRIPT=$(cat <<EOF
-- ============================================
-- 1. Create Warehouse
-- ============================================
CREATE WAREHOUSE IF NOT EXISTS ${WAREHOUSE}
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Transaction pipeline compute warehouse';

-- ============================================
-- 2. Create Database
-- ============================================
CREATE DATABASE IF NOT EXISTS ${DATABASE}
    COMMENT = 'Transaction Data Pipeline - Production';

USE DATABASE ${DATABASE};

-- ============================================
-- 3. Create Schemas
-- ============================================
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw ingested data from source systems';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned and deduplicated staging layer';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Star schema analytics layer (fact + dimensions)';

CREATE SCHEMA IF NOT EXISTS MONITORING
    COMMENT = 'Pipeline metrics and data quality tracking';

-- ============================================
-- 4. Create Raw Tables
-- ============================================
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id      VARCHAR(64),
    customer_id         VARCHAR(32),
    merchant_id         VARCHAR(32),
    amount              VARCHAR(50),
    currency            VARCHAR(10),
    transaction_timestamp VARCHAR(50),
    status              VARCHAR(50),
    channel             VARCHAR(50),
    card_type           VARCHAR(50),
    card_number         VARCHAR(50),
    merchant_category   VARCHAR(100),
    merchant_city       VARCHAR(100),
    merchant_country    VARCHAR(10),
    response_code       VARCHAR(10),
    auth_code           VARCHAR(20),
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS raw_customer_master (
    customer_id         VARCHAR(32),
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
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- 5. Create S3 External Stage (update with your bucket)
-- ============================================
-- CREATE OR REPLACE STAGE RAW.transaction_stage
--     URL = 's3://transaction-pipeline-raw/raw/transactions/'
--     CREDENTIALS = (AWS_KEY_ID = 'xxx' AWS_SECRET_KEY = 'xxx')
--     FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- ============================================
-- 6. Create File Formats
-- ============================================
CREATE OR REPLACE FILE FORMAT RAW.csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = 'AUTO';

CREATE OR REPLACE FILE FORMAT RAW.json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = 'AUTO';

-- ============================================
-- 7. Create Roles & Grants (Optional)
-- ============================================
-- CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;
-- GRANT USAGE ON DATABASE ${DATABASE} TO ROLE PIPELINE_ROLE;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE ${DATABASE} TO ROLE PIPELINE_ROLE;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA RAW TO ROLE PIPELINE_ROLE;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA STAGING TO ROLE PIPELINE_ROLE;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA MARTS TO ROLE PIPELINE_ROLE;
-- GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA MONITORING TO ROLE PIPELINE_ROLE;
-- GRANT USAGE ON WAREHOUSE ${WAREHOUSE} TO ROLE PIPELINE_ROLE;

-- ============================================
-- Verification
-- ============================================
SHOW SCHEMAS IN DATABASE ${DATABASE};
SHOW TABLES IN SCHEMA RAW;

SELECT 'Setup complete!' AS status;
EOF
)

echo "Generated SQL setup script."
echo ""
echo "To execute, run one of the following:"
echo ""
echo "  Option 1 (SnowSQL CLI):"
echo "    snowsql -a ${ACCOUNT} -u ${USER} -q \"${SQL_SCRIPT}\""
echo ""
echo "  Option 2 (Copy-paste into Snowflake Web UI):"
echo "    The SQL has been saved to: scripts/setup_snowflake.sql"
echo ""

# Save SQL to file for easy copy-paste
echo "${SQL_SCRIPT}" > scripts/setup_snowflake.sql
echo "✅ SQL script saved to scripts/setup_snowflake.sql"
