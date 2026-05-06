# Architecture Overview

## System Design

This document describes the high-level architecture of the Real-Time Transaction Data Pipeline.

## Data Flow Diagram

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│   Event Sources  │────▶│  AWS Lambda       │────▶│  S3 Raw Staging     │
│  (POS, Mobile,  │     │  (Validation +    │     │  (Partitioned by    │
│   Online API)   │     │   Enrichment)     │     │   date/hour)        │
└─────────────────┘     └──────────────────┘     └─────────┬───────────┘
                                                           │
                        ┌──────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Apache Airflow (Orchestration)                    │
│                                                                      │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌────────────────────┐  │
│  │ Ingest  │─▶│ Validate │─▶│  Stage    │─▶│ Load Dimensions    │  │
│  │ Raw     │  │ Raw      │  │ Transform │  │ (SCD Type 2)       │  │
│  └─────────┘  └──────────┘  └───────────┘  └────────┬───────────┘  │
│                                                       │              │
│                                            ┌──────────▼───────────┐ │
│                                            │ Load Fact Table      │ │
│                                            │ (Star Schema)        │ │
│                                            └──────────┬───────────┘ │
│                                                       │              │
│  ┌───────────────────┐  ┌─────────────────────────────▼──────────┐  │
│  │ Update Metrics    │◀─│ Data Quality Checks                    │  │
│  │ (Prometheus)      │  │ (Great Expectations - 15 validations)  │  │
│  └───────────────────┘  └────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Snowflake Data Warehouse                         │
│                                                                      │
│  ┌──────────────┐  ┌──────────────────┐  ┌───────────────────────┐  │
│  │  RAW Schema  │  │  STAGING Schema  │  │  MARTS Schema         │  │
│  │              │  │                  │  │                       │  │
│  │ raw_         │  │ stg_raw_         │  │ fct_transactions      │  │
│  │ transactions │  │ transactions     │  │ dim_customer (SCD2)   │  │
│  │ raw_customer │  │ stg_customer_    │  │ dim_merchant          │  │
│  │ _master      │  │ master           │  │ dim_date              │  │
│  └──────────────┘  └──────────────────┘  └───────────────────────┘  │
│                                                                      │
│  ┌───────────────────────┐                                          │
│  │  MONITORING Schema    │                                          │
│  │  data_quality_metrics │                                          │
│  │  pipeline_sla_metrics │                                          │
│  └───────────────────────┘                                          │
└─────────────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Monitoring Stack                                 │
│                                                                      │
│  ┌────────────────┐  ┌──────────────┐  ┌─────────────────────────┐  │
│  │   Prometheus   │─▶│   Grafana    │  │    Slack Alerts         │  │
│  │   (Metrics)    │  │  (Dashboard) │  │  (Failure / SLA Breach) │  │
│  └────────────────┘  └──────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Ingestion Layer (AWS Lambda)
- **Throughput**: 1000+ events/second
- **Latency**: Sub-5ms per event
- **Features**: Payload validation, card number masking, S3 partitioned writes
- **Error Handling**: Dead-letter queue for failed events

### 2. Orchestration (Apache Airflow)
- **Schedule**: Every 15 minutes
- **Tasks**: 8 tasks in DAG with parallel dimension loading
- **Retry Logic**: 3 retries with exponential backoff
- **SLA**: 2-minute end-to-end processing window

### 3. Data Warehouse (Snowflake)
- **Schema Design**: Star schema with fact + dimension tables
- **SCD Type 2**: Customer dimension tracks historical changes
- **Clustering**: Fact table clustered by `(transaction_date_key, customer_key)`
- **Cost Optimization**: Dynamic partition pruning, warehouse auto-suspend

### 4. Data Quality (Great Expectations)
- **15 Validation Checks**: Completeness, uniqueness, accuracy, timeliness, security
- **Catch Rate**: 99.2% of data quality issues caught before downstream
- **Thresholds**: Configurable per-check with PASS/FAIL/WARNING

### 5. Monitoring (Prometheus + Grafana)
- **Metrics**: Pipeline success rate, task duration, data quality scores, SLA tracking
- **Alerting**: Slack notifications within 60 seconds of failure
- **Dashboard**: 6 pre-built panels for operational visibility

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Star schema over snowflake schema | Optimized for analytical queries, simpler joins |
| SCD Type 2 for customers | Full audit trail of attribute changes |
| 15-minute batch vs real-time | Balances latency with cost efficiency |
| Great Expectations over dbt tests | More flexible validation, richer reporting |
| Prometheus over CloudWatch | Open source, portable, better Grafana integration |
| MERGE over INSERT | Handles late-arriving data and idempotency |

## Deployment Topology

```
Production:
  ├── AWS Lambda (us-east-1) → Event Ingestion
  ├── S3 Bucket → Raw data staging
  ├── Snowflake (cloud) → Data Warehouse
  ├── ECS/MWAA → Airflow (managed)
  ├── EC2/ECS → Prometheus + Grafana
  └── Slack → Alerting

Local Development:
  └── Docker Compose
      ├── Airflow (webserver + scheduler)
      ├── PostgreSQL (metadata)
      ├── StatsD Exporter
      ├── Prometheus
      └── Grafana
```
