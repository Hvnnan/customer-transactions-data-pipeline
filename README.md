# Customer Transactions Data Pipeline

An end-to-end Data Engineering project built using **Databricks**, implementing the **Medallion Architecture (Bronze, Silver, Gold)** with workflow orchestration, audit logging, dirty data validation, and pipeline execution reporting.

This project demonstrates how raw banking transaction data can be ingested, transformed, validated, and aggregated into business-ready datasets using Databricks notebooks and Delta tables.

---

# Architecture

> *(Architecture diagram will be added in a future update.)*

---

# Tech Stack

* Databricks Free Edition
* PySpark
* Delta Lake
* Unity Catalog
* Databricks Workflows (Jobs)
* Python
* SQL

---

# Project Structure

```
customer-transactions-data-pipeline
│
├── notebooks/
│   ├── 00_landing_validation.py
│   ├── 01_raw_file_ingestion.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_silver_to_gold.py
│   ├── 04_dirty_data.py
│   ├── 05_audit_log.py
│   └── 06_email_notification.py
│
├── raw_data/
│   └── sample_data_description.md
│
├── screenshots/
│
├── architecture/
│
└── README.md
```

---

# Pipeline Flow

```
Customer Transactions CSV
        │
        ▼
Landing Volume
        │
        ▼
Raw File Ingestion
        │
        ▼
Bronze Layer
        │
        ▼
Bronze → Silver Transformation
        │
        ├────────► Dirty Data Validation
        │
        ▼
Silver Layer
        │
        ▼
Silver → Gold Aggregation
        │
        ▼
Gold Layer
        │
        ▼
Audit Logging
        │
        ▼
Email Notification
```

---

# Medallion Architecture

## Landing

Stores the original CSV file inside a Databricks Volume before ingestion.

---

## Bronze Layer

Purpose:

* Store raw data
* Preserve original records
* Minimal transformation

Output Table

```
workspace.bronze.customer_transactions
```

---

## Silver Layer

Purpose:

* Data cleaning
* Standardization
* Type casting
* Deduplication

Output Table

```
workspace.silver.customer_transactions
```

---

## Dirty Data Validation

Performs data quality validation using the standardized Silver dataset.

Validation includes:

* Missing transaction ID
* Duplicate transaction ID
* Invalid transaction amount
* Invalid transaction type
* Missing channel
* Invalid customer age
* Invalid transaction timestamp

Rejected records are stored in

```
workspace.audit.dirty_customer_transactions
```

---

## Gold Layer

Creates business-ready aggregated datasets.

Output Table

```
workspace.gold.customer_transaction_summary
```

Example metrics:

* Transaction Count
* Total Transaction Amount
* Average Transaction Amount
* Minimum Transaction Amount
* Maximum Transaction Amount
* Average Customer Age

Grouped by

* Transaction Date
* Transaction Type
* Channel

---

## Audit Logging

Each pipeline execution generates an audit record containing:

* Run ID
* Pipeline Name
* Stage Name
* Execution Status
* Record Count
* Target Table
* Failure Reason (if any)
* Execution Timestamp

Audit Table

```
workspace.audit.audit_log
```

---

## Email Notification

After the workflow completes, the final notebook generates an execution summary including:

* Pipeline Status
* Run ID
* Execution Timestamp
* Bronze Row Count
* Silver Row Count
* Gold Row Count
* Dirty Data Count
* Stage Status Summary

---

# Databricks Workflow

The pipeline is orchestrated using Databricks Workflows.

Execution Order

```
00 Landing Validation
        ↓
01 Raw File Ingestion
        ↓
02 Bronze to Silver
        ↓
03 Silver to Gold
        ↓
04 Dirty Data Validation
        ↓
05 Audit Log
        ↓
06 Email Notification
```

Each notebook executes only after the previous stage completes successfully.

---

# Sample Output

The repository includes screenshots demonstrating:

* Databricks Workflow execution
* Catalog Explorer
* Gold Layer output
* Audit Log table
* Email Notification summary

---

# Dataset

This project uses a publicly available synthetic banking transactions dataset from Kaggle.

Dataset:

**Bank Transactions Dataset for Fraud Detection**

Author:

**Thuan Dao**

https://www.kaggle.com/datasets/thuandao/bank-transactions-dataset-for-fraud-detection

The original dataset is **not included** in this repository.

Please refer to:

```
raw_data/sample_data_description.md
```

for dataset information and download instructions.

---

# Future Improvements

* Architecture diagram
* Parameterized pipeline configuration
* Incremental data loading
* Data quality framework
* Automated email integration
* CI/CD deployment

---

# Author

**Nur Farah Hannan Ahmad**

Senior Data Engineer

AWS Certified Solutions Architect – Associate

GitHub:
https://github.com/Hvnnan

## Other Portfolio Projects

You can view my other hands-on cloud and data engineering projects here:

[NextWork Portfolio - Hannan Ahmad](https://nextwork.ai/hannanahmad/portfolio)
