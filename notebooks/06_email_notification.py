# Databricks notebook source
# =====================================================
# Customer Transactions Data Pipeline
# Notebook : 06_email_notification
# Stage    : Email Notification
# Purpose  : Generate pipeline completion notification summary
# =====================================================

from pyspark.sql.functions import col, max as spark_max

# =====================================================
# Pipeline Parameters
# =====================================================

pipeline_name = "customer_transactions"
audit_table = "workspace.audit.audit_log"

# =====================================================
# Read Latest Audit Run
# =====================================================

df_audit = spark.table(audit_table)

latest_run_id = (
    df_audit
    .orderBy(col("audit_timestamp").desc())
    .select("run_id")
    .first()[0]
)

df_latest_audit = df_audit.filter(col("run_id") == latest_run_id)

# =====================================================
# Get Final Status
# =====================================================

final_row = (
    df_latest_audit
    .filter(col("stage_name") == "FINAL_STATUS")
    .select("status", "reason")
    .first()
)

final_status = final_row["status"]
reason = final_row["reason"]

# =====================================================
# Helper Function
# =====================================================

def get_stage_count(stage_name):
    row = (
        df_latest_audit
        .filter(col("stage_name") == stage_name)
        .select("record_count")
        .first()
    )
    return row["record_count"] if row else 0

def get_stage_status(stage_name):
    row = (
        df_latest_audit
        .filter(col("stage_name") == stage_name)
        .select("status")
        .first()
    )
    return row["status"] if row else "NOT_FOUND"

def stage_icon(status):
    if status == "SUCCESS":
        return "PASS"
    elif status == "WARNING":
        return "WARNING"
    elif status == "FAILED":
        return "FAILED"
    else:
        return "UNKNOWN"

# =====================================================
# Row Count Summary
# =====================================================

bronze_rows = get_stage_count("01_raw_file_ingestion")
silver_rows = get_stage_count("02_bronze_to_silver")
gold_rows = get_stage_count("03_silver_to_gold")
dirty_rows = get_stage_count("04_dirty_data")

bronze_silver_check = "PASS" if bronze_rows == silver_rows else "FAILED"
dirty_data_check = "PASS" if dirty_rows == 0 else "WARNING"

# =====================================================
# Stage Status Summary
# =====================================================

stages = [
    ("01_raw_file_ingestion", "Raw File Ingestion"),
    ("02_bronze_to_silver", "Bronze to Silver"),
    ("03_silver_to_gold", "Silver to Gold"),
    ("04_dirty_data", "Dirty Data"),
    ("FINAL_STATUS", "Final Pipeline Status")
]

stage_summary_lines = []

for stage_code, stage_label in stages:
    status = get_stage_status(stage_code)
    stage_summary_lines.append(
        f"{stage_icon(status):<8} {stage_label:<25} : {status}"
    )

stage_summary_text = "\n".join(stage_summary_lines)

# =====================================================
# Execution Timestamp
# =====================================================

execution_timestamp = (
    df_latest_audit
    .agg(spark_max("audit_timestamp"))
    .first()[0]
)

# =====================================================
# Generate Notification Message
# =====================================================

email_subject = f"[{final_status}] Enterprise Financial Pipeline - {pipeline_name}"

email_body = f"""
========================================================
Enterprise Financial Pipeline Notification
========================================================

Pipeline Name       : {pipeline_name}
Run ID              : {latest_run_id}
Status              : {final_status}
Execution Timestamp : {execution_timestamp}

Row Count Summary
--------------------------------------------------------
Bronze Rows         : {bronze_rows}
Silver Rows         : {silver_rows}
Gold Rows           : {gold_rows}
Dirty Rows          : {dirty_rows}

Validation Summary
--------------------------------------------------------
Bronze = Silver     : {bronze_silver_check}
Dirty Data Check    : {dirty_data_check}
Pipeline Status     : {final_status}

Stage Summary
--------------------------------------------------------
{stage_summary_text}

Reason
--------------------------------------------------------
{reason}

========================================================
"""

# =====================================================
# Display Notification
# =====================================================

print("Email Subject:")
print(email_subject)

print("\nEmail Body:")
print(email_body)