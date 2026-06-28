# Databricks notebook source
# =====================================================
# Customer Transactions Data Pipeline
# Notebook : 05_audit_log
# Stage    : Audit Log
# Purpose  : Consolidate pipeline execution status and row counts
# =====================================================

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import uuid

run_id = str(uuid.uuid4())
pipeline_name = "customer_transactions"
audit_table = "workspace.audit.audit_log"
pipeline_end_timestamp = current_timestamp()

bronze_table = "workspace.bronze.customer_transactions"
silver_table = "workspace.silver.customer_transactions"
gold_table = "workspace.gold.customer_transaction_summary"
dirty_table = "workspace.audit.dirty_customer_transactions"

bronze_count = spark.table(bronze_table).count()
silver_count = spark.table(silver_table).count()
gold_count = spark.table(gold_table).count()
dirty_count = spark.table(dirty_table).count()

final_status = "SUCCESS" if bronze_count == silver_count and dirty_count == 0 and gold_count > 0 else "FAILED"

reason = (
    "Pipeline completed successfully"
    if final_status == "SUCCESS"
    else "Validation failed: check row reconciliation or dirty data"
)

audit_data = [
    (run_id, pipeline_name, "01_raw_file_ingestion", bronze_table, "SUCCESS", bronze_count, None),
    (run_id, pipeline_name, "02_bronze_to_silver", silver_table, "SUCCESS" if bronze_count == silver_count else "FAILED", silver_count, None),
    (run_id, pipeline_name, "03_silver_to_gold", gold_table, "SUCCESS" if gold_count > 0 else "FAILED", gold_count, None),
    (run_id, pipeline_name, "04_dirty_data", dirty_table, "SUCCESS" if dirty_count == 0 else "WARNING", dirty_count, None),
    (run_id, pipeline_name, "FINAL_STATUS", audit_table, final_status, None, reason),
]

schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("stage_name", StringType(), False),
    StructField("target_table", StringType(), True),
    StructField("status", StringType(), False),
    StructField("record_count", LongType(), True),
    StructField("reason", StringType(), True),
])

df_audit = spark.createDataFrame(audit_data, schema).withColumn("audit_timestamp", current_timestamp())

(
    df_audit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(audit_table)
)

print("=" * 60)
print("AUDIT LOG SUMMARY")
print("=" * 60)
print(f"Run ID          : {run_id}")
print(f"Pipeline name   : {pipeline_name}")
print(f"Final Status    : {final_status}")
print(f"Reason          : {reason}")
print(f"Audit Table     : {audit_table}")

display(df_audit)