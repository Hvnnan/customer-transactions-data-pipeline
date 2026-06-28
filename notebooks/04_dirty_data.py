# Databricks notebook source
# =====================================================
# Customer Transactions Data Pipeline
# Notebook : 04_dirty_data
# Stage    : Silver Delta -> Dirty Data Delta
# Purpose  : Identify data quality issues and store rejected/problematic records
# =====================================================

from pyspark.sql.functions import col, lit, current_timestamp, concat_ws, when, count
import uuid

# =====================================================
# Pipeline Parameters
# =====================================================

run_id = str(uuid.uuid4())
pipeline_name = "customer_transactions"
stage_name = "04_dirty_data"

silver_table = "workspace.silver.customer_transactions"
dirty_table = "workspace.audit.dirty_customer_transactions"

# =====================================================
# Read Silver Table
# =====================================================

df_silver = spark.table(silver_table)

# =====================================================
# Duplicate Transaction ID Check
# =====================================================

df_duplicate_ids = (
    df_silver
    .groupBy("transaction_id")
    .agg(count("*").alias("duplicate_count"))
    .filter(col("duplicate_count") > 1)
)

# =====================================================
# Dirty Data Rules
# =====================================================

df_dirty = (
    df_silver
    .join(df_duplicate_ids, on="transaction_id", how="left")
    .withColumn(
        "rejection_reason",
        concat_ws(
            "; ",
            when(col("transaction_id").isNull(), lit("Missing transaction_id")),
            when(col("duplicate_count").isNotNull(), lit("Duplicate transaction_id")),
            when(col("transaction_amount").isNull(), lit("Invalid transaction_amount")),
            when(col("transaction_amount") <= 0, lit("Non-positive transaction_amount")),
            when(col("transaction_datetime").isNull(), lit("Invalid transaction_timestamp")),
            when(~col("transaction_type").isin("Debit", "Credit"), lit("Invalid transaction_type")),
            when(col("channel").isNull(), lit("Missing channel")),
            when((col("customer_age") < 18) | (col("customer_age") > 100), lit("Invalid customer_age"))
        )
    )
    .filter(col("rejection_reason") != "")
    .withColumn("run_id", lit(run_id))
    .withColumn("pipeline_name", lit(pipeline_name))
    .withColumn("stage_name", lit(stage_name))
    .withColumn("dirty_load_timestamp", current_timestamp())
)

# =====================================================
# Write Dirty Data
# =====================================================

(
    df_dirty.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(dirty_table)
)

dirty_row_count = spark.table(dirty_table).count()
silver_row_count = df_silver.count()

validation_status = "SUCCESS"

# =====================================================
# Validation Summary
# =====================================================

print("=" * 60)
print("DIRTY DATA SUMMARY")
print("=" * 60)
print(f"Run ID           : {run_id}")
print(f"Pipeline name    : {pipeline_name}")
print(f"Stage Name       : {stage_name}")
print(f"Status           : {validation_status}")
print(f"Silver Table     : {silver_table}")
print(f"Dirty Table      : {dirty_table}")
print(f"Silver Row Count : {silver_row_count}")
print(f"Dirty Row Count  : {dirty_row_count}")

display(df_dirty)