# Databricks notebook source
# =====================================================
# Customer Transactions Data Pipeline
# Notebook : 02_bronze_to_silver
# Stage    : Bronze Delta -> Silver Delta
# Purpose  : Standardize column names, cast data types, and load clean typed data into Silver
# =====================================================

from pyspark.sql.functions import col, current_timestamp, lit, coalesce, try_to_timestamp
import uuid

# =====================================================
# Pipeline Parameters
# =====================================================

run_id = str(uuid.uuid4())
pipeline_name = "customer_transactions"
stage_name = "02_bronze_to_silver"

bronze_table = "workspace.bronze.customer_transactions"
silver_table = "workspace.silver.customer_transactions"

# =====================================================
# Read Bronze Table
# =====================================================

df_bronze = spark.table(bronze_table)

# =====================================================
# Standardize Columns + Cast Data Types
# =====================================================

df_silver = (
    df_bronze
    .select(
        col("TransactionID").alias("transaction_id"),
        col("AccountID").alias("account_id"),
        col("TransactionAmount").cast("decimal(18,2)").alias("transaction_amount"),
        coalesce(
            try_to_timestamp(col("TransactionDate"), lit("M/d/yyyy H:mm")),
            try_to_timestamp(col("TransactionDate"), lit("M/d/yyyy"))
        ).alias("transaction_datetime"),
        col("TransactionType").alias("transaction_type"),
        col("Location").alias("location"),
        col("DeviceID").alias("device_id"),
        col("IP Address").alias("ip_address"),
        col("MerchantID").alias("merchant_id"),
        col("Channel").alias("channel"),
        col("CustomerAge").cast("int").alias("customer_age"),
        col("CustomerOccupation").alias("customer_occupation"),
        col("TransactionDuration").cast("int").alias("transaction_duration"),
        col("LoginAttempts").cast("int").alias("login_attempts"),
        col("AccountBalance").cast("decimal(18,2)").alias("account_balance"),
        col("source_file_name"),
        col("ingestion_timestamp")
    )
    .withColumn("run_id", lit(run_id))
    .withColumn("pipeline_name", lit(pipeline_name))
    .withColumn("stage_name", lit(stage_name))
    .withColumn("silver_load_timestamp", current_timestamp())
)

# =====================================================
# Validation Counts
# =====================================================

bronze_row_count = df_bronze.count()
silver_row_count_prewrite = df_silver.count()

# =====================================================
# Write to Silver Delta Table
# =====================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

silver_row_count = spark.table(silver_table).count()
silver_column_count = len(spark.table(silver_table).columns)

# =====================================================
# Validation Summary
# =====================================================

validation_status = (
    "SUCCESS"
    if bronze_row_count == silver_row_count
    else "FAILED"
)

print("=" * 60)
print("BRONZE TO SILVER SUMMARY")
print("=" * 60)
print(f"Run ID                  : {run_id}")
print(f"Pipeline name           : {pipeline_name}")
print(f"Stage Name              : {stage_name}")
print(f"Status                  : {validation_status}")
print(f"Bronze Table            : {bronze_table}")
print(f"Silver Table            : {silver_table}")
print(f"Bronze Row Count        : {bronze_row_count}")
print(f"Silver Row Count Before : {silver_row_count_prewrite}")
print(f"Silver Row Count After  : {silver_row_count}")
print(f"Silver Column Count     : {silver_column_count}")

display(df_silver)