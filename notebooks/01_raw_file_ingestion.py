# Databricks notebook source
# =====================================================
# Customer Transactions Data Pipeline
# Notebook : 01_raw_file_ingestion
# Stage    : Landing CSV -> Bronze Delta
# Purpose  : Read raw CSV as string and load into Bronze
# =====================================================

from pyspark.sql.functions import current_timestamp, lit
import uuid

# =====================================================
# Pipeline Parameters
# =====================================================

run_id = str(uuid.uuid4())
pipeline_name = "customer_transactions"
stage_name = "01_raw_file_ingestion"

source_file_name = "bank_transactions_data_2_augmented_clean_2.csv"
source_file_path = f"/Volumes/workspace/landing/raw_files/{source_file_name}"
pipeline_start_timestamp = current_timestamp()

bronze_table = "workspace.bronze.customer_transactions"

# =====================================================
# Read Landing CSV as String
# =====================================================

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(source_file_path)
)

# =====================================================
# Source Validation Counts
# =====================================================

source_row_count = df_raw.count()
source_column_count = len(df_raw.columns)

# =====================================================
# Add Technical Metadata
# =====================================================

df_bronze = (
    df_raw
    .withColumn("run_id", lit(run_id))
    .withColumn("pipeline_name", lit(pipeline_name))
    .withColumn("stage_name", lit(stage_name))
    .withColumn("source_file_name", lit(source_file_name))
    .withColumn("ingestion_timestamp", current_timestamp())
)

bronze_row_count = df_bronze.count()
bronze_column_count = len(df_bronze.columns)

# =====================================================
# Write to Bronze Delta Table
# =====================================================

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .option("delta.minReaderVersion", "2")
    .option("delta.minWriterVersion", "5")
    .saveAsTable(bronze_table)
)

# =====================================================
# Validation Summary
# =====================================================

validation_status = (
    "SUCCESS"
    if source_row_count == bronze_row_count
    else "FAILED"
)

print("=" * 60)
print("RAW FILE INGESTION SUMMARY")
print("=" * 60)
print(f"Run ID              : {run_id}")
print(f"Pipeline name       : {pipeline_name}")
print(f"Stage Name          : {stage_name}")
print(f"Status              : {validation_status}")
print(f"Source File         : {source_file_path}")
print(f"Bronze Table        : {bronze_table}")
print(f"Source Row Count    : {source_row_count}")
print(f"Bronze Row Count    : {bronze_row_count}")
print(f"Source Column Count : {source_column_count}")
print(f"Bronze Column Count : {bronze_column_count}")

display(df_bronze)