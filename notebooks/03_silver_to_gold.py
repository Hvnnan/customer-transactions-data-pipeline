# Databricks notebook source
# =====================================================
# Customer Transactions Data Pipeline
# Notebook : 03_silver_to_gold
# Stage    : Silver Delta -> Gold Delta
# Purpose  : Create business-ready aggregated transaction summary
# =====================================================

from pyspark.sql.functions import (
    col, current_timestamp, lit, to_date,
    count, sum, avg, min, max
)
import uuid

# =====================================================
# Pipeline Parameters
# =====================================================

run_id = str(uuid.uuid4())
pipeline_name = "customer_transactions"
stage_name = "03_silver_to_gold"

silver_table = "workspace.silver.customer_transactions"
gold_table = "workspace.gold.customer_transaction_summary"

# =====================================================
# Read Silver Table
# =====================================================

df_silver = spark.table(silver_table)

# =====================================================
# Business Transformation
# Gold: Daily transaction summary by channel and transaction type
# =====================================================

df_gold = (
    df_silver
    .withColumn("transaction_date", to_date(col("transaction_datetime")))
    .groupBy(
        "transaction_date",
        "channel",
        "transaction_type"
    )
    .agg(
        count("*").alias("transaction_count"),
        sum("transaction_amount").alias("total_transaction_amount"),
        avg("transaction_amount").alias("avg_transaction_amount"),
        min("transaction_amount").alias("min_transaction_amount"),
        max("transaction_amount").alias("max_transaction_amount"),
        avg("customer_age").alias("avg_customer_age")
    )
    .withColumn("run_id", lit(run_id))
    .withColumn("pipeline_name", lit(pipeline_name))
    .withColumn("stage_name", lit(stage_name))
    .withColumn("gold_load_timestamp", current_timestamp())
)

# =====================================================
# Validation Counts
# =====================================================

silver_row_count = df_silver.count()
gold_row_count_prewrite = df_gold.count()

# =====================================================
# Write to Gold Delta Table
# =====================================================

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

gold_row_count = spark.table(gold_table).count()
gold_column_count = len(spark.table(gold_table).columns)

validation_status = "SUCCESS" if gold_row_count > 0 else "FAILED"

# =====================================================
# Validation Summary
# =====================================================

print("=" * 60)
print("SILVER TO GOLD SUMMARY")
print("=" * 60)
print(f"Run ID                 : {run_id}")
print(f"Pipeline name          : {pipeline_name}")
print(f"Stage Name             : {stage_name}")
print(f"Status                 : {validation_status}")
print(f"Silver Table           : {silver_table}")
print(f"Gold Table             : {gold_table}")
print(f"Silver Row Count       : {silver_row_count}")
print(f"Gold Row Count Before  : {gold_row_count_prewrite}")
print(f"Gold Row Count After   : {gold_row_count}")
print(f"Gold Column Count      : {gold_column_count}")

display(df_gold)