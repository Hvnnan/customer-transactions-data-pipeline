# Databricks notebook source
spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

# Create project schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.landing")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.audit")

# Check schemas
spark.sql("SHOW SCHEMAS IN workspace").show(truncate=False)

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.landing.raw_files
""")

# COMMAND ----------

spark.sql("""
SHOW VOLUMES IN workspace.landing
""").show(truncate=False)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/landing/raw_files"))

# COMMAND ----------

print("=" * 60)
print("LANDING VALIDATION COMPLETED")
print("=" * 60)
print("Schemas      : OK")
print("Landing Volume : OK")
print("Environment  : READY")