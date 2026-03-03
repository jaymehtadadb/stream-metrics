# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup Infrastructure
# MAGIC
# MAGIC Creates the Unity Catalog schema, managed volume, and validates connectivity.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog`: Unity Catalog name (default: `jay_mehta_catalog`)
# MAGIC - `schema`: Schema name (default: `stream_metrics`)

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"Schema {catalog}.{schema} is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for Raw CSV Files

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data")
print(f"Volume {catalog}.{schema}.raw_data is ready.")
print(f"Volume path: /Volumes/{catalog}/{schema}/raw_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Setup

# COMMAND ----------

schemas = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'").collect()
volumes = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").collect()

assert len(schemas) == 1, f"Schema {schema} not found in {catalog}"
assert any(v["volume_name"] == "raw_data" for v in volumes), "Volume raw_data not found"

print("Infrastructure setup validated successfully.")
print(f"  Schema:  {catalog}.{schema}")
print(f"  Volume:  {catalog}.{schema}.raw_data")
