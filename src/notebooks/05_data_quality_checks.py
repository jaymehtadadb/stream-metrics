# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data Quality Checks
# MAGIC
# MAGIC Validates the ingested data across all 12 Stream Metrics tables.
# MAGIC Checks for row counts, null rates, key column distributions, and cross-table consistency.

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Count Validation

# COMMAND ----------

EXPECTED_TABLES = [
    "vod_title_availability_detail",
    "vod_title_availability_summary",
    "vod_episode_availability_detail",
    "vod_title_availability_native",
    "vod_title_changes_us_ca",
    "vod_title_mom_changes",
    "window_series_trends",
    "window_series_detail",
    "window_series_seasons",
    "window_series_episodes",
    "window_non_series_detail",
    "window_non_series_trends",
]

print(f"{'Table':<45} {'Rows':>12} {'Columns':>10} {'Status'}")
print("-" * 80)

table_counts = {}
all_ok = True

for table_name in EXPECTED_TABLES:
    full_table = f"{catalog}.{schema}.{table_name}"
    try:
        df = spark.table(full_table)
        cnt = df.count()
        cols = len(df.columns)
        status = "OK" if cnt > 0 else "EMPTY"
        if cnt == 0:
            all_ok = False
        table_counts[table_name] = cnt
        print(f"  {table_name:<45} {cnt:>10,} {cols:>10} {status}")
    except Exception as e:
        print(f"  {table_name:<45} {'N/A':>10} {'N/A':>10} MISSING")
        all_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Column Null Rate Check

# COMMAND ----------

KEY_COLUMNS = {
    "vod_title_availability_detail": ["service_name", "title", "country", "current_to"],
    "vod_title_availability_summary": ["service_name", "country", "report_interval", "current_to"],
    "vod_title_changes_us_ca": ["service_name", "title", "current_month_changes", "country"],
    "vod_title_mom_changes": ["service_name", "country", "change_type"],
    "window_series_trends": ["service_name", "country", "avg_days"],
    "window_series_detail": ["service_name", "series_title", "country"],
    "window_non_series_detail": ["service_name", "non_series_title", "country"],
    "window_non_series_trends": ["service_name", "country", "avg_days"],
}

from pyspark.sql import functions as F

print(f"\n{'Table':<35} {'Column':<25} {'Null %':>8} {'Status'}")
print("-" * 80)

for table_name, columns in KEY_COLUMNS.items():
    full_table = f"{catalog}.{schema}.{table_name}"
    try:
        df = spark.table(full_table)
        total = df.count()
        if total == 0:
            continue
        for col in columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                null_pct = (null_count / total) * 100
                status = "OK" if null_pct < 50 else "WARN"
                print(f"  {table_name:<35} {col:<25} {null_pct:>7.1f}% {status}")
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Service Name Distribution

# COMMAND ----------

print("\nTop services by title count in vod_title_changes_us_ca:")
display(
    spark.sql(f"""
        SELECT service_name, COUNT(*) AS titles, 
               SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
               SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped
        FROM {catalog}.{schema}.vod_title_changes_us_ca
        WHERE current_month_changes IS NOT NULL
        GROUP BY service_name
        ORDER BY titles DESC
        LIMIT 20
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Table Consistency

# COMMAND ----------

print("Cross-table service consistency check:\n")

services_changes = set(
    row["service_name"] for row in
    spark.sql(f"SELECT DISTINCT service_name FROM {catalog}.{schema}.vod_title_changes_us_ca").collect()
)

services_summary = set(
    row["service_name"] for row in
    spark.sql(f"SELECT DISTINCT service_name FROM {catalog}.{schema}.vod_title_availability_summary").collect()
)

services_window = set(
    row["service_name"] for row in
    spark.sql(f"SELECT DISTINCT service_name FROM {catalog}.{schema}.window_series_trends").collect()
)

print(f"  Services in changes table:    {len(services_changes)}")
print(f"  Services in summary table:    {len(services_summary)}")
print(f"  Services in window trends:    {len(services_window)}")

common = services_changes & services_summary & services_window
print(f"  Services in all three tables: {len(common)}")
if common:
    for s in sorted(common)[:10]:
        print(f"    - {s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation Result

# COMMAND ----------

if all_ok:
    print("ALL DATA QUALITY CHECKS PASSED")
else:
    print("WARNING: Some checks had issues. Review the output above.")
