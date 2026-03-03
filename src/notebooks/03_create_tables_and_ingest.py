# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Create Tables & Ingest Data from CSV
# MAGIC
# MAGIC Creates Delta tables from the CSV files stored in the Unity Catalog volume.
# MAGIC Uses `read_files()` with schema inference to handle the diverse column sets.
# MAGIC
# MAGIC **Tables created (12 total):**
# MAGIC
# MAGIC | Category | Tables |
# MAGIC |----------|--------|
# MAGIC | VOD Title Availability | `vod_title_availability_detail`, `vod_title_availability_summary`, `vod_title_availability_native` |
# MAGIC | VOD Changes | `vod_title_changes_us_ca`, `vod_title_mom_changes` |
# MAGIC | VOD Episodes | `vod_episode_availability_detail` |
# MAGIC | Window - Series | `window_series_trends`, `window_series_detail`, `window_series_seasons`, `window_series_episodes` |
# MAGIC | Window - Non-Series | `window_non_series_detail`, `window_non_series_trends` |

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

VOLUME_PATH = f"/Volumes/{catalog}/{schema}/raw_data"
print(f"Source volume: {VOLUME_PATH}")
print(f"Target schema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Definitions
# MAGIC
# MAGIC Each table has a description and the CSV filename in the volume.

# COMMAND ----------

TABLE_DEFINITIONS = {
    "vod_title_availability_detail": {
        "description": "VOD title-level availability detail across streaming services. Each row represents a title on a specific service for a given month. Contains content metadata, distribution info, availability flags, and runtime metrics.",
        "csv": "vod_title_availability_detail.csv"
    },
    "vod_title_availability_summary": {
        "description": "Aggregated VOD title availability summary by service and time period. Contains title counts and total minutes across dimensions: series vs movies, originals vs acquired, scripted vs unscripted, US vs international.",
        "csv": "vod_title_availability_summary.csv"
    },
    "vod_episode_availability_detail": {
        "description": "Episode-level VOD availability detail for series content. Each row is an episode on a streaming service with episode metadata, content classification, distribution info, and runtime.",
        "csv": "vod_episode_availability_detail.csv"
    },
    "vod_title_availability_native": {
        "description": "Monthly presence grid showing which titles are available on each provider by month. Monthly columns indicate title presence. Includes provider-reported metadata.",
        "csv": "vod_title_availability_native.csv"
    },
    "vod_title_changes_us_ca": {
        "description": "Title-level changes (adds and drops) for US and Canada streaming services. Tracks month-over-month content changes with streaming dates, exclusivity flags, and content metadata.",
        "csv": "vod_title_changes_us_ca.csv"
    },
    "vod_title_mom_changes": {
        "description": "Aggregated month-over-month (MoM) change counts by service, country, and content type. Monthly columns show counts of titles added or dropped.",
        "csv": "vod_title_mom_changes.csv"
    },
    "window_series_trends": {
        "description": "Aggregated windowing trend metrics for series content. Shows average window duration (days) and series counts by service, segmented by exclusive/shared, scripted/unscripted, off-net, US/international, and genre.",
        "csv": "window_series_trends.csv"
    },
    "window_series_detail": {
        "description": "Detailed series-level windowing data showing up to 18 streaming windows per title. Each row is a series on a service with window start/end dates and durations.",
        "csv": "window_series_detail.csv"
    },
    "window_series_seasons": {
        "description": "Season-level windowing data with up to 27 streaming windows per season. Breaks down series windowing by season number with season-specific metadata.",
        "csv": "window_series_seasons.csv"
    },
    "window_series_episodes": {
        "description": "Episode-level windowing data with up to 26 streaming windows per episode. The most granular windowing view for licensing patterns.",
        "csv": "window_series_episodes.csv"
    },
    "window_non_series_detail": {
        "description": "Detailed windowing data for movies and specials with up to 23 streaming windows. Contains theatrical status, media type, genre, release year, and window durations.",
        "csv": "window_non_series_detail.csv"
    },
    "window_non_series_trends": {
        "description": "Aggregated windowing trend metrics for movies. Shows average window duration and title counts by service, segmented by theatrical status, genre, and major distributor.",
        "csv": "window_non_series_trends.csv"
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest CSV Files into Delta Tables

# COMMAND ----------

from pyspark.sql import functions as F

results = []

for table_name, table_def in TABLE_DEFINITIONS.items():
    csv_file = f"{VOLUME_PATH}/{table_def['csv']}"
    full_table = f"{catalog}.{schema}.{table_name}"
    description = table_def["description"]

    print(f"\n{'='*70}")
    print(f"Table: {full_table}")
    print(f"Source: {csv_file}")

    try:
        # Check if CSV exists
        try:
            dbutils.fs.ls(f"dbfs:{csv_file}")
        except Exception:
            print(f"  SKIPPED - CSV file not found: {csv_file}")
            results.append((table_name, 0, "SKIPPED"))
            continue

        # Create table from CSV using read_files
        spark.sql(f"""
            CREATE OR REPLACE TABLE {full_table}
            COMMENT '{description}'
            AS SELECT * FROM read_files(
                '{csv_file}',
                format => 'csv',
                header => 'true',
                inferSchema => 'true',
                rescuedDataColumn => '_rescued_data'
            )
        """)

        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_table}").collect()[0]["cnt"]
        col_count = len(spark.table(full_table).columns)
        print(f"  Ingested: {row_count:,} rows, {col_count} columns")
        results.append((table_name, row_count, "OK"))

    except Exception as e:
        print(f"  ERROR: {e}")
        results.append((table_name, 0, f"ERROR: {str(e)[:100]}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Summary

# COMMAND ----------

print("\n" + "=" * 70)
print("INGESTION SUMMARY")
print("=" * 70)
total = 0
for name, count, status in results:
    print(f"  {name:<45} {count:>10,} rows [{status}]")
    total += count
print(f"\n  Total rows ingested: {total:,}")

errors = [r for r in results if r[2].startswith("ERROR")]
skipped = [r for r in results if r[2] == "SKIPPED"]
if errors:
    print(f"\n  ERRORS ({len(errors)}):")
    for name, _, status in errors:
        print(f"    - {name}: {status}")
if skipped:
    print(f"\n  SKIPPED ({len(skipped)}):")
    for name, _, _ in skipped:
        print(f"    - {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Tables

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
print(f"\nTables in {catalog}.{schema}:")
for t in tables:
    tbl = f"{catalog}.{schema}.{t['tableName']}"
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {tbl}").collect()[0]["c"]
    print(f"  {t['tableName']:<45} {cnt:>10,} rows")
