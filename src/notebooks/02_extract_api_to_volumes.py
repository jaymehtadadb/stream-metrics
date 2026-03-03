# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Extract Stream Metrics API Data to Volumes
# MAGIC
# MAGIC Pulls data from the Stream Metrics Export API for all 12 reports,
# MAGIC saves as CSV files into a Unity Catalog volume.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Iterates through each report ID defined in the `REPORTS` mapping
# MAGIC 2. Calls the API in paginated batches (NDJSON format)
# MAGIC 3. Writes each report as a CSV file to `/Volumes/<catalog>/<schema>/raw_data/`
# MAGIC 4. Caps each report at `MAX_ROWS_PER_REPORT` rows to keep the dataset manageable
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema`: Schema name
# MAGIC - `api_key`: Stream Metrics API key
# MAGIC - `max_rows`: Maximum rows per report (default: 50000)

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")
dbutils.widgets.text("api_key", "", "Stream Metrics API Key")
dbutils.widgets.text("max_rows", "50000", "Max Rows Per Report")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
api_key = dbutils.widgets.get("api_key")
MAX_ROWS_PER_REPORT = int(dbutils.widgets.get("max_rows"))

if not api_key:
    raise ValueError("api_key parameter is required. Pass your Stream Metrics API key.")

VOLUME_PATH = f"/Volumes/{catalog}/{schema}/raw_data"
print(f"Target volume: {VOLUME_PATH}")
print(f"Max rows per report: {MAX_ROWS_PER_REPORT:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report Configuration
# MAGIC
# MAGIC Each report ID maps to a table name. The Stream Metrics API uses numeric report IDs.

# COMMAND ----------

REPORTS = {
    1:  "vod_title_availability_detail",
    2:  "vod_title_availability_summary",
    3:  "vod_episode_availability_detail",
    5:  "window_series_trends",
    6:  "window_series_detail",
    7:  "window_series_seasons",
    8:  "window_series_episodes",
    10: "window_non_series_detail",
    11: "window_non_series_trends",
    28: "vod_title_availability_native",
    32: "vod_title_mom_changes",
    34: "vod_title_changes_us_ca",
}

API_URL = "https://api.stream-metrics.com/api/public/v2/export-data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Function

# COMMAND ----------

import requests
import json
import csv
import os
from datetime import datetime


def export_report(report_id, report_name, api_key, volume_path, max_rows):
    """
    Export a single report from the Stream Metrics API to a CSV in the volume.
    Returns (csv_path, row_count).
    """
    csv_path = f"{volume_path}/{report_name}.csv"
    batch_number = 1
    total_fetched = 0
    start_time = datetime.now()
    header_written = False
    fieldnames = None

    print(f"\n--- Report {report_id}: {report_name} (max {max_rows:,} rows) ---")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "StreamMetrics-DAB-Export/1.0",
        "Accept": "application/x-ndjson, application/json",
        "Content-Type": "application/json",
    })

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = None

        while total_fetched < max_rows:
            try:
                params = {"apiKey": api_key, "reportId": report_id}
                json_data = {"batchNumber": batch_number, "compress": False}

                res = session.post(
                    API_URL, params=params, json=json_data,
                    stream=True, timeout=600
                )

                if res.status_code not in [200, 201]:
                    error_text = res.text
                    if res.status_code == 400 and "No data found" in error_text:
                        print(f"  Batch {batch_number}: End of data")
                    else:
                        print(f"  HTTP {res.status_code}: {error_text[:200]}")
                    break

                items = []
                for line in res.iter_lines(decode_unicode=True):
                    if line:
                        try:
                            items.append(json.loads(line))
                        except Exception as e:
                            print(f"  Parse error in batch {batch_number}: {e}")

                if not items:
                    print(f"  Batch {batch_number}: Empty - end of data")
                    break

                remaining = max_rows - total_fetched
                if len(items) > remaining:
                    items = items[:remaining]

                if writer is None:
                    fieldnames = list(items[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                for row in items:
                    writer.writerow({k: row.get(k, "") for k in fieldnames})

                total_fetched += len(items)
                print(f"  Batch {batch_number}: {len(items)} rows | Total: {total_fetched:,}")
                batch_number += 1

            except Exception as e:
                print(f"  Error in batch {batch_number}: {e}")
                break

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"  Done: {total_fetched:,} rows in {elapsed:.1f}s -> {csv_path}")
    return csv_path, total_fetched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Export for All Reports

# COMMAND ----------

results = []
for report_id, report_name in REPORTS.items():
    csv_path, row_count = export_report(
        report_id, report_name, api_key, VOLUME_PATH, MAX_ROWS_PER_REPORT
    )
    results.append((report_id, report_name, csv_path, row_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Summary

# COMMAND ----------

print("=" * 70)
print("EXPORT SUMMARY")
print("=" * 70)
total_rows = 0
for rid, name, path, count in results:
    status = "OK" if count > 0 else "EMPTY"
    print(f"  Report {rid:>2}: {name:<45} {count:>10,} rows [{status}]")
    total_rows += count

print(f"\n  Total: {total_rows:,} rows across {len(results)} reports")
print(f"  Volume: {VOLUME_PATH}")

failed = [r for r in results if r[3] == 0]
if failed:
    print(f"\n  WARNING: {len(failed)} report(s) returned 0 rows:")
    for rid, name, _, _ in failed:
        print(f"    - Report {rid}: {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Volume Contents

# COMMAND ----------

files = dbutils.fs.ls(f"dbfs:{VOLUME_PATH}")
print(f"\nFiles in {VOLUME_PATH}:")
for f in files:
    size_mb = f.size / (1024 * 1024)
    print(f"  {f.name:<55} {size_mb:>8.2f} MB")
