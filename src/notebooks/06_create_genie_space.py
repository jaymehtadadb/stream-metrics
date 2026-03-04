# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Create & Configure Genie Space
# MAGIC
# MAGIC Creates a new Genie Space from scratch and applies the full configuration:
# MAGIC - Data source tables (all 12 Stream Metrics tables)
# MAGIC - Text instructions (domain terms, query patterns, formatting rules)
# MAGIC - Join specifications (7 cross-table joins)
# MAGIC - SQL expression snippets (measures, filters, dimensions)
# MAGIC - Sample questions (10 curated examples)
# MAGIC - Benchmarks (15 question-SQL pairs for Genie training)
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema`: Schema name
# MAGIC - `warehouse_id`: SQL Warehouse ID for the Genie Space

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")
dbutils.widgets.text("warehouse_id", "862f1d757f0424f7", "SQL Warehouse ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")

FQN = f"{catalog}.{schema}"
print(f"Target: {FQN}")
print(f"Warehouse: {warehouse_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")
dbutils.widgets.text("warehouse_id", "862f1d757f0424f7", "SQL Warehouse ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")
FQN = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Definitions

# COMMAND ----------

TABLES = [
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text Instructions

# COMMAND ----------

GENERAL_INSTRUCTIONS = f"""This Genie Space contains real streaming media distribution data from Stream Metrics, covering two domains: VOD (Video On Demand) availability and content windowing. Data covers US and CA markets across 12+ streaming services.

**Key abbreviations and terms:**

- VOD = Video On Demand — content available to watch at any time
- SVOD = Subscription VOD (e.g., Amazon Prime, Netflix equivalents)
- AVOD = Ad-supported VOD (e.g., Pluto TV On Demand, Tubi On Demand, Roku On Demand)
- TVOD = Transactional VOD — pay-per-view or rental
- EST = Electronic Sell-Through — digital purchase to own
- MoM = Month-over-Month — refers to changes between consecutive months
- OTT = Over-The-Top — streaming content delivered directly over the internet
- Windowing = The sequential release strategy where content moves through exclusive distribution windows over time
- SM Original = Stream Metrics Original — title must premiere on that streaming platform before any other money-making platform globally AND the Network Name must be the same as the Provider Name
- Off-net = Series that premiered on linear TV before any streaming platform
- Exclusive = Title available on one and only one AVOD or SVOD service within a month

**Platform name conventions:**
- Always use exact service names: Amazon Prime, Pluto TV On Demand, Roku On Demand, Tubi On Demand, FilmRise, Samsung TV Plus On Demand, Vizio WatchFree+ On Demand, Xumo On Demand
- When users say "Amazon" they mean Amazon Prime
- When users say "Pluto" they mean Pluto TV On Demand
- When users say "Roku" they mean Roku On Demand
- When users say "Tubi" they mean Tubi On Demand
- When users say "Samsung" they mean Samsung TV Plus On Demand

**Data Domain Guidance:**

VOD Title Availability tables:
- Use vod_title_availability_detail for title-level questions: what titles are on which services, exclusivity, genre breakdowns, content type (Series vs Non-series), runtime, IMDb ratings, distributor analysis
- Use vod_title_availability_summary for aggregated service-level metrics: total titles, series vs movies counts, originals, scripted vs unscripted, off-net counts, runtime minutes
- Use vod_episode_availability_detail for episode-level drill-downs: season/episode counts per service, episode completeness
- Use vod_title_availability_native for monthly presence grids: columns like feb_2025, mar_2025 indicate title counts per service per month
- Use vod_title_changes_us_ca for title-level adds/drops: streaming_start_date, streaming_end_date, is_country_exclusive flag
- Use vod_title_mom_changes for aggregated MoM change counts by service: monthly columns show add/drop counts. The change_type column has values: Added, Dropped

Windowing tables:
- Use window_series_trends for aggregated series windowing metrics: avg_days, avg_days_trend, series counts by service
- Use window_series_detail for individual series streaming windows: up to 18 streaming windows with start_date, end_date, and days for each
- Use window_series_seasons for season-level windowing: up to 27 streaming windows per season
- Use window_series_episodes for episode-level windowing: up to 26 streaming windows per episode
- Use window_non_series_detail for movie/special windowing: similar to window_series_detail but for non-series content
- Use window_non_series_trends for aggregated movie windowing metrics: breakdowns by genre and by distributor

**Query Patterns & Business Logic:**

- When asked about "top services" or "biggest platforms," rank by matched_titles from vod_title_availability_summary WHERE offer_type = 'Total'
- When asked about "content diversity," count distinct category or genre values from vod_title_availability_detail
- When asked about "exclusive content," use is_exclusive_avod_and_svod = 1 from vod_title_availability_detail
- When asked about "originals," use sm_originals = 1 flag
- When asked about "off-net" content, use is_off_net = 1 flag
- When asked about "scripted vs unscripted," use is_scripted flag (1=Scripted, 0=Unscripted) or the category field
- For "MoM changes," use vod_title_mom_changes table. change_type = 'Added' or 'Dropped'
- For "recently added/dropped," use vod_title_changes_us_ca with current_month_changes column
- For "window duration," use avg_days from window_series_trends or window_non_series_trends
- Use offer_type = 'Total' for overall service comparisons
- Use MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) for chronological latest period

**Formatting & Presentation:**
- Always round percentages to 2 decimal places
- Format large numbers with commas for readability
- When showing date ranges, use YYYY-MM-DD format
- For duration analysis (avg_days), present averages rounded to whole days
- When showing top-N lists, default to top 10 unless the user specifies otherwise
- Order results by the most meaningful metric in descending order unless asked otherwise

**Summary Instructions:**
- Include the total row count when applicable
- When summarizing service data, always mention the country scope (US, CA)
- Use bullet points for multi-part summaries
- Highlight notable outliers or patterns
- When showing time-based trends, note the direction of change"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Questions

# COMMAND ----------

SAMPLE_QUESTIONS = [
    "How many total titles does each streaming service carry?",
    "Which service has the most exclusive AVOD or SVOD content?",
    "What is the average window duration in days for series content by streaming service?",
    "Show me the top 20 titles available on the most streaming services",
    "How does the scripted vs unscripted series split compare across services?",
    "What are the month-over-month content adds and drops by service?",
    "Which distributors have the most content across streaming platforms?",
    "Compare originals percentage across all streaming services",
    "What categories have the most titles available?",
    "Show episode counts by service for the top series",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark Question-SQL Pairs

# COMMAND ----------

BENCHMARKS = [
    {
        "question": "How many total titles does each streaming service carry?",
        "sql": f"SELECT service_name, CAST(matched_titles AS INT) AS total_titles, CAST(series AS INT) AS series_count, CAST(movies_non_series AS INT) AS movies_count FROM {FQN}.vod_title_availability_summary WHERE report_interval = 'Monthly' AND current_to = (SELECT MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) FROM {FQN}.vod_title_availability_summary) AND offer_type = 'Total' ORDER BY total_titles DESC"
    },
    {
        "question": "Which service has the most exclusive content?",
        "sql": f"SELECT service_name, COUNT(*) AS total_titles, SUM(CASE WHEN is_exclusive_avod_and_svod = 1 THEN 1 ELSE 0 END) AS exclusive_titles, ROUND(SUM(CASE WHEN is_exclusive_avod_and_svod = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS exclusive_pct FROM {FQN}.vod_title_availability_detail GROUP BY service_name ORDER BY exclusive_titles DESC"
    },
    {
        "question": "What is the average window duration in days for series content by streaming service?",
        "sql": f"SELECT service_name, ROUND(AVG(avg_days)) AS avg_window_days, SUM(CAST(series AS INT)) AS total_series FROM {FQN}.window_series_trends WHERE avg_days IS NOT NULL AND window_type IS NOT NULL GROUP BY service_name ORDER BY avg_window_days DESC"
    },
    {
        "question": "What categories have the most titles?",
        "sql": f"SELECT category, COUNT(DISTINCT title) AS title_count FROM {FQN}.vod_title_availability_detail WHERE category IS NOT NULL GROUP BY category ORDER BY title_count DESC"
    },
    {
        "question": "Compare originals percentage across streaming services",
        "sql": f"SELECT service_name, CAST(matched_titles AS INT) AS total_titles, CAST(sm_originals AS INT) AS originals, ROUND(CAST(sm_originals AS DOUBLE) * 100.0 / NULLIF(CAST(matched_titles AS INT), 0), 1) AS originals_pct FROM {FQN}.vod_title_availability_summary WHERE report_interval = 'Monthly' AND current_to = (SELECT MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) FROM {FQN}.vod_title_availability_summary) AND offer_type = 'Total' ORDER BY originals_pct DESC"
    },
    {
        "question": "How does scripted vs unscripted content compare across services?",
        "sql": f"SELECT service_name, CAST(scripted_series AS INT) AS scripted, CAST(unscripted_series AS INT) AS unscripted, ROUND(CAST(scripted_series AS DOUBLE) * 100.0 / NULLIF(CAST(series AS INT), 0), 1) AS scripted_pct FROM {FQN}.vod_title_availability_summary WHERE report_interval = 'Monthly' AND current_to = (SELECT MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) FROM {FQN}.vod_title_availability_summary) AND offer_type = 'Total' ORDER BY scripted_pct DESC"
    },
    {
        "question": "Which distributors have the most content?",
        "sql": f"SELECT distributor, COUNT(DISTINCT title) AS title_count, COUNT(DISTINCT service_name) AS services FROM {FQN}.vod_title_availability_detail WHERE distributor IS NOT NULL GROUP BY distributor ORDER BY title_count DESC LIMIT 20"
    },
    {
        "question": "How many episodes are available per service?",
        "sql": f"SELECT service_name, COUNT(DISTINCT series_title) AS series_count, COUNT(*) AS total_episodes FROM {FQN}.vod_episode_availability_detail GROUP BY service_name ORDER BY total_episodes DESC"
    },
    {
        "question": "What titles were recently added or dropped in the US?",
        "sql": f"SELECT title, service_name, current_month_changes, streaming_start_date, streaming_end_date, content_type, category FROM {FQN}.vod_title_changes_us_ca WHERE country = 'US' AND current_month_changes IS NOT NULL ORDER BY CASE WHEN current_month_changes = 'Added' THEN streaming_start_date ELSE streaming_end_date END DESC LIMIT 30"
    },
    {
        "question": "What is the average window duration for non-series content by service?",
        "sql": f"SELECT service_name, ROUND(AVG(avg_days)) AS avg_window_days, SUM(CAST(non_series AS INT)) AS total_movies FROM {FQN}.window_non_series_trends WHERE avg_days IS NOT NULL AND window_type IS NOT NULL GROUP BY service_name ORDER BY avg_window_days DESC"
    },
    {
        "question": "Show the top 20 titles that appear on the most streaming services",
        "sql": f"SELECT title, content_type, category, genre, COUNT(DISTINCT service_name) AS service_count FROM {FQN}.vod_title_availability_detail GROUP BY title, content_type, category, genre ORDER BY service_count DESC LIMIT 20"
    },
    {
        "question": "How does US content compare across services?",
        "sql": f"SELECT service_name, COUNT(*) AS total_titles, SUM(CASE WHEN is_us = 1 THEN 1 ELSE 0 END) AS us_titles, ROUND(SUM(CASE WHEN is_us = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS us_pct FROM {FQN}.vod_title_availability_detail GROUP BY service_name ORDER BY us_pct DESC"
    },
    {
        "question": "What is the average IMDb rating by category?",
        "sql": f"SELECT category, ROUND(AVG(imdb_rating), 2) AS avg_rating, COUNT(*) AS title_count FROM {FQN}.vod_title_availability_detail WHERE imdb_rating IS NOT NULL AND category IS NOT NULL GROUP BY category ORDER BY avg_rating DESC"
    },
    {
        "question": "Compare US vs CA title availability by service",
        "sql": f"SELECT service_name, country, SUM(CASE WHEN current_month_changes = 'Added' THEN 1 ELSE 0 END) AS added, SUM(CASE WHEN current_month_changes = 'Dropped' THEN 1 ELSE 0 END) AS dropped FROM {FQN}.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL GROUP BY service_name, country ORDER BY service_name, country"
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Specifications

# COMMAND ----------

JOIN_SPECS = [
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "vod_episode_availability_detail",
        "condition": "`vod_title_availability_detail`.`series_id` = `vod_episode_availability_detail`.`series_id` AND `vod_title_availability_detail`.`service_name` = `vod_episode_availability_detail`.`service_name`",
        "instruction": "Use to drill from title-level catalog data to episode-level detail. Match on series_id and service_name. Only series content has matching episode records.",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "vod_title_changes_us_ca",
        "condition": "`vod_title_availability_detail`.`series_id` = `vod_title_changes_us_ca`.`series_id` AND `vod_title_availability_detail`.`service_name` = `vod_title_changes_us_ca`.`service_name`",
        "instruction": "Use to correlate the full title catalog with recent adds/drops. The changes table has current_month_changes (Added/Dropped).",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "window_series_detail",
        "condition": "`vod_title_availability_detail`.`series_id` = `window_series_detail`.`series_id` AND `vod_title_availability_detail`.`service_name` = `window_series_detail`.`service_name`",
        "instruction": "Use to correlate VOD title availability with series streaming windows. The window_series_detail table has up to 18 streaming windows.",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "window_non_series_detail",
        "condition": "`vod_title_availability_detail`.`program_id` = `window_non_series_detail`.`program_id` AND `vod_title_availability_detail`.`service_name` = `window_non_series_detail`.`service_name`",
        "instruction": "Use to correlate VOD non-series (movie) availability with movie streaming windows. Only for content_type = 'Non-series'.",
    },
    {
        "left_table": "vod_title_availability_summary",
        "right_table": "window_series_trends",
        "condition": "`vod_title_availability_summary`.`service_name` = `window_series_trends`.`service_name` AND `vod_title_availability_summary`.`country` = `window_series_trends`.`country`",
        "instruction": "Use to compare service-level catalog size with windowing duration trends. Both tables have aggregated metrics per service.",
    },
    {
        "left_table": "vod_title_availability_summary",
        "right_table": "window_non_series_trends",
        "condition": "`vod_title_availability_summary`.`service_name` = `window_non_series_trends`.`service_name` AND `vod_title_availability_summary`.`country` = `window_non_series_trends`.`country`",
        "instruction": "Use to compare service-level catalog size with non-series windowing duration trends.",
    },
    {
        "left_table": "vod_episode_availability_detail",
        "right_table": "window_series_seasons",
        "condition": "`vod_episode_availability_detail`.`series_id` = `window_series_seasons`.`series_id` AND `vod_episode_availability_detail`.`season_number` = `window_series_seasons`.`season_number` AND `vod_episode_availability_detail`.`service_name` = `window_series_seasons`.`service_name`",
        "instruction": "Use cautiously - both tables have many rows. Always pre-aggregate one side first. Useful for checking if episode availability aligns with season-level streaming windows.",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Genie Space Payload

# COMMAND ----------

import json
import uuid


def make_id():
    return uuid.uuid4().hex


def build_genie_payload():
    """Build the complete serialized space JSON for the Genie API."""

    data_sources = {
        "tables": [{"identifier": f"{FQN}.{t}"} for t in TABLES]
    }

    sample_questions = [
        {"id": make_id(), "question": [q]}
        for q in SAMPLE_QUESTIONS
    ]
    sample_questions.sort(key=lambda x: x["id"])

    content_lines = GENERAL_INSTRUCTIONS.split("\n")
    content_array = [line + "\n" for line in content_lines]

    text_instructions = [{"id": make_id(), "content": content_array}]

    measures = [
        {"id": make_id(), "alias": "Exclusive Content Percentage",
         "sql": ["ROUND(SUM(CASE WHEN is_exclusive_avod_and_svod = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM vod_title_availability_detail"],
         "display_name": "Exclusive Content %"},
        {"id": make_id(), "alias": "SM Originals Count",
         "sql": ["SUM(CASE WHEN sm_originals = 1 THEN 1 ELSE 0 END) FROM vod_title_availability_detail"],
         "display_name": "Originals Count"},
        {"id": make_id(), "alias": "Average Window Duration",
         "sql": ["ROUND(AVG(avg_days), 0) FROM window_series_trends WHERE avg_days IS NOT NULL"],
         "display_name": "Avg Window Days"},
        {"id": make_id(), "alias": "Service Library Size",
         "sql": ["matched_titles FROM vod_title_availability_summary WHERE offer_type = 'Total'"],
         "display_name": "Library Size"},
        {"id": make_id(), "alias": "Scripted Ratio",
         "sql": ["ROUND(SUM(CASE WHEN is_scripted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM vod_title_availability_detail"],
         "display_name": "Scripted %"},
    ]

    filters = [
        {"id": make_id(), "sql": ["is_exclusive_avod_and_svod = 1"], "display_name": "Exclusive Only"},
        {"id": make_id(), "sql": ["sm_originals = 1"], "display_name": "SM Originals Only"},
        {"id": make_id(), "sql": ["is_off_net = 1"], "display_name": "Off-Net Only"},
        {"id": make_id(), "sql": ["is_scripted = 1"], "display_name": "Scripted Only"},
        {"id": make_id(), "sql": ["country = 'US' OR is_us = 1"], "display_name": "US Content"},
        {"id": make_id(), "sql": ["offer_type = 'Total'"], "display_name": "Total Offer Type"},
        {"id": make_id(), "sql": ["report_interval = 'Monthly'"], "display_name": "Monthly Interval"},
    ]

    expressions = [
        {"id": make_id(), "alias": "Content Type", "sql": ["content_type"]},
        {"id": make_id(), "alias": "Offer Type", "sql": ["offer_type"]},
        {"id": make_id(), "alias": "Category", "sql": ["category"]},
        {"id": make_id(), "alias": "Business Model", "sql": ["business_model"]},
        {"id": make_id(), "alias": "Country", "sql": ["country"]},
        {"id": make_id(), "alias": "Service Name", "sql": ["service_name"]},
    ]

    for arr in [measures, filters, expressions]:
        arr.sort(key=lambda x: x["id"])

    sql_snippets = {"measures": measures, "filters": filters, "expressions": expressions}

    example_question_sqls = [
        {"id": make_id(), "question": [b["question"]], "sql": [b["sql"]]}
        for b in BENCHMARKS
    ]
    example_question_sqls.sort(key=lambda x: x["id"])

    benchmark_questions = [
        {"id": make_id(), "question": [b["question"]],
         "answer": [{"format": "SQL", "content": [b["sql"]]}]}
        for b in BENCHMARKS
    ]
    benchmark_questions.sort(key=lambda x: x["id"])

    join_specs = []
    for js in JOIN_SPECS:
        join_specs.append({
            "id": make_id(),
            "left": {"identifier": f"{FQN}.{js['left_table']}", "alias": js["left_table"]},
            "right": {"identifier": f"{FQN}.{js['right_table']}", "alias": js["right_table"]},
            "sql": [js["condition"], "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"],
            "instruction": [js["instruction"]],
        })
    join_specs.sort(key=lambda x: x["id"])

    instructions = {
        "text_instructions": text_instructions,
        "example_question_sqls": example_question_sqls,
        "sql_snippets": sql_snippets,
        "join_specs": join_specs,
    }

    return {
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": data_sources,
        "instructions": instructions,
        "benchmarks": {"questions": benchmark_questions},
    }


payload = build_genie_payload()
print(f"Payload built with:")
print(f"  Tables:            {len(payload['data_sources']['tables'])}")
print(f"  Sample questions:  {len(payload['config']['sample_questions'])}")
print(f"  Benchmarks:        {len(payload['benchmarks']['questions'])}")
print(f"  Join specs:        {len(payload['instructions']['join_specs'])}")
print(f"  SQL expressions:   {len(payload['instructions']['example_question_sqls'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Genie Space

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

SPACE_TITLE = "Stream Metrics Intelligence"
SPACE_DESCRIPTION = "Streaming media distribution analytics powered by Stream Metrics data. Covers VOD title availability, content windowing, and competitive intelligence across US and CA markets."

print(f"Creating Genie Space: {SPACE_TITLE}")
print(f"Warehouse: {warehouse_id}")

resp = w.genie.create_space(
    title=SPACE_TITLE,
    description=SPACE_DESCRIPTION,
    warehouse_id=warehouse_id,
    serialized_space=json.dumps(payload),
)

space_id = resp.space_id
print(f"\nGenie Space created successfully!")
print(f"  Space ID: {space_id}")
print(f"  Title:    {SPACE_TITLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Genie Space

# COMMAND ----------

verify = w.genie.get_space(space_id=space_id, include_serialized_space=True)
verify_data = json.loads(verify.serialized_space)

print(f"Verification of Genie Space {space_id}:")
print(f"  Title:             {verify.title}")
print(f"  Tables:            {len(verify_data.get('data_sources', {}).get('tables', []))}")
print(f"  Sample questions:  {len(verify_data.get('config', {}).get('sample_questions', []))}")
print(f"  Text instructions: {len(verify_data.get('instructions', {}).get('text_instructions', []))}")
print(f"  Join specs:        {len(verify_data.get('instructions', {}).get('join_specs', []))}")
print(f"  Benchmarks:        {len(verify_data.get('benchmarks', {}).get('questions', []))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Space ID for Downstream Use
# MAGIC
# MAGIC The Space ID is set as a task value so the app deployment can reference it.

# COMMAND ----------

dbutils.jobs.taskValues.set(key="genie_space_id", value=space_id)
print(f"\nGenie Space ID '{space_id}' stored as task value 'genie_space_id'")
print(f"\nTo use in the app, set GENIE_SPACE_ID={space_id}")
