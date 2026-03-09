# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Best Practices Workshop
# MAGIC ## Building an AI-Powered Data Explorer with Stream Metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Welcome!** In this workshop, you'll learn how to build a production-quality Databricks Genie Space from scratch. We'll use real streaming media distribution data from [Stream Metrics](https://www.stream-metrics.com/) — the industry standard for tracking VOD title availability and content windowing across major streaming platforms.
# MAGIC
# MAGIC ### What You'll Build
# MAGIC
# MAGIC By the end of this workshop, you'll have a fully functional Genie Space that allows business users to ask natural language questions like:
# MAGIC
# MAGIC - *"How many total titles does each streaming service carry?"*
# MAGIC - *"Which service has the most exclusive content?"*
# MAGIC - *"What is the average window duration for series content by service?"*
# MAGIC - *"Compare originals percentage across all streaming services"*
# MAGIC
# MAGIC ...and Genie will automatically generate the correct SQL and return results.
# MAGIC
# MAGIC ### What is Genie?
# MAGIC
# MAGIC Databricks Genie is an AI-powered natural language interface that translates business questions into SQL queries. Unlike generic AI chatbots, Genie is purpose-built for structured data analysis:
# MAGIC
# MAGIC - It **understands your schema** — table relationships, column semantics, and data types
# MAGIC - It **follows your instructions** — domain-specific terminology, business logic, and formatting rules
# MAGIC - It **learns from benchmarks** — example question-SQL pairs that teach it the correct query patterns
# MAGIC - It **respects governance** — uses Unity Catalog permissions, so users only see data they're authorized to access
# MAGIC
# MAGIC ### Workshop Outline
# MAGIC
# MAGIC | Step | Topic | Why It Matters |
# MAGIC |------|-------|----------------|
# MAGIC | 1 | Understand the Data | You can't teach Genie what you don't understand yourself |
# MAGIC | 2 | Add Table & Column Comments | Comments are the #1 input Genie uses to understand your data |
# MAGIC | 3 | Create the Genie Space | The container for all your configuration |
# MAGIC | 4 | Write Text Instructions | Domain knowledge, abbreviations, query patterns |
# MAGIC | 5 | Define Join Specifications | Teach Genie how tables relate to each other |
# MAGIC | 6 | Add SQL Expression Snippets | Reusable measures, filters, and dimensions |
# MAGIC | 7 | Add Sample Questions | Curated examples shown in the Genie UI |
# MAGIC | 8 | Add Benchmarks | Question-SQL pairs that train Genie's accuracy |
# MAGIC | 9 | Test & Iterate | Verify results and refine |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - Databricks workspace with Unity Catalog enabled
# MAGIC - Access to a SQL Warehouse
# MAGIC - The Stream Metrics tables already ingested into `<your_catalog>.stream_metrics`
# MAGIC - `databricks-sdk` Python package

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 0: Configuration
# MAGIC
# MAGIC Set your catalog, schema, and warehouse ID. These will be used throughout the notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")
dbutils.widgets.text("warehouse_id", "862f1d757f0424f7", "SQL Warehouse ID")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")

FQN = f"{catalog}.{schema}"
print(f"📍 Working with: {FQN}")
print(f"📍 SQL Warehouse: {warehouse_id}")

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Re-read widgets after Python restart
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")
FQN = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 1: Understand the Data
# MAGIC
# MAGIC Before building a Genie Space, you need to deeply understand the data. This is the most important step — **Genie is only as good as the context you give it.**
# MAGIC
# MAGIC ### The Stream Metrics Dataset
# MAGIC
# MAGIC Stream Metrics tracks the availability, licensing, and windowing of content across streaming platforms. Our dataset has **12 tables** organized into three domains:
# MAGIC
# MAGIC #### Domain 1: VOD Title Availability (6 tables)
# MAGIC
# MAGIC | Table | Grain | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | `vod_title_availability_detail` | One row per title × service × month | The core catalog — every title on every service with full metadata (genre, distributor, IMDb rating, runtime, originals flags, etc.) |
# MAGIC | `vod_title_availability_summary` | One row per service × country × period | Aggregated counts — total titles, series, movies, originals, scripted/unscripted, runtime minutes |
# MAGIC | `vod_episode_availability_detail` | One row per episode × service × month | Episode-level detail with season/episode numbers, runtime, and content classification |
# MAGIC | `vod_title_availability_native` | One row per title × service | Monthly presence grid — columns like `feb_2025`, `mar_2025` show when titles appeared |
# MAGIC | `vod_title_changes_us_ca` | One row per title change event | Title-level adds/drops with streaming start/end dates, exclusivity, pageviews, and IMDb data |
# MAGIC | `vod_title_mom_changes` | One row per service × content type × change type | Month-over-month aggregated add/drop counts with monthly columns |
# MAGIC
# MAGIC #### Domain 2: Content Windowing — Series (4 tables)
# MAGIC
# MAGIC | Table | Grain | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | `window_series_trends` | One row per service × country × period | Aggregated avg window duration, breakdowns by scripted/unscripted, off-net, US/international |
# MAGIC | `window_series_detail` | One row per series × service | Up to 18 streaming windows with start/end dates and day counts |
# MAGIC | `window_series_seasons` | One row per season × service | Season-level windowing with up to 27 streaming windows |
# MAGIC | `window_series_episodes` | One row per episode × service | Episode-level windowing with up to 26 streaming windows |
# MAGIC
# MAGIC #### Domain 3: Content Windowing — Non-Series (2 tables)
# MAGIC
# MAGIC | Table | Grain | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | `window_non_series_detail` | One row per movie × service | Movie windowing with theatrical flags, media type, and up to 23 windows |
# MAGIC | `window_non_series_trends` | One row per service × country × period | Aggregated movie windowing with breakdowns by genre and major distributor |
# MAGIC
# MAGIC ### Key Concepts for Genie
# MAGIC
# MAGIC Understanding these domain concepts is critical for writing good Genie instructions:
# MAGIC
# MAGIC - **SM Original** = Title that premiered on that streaming platform before any other money-making platform globally, AND the Network Name matches the Provider Name
# MAGIC - **Off-net** = Series that first aired on traditional linear TV before moving to streaming
# MAGIC - **Exclusive** = Title available on one and only one AVOD or SVOD service within a month
# MAGIC - **Windowing** = The strategy of releasing content through sequential exclusive distribution windows over time
# MAGIC - **offer_type = 'Total'** = Rollup across all offer types (AVOD, SVOD, etc.) — use this for service-level comparisons
# MAGIC - **report_interval = 'Monthly'** = Monthly aggregation — the most common interval for current data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's Explore the Data
# MAGIC
# MAGIC Run these queries to familiarize yourself with the data shape and content.

# COMMAND ----------

# MAGIC %md
# MAGIC #### How many rows are in each table?

# COMMAND ----------

for table_name in [
    "vod_title_availability_detail", "vod_title_availability_summary",
    "vod_episode_availability_detail", "vod_title_availability_native",
    "vod_title_changes_us_ca", "vod_title_mom_changes",
    "window_series_trends", "window_series_detail",
    "window_series_seasons", "window_series_episodes",
    "window_non_series_detail", "window_non_series_trends",
]:
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {FQN}.{table_name}").collect()[0]["c"]
    print(f"  {table_name:<45} {cnt:>10,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC #### What streaming services are in the data?

# COMMAND ----------

display(spark.sql(f"""
    SELECT service_name, COUNT(DISTINCT title) AS titles, country
    FROM {FQN}.vod_title_changes_us_ca
    WHERE current_month_changes IS NOT NULL
    GROUP BY service_name, country
    ORDER BY titles DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC #### What does the summary table look like?

# COMMAND ----------

display(spark.sql(f"""
    SELECT service_name, country, offer_type, 
           CAST(matched_titles AS INT) AS total_titles,
           CAST(series AS INT) AS series_count,
           CAST(movies_non_series AS INT) AS movies,
           CAST(sm_originals AS INT) AS originals
    FROM {FQN}.vod_title_availability_summary
    WHERE report_interval = 'Monthly'
      AND offer_type = 'Total'
      AND current_to = (
          SELECT MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy'))
          FROM {FQN}.vod_title_availability_summary
      )
    ORDER BY CAST(matched_titles AS INT) DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 2: Add Table & Column Comments
# MAGIC
# MAGIC ### Why This Is Critical
# MAGIC
# MAGIC **Table and column comments are the single most important input to Genie.** When a user asks a question, Genie reads the comments to understand:
# MAGIC
# MAGIC - What each table represents
# MAGIC - What each column means
# MAGIC - What values a column can take
# MAGIC - How columns relate to business concepts
# MAGIC
# MAGIC Without comments, Genie is guessing from column names alone. With good comments, Genie can reliably map business terms to the right columns and tables.
# MAGIC
# MAGIC ### Best Practices for Comments
# MAGIC
# MAGIC | Practice | Example |
# MAGIC |----------|---------|
# MAGIC | **Describe the business meaning**, not the data type | `"Flag (1,0) - SM Original: premiered on this streaming platform first"` not `"Integer column"` |
# MAGIC | **List valid values** for categorical columns | `"Offer type: AVOD, SVOD, TVOD, EST, FAST"` |
# MAGIC | **Explain calculations** | `"Calc: Streaming 1 End Date minus Streaming 1 Start Date"` |
# MAGIC | **Note gotchas** | `"Not fully populated — sourced from multiple public sites"` |
# MAGIC | **Use synonyms** | Include terms users might say: `"Total titles available (aka catalog size, library size)"` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Table-Level Comments
# MAGIC
# MAGIC First, add a clear description to each table. This tells Genie *when* to use each table.

# COMMAND ----------

TABLE_COMMENTS = {
    "vod_title_availability_detail": "VOD title-level availability detail across streaming services. Each row represents a title on a specific service for a given month. Contains content metadata, distribution info, availability flags, and runtime metrics. Use for title-level questions about what content is on which services.",
    "vod_title_availability_summary": "Aggregated VOD title availability summary by service and time period. Contains title counts and total minutes across dimensions: series vs movies, originals vs acquired, scripted vs unscripted, US vs international. Use for service-level comparisons and catalog size analysis.",
    "vod_episode_availability_detail": "Episode-level VOD availability detail for series content. Each row is an episode on a streaming service with episode metadata, content classification, distribution info, and runtime. Use for episode count analysis and season completeness questions.",
    "vod_title_availability_native": "Monthly presence grid showing which titles are available on each provider by month. Monthly columns (feb_2025, mar_2025, etc.) indicate title presence. Includes provider-reported metadata. Use for tracking title availability over time.",
    "vod_title_changes_us_ca": "Title-level changes (adds and drops) for US and Canada streaming services. Tracks month-over-month content changes with streaming dates, exclusivity flags, pageviews, IMDb ratings, and content metadata. Use for analyzing what was recently added or dropped.",
    "vod_title_mom_changes": "Aggregated month-over-month (MoM) change counts by service, country, and content type. Monthly columns show counts of titles added or dropped. Use for trend analysis of content churn across services.",
    "window_series_trends": "Aggregated windowing trend metrics for series content. Shows average window duration (days) and series counts by service, segmented by exclusive/shared, scripted/unscripted, off-net, US/international, and genre. Use for comparing windowing strategies across services.",
    "window_series_detail": "Detailed series-level windowing data showing up to 18 streaming windows per title. Each row is a series on a service with window start/end dates and durations. Use for individual title windowing analysis.",
    "window_series_seasons": "Season-level windowing data with up to 27 streaming windows per season. Breaks down series windowing by season number with season-specific metadata.",
    "window_series_episodes": "Episode-level windowing data with up to 26 streaming windows per episode. The most granular windowing view for licensing patterns.",
    "window_non_series_detail": "Detailed windowing data for movies and specials with up to 23 streaming windows. Contains theatrical status, media type, genre, release year, and window durations.",
    "window_non_series_trends": "Aggregated windowing trend metrics for movies. Shows average window duration and title counts by service, segmented by theatrical status, genre, and major distributor (Disney, Sony, Warner Bros, Universal, Paramount, Lionsgate).",
}

for table_name, comment in TABLE_COMMENTS.items():
    safe = comment.replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {FQN}.{table_name} IS '{safe}'")
    print(f"  ✓ {table_name}")

print(f"\n{len(TABLE_COMMENTS)} table comments applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Column-Level Comments
# MAGIC
# MAGIC Now add comments to the most important columns. You don't need to comment every column — focus on:
# MAGIC
# MAGIC 1. **Columns users will ask about** (service_name, title, category, etc.)
# MAGIC 2. **Flag columns** that need explanation (sm_originals, is_off_net, is_exclusive, etc.)
# MAGIC 3. **Columns with non-obvious semantics** (current_to, offer_type, report_interval)
# MAGIC
# MAGIC Here are the key columns for the most-used tables:

# COMMAND ----------

KEY_COLUMN_COMMENTS = {
    "vod_title_availability_detail": {
        "service_name": "Name of the provider service brand (e.g., Amazon Prime, Pluto TV On Demand, Roku On Demand, Tubi On Demand).",
        "title": "Title of a series or non-series (movie, event, other).",
        "country": "Country of the provider site services (US or CA).",
        "business_model": "Primary revenue source for the streaming service (SVOD, AVOD, vMVPD, TVE, BVOD).",
        "offer_type": "Name of the offer type (AVOD, SVOD, TVOD, etc.). Use 'Total' for rollup across all offer types.",
        "content_type": "Whether the title is a Series or Non-series.",
        "category": "Macro Genre (9 categories): Animation, Anime, Documentary, Film, Scripted, Sport, Stage, Stand up, Unscripted.",
        "genre": "Micro Genre - detailed descriptive genre with unlimited variations.",
        "sm_originals": "Flag (1,0) - SM Original: title premiered on this streaming platform before any other money-making platform globally AND the Network Name matches the Provider Name.",
        "is_exclusive_avod_and_svod": "Flag (1,0) - Title is exclusively available on one AVOD or SVOD service within a month.",
        "is_off_net": "Flag (1,0) - Series premiered on linear TV before any streaming platform.",
        "is_scripted": "Flag (1,0) - Scripted=1 (Anime, Animation, Film, Scripted), Unscripted=0 (Documentary, Sport, Stand up, Stage, Unscripted).",
        "is_us": "Flag (1,0) - Country of origin is the US.",
        "original_country": "Country of origin or production.",
        "start_year": "Premiere year for the title.",
        "runtime_sec": "Duration of a program in seconds.",
        "imdb_rating": "User rating from IMDb.com (0-10 scale).",
        "distributor": "Company that owns the distribution rights.",
        "network_group": "Company or media group that owns the original network.",
        "current_to": "The month the data covers. Format: 'MMM yyyy' (e.g., 'Jan 2026'). Use MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) for the latest period.",
        "mom_changes": "Month-over-month changes: 'added' or 'dropped' for that month and service.",
    },
    "vod_title_availability_summary": {
        "service_name": "Name of the provider service brand.",
        "report_interval": "Comparison time frame: 'Monthly', 'Quarterly', 'Annual'. Default to 'Monthly' for current data.",
        "current_to": "Period the data covers. Use MAX_BY for chronological latest.",
        "offer_type": "Offer type. Use 'Total' for service-level comparisons across all offer types.",
        "matched_titles": "Total tagged titles available (series + non-series). This is the catalog size.",
        "series": "Total series titles available.",
        "movies_non_series": "Total movie titles available.",
        "sm_originals": "Count of SM Originals on this service.",
        "scripted_series": "Total scripted series (Anime, Animation, Scripted genres).",
        "unscripted_series": "Total unscripted series (Documentary, Stand up, Stage, Unscripted genres).",
        "off_net_series": "Total Off-net Series (premiered on linear TV first).",
        "us": "Count of US-origin titles.",
        "intl": "Count of international (non-US) origin titles.",
    },
    "vod_title_changes_us_ca": {
        "service_name": "Name of the provider service brand.",
        "title": "Title of the series or movie.",
        "current_month_changes": "Whether the title was 'Added' or 'Dropped' comparing current month to previous.",
        "streaming_start_date": "Date the title was first carried by the service.",
        "streaming_end_date": "Date the title ceased streaming on the service.",
        "is_country_exclusive": "Exclusivity flag for the specific country across all tracked providers.",
        "pageviews_views": "Total page views for the title during that month — a proxy for audience interest.",
        "imdb_rating": "IMDb user rating (0-10 scale).",
        "imdb_votes": "Number of IMDb votes — indicates title popularity/awareness.",
    },
    "window_series_trends": {
        "service_name": "Name of the provider service brand.",
        "avg_days": "Average window duration in days. Excludes titles premiered before Sep 2020 and SM Originals.",
        "avg_days_trend": "Quarterly trend direction for the average window.",
        "series": "Count of series used to calculate the trend.",
        "avg_currently_shared_days": "Avg window for titles available on at least one other AVOD/SVOD provider.",
        "avg_currently_exclusive_days": "Avg window for titles NOT available on any other AVOD/SVOD provider.",
    },
}

total_applied = 0
for table_name, columns in KEY_COLUMN_COMMENTS.items():
    existing_cols = [c.name for c in spark.table(f"{FQN}.{table_name}").schema]
    applied = 0
    for col_name, comment in columns.items():
        if col_name in existing_cols:
            safe = comment.replace("'", "\\'")
            try:
                spark.sql(f"ALTER TABLE {FQN}.{table_name} ALTER COLUMN `{col_name}` COMMENT '{safe}'")
                applied += 1
            except Exception as e:
                print(f"  ⚠ {table_name}.{col_name}: {e}")
    print(f"  ✓ {table_name}: {applied} column comments")
    total_applied += applied

print(f"\nTotal: {total_applied} column comments applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Your Comments
# MAGIC
# MAGIC Check that comments are showing up correctly. This is what Genie will read.

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE {FQN}.vod_title_changes_us_ca"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 3: Create the Genie Space
# MAGIC
# MAGIC Now we create the Genie Space itself. A Genie Space is a container that holds:
# MAGIC
# MAGIC - **Data sources** — which tables Genie can query
# MAGIC - **Instructions** — text guidance, joins, SQL expressions
# MAGIC - **Sample questions** — examples shown in the UI
# MAGIC - **Benchmarks** — question-SQL pairs for training accuracy
# MAGIC
# MAGIC We'll create it with just the data sources first, then incrementally add instructions, joins, and benchmarks.
# MAGIC
# MAGIC ### Why Incremental?
# MAGIC
# MAGIC Building a Genie Space is iterative. You should:
# MAGIC 1. Start with tables and basic instructions
# MAGIC 2. Test with sample questions
# MAGIC 3. See where Genie gets confused
# MAGIC 4. Add joins, expressions, and benchmarks to fix those gaps
# MAGIC 5. Repeat

# COMMAND ----------

import json
import uuid
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def make_id():
    """Generate 32-char hex UUID required by the Genie API."""
    return uuid.uuid4().hex

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the Table List
# MAGIC
# MAGIC These are all 12 Stream Metrics tables that Genie will have access to.

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

data_sources = {"tables": [{"identifier": f"{FQN}.{t}"} for t in TABLES]}
print(f"Data sources: {len(data_sources['tables'])} tables")
for t in data_sources["tables"]:
    print(f"  • {t['identifier']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Space with Minimal Config
# MAGIC
# MAGIC We pass an initial `serialized_space` JSON with just the data sources. We'll add everything else in subsequent steps.

# COMMAND ----------

initial_payload = {
    "version": 2,
    "config": {"sample_questions": []},
    "data_sources": data_sources,
    "instructions": {},
    "benchmarks": {"questions": []},
}

resp = w.genie.create_space(
    title="Stream Metrics Workshop",
    description="Workshop Genie Space for exploring VOD availability and content windowing across streaming platforms.",
    warehouse_id=warehouse_id,
    serialized_space=json.dumps(initial_payload),
)

SPACE_ID = resp.space_id
print(f"✅ Genie Space created!")
print(f"   Space ID: {SPACE_ID}")
print(f"   Title:    Stream Metrics Workshop")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 4: Write Text Instructions
# MAGIC
# MAGIC Text instructions are free-form guidance that Genie reads before answering every question. Think of them as a **briefing document** for an analyst who's new to your data.
# MAGIC
# MAGIC ### What to Include
# MAGIC
# MAGIC | Category | What to Write | Why |
# MAGIC |----------|---------------|-----|
# MAGIC | **Domain terms & abbreviations** | Define VOD, SVOD, AVOD, windowing, SM Original, off-net, etc. | Users will use these terms; Genie needs to know what they mean |
# MAGIC | **Platform name conventions** | Map casual names ("Amazon") to exact values ("Amazon Prime") | Prevents mismatches on WHERE clauses |
# MAGIC | **Table selection guidance** | "Use vod_title_availability_summary for service-level comparisons" | Steers Genie to the right table |
# MAGIC | **Query patterns** | "When asked about 'top services', rank by matched_titles WHERE offer_type = 'Total'" | Teaches correct business logic |
# MAGIC | **Formatting rules** | "Round percentages to 2 decimal places" | Consistent, professional output |
# MAGIC | **Clarification prompts** | "When users ask about 'availability' without specifying, ask which table domain" | Prevents Genie from guessing wrong |
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Be specific, not generic.** "Use offer_type = 'Total' for service comparisons" is better than "filter appropriately"
# MAGIC - **Include column name references.** Genie needs to map concepts to actual columns
# MAGIC - **Use examples.** "When users say 'Amazon' they mean Amazon Prime"
# MAGIC - **Keep it structured.** Use headers, bullet points, and clear sections

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
- Always use exact service names found in the data: Amazon Prime, Pluto TV On Demand, Roku On Demand, Tubi On Demand, FilmRise, Samsung TV Plus On Demand, Vizio WatchFree+ On Demand, Xumo On Demand
- When users say "Amazon" they mean Amazon Prime
- When users say "Pluto" they mean Pluto TV On Demand
- When users say "Roku" they mean Roku On Demand
- When users say "Tubi" they mean Tubi On Demand
- When users say "Samsung" they mean Samsung TV Plus On Demand

**Data Domain Guidance:**

VOD Title Availability tables:
- Use vod_title_availability_detail for title-level questions: what titles are on which services, exclusivity, genre breakdowns, content type, runtime, IMDb ratings, distributor analysis
- Use vod_title_availability_summary for aggregated service-level metrics: total titles, series vs movies counts, originals, scripted vs unscripted, off-net counts, runtime minutes. Always filter offer_type = 'Total' for service-level comparisons
- Use vod_episode_availability_detail for episode-level drill-downs: season/episode counts per service
- Use vod_title_availability_native for monthly presence grids
- Use vod_title_changes_us_ca for title-level adds/drops: streaming_start_date, streaming_end_date, is_country_exclusive flag, pageviews_views, imdb_rating
- Use vod_title_mom_changes for aggregated MoM change counts by service. The change_type column has values: Added, Dropped

Windowing tables:
- Use window_series_trends for aggregated series windowing metrics: avg_days, avg_days_trend, series counts by service
- Use window_series_detail for individual series streaming windows: up to 18 windows with start/end dates
- Use window_series_seasons for season-level windowing
- Use window_series_episodes for episode-level windowing
- Use window_non_series_detail for movie/special windowing
- Use window_non_series_trends for aggregated movie windowing metrics

**Query Patterns & Business Logic:**

- When asked about "top services" or "biggest platforms," rank by matched_titles from vod_title_availability_summary WHERE offer_type = 'Total'
- When asked about "content diversity," count distinct category or genre values
- When asked about "exclusive content," use is_exclusive_avod_and_svod = 1
- When asked about "originals," use sm_originals = 1
- When asked about "off-net" content, use is_off_net = 1
- When asked about "scripted vs unscripted," use is_scripted flag (1=Scripted, 0=Unscripted)
- For "MoM changes," use vod_title_mom_changes with change_type = 'Added' or 'Dropped'
- For "recently added/dropped," use vod_title_changes_us_ca with current_month_changes column
- For "window duration," use avg_days from window_series_trends or window_non_series_trends
- Use report_interval = 'Monthly' for current data in summary tables
- Use MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) for chronological latest period (current_to is a string like 'Jan 2026')

**Formatting & Presentation:**
- Always round percentages to 2 decimal places
- Format large numbers with commas for readability
- For duration analysis (avg_days), present averages rounded to whole days
- When showing top-N lists, default to top 10 unless the user specifies otherwise
- Order results by the most meaningful metric in descending order
- When presenting service comparisons, always include the country context

**Summary Instructions:**
- Include the total row count when applicable
- When summarizing service data, always mention the country scope (US, CA)
- Use bullet points for multi-part summaries
- Highlight notable outliers or patterns
- When showing time-based trends, note the direction of change (increasing, decreasing, stable)"""

print(f"Instructions length: {len(GENERAL_INSTRUCTIONS):,} characters")
print(f"Instructions preview:\n{GENERAL_INSTRUCTIONS[:500]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Text Instructions to the Genie Space
# MAGIC
# MAGIC The `serialized_space` JSON uses this structure for text instructions:
# MAGIC ```json
# MAGIC {
# MAGIC   "instructions": {
# MAGIC     "text_instructions": [
# MAGIC       {
# MAGIC         "id": "<32-char hex UUID>",
# MAGIC         "content": ["line 1\n", "line 2\n", ...]
# MAGIC       }
# MAGIC     ]
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

resp = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
space_data = json.loads(resp.serialized_space)

content_lines = GENERAL_INSTRUCTIONS.split("\n")
content_array = [line + "\n" for line in content_lines]

if "instructions" not in space_data:
    space_data["instructions"] = {}

space_data["instructions"]["text_instructions"] = [
    {"id": make_id(), "content": content_array}
]

w.genie.update_space(
    space_id=SPACE_ID,
    serialized_space=json.dumps(space_data),
)

print("✅ Text instructions applied to Genie Space")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 5: Define Join Specifications
# MAGIC
# MAGIC ### Why Joins Matter for Genie
# MAGIC
# MAGIC Without explicit join specs, Genie has to **guess** how tables relate. It might:
# MAGIC - Join on the wrong columns
# MAGIC - Create Cartesian products
# MAGIC - Miss important filter conditions
# MAGIC
# MAGIC By defining joins explicitly, you teach Genie the correct relationships and when to use them.
# MAGIC
# MAGIC ### Join Spec Structure
# MAGIC
# MAGIC Each join spec has:
# MAGIC - **left / right** — the two tables being joined (with their fully qualified identifiers)
# MAGIC - **sql** — the join condition in backtick format: `` `table`.`column` = `table`.`column` ``
# MAGIC - **instruction** — guidance on *when* to use this join
# MAGIC - **relationship type** — appended as `--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--`
# MAGIC
# MAGIC ### Best Practices for Join Specs
# MAGIC
# MAGIC - **Always use composite keys** when a single column isn't unique (e.g., `series_id` + `service_name`)
# MAGIC - **Include instructions** that explain when to use the join and any gotchas
# MAGIC - **Mark many-to-many joins** with caution notes ("pre-aggregate one side first")
# MAGIC - **Use backtick format** for all table and column names in the SQL condition

# COMMAND ----------

JOIN_SPECS = [
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "vod_episode_availability_detail",
        "sql": "`vod_title_availability_detail`.`series_id` = `vod_episode_availability_detail`.`series_id` AND `vod_title_availability_detail`.`service_name` = `vod_episode_availability_detail`.`service_name`",
        "instruction": "Use to drill from title-level catalog data to episode-level detail. Match on series_id AND service_name for accurate episode counts per title per service. Only series content has matching episode records (content_type = 'Series').",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "vod_title_changes_us_ca",
        "sql": "`vod_title_availability_detail`.`series_id` = `vod_title_changes_us_ca`.`series_id` AND `vod_title_availability_detail`.`service_name` = `vod_title_changes_us_ca`.`service_name`",
        "instruction": "Use to correlate the full title catalog with recent adds/drops. The changes table has current_month_changes (Added/Dropped) and streaming start/end dates.",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "window_series_detail",
        "sql": "`vod_title_availability_detail`.`series_id` = `window_series_detail`.`series_id` AND `vod_title_availability_detail`.`service_name` = `window_series_detail`.`service_name`",
        "instruction": "Use to correlate VOD title availability with series streaming windows. The window_series_detail table has up to 18 streaming windows with start/end dates and day counts.",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "window_non_series_detail",
        "sql": "`vod_title_availability_detail`.`program_id` = `window_non_series_detail`.`program_id` AND `vod_title_availability_detail`.`service_name` = `window_non_series_detail`.`service_name`",
        "instruction": "Use to correlate VOD non-series (movie) availability with movie streaming windows. Match on program_id and service_name. Only for content_type = 'Non-series'.",
    },
    {
        "left_table": "vod_title_availability_summary",
        "right_table": "window_series_trends",
        "sql": "`vod_title_availability_summary`.`service_name` = `window_series_trends`.`service_name` AND `vod_title_availability_summary`.`country` = `window_series_trends`.`country`",
        "instruction": "Use to compare service-level catalog size with windowing duration trends. Both tables have aggregated metrics per service. Match on service_name and country.",
    },
    {
        "left_table": "vod_title_availability_summary",
        "right_table": "window_non_series_trends",
        "sql": "`vod_title_availability_summary`.`service_name` = `window_non_series_trends`.`service_name` AND `vod_title_availability_summary`.`country` = `window_non_series_trends`.`country`",
        "instruction": "Use to compare service-level catalog size with non-series (movie) windowing duration trends. Match on service_name and country.",
    },
    {
        "left_table": "vod_episode_availability_detail",
        "right_table": "window_series_seasons",
        "sql": "`vod_episode_availability_detail`.`series_id` = `window_series_seasons`.`series_id` AND `vod_episode_availability_detail`.`season_number` = `window_series_seasons`.`season_number` AND `vod_episode_availability_detail`.`service_name` = `window_series_seasons`.`service_name`",
        "instruction": "Use cautiously — both tables have many rows per title/season. Always pre-aggregate one side first. Useful for checking if episode availability aligns with season-level streaming windows.",
    },
]

join_specs_payload = []
for js in JOIN_SPECS:
    join_specs_payload.append({
        "id": make_id(),
        "left": {"identifier": f"{FQN}.{js['left_table']}", "alias": js["left_table"]},
        "right": {"identifier": f"{FQN}.{js['right_table']}", "alias": js["right_table"]},
        "sql": [js["sql"], "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"],
        "instruction": [js["instruction"]],
    })

join_specs_payload.sort(key=lambda x: x["id"])
print(f"Prepared {len(join_specs_payload)} join specifications:")
for js in JOIN_SPECS:
    print(f"  • {js['left_table']} ↔ {js['right_table']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Join Specs

# COMMAND ----------

resp = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
space_data = json.loads(resp.serialized_space)

space_data["instructions"]["join_specs"] = join_specs_payload

w.genie.update_space(
    space_id=SPACE_ID,
    serialized_space=json.dumps(space_data),
)

print(f"✅ {len(join_specs_payload)} join specifications applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 6: Add SQL Expression Snippets
# MAGIC
# MAGIC SQL snippets are pre-defined expressions that Genie can reuse. There are three types:
# MAGIC
# MAGIC | Type | Purpose | Example |
# MAGIC |------|---------|---------|
# MAGIC | **Measures** | Aggregation formulas | `ROUND(SUM(CASE WHEN sm_originals = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)` |
# MAGIC | **Filters** | Boolean conditions | `offer_type = 'Total'` |
# MAGIC | **Expressions** | Dimension/grouping columns | `service_name`, `category`, `content_type` |
# MAGIC
# MAGIC ### Why This Helps
# MAGIC
# MAGIC Instead of Genie inventing its own aggregation logic, it can reference these trusted expressions. This is especially important for:
# MAGIC - Complex CASE statements (exclusive content %, scripted ratio)
# MAGIC - Non-obvious filter conditions (offer_type = 'Total', report_interval = 'Monthly')
# MAGIC - Derived metrics (content churn rate, originals percentage)

# COMMAND ----------

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

print(f"Measures:    {len(measures)}")
print(f"Filters:     {len(filters)}")
print(f"Expressions: {len(expressions)}")

# COMMAND ----------

resp = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
space_data = json.loads(resp.serialized_space)

space_data["instructions"]["sql_snippets"] = {
    "measures": measures,
    "filters": filters,
    "expressions": expressions,
}

w.genie.update_space(
    space_id=SPACE_ID,
    serialized_space=json.dumps(space_data),
)

print(f"✅ SQL snippets applied ({len(measures)} measures, {len(filters)} filters, {len(expressions)} expressions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 7: Add Sample Questions
# MAGIC
# MAGIC Sample questions appear in the Genie UI when users first open the space. They serve two purposes:
# MAGIC
# MAGIC 1. **Onboarding** — Show users what kinds of questions they can ask
# MAGIC 2. **Guidance** — Help users understand the data's scope
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - Cover **each major table/domain** with at least one question
# MAGIC - Mix **simple** questions ("How many titles...") with **analytical** ones ("Compare... across services")
# MAGIC - Use the **same language** your business users would use
# MAGIC - Include questions that require **joins** to test cross-table capabilities

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

sample_q_payload = [
    {"id": make_id(), "question": [q]}
    for q in SAMPLE_QUESTIONS
]
sample_q_payload.sort(key=lambda x: x["id"])

resp = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
space_data = json.loads(resp.serialized_space)

space_data["config"]["sample_questions"] = sample_q_payload

w.genie.update_space(
    space_id=SPACE_ID,
    serialized_space=json.dumps(space_data),
)

print(f"✅ {len(SAMPLE_QUESTIONS)} sample questions added:")
for i, q in enumerate(SAMPLE_QUESTIONS, 1):
    print(f"  {i:>2}. {q}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 8: Add Benchmarks
# MAGIC
# MAGIC ### What Are Benchmarks?
# MAGIC
# MAGIC Benchmarks are **question-SQL pairs** that teach Genie the expected SQL for specific questions. They are the most powerful tool for improving Genie accuracy.
# MAGIC
# MAGIC When a user asks a question similar to a benchmark, Genie uses the benchmark SQL as a **template**, adapting it to the specific question.
# MAGIC
# MAGIC ### Best Practices for Benchmarks
# MAGIC
# MAGIC | Practice | Why |
# MAGIC |----------|-----|
# MAGIC | **Cover the most common questions** | These are the highest-impact improvements |
# MAGIC | **Include complex queries** (JOINs, CASE, subqueries) | These are where Genie needs the most help |
# MAGIC | **Use fully qualified table names** | Prevents ambiguity |
# MAGIC | **Include both simple and complex queries** | Covers the full range of user sophistication |
# MAGIC | **Ensure the SQL actually works** | Test every benchmark query before adding it |
# MAGIC | **Write questions in natural language** | Match how users would actually ask |
# MAGIC
# MAGIC ### Example Question-SQL Pairs (Benchmarks)
# MAGIC
# MAGIC These also serve as **example_question_sqls** — inline SQL references Genie can use.

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

print(f"Prepared {len(BENCHMARKS)} benchmarks covering:")
print(f"  • Service catalog size")
print(f"  • Exclusive content analysis")
print(f"  • Windowing duration trends")
print(f"  • Genre/category breakdowns")
print(f"  • Originals & scripted ratios")
print(f"  • Distributor analysis")
print(f"  • Episode availability")
print(f"  • Title changes (adds/drops)")
print(f"  • US vs international content")
print(f"  • IMDb ratings analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Benchmarks and Example SQL

# COMMAND ----------

benchmark_questions = [
    {"id": make_id(), "question": [b["question"]],
     "answer": [{"format": "SQL", "content": [b["sql"]]}]}
    for b in BENCHMARKS
]
benchmark_questions.sort(key=lambda x: x["id"])

example_question_sqls = [
    {"id": make_id(), "question": [b["question"]], "sql": [b["sql"]]}
    for b in BENCHMARKS
]
example_question_sqls.sort(key=lambda x: x["id"])

resp = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
space_data = json.loads(resp.serialized_space)

space_data["benchmarks"] = {"questions": benchmark_questions}
space_data["instructions"]["example_question_sqls"] = example_question_sqls

w.genie.update_space(
    space_id=SPACE_ID,
    serialized_space=json.dumps(space_data),
)

print(f"✅ {len(BENCHMARKS)} benchmarks applied")
print(f"✅ {len(BENCHMARKS)} example question SQLs applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 9: Verify & Test Your Genie Space
# MAGIC
# MAGIC Let's verify everything was applied correctly and test the space.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Configuration

# COMMAND ----------

verify = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
vdata = json.loads(verify.serialized_space)

print(f"Genie Space: {verify.title}")
print(f"Space ID:    {SPACE_ID}")
print(f"")
print(f"Configuration Summary:")
print(f"  Data source tables:      {len(vdata.get('data_sources', {}).get('tables', []))}")
print(f"  Text instructions:       {len(vdata.get('instructions', {}).get('text_instructions', []))}")
print(f"  Join specifications:     {len(vdata.get('instructions', {}).get('join_specs', []))}")
print(f"  SQL measures:            {len(vdata.get('instructions', {}).get('sql_snippets', {}).get('measures', []))}")
print(f"  SQL filters:             {len(vdata.get('instructions', {}).get('sql_snippets', {}).get('filters', []))}")
print(f"  SQL expressions:         {len(vdata.get('instructions', {}).get('sql_snippets', {}).get('expressions', []))}")
print(f"  Example question SQLs:   {len(vdata.get('instructions', {}).get('example_question_sqls', []))}")
print(f"  Sample questions:        {len(vdata.get('config', {}).get('sample_questions', []))}")
print(f"  Benchmark questions:     {len(vdata.get('benchmarks', {}).get('questions', []))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test with a Conversation
# MAGIC
# MAGIC Let's ask Genie a question programmatically to verify it works.

# COMMAND ----------

import time

conversation = w.genie.create_conversation(space_id=SPACE_ID)
conversation_id = conversation.conversation_id

test_question = "How many total titles does each streaming service carry?"
print(f"Asking: {test_question}")

message = w.genie.create_message(
    space_id=SPACE_ID,
    conversation_id=conversation_id,
    content=test_question,
)

message_id = message.message_id
print(f"Message ID: {message_id}")

for attempt in range(30):
    time.sleep(2)
    result = w.genie.get_message(
        space_id=SPACE_ID,
        conversation_id=conversation_id,
        message_id=message_id,
    )
    if result.status and result.status.value in ("COMPLETED", "FAILED"):
        break

if result.status and result.status.value == "COMPLETED":
    print(f"\n✅ Genie responded successfully!")
    if result.attachments:
        for att in result.attachments:
            if att.query and att.query.query:
                print(f"\nGenerated SQL:\n{att.query.query}")
else:
    print(f"\n⚠ Status: {result.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Recap: Genie Best Practices Checklist
# MAGIC
# MAGIC Use this checklist when building any Genie Space:
# MAGIC
# MAGIC ### Data Foundation
# MAGIC - [ ] All tables have descriptive **table comments**
# MAGIC - [ ] Key columns have **column comments** with business meaning, valid values, and synonyms
# MAGIC - [ ] Data types are correct (dates are dates, numbers are numbers)
# MAGIC - [ ] Column names are clear and consistent across tables
# MAGIC
# MAGIC ### Text Instructions
# MAGIC - [ ] **Domain terms** and abbreviations are defined
# MAGIC - [ ] **Platform/entity name mappings** are specified (casual name → exact value)
# MAGIC - [ ] **Table selection guidance** steers Genie to the right table per question type
# MAGIC - [ ] **Query patterns** document business logic (e.g., "use offer_type = 'Total' for comparisons")
# MAGIC - [ ] **Formatting rules** ensure consistent output
# MAGIC - [ ] **Clarification prompts** prevent ambiguous queries
# MAGIC
# MAGIC ### Join Specifications
# MAGIC - [ ] All important **cross-table relationships** are defined
# MAGIC - [ ] Join conditions use **composite keys** where needed
# MAGIC - [ ] Each join has an **instruction** explaining when to use it
# MAGIC - [ ] **Many-to-many** joins have caution notes
# MAGIC
# MAGIC ### SQL Expressions
# MAGIC - [ ] Complex **measures** (CASE statements, ratios) are pre-defined
# MAGIC - [ ] Common **filters** are named and documented
# MAGIC - [ ] Key **dimensions** (grouping columns) are listed
# MAGIC
# MAGIC ### Sample Questions & Benchmarks
# MAGIC - [ ] **Sample questions** cover all major data domains
# MAGIC - [ ] **Benchmarks** cover the most common and complex query patterns
# MAGIC - [ ] All benchmark SQL has been **tested and verified**
# MAGIC - [ ] Questions are written in **natural language** matching user vocabulary
# MAGIC
# MAGIC ### Iteration
# MAGIC - [ ] Tested with real questions from actual users
# MAGIC - [ ] Added benchmarks for any questions Genie got wrong
# MAGIC - [ ] Refined instructions based on observed failure patterns
# MAGIC - [ ] Reviewed generated SQL for correctness

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Your Genie Space is Ready! 🎉
# MAGIC
# MAGIC **Space ID:** Use the value from Step 3 above.
# MAGIC
# MAGIC **Next steps:**
# MAGIC 1. Open the Genie Space in the Databricks UI
# MAGIC 2. Try the sample questions
# MAGIC 3. Ask your own questions
# MAGIC 4. When Genie gets something wrong, add a benchmark with the correct SQL
# MAGIC 5. Share the space with your team!
# MAGIC
# MAGIC **Pro tip:** The best Genie Spaces are built iteratively. Start with what you have, test with real users, and keep adding benchmarks and refining instructions based on what you learn.

# COMMAND ----------

print(f"""
Workshop Complete!

Your Genie Space:
  Space ID:    {SPACE_ID}
  Tables:      {len(TABLES)}
  Joins:       {len(JOIN_SPECS)}
  Benchmarks:  {len(BENCHMARKS)}
  Questions:   {len(SAMPLE_QUESTIONS)}

Open it in the Databricks UI to start asking questions!
""")
