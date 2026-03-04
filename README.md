# Versant Stream Metrics — Solution Accelerator

An end-to-end Databricks Asset Bundle (DAB) that extracts data from the Stream Metrics API, creates Delta tables in Unity Catalog, and deploys a full-stack analytics application with Genie AI-powered exploration.

Built for the **Versant Genie Best Practices Workshop**.

## What's Included

| Component | Description |
|-----------|-------------|
| **Data Pipeline** | 6 Databricks notebooks orchestrated as a multi-task job |
| **12 Delta Tables** | VOD availability, content windowing, and title change tracking |
| **Databricks App** | FastAPI app with Key Metrics, Content Explorer, Competitive Analysis, Dashboard, and Genie AI |
| **Genie Space** | Pre-configured with best practices, joins, SQL expressions, and benchmarks |

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Databricks Asset Bundle                │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Data Pipeline (Databricks Job - 6 tasks)          │  │
│  │                                                    │  │
│  │  01_setup_infrastructure                           │  │
│  │    └─▶ 02_extract_api_to_volumes                   │  │
│  │          └─▶ 03_create_tables_and_ingest            │  │
│  │                └─▶ 04_add_table_comments             │  │
│  │                      └─▶ 05_data_quality_checks      │  │
│  │                            └─▶ 06_create_genie_space  │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Databricks App (FastAPI)                          │  │
│  │  ├── Key Metrics (Engagement, QoE, Business KPIs)  │  │
│  │  ├── Content Explorer (searchable title-level)     │  │
│  │  ├── Competitive Analysis (Versant-specific)       │  │
│  │  ├── Dashboard (executive charts & tables)         │  │
│  │  └── Genie AI Assistant (NL → SQL)                 │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Unity Catalog: <catalog>.stream_metrics            │  │
│  │  12 Delta tables with column-level documentation    │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Genie Space                                       │  │
│  │  Joins, SQL expressions, benchmarks, sample Qs     │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

## Project Structure

```
stream-metrics/
├── databricks.yml                         # DAB bundle config with variables
├── resources/
│   ├── stream_metrics_pipeline.yml        # Job definition (5-task DAG)
│   └── stream_metrics_app.yml             # App resource definition
├── src/
│   └── notebooks/
│       ├── 01_setup_infrastructure.py     # Create catalog, schema, volume
│       ├── 02_extract_api_to_volumes.py   # Pull API data → CSV in volumes
│       ├── 03_create_tables_and_ingest.py # CSV → Delta tables via read_files()
│       ├── 04_add_table_comments.py       # Column-level docs for Genie
│       ├── 05_data_quality_checks.py      # Validate data quality
│       └── 06_create_genie_space.py       # Create & configure Genie Space
├── app/
│   ├── app.py                             # FastAPI app (backend + frontend)
│   ├── app.yaml                           # App runtime config
│   ├── requirements.txt                   # Python dependencies
│   └── static/
│       └── bg.png                         # Background image
├── genie_space_config.json                # Genie Space configuration export
├── add_genie_joins.py                     # Genie join management script
├── update_genie_space.py                  # Genie space update script
└── .gitignore
```

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+
- Stream Metrics API key
- SQL Warehouse access

### 1. Configure Variables

Edit `databricks.yml` or pass variables at deploy time:

```yaml
variables:
  catalog: jay_mehta_catalog        # Your UC catalog
  schema: stream_metrics            # Schema name
  warehouse_id: 862f1d757f0424f7    # SQL Warehouse ID
  genie_space_id: <your-genie-id>   # Genie Space ID
  api_key: <your-api-key>           # Stream Metrics API key
```

### 2. Deploy the Bundle

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev target
databricks bundle deploy --target dev

# Run the data ingestion pipeline
databricks bundle run stream_metrics_ingestion --var="api_key=YOUR_API_KEY"
```

### 3. Deploy the App

```bash
# The app is deployed as part of the bundle, or manually:
databricks apps deploy versant-stream-metrics \
  --source-code-path /Workspace/Users/<your-email>/stream-metrics-accelerator/app
```

## Data Pipeline

The pipeline runs as a Databricks Job with 6 sequential tasks:

### Task 1: Setup Infrastructure
Creates the Unity Catalog schema and managed volume for raw CSV storage.

### Task 2: Extract API Data
Pulls data from the [Stream Metrics Export API](https://api.stream-metrics.com/public-api/v2) for 12 reports. Each report is exported as a CSV file (capped at 50,000 rows) into `/Volumes/<catalog>/<schema>/raw_data/`.

| Report ID | Table Name | Description |
|-----------|------------|-------------|
| 1 | `vod_title_availability_detail` | Title-level availability across streaming services |
| 2 | `vod_title_availability_summary` | Aggregated availability by service and period |
| 3 | `vod_episode_availability_detail` | Episode-level availability for series |
| 5 | `window_series_trends` | Series windowing trend metrics |
| 6 | `window_series_detail` | Series-level windowing with up to 18 windows |
| 7 | `window_series_seasons` | Season-level windowing data |
| 8 | `window_series_episodes` | Episode-level windowing data |
| 10 | `window_non_series_detail` | Movie/special windowing data |
| 11 | `window_non_series_trends` | Movie windowing trend metrics |
| 28 | `vod_title_availability_native` | Provider-reported native availability |
| 32 | `vod_title_mom_changes` | Month-over-month change aggregates |
| 34 | `vod_title_changes_us_ca` | Title-level adds/drops for US and Canada |

### Task 3: Create Tables & Ingest
Creates Delta tables using `read_files()` with CSV schema inference. All tables are created with table-level comments.

### Task 4: Add Table Comments
Applies column-level descriptions from the Stream Metrics data dictionary. These comments are critical for Genie to understand data semantics.

### Task 5: Data Quality Checks
Validates row counts, null rates on key columns, service name distributions, and cross-table consistency.

### Task 6: Create Genie Space
Creates a new Databricks Genie Space via the SDK and configures it with:
- All 12 tables as data sources
- Comprehensive text instructions (domain terms, abbreviations, query patterns, formatting rules)
- 7 cross-table join specifications with relationship types and usage guidance
- SQL expression snippets (measures, filters, dimensions)
- 10 curated sample questions
- 14 benchmark question-SQL pairs for Genie training
- The created Space ID is passed as a task value for downstream use

## Key Metrics Computed

### Viewer Engagement
- **Page Views by Service** — total and per-title audience reach
- **Content Runtime** — series vs non-series average duration
- **Drop-off Analysis** — avg page views of dropped vs added titles
- **IMDb Engagement** — rating distribution, high-rated vs low-rated counts

### Quality of Experience
- **Content Quality Score** — avg IMDb rating for added vs dropped content
- **Availability Window** — avg and median days content stays on platform
- **Runtime Distribution** — short/mid/standard/long-form breakdown
- **Exclusivity Rate** — platform-exclusive content percentage

### Business Health
- **Content Churn Rate** — titles dropped as % of total movement per service
- **AVOD vs SVOD Mix** — title counts and engagement by offer model
- **Originals Rate** — streaming originals as % of catalog
- **Geographic Breakdown** — US vs CA catalog size and content origin countries

## Competitive Analysis

The app includes a Versant-focused competitive intelligence tab:
- Platform growth scorecard (net adds, churn %, exclusivity)
- Versant (NBCUniversal) content on competitor platforms
- Window strategy comparison across services
- High-value content being dropped by competitors
- Multi-platform content overlap analysis
- Network group distribution

## Tech Stack

- **Pipeline:** Databricks Notebooks, Databricks Jobs, Unity Catalog
- **Backend:** Python, FastAPI, aiohttp, Databricks SDK
- **Frontend:** HTML/CSS/JS (inline), Chart.js 4.x (CDN)
- **Data:** Databricks SQL, Delta Lake, Unity Catalog Volumes
- **AI:** Databricks Genie Space API
- **Deployment:** Databricks Asset Bundles (DABs)

## Customization

To adapt this accelerator for a different customer:

1. Update `databricks.yml` variables with your catalog and schema
2. Pass your Stream Metrics API key when running the pipeline
3. Create a new Genie Space and update `genie_space_id`
4. Modify the competitive analysis queries in `app/app.py` for the target company
