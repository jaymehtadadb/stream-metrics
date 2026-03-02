# Versant Stream Metrics

A Databricks App for exploring VOD title availability, content windowing, and streaming metrics across major AVOD/SVOD platforms. Built for the **Versant Genie Best Practices Workshop**.

## Overview

This application provides three views into Stream Metrics data:

1. **Key Metrics** — Streaming industry KPIs computed from live data across Viewer Engagement, Quality of Experience, and Business Health categories
2. **Content Explorer** — Searchable, filterable title-level data with per-title metrics (page views, IMDb rating, runtime, exclusivity, window duration, etc.)
3. **Dashboard** — Executive-level charts and tables covering catalog size, content trends, category mix, and platform activity

An integrated **Genie AI Assistant** (powered by a Databricks Genie Space) allows natural language questions against the full dataset.

## Data

Data is sourced from the [Stream Metrics Export API](https://api.stream-metrics.com/public-api/v2) and stored in Unity Catalog:

| Table | Description |
|---|---|
| `vod_title_availability_summary` | Catalog-level aggregates by service, country, offer type |
| `vod_title_availability_detail` | Title-level availability with metadata |
| `vod_title_changes_us_ca` | Monthly title adds/drops with page views, IMDb, and exclusivity |
| `vod_title_mom_changes` | Month-over-month change trends |
| `window_series_trends` | Series content windowing trends |
| `window_non_series_trends` | Non-series content windowing trends |
| `window_series_detail` | Series window detail records |
| `window_non_series_detail` | Non-series window detail records |
| `window_series_episodes` | Episode-level window data |
| `window_series_seasons` | Season-level window data |
| `vod_episode_availability_detail` | Episode availability detail |
| `vod_title_availability_native` | Native availability data |

**Catalog:** `jay_mehta_catalog.stream_metrics`

## Architecture

```
┌─────────────────────────────────────────────┐
│              Databricks App                 │
│  ┌────────────────────────────────────────┐ │
│  │  FastAPI Backend (app.py)              │ │
│  │  ├── /api/metrics/dashboard            │ │
│  │  ├── /api/metrics/streaming            │ │
│  │  ├── /api/titles                       │ │
│  │  ├── /api/genie/ask                    │ │
│  │  └── /api/genie/feedback               │ │
│  └────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────┐ │
│  │  Inline HTML/CSS/JS Frontend           │ │
│  │  ├── Key Metrics (Chart.js)            │ │
│  │  ├── Content Explorer (Data Table)     │ │
│  │  ├── Dashboard (Chart.js)              │ │
│  │  └── Genie Panel (Slide-in Chat)       │ │
│  └────────────────────────────────────────┘ │
└──────────────┬──────────────────────────────┘
               │
    ┌──────────▼──────────┐
    │  Databricks SQL     │
    │  Warehouse          │
    └──────────┬──────────┘
               │
    ┌──────────▼──────────┐
    │  Unity Catalog      │
    │  jay_mehta_catalog   │
    │  .stream_metrics    │
    └──────────┬──────────┘
               │
    ┌──────────▼──────────┐
    │  Genie Space        │
    │  (NL → SQL)         │
    └─────────────────────┘
```

## Deployment

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse access
- Databricks CLI configured

### Deploy

```bash
# Upload app files to workspace
databricks workspace import-dir ./app /Workspace/Users/<your-email>/stream-metrics/app --overwrite

# Deploy the app
databricks apps deploy versant-stream-metrics \
  --source-code-path /Workspace/Users/<your-email>/stream-metrics/app
```

### Configuration

Edit `app/app.yaml` to set your Genie Space ID and SQL Warehouse ID:

```yaml
env:
  - name: GENIE_SPACE_ID
    value: "<your-genie-space-id>"
  - name: WAREHOUSE_ID
    value: "<your-warehouse-id>"
```

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
- **Originals Rate** — streaming originals as % of catalog (CAC proxy)
- **Geographic Breakdown** — US vs CA catalog size and content origin countries

## Tech Stack

- **Backend:** Python, FastAPI, aiohttp, Databricks SDK
- **Frontend:** HTML/CSS/JS (inline), Chart.js 4.x (CDN)
- **Data:** Databricks SQL, Unity Catalog
- **AI:** Databricks Genie Space API
