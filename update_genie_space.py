#!/usr/bin/env python3
"""
Update Genie space 01f1142ce4e51e52908a9772dfa7cd23 with comprehensive
instructions, SQL expressions, sample questions, and benchmarks.
"""
import json
import uuid
from databricks.sdk import WorkspaceClient

SPACE_ID = "01f1142ce4e51e52908a9772dfa7cd23"

GENERAL_INSTRUCTIONS = """This Genie Space contains real streaming media distribution data from Stream Metrics, covering two domains: VOD (Video On Demand) availability and content windowing. Data covers US and CA markets across 12+ streaming services.

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
- Use vod_title_mom_changes for aggregated MoM change counts by service: monthly columns (feb_2025 through jan_2026) show add/drop counts. The change_type column has values: Added, Dropped

Windowing tables:
- Use window_series_trends for aggregated series windowing metrics: avg_days, avg_days_trend, series counts by service. Contains breakdowns by scripted/unscripted, off-net, US/international, and by unscripted genre
- Use window_series_detail for individual series streaming windows: up to 18 streaming windows with start_date, end_date, and days for each. Contains pre_streaming_window data
- Use window_series_seasons for season-level windowing: up to 27 streaming windows per season
- Use window_series_episodes for episode-level windowing: up to 26 streaming windows per episode
- Use window_non_series_detail for movie/special windowing: similar structure to window_series_detail but for non-series content. Contains is_theatrical flag and media_type
- Use window_non_series_trends for aggregated movie windowing metrics: breakdowns by genre (animation, anime, documentary, film, sport, stage, stand_up) and by distributor (Disney, Sony, Warner Bros, Universal, Paramount, Lionsgate, etc.)

**Query Patterns & Business Logic:**

- When asked about "top services" or "biggest platforms," rank by matched_titles from vod_title_availability_summary WHERE offer_type = 'Total'
- When asked about "content diversity," count distinct category or genre values from vod_title_availability_detail
- When asked about "exclusive content," use is_exclusive_avod_and_svod = 1 from vod_title_availability_detail
- When asked about "originals," use sm_originals = 1 flag
- When asked about "off-net" content, use is_off_net = 1 flag
- When asked about "scripted vs unscripted," use is_scripted flag (1=Scripted, 0=Unscripted) or the category field
- For "MoM changes," use vod_title_mom_changes table. The monthly columns (feb_2025, mar_2025, etc.) contain counts. change_type = 'Added' or 'Dropped'
- For "recently added/dropped," use vod_title_changes_us_ca with current_month_changes column
- For "window duration," use avg_days from window_series_trends or window_non_series_trends
- For "how long titles stay on a service," use streaming_window_1_days (or window N) from window_series_detail
- The offer_type field often has 'Total' as a rollup value — use offer_type = 'Total' for overall service comparisons
- The report_interval field in summary tables has values like 'Month', 'Quarter', 'Annual' — default to 'Month' for current data
- Use current_to = (SELECT MAX(current_to) FROM table) to get the most recent reporting period

**Formatting & Presentation:**
- Always round percentages to 2 decimal places
- Format large numbers with commas for readability
- When showing date ranges, use YYYY-MM-DD format
- For duration analysis (avg_days), present averages rounded to whole days
- When showing top-N lists, default to top 10 unless the user specifies otherwise
- Order results by the most meaningful metric in descending order unless asked otherwise
- When presenting service comparisons, always include the country context

**Clarification Questions:**
- When users ask about "availability" without specifying, ask: "Are you asking about the detailed title catalog (vod_title_availability_detail), the service-level summary (vod_title_availability_summary), or episode availability (vod_episode_availability_detail)?"
- When users ask about "changes" without specifying, ask: "Would you like to see individual title-level changes (vod_title_changes_us_ca) or aggregated monthly add/drop counts by service (vod_title_mom_changes)?"
- When users ask about "windowing" without specifying, ask: "Are you interested in series windowing or non-series (movie) windowing? And do you want aggregated trends or individual title details?"

**Summary Instructions:**
- Include the total row count when applicable
- When summarizing service data, always mention the country scope (US, CA)
- Use bullet points for multi-part summaries
- Cite specific table and column names when the answer involves joins
- Highlight notable outliers or patterns
- When showing time-based trends, note the direction of change (increasing, decreasing, stable)"""

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

BENCHMARKS = [
    {
        "question": "How many total titles does each streaming service carry?",
        "sql": "SELECT service_name, CAST(matched_titles AS INT) AS total_titles, CAST(series AS INT) AS series_count, CAST(movies_non_series AS INT) AS movies_count FROM jay_mehta_catalog.stream_metrics.vod_title_availability_summary WHERE report_interval = 'Month' AND current_to = (SELECT MAX(current_to) FROM jay_mehta_catalog.stream_metrics.vod_title_availability_summary) AND offer_type = 'Total' ORDER BY total_titles DESC"
    },
    {
        "question": "Which service has the most exclusive content?",
        "sql": "SELECT service_name, COUNT(*) AS total_titles, SUM(CASE WHEN is_exclusive_avod_and_svod = 1 THEN 1 ELSE 0 END) AS exclusive_titles, ROUND(SUM(CASE WHEN is_exclusive_avod_and_svod = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS exclusive_pct FROM jay_mehta_catalog.stream_metrics.vod_title_availability_detail GROUP BY service_name ORDER BY exclusive_titles DESC"
    },
    {
        "question": "What is the average window duration in days for series content by streaming service?",
        "sql": "SELECT service_name, ROUND(AVG(avg_days)) AS avg_window_days, SUM(CAST(series AS INT)) AS total_series FROM jay_mehta_catalog.stream_metrics.window_series_trends WHERE avg_days IS NOT NULL AND window_type IS NOT NULL GROUP BY service_name ORDER BY avg_window_days DESC"
    },
    {
        "question": "Show month-over-month content adds and drops across all services",
        "sql": "SELECT service_name, change_type, COALESCE(CAST(feb_2025 AS INT), 0) AS feb_2025, COALESCE(CAST(mar_2025 AS INT), 0) AS mar_2025, COALESCE(CAST(apr_2025 AS INT), 0) AS apr_2025, COALESCE(CAST(may_2025 AS INT), 0) AS may_2025, COALESCE(CAST(jun_2025 AS INT), 0) AS jun_2025, COALESCE(CAST(jul_2025 AS INT), 0) AS jul_2025, COALESCE(CAST(aug_2025 AS INT), 0) AS aug_2025, COALESCE(CAST(sep_2025 AS INT), 0) AS sep_2025, COALESCE(CAST(oct_2025 AS INT), 0) AS oct_2025, COALESCE(CAST(nov_2025 AS INT), 0) AS nov_2025, COALESCE(CAST(dec_2025 AS INT), 0) AS dec_2025, COALESCE(CAST(jan_2026 AS INT), 0) AS jan_2026 FROM jay_mehta_catalog.stream_metrics.vod_title_mom_changes WHERE offer_type = 'Total' ORDER BY service_name, change_type"
    },
    {
        "question": "What categories have the most titles?",
        "sql": "SELECT category, COUNT(DISTINCT title) AS title_count FROM jay_mehta_catalog.stream_metrics.vod_title_availability_detail WHERE category IS NOT NULL GROUP BY category ORDER BY title_count DESC"
    },
    {
        "question": "Compare originals percentage across streaming services",
        "sql": "SELECT service_name, CAST(matched_titles AS INT) AS total_titles, CAST(sm_originals AS INT) AS originals, ROUND(CAST(sm_originals AS DOUBLE) * 100.0 / NULLIF(CAST(matched_titles AS INT), 0), 1) AS originals_pct FROM jay_mehta_catalog.stream_metrics.vod_title_availability_summary WHERE report_interval = 'Month' AND current_to = (SELECT MAX(current_to) FROM jay_mehta_catalog.stream_metrics.vod_title_availability_summary) AND offer_type = 'Total' ORDER BY originals_pct DESC"
    },
    {
        "question": "How does scripted vs unscripted content compare across services?",
        "sql": "SELECT service_name, CAST(scripted_series AS INT) AS scripted, CAST(unscripted_series AS INT) AS unscripted, ROUND(CAST(scripted_series AS DOUBLE) * 100.0 / NULLIF(CAST(series AS INT), 0), 1) AS scripted_pct FROM jay_mehta_catalog.stream_metrics.vod_title_availability_summary WHERE report_interval = 'Month' AND current_to = (SELECT MAX(current_to) FROM jay_mehta_catalog.stream_metrics.vod_title_availability_summary) AND offer_type = 'Total' ORDER BY scripted_pct DESC"
    },
    {
        "question": "Which distributors have the most content?",
        "sql": "SELECT distributor, COUNT(DISTINCT title) AS title_count, COUNT(DISTINCT service_name) AS services FROM jay_mehta_catalog.stream_metrics.vod_title_availability_detail WHERE distributor IS NOT NULL GROUP BY distributor ORDER BY title_count DESC LIMIT 20"
    },
    {
        "question": "How many episodes are available per service?",
        "sql": "SELECT service_name, COUNT(DISTINCT series_title) AS series_count, COUNT(*) AS total_episodes FROM jay_mehta_catalog.stream_metrics.vod_episode_availability_detail GROUP BY service_name ORDER BY total_episodes DESC"
    },
    {
        "question": "What titles were recently added or dropped in the US?",
        "sql": "SELECT title, service_name, current_month_changes, streaming_start_date, streaming_end_date, content_type, category FROM jay_mehta_catalog.stream_metrics.vod_title_changes_us_ca WHERE country = 'US' AND current_month_changes IS NOT NULL ORDER BY CASE WHEN current_month_changes = 'Added' THEN streaming_start_date ELSE streaming_end_date END DESC LIMIT 30"
    },
    {
        "question": "What is the average window duration for non-series content by service?",
        "sql": "SELECT service_name, ROUND(AVG(avg_days)) AS avg_window_days, SUM(CAST(non_series AS INT)) AS total_movies FROM jay_mehta_catalog.stream_metrics.window_non_series_trends WHERE avg_days IS NOT NULL AND window_type IS NOT NULL GROUP BY service_name ORDER BY avg_window_days DESC"
    },
    {
        "question": "Show the top 20 titles that appear on the most streaming services",
        "sql": "SELECT title, content_type, category, genre, COUNT(DISTINCT service_name) AS service_count FROM jay_mehta_catalog.stream_metrics.vod_title_availability_detail GROUP BY title, content_type, category, genre ORDER BY service_count DESC LIMIT 20"
    },
    {
        "question": "How does US content compare across services?",
        "sql": "SELECT service_name, COUNT(*) AS total_titles, SUM(CASE WHEN is_us = 1 THEN 1 ELSE 0 END) AS us_titles, ROUND(SUM(CASE WHEN is_us = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS us_pct FROM jay_mehta_catalog.stream_metrics.vod_title_availability_detail GROUP BY service_name ORDER BY us_pct DESC"
    },
    {
        "question": "What is the average IMDb rating by category?",
        "sql": "SELECT category, ROUND(AVG(imdb_rating), 2) AS avg_rating, COUNT(*) AS title_count FROM jay_mehta_catalog.stream_metrics.vod_title_availability_detail WHERE imdb_rating IS NOT NULL AND category IS NOT NULL GROUP BY category ORDER BY avg_rating DESC"
    },
    {
        "question": "Compare US vs CA title availability by service",
        "sql": "SELECT service_name, country, SUM(CASE WHEN current_month_changes = 'Added' THEN 1 ELSE 0 END) AS added, SUM(CASE WHEN current_month_changes = 'Dropped' THEN 1 ELSE 0 END) AS dropped FROM jay_mehta_catalog.stream_metrics.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL GROUP BY service_name, country ORDER BY service_name, country"
    },
]


def make_id():
    """Generate 32-char lowercase hex UUID for Databricks Genie API."""
    return uuid.uuid4().hex


def build_serialized_space(current_data):
    """Build the full serialized space JSON."""
    # Keep data_sources from current
    data_sources = current_data.get("data_sources", {"tables": []})
    
    # Sample questions with IDs (must be sorted by id)
    sample_questions = [
        {"id": make_id(), "question": [q]}
        for q in SAMPLE_QUESTIONS
    ]
    sample_questions.sort(key=lambda x: x["id"])
    
    # Text instructions - split into array of strings for content
    content_lines = GENERAL_INSTRUCTIONS.split("\n")
    content_array = [line + "\n" for line in content_lines]
    if content_array and not content_array[-1].endswith("\n"):
        content_array[-1] += "\n"
    
    text_instructions = [
        {
            "id": make_id(),
            "content": content_array
        }
    ]
    
    # SQL snippets: measures, filters, dimensions
    measures = [
        {"id": make_id(), "alias": "Exclusive Content Percentage", "sql": ["ROUND(SUM(CASE WHEN is_exclusive_avod_and_svod = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM vod_title_availability_detail"], "display_name": "Exclusive Content %"},
        {"id": make_id(), "alias": "SM Originals Count", "sql": ["SUM(CASE WHEN sm_originals = 1 THEN 1 ELSE 0 END) FROM vod_title_availability_detail"], "display_name": "Originals Count"},
        {"id": make_id(), "alias": "Average Window Duration", "sql": ["ROUND(AVG(avg_days), 0) FROM window_series_trends WHERE avg_days IS NOT NULL"], "display_name": "Avg Window Days"},
        {"id": make_id(), "alias": "Service Library Size", "sql": ["matched_titles FROM vod_title_availability_summary WHERE offer_type = 'Total'"], "display_name": "Library Size"},
        {"id": make_id(), "alias": "Scripted Ratio", "sql": ["ROUND(SUM(CASE WHEN is_scripted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM vod_title_availability_detail"], "display_name": "Scripted %"},
    ]
    
    filters = [
        {"id": make_id(), "sql": ["current_to = (SELECT MAX(current_to) FROM table_name)"], "display_name": "Current Period Only"},
        {"id": make_id(), "sql": ["is_exclusive_avod_and_svod = 1"], "display_name": "Exclusive Only"},
        {"id": make_id(), "sql": ["sm_originals = 1"], "display_name": "SM Originals Only"},
        {"id": make_id(), "sql": ["is_off_net = 1"], "display_name": "Off-Net Only"},
        {"id": make_id(), "sql": ["is_scripted = 1"], "display_name": "Scripted Only"},
        {"id": make_id(), "sql": ["country = 'US' OR is_us = 1"], "display_name": "US Content"},
        {"id": make_id(), "sql": ["offer_type = 'Total'"], "display_name": "Total Offer Type"},
        {"id": make_id(), "sql": ["report_interval = 'Month'"], "display_name": "Monthly Interval"},
    ]
    
    expressions = [
        {"id": make_id(), "alias": "Content Type", "sql": ["content_type"]},
        {"id": make_id(), "alias": "Offer Type", "sql": ["offer_type"]},
        {"id": make_id(), "alias": "Category", "sql": ["category"]},
        {"id": make_id(), "alias": "Business Model", "sql": ["business_model"]},
        {"id": make_id(), "alias": "Country", "sql": ["country"]},
        {"id": make_id(), "alias": "Service Name", "sql": ["service_name"]},
    ]
    
    # API requires all arrays sorted by id
    for arr in [measures, filters, expressions]:
        arr.sort(key=lambda x: x["id"])
    sql_snippets = {
        "measures": measures,
        "filters": filters,
        "expressions": expressions,
    }
    
    # Example question SQLs (for benchmarks - these teach Genie)
    # API requires sorted by id
    example_question_sqls = []
    for b in BENCHMARKS:
        example_question_sqls.append({
            "id": make_id(),
            "question": [b["question"]],
            "sql": [b["sql"]]
        })
    example_question_sqls.sort(key=lambda x: x["id"])
    
    # Benchmarks
    benchmark_questions = [
        {
            "id": make_id(),
            "question": [b["question"]],
            "answer": [{"format": "SQL", "content": [b["sql"]]}]
        }
        for b in BENCHMARKS
    ]
    benchmark_questions.sort(key=lambda x: x["id"])
    
    instructions = {
        "text_instructions": text_instructions,
        "example_question_sqls": example_question_sqls,
        "sql_snippets": sql_snippets,
    }
    
    return {
        "version": 2,
        "config": {
            "sample_questions": sample_questions,
        },
        "data_sources": data_sources,
        "instructions": instructions,
        "benchmarks": {
            "questions": benchmark_questions,
        },
    }


def main():
    w = WorkspaceClient()
    
    print("Fetching current Genie space...")
    resp = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
    current_data = json.loads(resp.serialized_space) if resp.serialized_space else {"version": 2, "config": {}, "data_sources": {"tables": []}}
    
    print("Building updated serialized space...")
    serialized = build_serialized_space(current_data)
    
    # Save for debugging
    with open("genie_update_payload.json", "w") as f:
        json.dump(serialized, f, indent=2)
    print("Saved payload to genie_update_payload.json")
    
    print("Updating Genie space via API...")
    w.genie.update_space(
        space_id=SPACE_ID,
        serialized_space=json.dumps(serialized),
    )
    print("SUCCESS: Genie space updated.")
    
    return 0


if __name__ == "__main__":
    exit(main())
