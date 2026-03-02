"""Versant Stream Metrics — Databricks App"""
import os
import json
import asyncio
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import aiohttp

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("genie")

app = FastAPI(title="Versant Stream Metrics")

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "01f1142ce4e51e52908a9772dfa7cd23")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "862f1d757f0424f7")
CATALOG = "jay_mehta_catalog"
SCHEMA = "stream_metrics"

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def _get_host() -> str:
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    if not host:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            host = w.config.host
        except Exception:
            host = "https://e2-demo-field-eng.cloud.databricks.com"
    return host.rstrip("/")


def _get_auth_header(request: Request) -> str:
    user_auth = request.headers.get("Authorization", "")
    if user_auth:
        return user_auth
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        header_factory = w.config.authenticate()
        if callable(header_factory):
            h = header_factory()
        elif isinstance(header_factory, dict):
            h = header_factory
        else:
            h = {}
        return h.get("Authorization", "")
    except Exception as e:
        log.error(f"Auth fallback failed: {e}")
        return ""


class GenieRequest(BaseModel):
    question: str
    conversation_id: str | None = None


class FeedbackRequest(BaseModel):
    conversation_id: str
    message_id: str
    feedback_type: str


async def _genie_api(session, method, url, auth, payload=None):
    headers = {"Authorization": auth, "Content-Type": "application/json"}
    if method == "GET":
        async with session.get(url, headers=headers) as resp:
            return resp.status, await resp.json()
    else:
        async with session.post(url, json=payload, headers=headers) as resp:
            return resp.status, await resp.json()


async def _execute_sql(session, host, auth, sql):
    url = f"{host}/api/2.0/sql/statements"
    payload = {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "30s", "disposition": "INLINE", "format": "JSON_ARRAY"}
    headers = {"Authorization": auth, "Content-Type": "application/json"}
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            data = await resp.json()
        state = data.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
            return {"columns": cols, "rows": data.get("result", {}).get("data_array", [])}
        stmt_id = data.get("statement_id")
        if stmt_id and state in ("PENDING", "RUNNING"):
            for _ in range(20):
                await asyncio.sleep(2)
                async with session.get(f"{url}/{stmt_id}", headers=headers) as poll_resp:
                    data = await poll_resp.json()
                state = data.get("status", {}).get("state", "")
                if state == "SUCCEEDED":
                    cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
                    return {"columns": cols, "rows": data.get("result", {}).get("data_array", [])}
                if state in ("FAILED", "CANCELLED", "CLOSED"):
                    break
        return {"columns": [], "rows": [], "error": data.get("status", {}).get("error", {}).get("message", f"Query {state}")}
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}


@app.post("/api/genie/ask")
async def genie_ask(req: GenieRequest, request: Request):
    try:
        host = _get_host()
        auth = _get_auth_header(request)
        if not auth:
            return JSONResponse({"error": "No authentication available"}, status_code=401)
        base = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}"
        async with aiohttp.ClientSession() as session:
            url = f"{base}/conversations/{req.conversation_id}/messages" if req.conversation_id else f"{base}/start-conversation"
            status_code, start_data = await _genie_api(session, "POST", url, auth, {"content": req.question})
            if status_code != 200:
                return JSONResponse({"error": f"Genie API error: {json.dumps(start_data)[:300]}"}, status_code=status_code)
            conv_id = start_data.get("conversation_id", req.conversation_id)
            msg_id = start_data.get("message_id") or start_data.get("id")
            if not msg_id:
                return JSONResponse({"error": "No message_id"}, status_code=500)
            msg_url = f"{base}/conversations/{conv_id}/messages/{msg_id}"
            msg_data, msg_status = {}, ""
            for _ in range(60):
                await asyncio.sleep(2)
                _, msg_data = await _genie_api(session, "GET", msg_url, auth)
                msg_status = msg_data.get("status", "")
                if msg_status in ("COMPLETED", "FAILED", "CANCELLED"):
                    break
            text_parts, sql_query, query_description, query_result, query_error, follow_up_questions = [], "", "", None, None, []
            for att in msg_data.get("attachments", []):
                txt = att.get("text")
                if txt:
                    if isinstance(txt, dict) and txt.get("content"):
                        text_parts.append(txt["content"])
                    elif isinstance(txt, str):
                        text_parts.append(txt)
                q = att.get("query")
                if q and isinstance(q, dict) and q.get("query"):
                    sql_query = q["query"]
                    query_description = q.get("description", "")
                    att_id = att.get("attachment_id") or att.get("id", "")
                    if att_id:
                        qr_url = f"{base}/conversations/{conv_id}/messages/{msg_id}/query-result/{att_id}"
                        for attempt in range(8):
                            try:
                                qr_code, qr_data = await _genie_api(session, "GET", qr_url, auth)
                                if qr_code != 200:
                                    query_error = f"HTTP {qr_code}"
                                    await asyncio.sleep(2)
                                    continue
                                stmt = qr_data.get("statement_response", {})
                                stmt_state = stmt.get("status", {}).get("state", "")
                                if stmt_state in ("PENDING", "RUNNING"):
                                    await asyncio.sleep(3)
                                    continue
                                if stmt_state == "FAILED":
                                    query_error = stmt.get("status", {}).get("error", {}).get("message", "Query failed")
                                    break
                                col_list = stmt.get("manifest", {}).get("schema", {}).get("columns", [])
                                columns = [c.get("name", f"col_{i}") for i, c in enumerate(col_list)]
                                col_types = [c.get("type_name", "STRING") for c in col_list]
                                rows = stmt.get("result", {}).get("data_array", [])
                                query_result = {"columns": columns, "column_types": col_types, "rows": rows[:200], "total_rows": len(rows)}
                                break
                            except Exception as e:
                                query_error = str(e)
                                await asyncio.sleep(2)
                sq = att.get("suggested_questions")
                if sq and isinstance(sq, dict):
                    follow_up_questions = sq.get("questions", [])
            return {"conversation_id": conv_id, "message_id": msg_id, "status": msg_status,
                    "text": "\n\n".join(filter(None, text_parts)), "query_description": query_description,
                    "sql": sql_query, "query_result": query_result, "query_error": query_error,
                    "follow_up_questions": follow_up_questions}
    except Exception as e:
        import traceback
        log.error(f"genie_ask error: {traceback.format_exc()}")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/genie/feedback")
async def genie_feedback(req: FeedbackRequest, request: Request):
    try:
        host, auth = _get_host(), _get_auth_header(request)
        rating = "POSITIVE" if req.feedback_type == "CORRECT" else "NEGATIVE"
        async with aiohttp.ClientSession() as session:
            url = f"{host}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{req.conversation_id}/messages/{req.message_id}/feedback"
            _, result = await _genie_api(session, "POST", url, auth, {"rating": rating})
            return {"status": "ok", "detail": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/metrics/dashboard")
async def dashboard_metrics(request: Request):
    host, auth = _get_host(), _get_auth_header(request)
    if not auth:
        return JSONResponse({"error": "No authentication"}, status_code=401)
    S = f"{CATALOG}.{SCHEMA}"

    # First, get the latest month to avoid complex subqueries
    async with aiohttp.ClientSession() as session:
        latest_res = await _execute_sql(session, host, auth,
            f"SELECT MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) AS latest FROM {S}.vod_title_availability_summary")
        latest_month = "Feb 2026"
        if latest_res.get("rows") and latest_res["rows"][0] and latest_res["rows"][0][0]:
            latest_month = latest_res["rows"][0][0]
        log.info(f"Latest month resolved to: {latest_month}")

    queries = {
        "kpis": f"""
            SELECT
              (SELECT SUM(CAST(matched_titles AS INT)) FROM {S}.vod_title_availability_summary
               WHERE report_interval='Monthly' AND country='US' AND current_to='{latest_month}' AND offer_type='Total') AS platform_listings,
              (SELECT COUNT(DISTINCT service_name) FROM {S}.vod_title_availability_summary) AS services,
              (SELECT COUNT(DISTINCT category) FROM {S}.vod_title_availability_detail WHERE category IS NOT NULL) AS categories,
              (SELECT ROUND(AVG(avg_days)) FROM {S}.window_series_trends WHERE avg_days IS NOT NULL) AS avg_window_days,
              (SELECT SUM(COALESCE(CAST(jan_2026 AS INT),0)) FROM {S}.vod_title_mom_changes WHERE offer_type='Total' AND change_type='Added') AS monthly_adds,
              (SELECT SUM(COALESCE(CAST(jan_2026 AS INT),0)) FROM {S}.vod_title_mom_changes WHERE offer_type='Total' AND change_type='Dropped') AS monthly_drops
        """,
        "platform_activity": f"""
            SELECT service_name,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 WHEN current_month_changes='Dropped' THEN -1 ELSE 0 END) AS net_change
            FROM {S}.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL
            GROUP BY service_name ORDER BY net_change DESC
        """,
        "catalog_by_service": f"""
            SELECT service_name, CAST(matched_titles AS INT) AS total_titles
            FROM {S}.vod_title_availability_summary
            WHERE report_interval='Monthly' AND country='US' AND current_to='{latest_month}' AND offer_type='Total'
            ORDER BY total_titles DESC
        """,
        "category_mix": f"""
            SELECT category, COUNT(DISTINCT title) AS titles
            FROM {S}.vod_title_availability_detail WHERE category IS NOT NULL
            GROUP BY category ORDER BY titles DESC
        """,
        "mom_trend": f"""
            SELECT change_type,
              SUM(COALESCE(CAST(feb_2025 AS INT),0)) AS `Feb`,
              SUM(COALESCE(CAST(mar_2025 AS INT),0)) AS `Mar`,
              SUM(COALESCE(CAST(apr_2025 AS INT),0)) AS `Apr`,
              SUM(COALESCE(CAST(may_2025 AS INT),0)) AS `May`,
              SUM(COALESCE(CAST(jun_2025 AS INT),0)) AS `Jun`,
              SUM(COALESCE(CAST(jul_2025 AS INT),0)) AS `Jul`,
              SUM(COALESCE(CAST(aug_2025 AS INT),0)) AS `Aug`,
              SUM(COALESCE(CAST(sep_2025 AS INT),0)) AS `Sep`,
              SUM(COALESCE(CAST(oct_2025 AS INT),0)) AS `Oct`,
              SUM(COALESCE(CAST(nov_2025 AS INT),0)) AS `Nov`,
              SUM(COALESCE(CAST(dec_2025 AS INT),0)) AS `Dec`,
              SUM(COALESCE(CAST(jan_2026 AS INT),0)) AS `Jan`
            FROM {S}.vod_title_mom_changes WHERE offer_type='Total' GROUP BY change_type ORDER BY change_type
        """,
        "category_movement": f"""
            SELECT category,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped
            FROM {S}.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL AND category IS NOT NULL
            GROUP BY category ORDER BY added+dropped DESC
        """,
        "window_by_service": f"""
            SELECT service_name, ROUND(AVG(avg_days)) AS avg_days
            FROM {S}.window_series_trends WHERE avg_days IS NOT NULL AND window_type IS NOT NULL AND service_name != 'Estimated LINEAR'
            GROUP BY service_name ORDER BY avg_days DESC
        """,
    }
    try:
        async with aiohttp.ClientSession() as session:
            keys = list(queries.keys())
            raw = await asyncio.gather(*[_execute_sql(session, host, auth, queries[k]) for k in keys], return_exceptions=True)
            return {k: (r if not isinstance(r, Exception) else {"columns": [], "rows": [], "error": str(r)}) for k, r in zip(keys, raw)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/titles")
async def get_titles(request: Request, search: str = "", content_type: str = "", status: str = "", service: str = "", page: int = 1):
    host, auth = _get_host(), _get_auth_header(request)
    if not auth:
        return JSONResponse({"error": "No authentication"}, status_code=401)
    S = f"{CATALOG}.{SCHEMA}"
    where = ["current_month_changes IS NOT NULL"]
    if search:
        safe = search.replace("'", "''")
        where.append(f"LOWER(title) LIKE '%{safe.lower()}%'")
    if content_type:
        where.append(f"content_type = '{content_type.replace(chr(39), '')}'")
    if status:
        where.append(f"current_month_changes = '{status.replace(chr(39), '')}'")
    if service:
        where.append(f"service_name = '{service.replace(chr(39), '')}'")
    wc = " AND ".join(where)
    limit = 20
    offset = (page - 1) * limit
    count_sql = f"SELECT COUNT(*) AS total FROM {S}.vod_title_changes_us_ca WHERE {wc}"
    data_sql = f"""SELECT title, service_name, content_type, category, genre, current_month_changes, offer_type,
        pageviews_views, imdb_rating, CAST(imdb_votes AS INT) AS imdb_votes,
        ROUND(runtime_sec/60.0,1) AS runtime_min, is_country_exclusive, sm_originals,
        original_country, start_year,
        CASE WHEN streaming_start_date IS NOT NULL AND streaming_end_date IS NOT NULL
          THEN DATEDIFF(streaming_end_date, streaming_start_date) ELSE NULL END AS window_days
        FROM {S}.vod_title_changes_us_ca WHERE {wc} ORDER BY service_name, title LIMIT {limit} OFFSET {offset}"""
    try:
        async with aiohttp.ClientSession() as session:
            count_res, data_res = await asyncio.gather(
                _execute_sql(session, host, auth, count_sql),
                _execute_sql(session, host, auth, data_sql), return_exceptions=True)
            total = int(count_res["rows"][0][0]) if not isinstance(count_res, Exception) and count_res.get("rows") else 0
            rows = data_res["rows"] if not isinstance(data_res, Exception) else []
            cols = data_res.get("columns", []) if not isinstance(data_res, Exception) else []
            return {"columns": cols, "rows": rows, "total": total, "page": page, "pages": max(1, (total + limit - 1) // limit)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/metrics/streaming")
async def streaming_metrics(request: Request):
    host, auth = _get_host(), _get_auth_header(request)
    if not auth:
        return JSONResponse({"error": "No authentication"}, status_code=401)
    S = f"{CATALOG}.{SCHEMA}"
    queries = {
        "pageviews_by_service": f"""
            SELECT service_name,
              SUM(pageviews_views) AS total_pageviews,
              COUNT(DISTINCT title) AS unique_titles,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews,
              MAX(pageviews_views) AS peak_pageviews
            FROM {S}.vod_title_changes_us_ca
            WHERE pageviews_views IS NOT NULL AND pageviews_views > 0
            GROUP BY service_name ORDER BY total_pageviews DESC
        """,
        "runtime_distribution": f"""
            SELECT
              ROUND(AVG(runtime_sec)/60.0,1) AS avg_runtime_min,
              ROUND(PERCENTILE(runtime_sec,0.5)/60.0,1) AS median_runtime_min,
              COUNT(*) AS total,
              SUM(CASE WHEN runtime_sec<600 THEN 1 ELSE 0 END) AS short_under_10min,
              SUM(CASE WHEN runtime_sec>=600 AND runtime_sec<1800 THEN 1 ELSE 0 END) AS mid_10_30min,
              SUM(CASE WHEN runtime_sec>=1800 AND runtime_sec<5400 THEN 1 ELSE 0 END) AS standard_30_90min,
              SUM(CASE WHEN runtime_sec>=5400 THEN 1 ELSE 0 END) AS long_over_90min
            FROM {S}.vod_title_changes_us_ca WHERE runtime_sec IS NOT NULL AND runtime_sec > 0
        """,
        "dropoff_added_vs_dropped": f"""
            SELECT current_month_changes,
              COUNT(*) AS titles,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews,
              ROUND(AVG(imdb_rating),2) AS avg_imdb_rating,
              ROUND(AVG(imdb_votes),0) AS avg_imdb_votes,
              SUM(CASE WHEN is_country_exclusive=1 THEN 1 ELSE 0 END) AS exclusive_count
            FROM {S}.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL
            GROUP BY current_month_changes
        """,
        "engagement_by_category": f"""
            SELECT category,
              COUNT(*) AS titles,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews,
              ROUND(AVG(imdb_rating),2) AS avg_rating,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL AND category IS NOT NULL
            GROUP BY category ORDER BY avg_pageviews DESC
        """,
        "content_type_stats": f"""
            SELECT content_type,
              COUNT(*) AS titles,
              ROUND(AVG(runtime_sec/60.0),1) AS avg_runtime_min,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews,
              ROUND(AVG(imdb_rating),2) AS avg_rating
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL AND content_type IS NOT NULL
            GROUP BY content_type
        """,
        "imdb_overview": f"""
            SELECT
              ROUND(AVG(imdb_rating),2) AS avg_rating,
              SUM(CAST(imdb_votes AS BIGINT)) AS total_votes,
              COUNT(CASE WHEN imdb_rating>=7 THEN 1 END) AS high_rated,
              COUNT(CASE WHEN imdb_rating<5 THEN 1 END) AS low_rated,
              COUNT(*) AS rated_titles
            FROM {S}.vod_title_changes_us_ca WHERE imdb_rating IS NOT NULL
        """,
        "content_churn": f"""
            SELECT service_name,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              COUNT(*) AS total,
              ROUND(SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS churn_pct,
              ROUND(SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS acquisition_pct
            FROM {S}.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL
            GROUP BY service_name ORDER BY churn_pct DESC
        """,
        "avod_svod": f"""
            SELECT offer_type, COUNT(*) AS titles,
              SUM(pageviews_views) AS total_pageviews,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews,
              SUM(CASE WHEN is_country_exclusive=1 THEN 1 ELSE 0 END) AS exclusive
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL AND offer_type IS NOT NULL
            GROUP BY offer_type
        """,
        "business_model": f"""
            SELECT business_model,
              COUNT(DISTINCT service_name) AS services,
              SUM(CAST(matched_titles AS INT)) AS total_titles,
              SUM(CAST(sm_originals AS INT)) AS originals,
              ROUND(SUM(CAST(sm_originals AS INT))*100.0/SUM(CAST(matched_titles AS INT)),1) AS originals_pct
            FROM {S}.vod_title_availability_summary
            WHERE report_interval='Monthly' AND country='US'
              AND current_to=(SELECT MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) FROM {S}.vod_title_availability_summary)
              AND offer_type='Total'
            GROUP BY business_model ORDER BY total_titles DESC
        """,
        "geo_summary": f"""
            SELECT country,
              SUM(CAST(matched_titles AS INT)) AS total_titles,
              SUM(CAST(series AS INT)) AS series,
              SUM(CAST(movies_non_series AS INT)) AS movies,
              SUM(CAST(sm_originals AS INT)) AS originals,
              SUM(CAST(us AS INT)) AS us_origin,
              SUM(CAST(intl AS INT)) AS intl_origin
            FROM {S}.vod_title_availability_summary
            WHERE report_interval='Monthly'
              AND current_to=(SELECT MAX_BY(current_to, to_date(concat('01 ', current_to), 'dd MMM yyyy')) FROM {S}.vod_title_availability_summary)
              AND offer_type='Total'
            GROUP BY country ORDER BY total_titles DESC
        """,
        "origin_country": f"""
            SELECT original_country, COUNT(*) AS titles
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL AND original_country IS NOT NULL
            GROUP BY original_country ORDER BY titles DESC LIMIT 10
        """,
        "window_by_service": f"""
            SELECT service_name, ROUND(AVG(avg_days),0) AS avg_days, MIN(avg_days) AS min_days, MAX(avg_days) AS max_days
            FROM {S}.window_series_trends
            WHERE avg_days IS NOT NULL AND service_name != 'Estimated LINEAR'
            GROUP BY service_name ORDER BY avg_days DESC
        """,
        "window_overall": f"""
            SELECT
              ROUND(AVG(DATEDIFF(streaming_end_date, streaming_start_date)),0) AS avg_days,
              ROUND(PERCENTILE(DATEDIFF(streaming_end_date, streaming_start_date),0.5),0) AS median_days,
              COUNT(*) AS titles_with_window
            FROM {S}.vod_title_changes_us_ca
            WHERE streaming_start_date IS NOT NULL AND streaming_end_date IS NOT NULL AND current_month_changes='Dropped'
        """,
        "exclusivity_by_service": f"""
            SELECT service_name,
              SUM(CASE WHEN is_country_exclusive=1 THEN 1 ELSE 0 END) AS exclusive,
              COUNT(*) AS total,
              ROUND(SUM(CASE WHEN is_country_exclusive=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS exclusivity_pct,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews
            FROM {S}.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL
            GROUP BY service_name ORDER BY exclusivity_pct DESC
        """,
    }
    try:
        async with aiohttp.ClientSession() as session:
            keys = list(queries.keys())
            raw = await asyncio.gather(*[_execute_sql(session, host, auth, queries[k]) for k in keys], return_exceptions=True)
            return {k: (r if not isinstance(r, Exception) else {"columns": [], "rows": [], "error": str(r)}) for k, r in zip(keys, raw)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/metrics/competitive")
async def competitive_metrics(request: Request):
    host, auth = _get_host(), _get_auth_header(request)
    if not auth:
        return JSONResponse({"error": "No authentication"}, status_code=401)
    S = f"{CATALOG}.{SCHEMA}"
    queries = {
        "platform_growth": f"""
            SELECT service_name,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) - SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS net,
              ROUND(SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS churn_pct,
              COUNT(DISTINCT title) AS unique_titles,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews,
              SUM(CASE WHEN is_country_exclusive=1 THEN 1 ELSE 0 END) AS exclusive_titles,
              ROUND(SUM(CASE WHEN is_country_exclusive=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS exclusivity_pct
            FROM {S}.vod_title_changes_us_ca WHERE current_month_changes IS NOT NULL
            GROUP BY service_name ORDER BY net DESC
        """,
        "nbcu_content": f"""
            SELECT original_network, COUNT(DISTINCT title) AS titles,
              ROUND(AVG(pageviews_views),0) AS avg_views,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL
              AND original_network IN ('USA Network','Syfy','E!','Bravo','CNBC','Golf Channel','Oxygen','MSNBC','NBC')
            GROUP BY original_network ORDER BY titles DESC
        """,
        "window_comparison": f"""
            SELECT service_name, ROUND(AVG(avg_days),0) AS series_window
            FROM {S}.window_series_trends
            WHERE avg_days IS NOT NULL AND service_name != 'Estimated LINEAR'
            GROUP BY service_name ORDER BY series_window DESC
        """,
        "window_nonseries": f"""
            SELECT service_name, ROUND(AVG(avg_days),0) AS nonseries_window
            FROM {S}.window_non_series_trends
            WHERE avg_days IS NOT NULL AND service_name != 'Estimated LINEAR'
            GROUP BY service_name ORDER BY nonseries_window DESC
        """,
        "high_value_drops": f"""
            SELECT title, service_name, pageviews_views, imdb_rating, category, is_country_exclusive
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes='Dropped' AND pageviews_views > 50000
            ORDER BY pageviews_views DESC LIMIT 15
        """,
        "high_value_adds": f"""
            SELECT title, service_name, pageviews_views, imdb_rating, category, is_country_exclusive
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes='Added' AND pageviews_views > 30000 AND is_country_exclusive=1
            ORDER BY pageviews_views DESC LIMIT 15
        """,
        "category_engagement": f"""
            SELECT category, COUNT(*) AS titles,
              ROUND(AVG(pageviews_views),0) AS avg_pageviews,
              ROUND(AVG(imdb_rating),2) AS avg_rating,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL AND category IS NOT NULL
            GROUP BY category ORDER BY avg_pageviews DESC
        """,
        "multi_platform_titles": f"""
            SELECT title, COUNT(DISTINCT service_name) AS platforms,
              CONCAT_WS(', ', COLLECT_SET(service_name)) AS services,
              MAX(pageviews_views) AS peak_views, MAX(imdb_rating) AS rating
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL
            GROUP BY title HAVING COUNT(DISTINCT service_name) >= 4
            ORDER BY peak_views DESC LIMIT 12
        """,
        "network_groups": f"""
            SELECT network_group, COUNT(DISTINCT title) AS titles,
              ROUND(AVG(pageviews_views),0) AS avg_views,
              SUM(CASE WHEN current_month_changes='Added' THEN 1 ELSE 0 END) AS added,
              SUM(CASE WHEN current_month_changes='Dropped' THEN 1 ELSE 0 END) AS dropped
            FROM {S}.vod_title_changes_us_ca
            WHERE current_month_changes IS NOT NULL AND network_group IS NOT NULL
            GROUP BY network_group ORDER BY titles DESC LIMIT 15
        """,
    }
    try:
        async with aiohttp.ClientSession() as session:
            keys = list(queries.keys())
            raw = await asyncio.gather(*[_execute_sql(session, host, auth, queries[k]) for k in keys], return_exceptions=True)
            return {k: (r if not isinstance(r, Exception) else {"columns": [], "rows": [], "error": str(r)}) for k, r in zip(keys, raw)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/debug")
async def debug_info(request: Request):
    host = _get_host()
    auth = _get_auth_header(request)
    return {"host": host, "has_auth": bool(auth), "auth_prefix": auth[:20] + "..." if auth else "none",
            "warehouse_id": WAREHOUSE_ID, "catalog": CATALOG, "schema": SCHEMA}


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return HTMLResponse(HTML_TEMPLATE)


@app.get("/health")
async def health():
    return {"status": "ok"}


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Versant Stream Metrics</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300..800&family=JetBrains+Mono:wght@300..700&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0b0f19;--sidebar:#0f1320;--surface:#131825;--surface2:#181d2e;--border:rgba(255,255,255,.06);--border2:rgba(255,255,255,.1);--text:#e2e8f0;--text2:#94a3b8;--text3:#64748b;--accent:#3b82f6;--green:#22c55e;--red:#ef4444;--font:'Inter',sans-serif;--mono:'JetBrains Mono',monospace}
body{font-family:var(--font);background:var(--bg);color:var(--text);height:100vh;overflow:hidden}
.layout{display:flex;height:100vh}
.sidebar{width:200px;background:var(--sidebar);border-right:1px solid var(--border);display:flex;flex-direction:column;flex-shrink:0}
.brand{padding:24px 20px 32px;font-size:14px;font-weight:700;letter-spacing:.18em;color:#f8fafc;text-transform:uppercase}
.sidebar nav{flex:1;display:flex;flex-direction:column;gap:2px;padding:0 8px}
.nav-item{display:flex;align-items:center;gap:10px;padding:10px 12px;border:none;background:0 0;color:var(--text3);font-family:var(--font);font-size:13px;font-weight:500;border-radius:8px;cursor:pointer;transition:all .15s;text-align:left;width:100%}
.nav-item:hover{color:var(--text2);background:rgba(255,255,255,.04)}
.nav-item.active{color:#f8fafc;background:rgba(59,130,246,.12);border:1px solid rgba(59,130,246,.2)}
.nav-item svg{width:16px;height:16px;opacity:.5;flex-shrink:0}
.nav-item.active svg{opacity:.9}
.sidebar-footer{padding:16px 20px;border-top:1px solid var(--border);font-size:11px;color:var(--text3);display:flex;align-items:center;gap:8px}
.sidebar-footer .dot{width:7px;height:7px;border-radius:50%;background:var(--green)}
.main{flex:1;overflow-y:auto;overflow-x:hidden}
.page{display:none;padding:32px 36px 60px}
.page.active{display:block}
.page-header{margin-bottom:28px}
.page-header h1{font-size:24px;font-weight:700;color:#f8fafc;letter-spacing:-.02em}
.page-header p{font-size:13px;color:var(--text3);margin-top:4px}
.kpi-row{display:grid;grid-template-columns:repeat(6,1fr);gap:14px;margin-bottom:32px}
.kpi-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:18px 16px}
.kpi-label{font-size:10px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between}
.kpi-label .kpi-icon{font-size:16px;opacity:.4}
.kpi-value{font-size:26px;font-weight:700;color:#f8fafc;letter-spacing:-.03em;line-height:1}
.section{margin-bottom:32px}
.section-head{margin-bottom:16px}
.section-head h2{font-size:16px;font-weight:600;color:#f1f5f9}
.section-head p{font-size:12px;color:var(--text3);margin-top:2px}
.section-body{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.row-2{display:grid;grid-template-columns:1fr 1fr;gap:18px}
.chart-wrap{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px}
.chart-wrap h3{font-size:12px;font-weight:600;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px}
.chart-wrap .chart-sub{font-size:11px;color:var(--text3);margin-bottom:14px}
.chart-box{height:280px;position:relative}
.data-table{width:100%;border-collapse:collapse;font-size:13px}
.data-table th{padding:10px 16px;background:rgba(255,255,255,.03);color:var(--text3);font-weight:500;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.04em;border-bottom:1px solid var(--border)}
.data-table td{padding:10px 16px;color:var(--text);border-bottom:1px solid var(--border)}
.data-table tr:hover td{background:rgba(255,255,255,.02)}
.data-table .num{text-align:right;font-weight:600;font-variant-numeric:tabular-nums}
.badge{display:inline-block;padding:2px 10px;border-radius:10px;font-size:11px;font-weight:500}
.badge-green{background:rgba(34,197,94,.12);color:#4ade80;border:1px solid rgba(34,197,94,.2)}
.badge-red{background:rgba(239,68,68,.12);color:#f87171;border:1px solid rgba(239,68,68,.2)}
.badge-blue{background:rgba(59,130,246,.12);color:#93bbfc;border:1px solid rgba(59,130,246,.2)}
.explorer-bar{display:flex;align-items:center;gap:12px;margin-bottom:18px;flex-wrap:wrap}
.search-box{flex:1;min-width:200px;padding:9px 14px 9px 36px;background:var(--surface);border:1px solid var(--border2);border-radius:8px;color:var(--text);font-family:var(--font);font-size:13px;outline:0}
.search-box:focus{border-color:var(--accent)}
.search-wrap{position:relative;flex:1;min-width:200px}
.search-wrap svg{position:absolute;left:12px;top:50%;transform:translateY(-50%);width:14px;height:14px;color:var(--text3)}
.filter-chips{display:flex;gap:6px;flex-wrap:wrap}
.chip{padding:6px 14px;background:var(--surface);border:1px solid var(--border2);border-radius:20px;color:var(--text3);font-family:var(--font);font-size:12px;cursor:pointer;transition:all .15s}
.chip:hover{color:var(--text2);border-color:rgba(255,255,255,.15)}
.chip.active{background:rgba(59,130,246,.15);border-color:rgba(59,130,246,.3);color:#93bbfc}
.pagination{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;font-size:12px;color:var(--text3);border-top:1px solid var(--border)}
.pagination button{padding:6px 14px;background:var(--surface2);border:1px solid var(--border2);border-radius:6px;color:var(--text2);font-family:var(--font);font-size:12px;cursor:pointer}
.pagination button:disabled{opacity:.3;cursor:not-allowed}
.pagination button:hover:not(:disabled){background:rgba(255,255,255,.06)}
.fab{position:fixed;bottom:24px;right:24px;width:48px;height:48px;border-radius:50%;background:var(--accent);border:none;color:#fff;font-size:20px;cursor:pointer;z-index:90;box-shadow:0 4px 16px rgba(59,130,246,.4);transition:all .2s;display:flex;align-items:center;justify-content:center}
.fab:hover{transform:scale(1.08);box-shadow:0 6px 20px rgba(59,130,246,.5)}
.fab.hidden{display:none}
.genie-panel{position:fixed;top:0;right:0;bottom:0;width:400px;background:var(--sidebar);border-left:1px solid var(--border);transform:translateX(100%);transition:transform .3s cubic-bezier(.4,0,.2,1);z-index:100;display:flex;flex-direction:column}
.genie-panel.open{transform:translateX(0)}
.gp-head{display:flex;align-items:center;justify-content:space-between;padding:16px 20px;border-bottom:1px solid var(--border)}
.gp-head h3{font-size:15px;font-weight:600;color:#f1f5f9;display:flex;align-items:center;gap:8px}
.gp-badge{font-size:11px;color:var(--text3);font-weight:400;background:rgba(255,255,255,.05);padding:2px 8px;border-radius:10px}
.gp-actions{display:flex;gap:6px}
.gp-actions button{width:30px;height:30px;border-radius:6px;border:1px solid var(--border2);background:0 0;color:var(--text3);font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center}
.gp-actions button:hover{color:var(--text);background:rgba(255,255,255,.05)}
.gp-messages{flex:1;overflow-y:auto;padding:20px}
.gp-welcome{text-align:center;padding:30px 10px}
.gp-welcome svg{width:32px;height:32px;margin-bottom:12px;color:var(--accent)}
.gp-welcome p{font-size:13px;color:var(--text3);line-height:1.6;margin-bottom:20px}
.gp-suggestions{display:flex;flex-direction:column;gap:6px}
.gp-suggestions button{padding:8px 14px;background:rgba(255,255,255,.03);border:1px solid var(--border2);border-radius:10px;color:var(--text2);font-family:var(--font);font-size:12px;cursor:pointer;transition:all .15s;text-align:left}
.gp-suggestions button:hover{background:rgba(59,130,246,.08);border-color:rgba(59,130,246,.2);color:#93bbfc}
.gp-input{padding:12px 16px;border-top:1px solid var(--border)}
.gp-input-wrap{display:flex;gap:8px;align-items:center;background:var(--surface);border:1px solid var(--border2);border-radius:10px;padding:4px 4px 4px 14px}
.gp-input-wrap input{flex:1;background:0 0;border:none;outline:0;color:var(--text);font-family:var(--font);font-size:13px}
.gp-input-wrap input::placeholder{color:var(--text3)}
.gp-input-wrap button{padding:8px 14px;background:var(--accent);border:none;border-radius:7px;color:#fff;font-size:13px;cursor:pointer}
.gp-input-wrap button:disabled{opacity:.3}
.gp-msg{margin-bottom:14px;animation:fadeIn .3s}
.gp-msg.user{text-align:right}
.gp-msg .bubble{display:inline-block;max-width:90%;padding:10px 14px;border-radius:12px;font-size:13px;line-height:1.6;text-align:left}
.gp-msg.user .bubble{background:rgba(59,130,246,.15);border:1px solid rgba(59,130,246,.25);color:var(--text)}
.gp-msg.assistant .bubble{background:var(--surface);border:1px solid var(--border);color:#cbd5e1}
.gp-msg .response-text{margin-bottom:8px;line-height:1.7;font-size:13px}
.gp-msg .query-desc{font-size:12px;color:var(--text3);font-style:italic;margin-bottom:6px}
.gp-msg .sql-toggle{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;margin-top:6px;background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:6px;color:var(--text3);font-size:11px;cursor:pointer}
.gp-msg .sql-block{display:none;margin-top:6px;padding:8px 10px;background:rgba(0,0,0,.3);border-radius:6px;font-family:var(--mono);font-size:11px;color:var(--text3);white-space:pre-wrap;word-break:break-word}
.gp-msg .sql-block.visible{display:block}
.gp-msg .chart-container{margin:8px 0;padding:12px;background:rgba(0,0,0,.2);border:1px solid var(--border);border-radius:8px;height:220px}
.gp-msg .chart-container canvas{max-height:200px}
.gp-msg .table-wrap{max-height:200px;overflow:auto;border-radius:6px;border:1px solid var(--border);margin:6px 0}
.gp-msg .result-table{width:100%;border-collapse:collapse;font-size:11px;background:rgba(0,0,0,.2)}
.gp-msg .result-table th{padding:6px 10px;background:rgba(255,255,255,.04);color:var(--text3);font-weight:500;text-align:left;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:1}
.gp-msg .result-table td{padding:5px 10px;color:#cbd5e1;border-bottom:1px solid rgba(255,255,255,.03)}
.gp-msg .follow-up-questions{display:flex;flex-wrap:wrap;gap:4px;margin-top:8px}
.gp-msg .follow-up-btn{padding:4px 10px;background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:12px;color:var(--text3);font-family:var(--font);font-size:10px;cursor:pointer}
.gp-msg .follow-up-btn:hover{background:rgba(59,130,246,.08);color:#93bbfc}
.thinking{display:flex;align-items:center;gap:6px;color:var(--text3);font-size:12px}
.thinking .dot{width:5px;height:5px;border-radius:50%;background:var(--accent);animation:pulse 1.2s infinite}
.thinking .dot:nth-child(2){animation-delay:.2s}
.thinking .dot:nth-child(3){animation-delay:.4s}
@keyframes pulse{0%,100%{opacity:.3}50%{opacity:1}}
@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
.error-box{padding:12px 16px;background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.15);border-radius:8px;color:#fca5a5;font-size:12px;margin-bottom:16px}
.loading-msg{color:var(--text3);font-size:13px;padding:20px;text-align:center}
.metrics-section{margin-bottom:36px}
.metrics-section-head{display:flex;align-items:center;gap:10px;margin-bottom:16px}
.metrics-section-head .sec-icon{width:36px;height:36px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0}
.metrics-section-head h2{font-size:17px;font-weight:600;color:#f1f5f9}
.metrics-section-head p{font-size:12px;color:var(--text3);margin-top:2px}
.metric-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}
.metric-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;position:relative;overflow:hidden;transition:border-color .2s}
.metric-card:hover{border-color:rgba(255,255,255,.12)}
.metric-card .accent-bar{position:absolute;top:0;left:0;right:0;height:3px;border-radius:12px 12px 0 0}
.mc-header{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:10px}
.mc-name{font-size:14px;font-weight:600;color:#f1f5f9}
.mc-refs{display:flex;gap:4px;flex-wrap:wrap}
.mc-ref{font-size:9px;padding:2px 6px;background:rgba(255,255,255,.04);border:1px solid var(--border);border-radius:4px;color:var(--text3);font-family:var(--mono)}
.mc-desc{font-size:12px;color:var(--text2);line-height:1.6;margin-bottom:12px}
.mc-benchmark{display:flex;align-items:center;gap:8px;padding:8px 10px;background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:8px;font-size:11px;color:var(--text3)}
.mc-benchmark .bm-label{font-weight:500;color:var(--text2)}
.mc-benchmark .bm-value{font-weight:600;font-family:var(--mono)}
.mc-live{margin-top:10px;display:flex;align-items:center;gap:8px}
.mc-live .live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
.mc-live .live-label{font-size:10px;font-weight:500;color:var(--green);text-transform:uppercase;letter-spacing:.04em}
.mc-live .live-val{font-size:18px;font-weight:700;color:#f8fafc;margin-left:auto;font-variant-numeric:tabular-nums}
.mc-chart-area{margin-top:12px;height:180px}
@media(max-width:1200px){.kpi-row{grid-template-columns:repeat(3,1fr)}.row-2{grid-template-columns:1fr}.metric-grid{grid-template-columns:1fr}}
@media(max-width:768px){.sidebar{display:none}.kpi-row{grid-template-columns:repeat(2,1fr)}}
</style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
    <div class="brand">Versant</div>
    <nav>
      <button class="nav-item active" onclick="showPage('metrics')" data-page="metrics">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
        Key Metrics
      </button>
      <button class="nav-item" onclick="showPage('explorer')" data-page="explorer">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
        Content Explorer
      </button>
      <button class="nav-item" onclick="showPage('competitive')" data-page="competitive">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg>
        Competitive Analysis
      </button>
      <button class="nav-item" onclick="showPage('dashboard')" data-page="dashboard">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
        Dashboard
      </button>
    </nav>
    <div class="sidebar-footer"><div class="dot"></div> Powered by Databricks</div>
  </aside>

  <div class="main">
    <div id="page-dashboard" class="page">
      <div class="page-header">
        <h1>Stream Metrics Dashboard</h1>
        <p>VOD title availability and content windowing analytics across streaming services</p>
      </div>
      <div id="dashError"></div>
      <div class="kpi-row" id="kpiRow">
        <div class="kpi-card"><div class="kpi-label">Platform Listings <span class="kpi-icon">&#x1F4CB;</span></div><div class="kpi-value" id="kpi0">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Services Tracked <span class="kpi-icon">&#x1F4FA;</span></div><div class="kpi-value" id="kpi1">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Categories <span class="kpi-icon">&#x1F3AD;</span></div><div class="kpi-value" id="kpi2">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Avg Window Days <span class="kpi-icon">&#x1F4C5;</span></div><div class="kpi-value" id="kpi3">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Titles Added (Jan) <span class="kpi-icon">&#x2B06;</span></div><div class="kpi-value" id="kpi4" style="color:#4ade80">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Titles Dropped (Jan) <span class="kpi-icon">&#x2B07;</span></div><div class="kpi-value" id="kpi5" style="color:#f87171">&mdash;</div></div>
      </div>

      <div class="section">
        <div class="section-head"><h2>Platform Activity This Month</h2><p>Content adds, drops, and net change by streaming service</p></div>
        <div class="section-body"><table class="data-table"><thead><tr><th>Service</th><th class="num">Added</th><th class="num">Dropped</th><th class="num">Net Change</th></tr></thead><tbody id="platformBody"><tr><td colspan="4" class="loading-msg">Loading...</td></tr></tbody></table></div>
      </div>

      <div class="row-2">
        <div class="chart-wrap"><h3>Catalog Size by Service</h3><p class="chart-sub">Total titles per platform (latest month)</p><div class="chart-box"><canvas id="chartCatalog"></canvas></div></div>
        <div class="chart-wrap"><h3>Content Category Mix</h3><p class="chart-sub">Title distribution by category</p><div class="chart-box"><canvas id="chartCategory"></canvas></div></div>
      </div>

      <div class="section" style="margin-top:18px">
        <div class="section-head"><h2>Monthly Content Trends</h2><p>Titles added vs dropped across all services (Feb 2025 &ndash; Jan 2026)</p></div>
        <div class="chart-wrap"><div class="chart-box" style="height:300px"><canvas id="chartMoM"></canvas></div></div>
      </div>

      <div class="row-2" style="margin-top:18px">
        <div class="chart-wrap"><h3>Category Movement This Month</h3><p class="chart-sub">Adds vs drops by content category</p><div class="chart-box"><canvas id="chartCatMove"></canvas></div></div>
        <div class="chart-wrap"><h3>Avg Window Duration by Service</h3><p class="chart-sub">Mean streaming window for series content (days)</p><div class="chart-box"><canvas id="chartWindow"></canvas></div></div>
      </div>
    </div>

    <!-- Key Metrics -->
    <div id="page-metrics" class="page active">
      <div class="page-header">
        <h1>Key Streaming Metrics</h1>
        <p>First-party stream metrics framework &mdash; Engagement, Quality of Experience, and Business Health &mdash; computed from live data</p>
      </div>
      <div id="metricsError"></div>

      <!-- ═══ KPI SUMMARY ROW ═══ -->
      <div class="kpi-row" id="metricsKpiRow" style="grid-template-columns:repeat(5,1fr);margin-bottom:28px">
        <div class="kpi-card"><div class="kpi-label">Total Page Views <span class="kpi-icon">&#x1F441;</span></div><div class="kpi-value" id="mkpi0">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Avg IMDb Rating <span class="kpi-icon">&#x2B50;</span></div><div class="kpi-value" id="mkpi1">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Content Churn Rate <span class="kpi-icon">&#x1F4C9;</span></div><div class="kpi-value" id="mkpi2" style="color:#f87171">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Avg Runtime <span class="kpi-icon">&#x23F1;</span></div><div class="kpi-value" id="mkpi3">&mdash;</div></div>
        <div class="kpi-card"><div class="kpi-label">Avg Window Days <span class="kpi-icon">&#x1F4C5;</span></div><div class="kpi-value" id="mkpi4">&mdash;</div></div>
      </div>

      <!-- ═══ 1. VIEWER ENGAGEMENT ═══ -->
      <div class="metrics-section">
        <div class="metrics-section-head">
          <div class="sec-icon" style="background:rgba(59,130,246,.12);color:#60a5fa">&#x1F4CA;</div>
          <div><h2>Viewer Engagement Metrics</h2><p>How users interact with content &mdash; measured via page views, ratings, and content velocity</p></div>
        </div>

        <div class="metric-grid" style="margin-bottom:16px">
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#3b82f6,#60a5fa)"></div>
            <div class="mc-header"><div class="mc-name">Concurrent Viewers (Page Views Proxy)</div></div>
            <div class="mc-desc">Monthly page views measure audience interest and reach. Peak page views identify breakout titles that drive platform traffic.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Peak title</div><div class="live-val" id="mPeakTitle">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Peak views</div><div class="live-val" id="mPeakViews">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#3b82f6,#60a5fa)"></div>
            <div class="mc-header"><div class="mc-name">Avg Watch Time / View Duration</div></div>
            <div class="mc-desc">Content runtime is the upper bound for view duration. Series average shorter per-episode but drive habitual viewing.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Series avg</div><div class="live-val" id="mRtSeries">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Non-Series avg</div><div class="live-val" id="mRtFilm">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#3b82f6,#60a5fa)"></div>
            <div class="mc-header"><div class="mc-name">Viewer Drop-off Patterns</div></div>
            <div class="mc-desc">Dropped titles had <strong>higher</strong> avg page views than added titles &mdash; audiences lose access to popular content.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Dropped avg views</div><div class="live-val" id="mDropViews">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Added avg views</div><div class="live-val" id="mAddViews">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#3b82f6,#60a5fa)"></div>
            <div class="mc-header"><div class="mc-name">Interaction Rates (IMDb Engagement)</div></div>
            <div class="mc-desc">IMDb votes measure active audience participation. High-rated titles (&ge;7.0) vs low-rated (&lt;5.0) indicate content quality health.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">High rated (&ge;7.0)</div><div class="live-val" id="mHighRated">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Low rated (&lt;5.0)</div><div class="live-val" id="mLowRated" style="color:#f87171">&mdash;</div></div>
          </div>
        </div>

        <div class="row-2">
          <div class="chart-wrap"><h3>Page Views by Service</h3><p class="chart-sub">Total monthly page views across platforms</p><div class="chart-box"><canvas id="chartPageviews"></canvas></div></div>
          <div class="chart-wrap"><h3>Engagement by Category</h3><p class="chart-sub">Avg page views per title by content category</p><div class="chart-box"><canvas id="chartEngCategory"></canvas></div></div>
        </div>
      </div>

      <!-- ═══ 2. QoE ═══ -->
      <div class="metrics-section">
        <div class="metrics-section-head">
          <div class="sec-icon" style="background:rgba(16,185,129,.12);color:#34d399">&#x26A1;</div>
          <div><h2>Quality of Experience (QoE) Metrics</h2><p>Content quality and availability window health &mdash; proxied through ratings, runtime, and window duration</p></div>
        </div>

        <div class="metric-grid" style="margin-bottom:16px">
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#10b981,#34d399)"></div>
            <div class="mc-header"><div class="mc-name">Content Quality Score</div></div>
            <div class="mc-desc">Avg IMDb rating across the catalog reflects perceived quality. Dropped content had marginally lower ratings &mdash; platforms shed lower-quality titles first.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Added avg rating</div><div class="live-val" id="mRatingAdded">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Dropped avg rating</div><div class="live-val" id="mRatingDropped">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#10b981,#34d399)"></div>
            <div class="mc-header"><div class="mc-name">Content Availability Window</div></div>
            <div class="mc-desc">How long content stays on a platform. Median window duration reveals the &ldquo;half-life&rdquo; of content &mdash; shorter windows mean faster churn and worse user experience.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Avg window</div><div class="live-val" id="mWindowAvg">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Median window</div><div class="live-val" id="mWindowMed">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#10b981,#34d399)"></div>
            <div class="mc-header"><div class="mc-name">Runtime Distribution</div></div>
            <div class="mc-desc">Content length mix impacts encoding, CDN cost, and engagement patterns. Long-form (&gt;90min) dominates this catalog.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Long-form (&gt;90 min)</div><div class="live-val" id="mLongForm">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Short-form (&lt;10 min)</div><div class="live-val" id="mShortForm">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#10b981,#34d399)"></div>
            <div class="mc-header"><div class="mc-name">Exclusivity Rate</div></div>
            <div class="mc-desc">Exclusive titles drive platform differentiation and reduce churn. Higher exclusivity = stronger competitive moat and better QoE through unique content.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Exclusive titles</div><div class="live-val" id="mExclTotal">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Overall rate</div><div class="live-val" id="mExclRate">&mdash;</div></div>
          </div>
        </div>

        <div class="row-2">
          <div class="chart-wrap"><h3>Window Duration by Service</h3><p class="chart-sub">Avg days content stays on each platform (series)</p><div class="chart-box"><canvas id="chartWindowSvc"></canvas></div></div>
          <div class="chart-wrap"><h3>Runtime Distribution</h3><p class="chart-sub">Breakdown by content length bucket</p><div class="chart-box"><canvas id="chartRuntime"></canvas></div></div>
        </div>
      </div>

      <!-- ═══ 3. BUSINESS ═══ -->
      <div class="metrics-section">
        <div class="metrics-section-head">
          <div class="sec-icon" style="background:rgba(245,158,11,.12);color:#fbbf24">&#x1F4B0;</div>
          <div><h2>Business &amp; Audience Growth KPIs</h2><p>Content economics, geographic reach, and competitive positioning</p></div>
        </div>

        <div class="metric-grid" style="margin-bottom:16px">
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#f59e0b,#fbbf24)"></div>
            <div class="mc-header"><div class="mc-name">Content Churn Rate</div></div>
            <div class="mc-desc">Titles dropped vs total title movement. Platforms with &gt;50% content churn are losing library faster than they replenish &mdash; a leading indicator of subscriber churn.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">Overall churn</div><div class="live-val" id="mChurnRate" style="color:#f87171">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">Highest service</div><div class="live-val" id="mChurnWorst">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#f59e0b,#fbbf24)"></div>
            <div class="mc-header"><div class="mc-name">ARPU: AVOD vs SVOD Mix</div></div>
            <div class="mc-desc">AVOD dominates title movement but SVOD shows higher per-title engagement. The AVOD/SVOD split signals monetization model health.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">AVOD titles</div><div class="live-val" id="mAvodTitles">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">SVOD titles</div><div class="live-val" id="mSvodTitles">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#f59e0b,#fbbf24)"></div>
            <div class="mc-header"><div class="mc-name">Originals Rate (CAC proxy)</div></div>
            <div class="mc-desc">SM Originals as % of catalog measures investment in proprietary content. Higher originals % drives organic discovery and lower CAC.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">AVOD originals</div><div class="live-val" id="mOrigAvod">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">SVOD originals</div><div class="live-val" id="mOrigSvod">&mdash;</div></div>
          </div>
          <div class="metric-card"><div class="accent-bar" style="background:linear-gradient(90deg,#f59e0b,#fbbf24)"></div>
            <div class="mc-header"><div class="mc-name">Geographic &amp; Origin Breakdown</div></div>
            <div class="mc-desc">Content origin country mix reveals international licensing depth. US-origin titles dominate but intl content is growing &mdash; key for global expansion.</div>
            <div class="mc-live"><div class="live-dot"></div><div class="live-label">US catalog</div><div class="live-val" id="mGeoUS">&mdash;</div></div>
            <div class="mc-live" style="margin-top:4px"><div class="live-dot"></div><div class="live-label">CA catalog</div><div class="live-val" id="mGeoCA">&mdash;</div></div>
          </div>
        </div>

        <div class="row-2">
          <div class="chart-wrap"><h3>Content Churn by Service</h3><p class="chart-sub">% of titles dropped this month per platform</p><div class="chart-box"><canvas id="chartChurn"></canvas></div></div>
          <div class="chart-wrap"><h3>Content Origin Countries</h3><p class="chart-sub">Top 10 countries of origin across all title changes</p><div class="chart-box"><canvas id="chartOrigin"></canvas></div></div>
        </div>

        <div class="section" style="margin-top:16px">
          <div class="section-head"><h2>Exclusivity &amp; Engagement by Service</h2><p>Platform competitive positioning: exclusive content rate vs audience reach</p></div>
          <div class="section-body">
            <table class="data-table"><thead><tr><th>Service</th><th class="num">Exclusive Titles</th><th class="num">Total</th><th class="num">Exclusivity %</th><th class="num">Avg Page Views</th></tr></thead>
            <tbody id="exclusivityBody"><tr><td colspan="5" class="loading-msg">Loading...</td></tr></tbody></table>
          </div>
        </div>
      </div>
    </div>

    <div id="page-explorer" class="page">
      <div class="page-header">
        <h1>Content Explorer</h1>
        <p>Browse title-level content changes across streaming services</p>
      </div>
      <div class="explorer-bar">
        <div class="search-wrap">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input class="search-box" id="searchInput" type="text" placeholder="Search titles...">
        </div>
        <div class="filter-chips" id="filterChips">
          <button class="chip active" data-filter="" data-status="" onclick="applyFilter(this)">All</button>
          <button class="chip" data-filter="Series" data-status="" onclick="applyFilter(this)">Series</button>
          <button class="chip" data-filter="Non-Series" data-status="" onclick="applyFilter(this)">Film</button>
          <button class="chip" data-filter="" data-status="Added" onclick="applyFilter(this)">Added</button>
          <button class="chip" data-filter="" data-status="Dropped" onclick="applyFilter(this)">Dropped</button>
        </div>
      </div>
      <div class="section-body" style="overflow-x:auto">
        <table class="data-table" style="min-width:1400px"><thead><tr><th>Title</th><th>Service</th><th>Type</th><th>Category</th><th>Genre</th><th>Status</th><th>Offer</th><th class="num">Page Views</th><th class="num">IMDb</th><th class="num">IMDb Votes</th><th class="num">Runtime</th><th>Exclusive</th><th>Original</th><th>Origin</th><th class="num">Year</th><th class="num">Window</th></tr></thead><tbody id="explorerBody"><tr><td colspan="16" class="loading-msg">Click to load...</td></tr></tbody></table>
        <div class="pagination"><span id="pageInfo"></span><div><button id="prevBtn" onclick="changePage(-1)" disabled>Previous</button> <button id="nextBtn" onclick="changePage(1)">Next</button></div></div>
      </div>
    </div>

    <!-- Competitive Analysis -->
    <div id="page-competitive" class="page">
      <div class="page-header">
        <h1>Competitive Analysis</h1>
        <p>AVOD/FAST landscape intelligence &mdash; platform growth, content economics, and strategic opportunities for Versant</p>
      </div>
      <div id="compError"></div>

      <!-- 1. FAST Land Grab -->
      <div class="section">
        <div class="section-head">
          <h2>1. The FAST Land Grab &mdash; Platform Growth Scorecard</h2>
          <p>Net title adds/drops this month. Positive net = growing catalog, negative = shrinking. Tubi is the only major platform with strong growth.</p>
        </div>
        <div class="section-body" style="overflow-x:auto">
          <table class="data-table" id="compGrowthTable" style="min-width:1000px"><thead><tr><th>Platform</th><th class="num">Added</th><th class="num">Dropped</th><th class="num">Net Change</th><th class="num">Churn %</th><th class="num">Unique Titles</th><th class="num">Avg Views</th><th class="num">Exclusive</th><th class="num">Excl %</th></tr></thead>
          <tbody id="compGrowthBody"><tr><td colspan="9" class="loading-msg">Loading...</td></tr></tbody></table>
        </div>
      </div>

      <div class="row-2" style="margin-top:18px">
        <div class="chart-wrap"><h3>Net Title Growth by Platform</h3><p class="chart-sub">Positive = growing, negative = shrinking catalog</p><div class="chart-box"><canvas id="chartCompNet"></canvas></div></div>
        <div class="chart-wrap"><h3>Exclusivity vs Engagement</h3><p class="chart-sub">Exclusivity % vs avg page views per platform</p><div class="chart-box"><canvas id="chartCompExcl"></canvas></div></div>
      </div>

      <!-- 2. Versant Content on Competitors -->
      <div class="section" style="margin-top:28px">
        <div class="section-head">
          <h2>2. Versant Content on Competitor Platforms</h2>
          <p>NBCU network content (NBC, Syfy, USA Network, E!, Oxygen) currently distributed across AVOD services. Every title here is one that doesn&rsquo;t differentiate your own FAST channels.</p>
        </div>
        <div class="section-body">
          <table class="data-table"><thead><tr><th>Network</th><th class="num">Titles on AVOD</th><th class="num">Avg Page Views</th><th class="num">Added</th><th class="num">Dropped</th><th class="num">Net</th></tr></thead>
          <tbody id="compNbcuBody"><tr><td colspan="6" class="loading-msg">Loading...</td></tr></tbody></table>
        </div>
      </div>

      <!-- 3. Window Strategy -->
      <div class="section" style="margin-top:28px">
        <div class="section-head">
          <h2>3. Fandango at Home Window Strategy vs Competitors</h2>
          <p>Average days content stays on each platform. Longer windows = lower re-licensing cost, better user experience. Fandango at Home is mid-pack.</p>
        </div>
        <div class="row-2">
          <div class="chart-wrap"><h3>Series Window Duration</h3><p class="chart-sub">Avg days series content stays on platform</p><div class="chart-box"><canvas id="chartCompWindowS"></canvas></div></div>
          <div class="chart-wrap"><h3>Non-Series Window Duration</h3><p class="chart-sub">Avg days film/non-series content stays on platform</p><div class="chart-box"><canvas id="chartCompWindowNS"></canvas></div></div>
        </div>
      </div>

      <!-- 4. High-Value Drops -->
      <div class="section" style="margin-top:28px">
        <div class="section-head">
          <h2>4. High-Value Content Being Dropped by Competitors</h2>
          <p>Titles with &gt;50K page views being shed this month. These are proven audience draws &mdash; potential acquisition targets at lower licensing costs.</p>
        </div>
        <div class="section-body" style="overflow-x:auto">
          <table class="data-table"><thead><tr><th>Title</th><th>Dropped From</th><th class="num">Page Views</th><th class="num">IMDb</th><th>Category</th><th>Exclusive?</th></tr></thead>
          <tbody id="compDropsBody"><tr><td colspan="6" class="loading-msg">Loading...</td></tr></tbody></table>
        </div>
      </div>

      <!-- 5. Exclusive Acquisitions -->
      <div class="section" style="margin-top:28px">
        <div class="section-head">
          <h2>5. High-Value Exclusive Adds This Month</h2>
          <p>Titles added as exclusives with &gt;30K page views. These are the competitive moves happening right now &mdash; content being locked up by rivals.</p>
        </div>
        <div class="section-body" style="overflow-x:auto">
          <table class="data-table"><thead><tr><th>Title</th><th>Added To</th><th class="num">Page Views</th><th class="num">IMDb</th><th>Category</th></tr></thead>
          <tbody id="compAddsBody"><tr><td colspan="5" class="loading-msg">Loading...</td></tr></tbody></table>
        </div>
      </div>

      <!-- 6. Category Engagement -->
      <div class="section" style="margin-top:28px">
        <div class="section-head">
          <h2>6. Scripted Content Drives Disproportionate Engagement</h2>
          <p>Avg page views by content category. Scripted series generate 2&ndash;6x the engagement of any other category &mdash; Versant&rsquo;s Syfy &amp; USA Network libraries are scripted-heavy.</p>
        </div>
        <div class="row-2">
          <div class="chart-wrap"><h3>Engagement by Category</h3><p class="chart-sub">Avg page views per title by content category</p><div class="chart-box"><canvas id="chartCompCatEng"></canvas></div></div>
          <div class="chart-wrap"><h3>Category Add/Drop Balance</h3><p class="chart-sub">Net title movement by category</p><div class="chart-box"><canvas id="chartCompCatFlow"></canvas></div></div>
        </div>
      </div>

      <!-- 7. Multi-Platform Overlap -->
      <div class="section" style="margin-top:28px">
        <div class="section-head">
          <h2>7. Multi-Platform Content Overlap</h2>
          <p>Titles available on 4+ platforms simultaneously. This content has zero differentiation value &mdash; everyone carries it.</p>
        </div>
        <div class="section-body" style="overflow-x:auto">
          <table class="data-table"><thead><tr><th>Title</th><th class="num">Platforms</th><th>Available On</th><th class="num">Peak Views</th><th class="num">IMDb</th></tr></thead>
          <tbody id="compOverlapBody"><tr><td colspan="5" class="loading-msg">Loading...</td></tr></tbody></table>
        </div>
      </div>

      <!-- 8. Network Groups -->
      <div class="section" style="margin-top:28px">
        <div class="section-head">
          <h2>8. Content by Network Group</h2>
          <p>Which media conglomerates have the most content flowing through AVOD platforms. Identifies whose content is powering which platforms.</p>
        </div>
        <div class="section-body">
          <table class="data-table"><thead><tr><th>Network Group</th><th class="num">Titles</th><th class="num">Avg Views</th><th class="num">Added</th><th class="num">Dropped</th><th class="num">Net</th></tr></thead>
          <tbody id="compNetworkBody"><tr><td colspan="6" class="loading-msg">Loading...</td></tr></tbody></table>
        </div>
      </div>
    </div>
  </div>
</div>

<button class="fab" id="fabBtn" onclick="openGenie()" title="Ask Assistant">&#x1F4AC;</button>

<div class="genie-panel" id="geniePanel">
  <div class="gp-head">
    <h3>&#x2728; Ask Assistant <span class="gp-badge">Genie AI</span></h3>
    <div class="gp-actions"><button onclick="newConversation()" title="New conversation">+</button><button onclick="closeGenie()" title="Close">&times;</button></div>
  </div>
  <div class="gp-messages" id="chatMessages">
    <div class="gp-welcome" id="genieWelcome">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
      <p>Ask questions about content performance, windowing, platform comparisons, and trends.</p>
      <div class="gp-suggestions">
        <button onclick="askGenie('How many total titles does each streaming service carry?')">Titles by service</button>
        <button onclick="askGenie('Which service has the most exclusive content?')">Exclusive content leaders</button>
        <button onclick="askGenie('What is the average window duration by service for series?')">Window duration by service</button>
        <button onclick="askGenie('Show the top 10 genres by title count')">Top genres breakdown</button>
        <button onclick="askGenie('Compare scripted vs unscripted series across services')">Scripted vs unscripted split</button>
        <button onclick="askGenie('Which categories had the most titles added this month?')">Fastest growing categories</button>
      </div>
    </div>
  </div>
  <div class="gp-input">
    <div class="gp-input-wrap">
      <input type="text" id="chatInput" placeholder="Ask about your content..." onkeydown="if(event.key==='Enter')sendGenie()">
      <button id="sendBtn" onclick="sendGenie()">&#x27A4;</button>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<script>
/* ── Globals ── */
var conversationId = null, isLoading = false, chartCounter = 0;
var explorerState = {page:1, search:'', contentType:'', status:''};
var dashLoaded = false;
var COLORS = ['rgba(59,130,246,.8)','rgba(16,185,129,.8)','rgba(245,158,11,.8)','rgba(239,68,68,.8)','rgba(139,92,246,.8)','rgba(236,72,153,.8)','rgba(20,184,166,.8)','rgba(249,115,22,.8)','rgba(99,102,241,.8)','rgba(34,197,94,.8)','rgba(168,85,247,.8)','rgba(251,146,60,.8)'];
var BORDERS = COLORS.map(function(c){return c.replace('.8)',  '1)')});
var TT = {backgroundColor:'rgba(15,23,42,.95)',titleColor:'#f1f5f9',bodyColor:'#cbd5e1',borderColor:'rgba(255,255,255,.08)',borderWidth:1,padding:10,cornerRadius:8};
var GRID = 'rgba(255,255,255,.04)', TICK = '#64748b', LABEL = '#94a3b8';

function fmt(v){if(v==null||v==='')return '\\u2014';var n=Number(v);if(isNaN(n))return String(v);return Number.isInteger(n)?n.toLocaleString():n.toLocaleString(undefined,{maximumFractionDigits:1})}
function esc(s){return s==null?'':String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}

/* ── Navigation ── */
function showPage(page){
  document.querySelectorAll('.page').forEach(function(p){p.classList.remove('active')});
  document.querySelectorAll('.nav-item').forEach(function(b){b.classList.remove('active')});
  var el = document.getElementById('page-'+page);
  if(el) el.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(function(b){if(b.getAttribute('data-page')===page) b.classList.add('active')});
  if(page==='dashboard') loadDashboard();
  if(page==='metrics') loadMetrics();
  if(page==='explorer') loadExplorer();
  if(page==='competitive') loadCompetitive();
}

/* ── Dashboard ── */
function loadDashboard(){
  if(dashLoaded) return;
  fetch('/api/metrics/dashboard').then(function(r){return r.json()}).then(function(data){
    if(data.error){
      document.getElementById('dashError').innerHTML='<div class="error-box">API Error: '+esc(data.error)+'</div>';
      return;
    }
    renderDashboard(data);
    dashLoaded = true;
  }).catch(function(e){
    document.getElementById('dashError').innerHTML='<div class="error-box">Failed to load: '+esc(e.message)+'</div>';
  });
}

function renderDashboard(data){
  if(data.kpis && data.kpis.rows && data.kpis.rows[0]){
    var r=data.kpis.rows[0];
    for(var i=0;i<6;i++){var el=document.getElementById('kpi'+i);if(el)el.textContent=fmt(r[i])}
  }
  if(data.platform_activity && data.platform_activity.rows && data.platform_activity.rows.length){
    document.getElementById('platformBody').innerHTML=data.platform_activity.rows.map(function(r){
      var net=parseInt(r[3])||0,cls=net>0?'badge-green':net<0?'badge-red':'badge-blue',sign=net>0?'+':'';
      return '<tr><td>'+esc(r[0])+'</td><td class="num" style="color:#4ade80">'+fmt(r[1])+'</td><td class="num" style="color:#f87171">'+fmt(r[2])+'</td><td class="num"><span class="badge '+cls+'">'+sign+fmt(net)+'</span></td></tr>';
    }).join('');
  } else {
    document.getElementById('platformBody').innerHTML='<tr><td colspan="4" class="loading-msg">No data available</td></tr>';
  }
  if(typeof Chart==='undefined') return;
  try{
    if(data.catalog_by_service && data.catalog_by_service.rows && data.catalog_by_service.rows.length)
      makeHBar('chartCatalog',data.catalog_by_service.rows.map(function(r){return r[0]}),data.catalog_by_service.rows.map(function(r){return parseInt(r[1])||0}),'Total Titles',0);
    if(data.category_mix && data.category_mix.rows && data.category_mix.rows.length)
      makeDonut('chartCategory',data.category_mix.rows.map(function(r){return r[0]}),data.category_mix.rows.map(function(r){return parseInt(r[1])||0}));
    if(data.mom_trend && data.mom_trend.rows && data.mom_trend.rows.length){
      var months=data.mom_trend.columns.slice(1);
      var addedRow=data.mom_trend.rows.find(function(r){return r[0]==='Added'});
      var droppedRow=data.mom_trend.rows.find(function(r){return r[0]==='Dropped'});
      var added=addedRow?months.map(function(_,i){return parseInt(addedRow[i+1])||0}):months.map(function(){return 0});
      var dropped=droppedRow?months.map(function(_,i){return parseInt(droppedRow[i+1])||0}):months.map(function(){return 0});
      makeLine('chartMoM',months,added,dropped);
    }
    if(data.category_movement && data.category_movement.rows && data.category_movement.rows.length)
      makeGrouped('chartCatMove',data.category_movement.rows.map(function(r){return r[0]}),data.category_movement.rows.map(function(r){return parseInt(r[1])||0}),data.category_movement.rows.map(function(r){return parseInt(r[2])||0}));
    if(data.window_by_service && data.window_by_service.rows && data.window_by_service.rows.length)
      makeHBar('chartWindow',data.window_by_service.rows.map(function(r){return r[0]}),data.window_by_service.rows.map(function(r){return parseFloat(r[1])||0}),'Avg Days',4);
  }catch(e){console.error('Chart render error:',e)}
}

/* ── Charts ── */
function makeHBar(id,labels,values,label,ci){
  var c=document.getElementById(id);if(!c)return;
  new Chart(c.getContext('2d'),{type:'bar',data:{labels:labels,datasets:[{label:label,data:values,backgroundColor:COLORS[ci%COLORS.length],borderColor:BORDERS[ci%BORDERS.length],borderWidth:1,borderRadius:4,maxBarThickness:24}]},
    options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false},tooltip:Object.assign({},TT,{callbacks:{label:function(ctx){return fmt(ctx.raw)}}})},
      scales:{x:{ticks:{color:TICK,font:{size:10},callback:function(v){return fmt(v)}},grid:{color:GRID}},y:{ticks:{color:LABEL,font:{size:11}},grid:{color:GRID}}}}});
}
function makeDonut(id,labels,values){
  var c=document.getElementById(id);if(!c)return;
  new Chart(c.getContext('2d'),{type:'doughnut',data:{labels:labels,datasets:[{data:values,backgroundColor:COLORS.slice(0,values.length),borderColor:BORDERS.slice(0,values.length),borderWidth:1}]},
    options:{responsive:true,maintainAspectRatio:false,cutout:'55%',plugins:{legend:{position:'right',labels:{color:LABEL,font:{size:11},padding:10,boxWidth:12}},tooltip:Object.assign({},TT,{callbacks:{label:function(ctx){return ctx.label+': '+fmt(ctx.raw)}}})}}});
}
function makeLine(id,labels,added,dropped){
  var c=document.getElementById(id);if(!c)return;
  new Chart(c.getContext('2d'),{type:'line',data:{labels:labels,datasets:[
    {label:'Added',data:added,borderColor:'rgba(34,197,94,1)',backgroundColor:'rgba(34,197,94,.1)',fill:true,tension:.35,pointRadius:4,pointHoverRadius:6,borderWidth:2},
    {label:'Dropped',data:dropped,borderColor:'rgba(239,68,68,1)',backgroundColor:'rgba(239,68,68,.1)',fill:true,tension:.35,pointRadius:4,pointHoverRadius:6,borderWidth:2}
  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:true,position:'top',labels:{color:LABEL,font:{size:12},padding:20,usePointStyle:true,pointStyle:'circle'}},tooltip:Object.assign({},TT,{mode:'index',intersect:false})},
    scales:{x:{ticks:{color:TICK,font:{size:11}},grid:{color:GRID}},y:{ticks:{color:TICK,font:{size:11},callback:function(v){return fmt(v)}},grid:{color:GRID}}},interaction:{mode:'index',intersect:false}}});
}
function makeGrouped(id,labels,added,dropped){
  var c=document.getElementById(id);if(!c)return;
  new Chart(c.getContext('2d'),{type:'bar',data:{labels:labels,datasets:[
    {label:'Added',data:added,backgroundColor:'rgba(34,197,94,.7)',borderColor:'rgba(34,197,94,1)',borderWidth:1,borderRadius:4},
    {label:'Dropped',data:dropped,backgroundColor:'rgba(239,68,68,.7)',borderColor:'rgba(239,68,68,1)',borderWidth:1,borderRadius:4}
  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:true,position:'top',labels:{color:LABEL,font:{size:11}}},tooltip:Object.assign({},TT,{mode:'index'})},
    scales:{x:{ticks:{color:LABEL,font:{size:10}},grid:{color:GRID}},y:{ticks:{color:TICK,font:{size:10},callback:function(v){return fmt(v)}},grid:{color:GRID}}}}});
}

/* ── Content Explorer ── */
function loadExplorer(){
  var s=explorerState,params=new URLSearchParams({page:s.page});
  if(s.search)params.set('search',s.search);
  if(s.contentType)params.set('content_type',s.contentType);
  if(s.status)params.set('status',s.status);
  document.getElementById('explorerBody').innerHTML='<tr><td colspan="16" class="loading-msg">Loading...</td></tr>';
  fetch('/api/titles?'+params.toString()).then(function(r){return r.json()}).then(function(data){
    if(data.error){document.getElementById('explorerBody').innerHTML='<tr><td colspan="16" class="loading-msg">Error: '+esc(data.error)+'</td></tr>';return;}
    renderExplorer(data);
  }).catch(function(e){document.getElementById('explorerBody').innerHTML='<tr><td colspan="16" class="loading-msg">Error: '+esc(e.message)+'</td></tr>'});
}
function renderExplorer(data){
  var body=document.getElementById('explorerBody');
  if(!data.rows||!data.rows.length){body.innerHTML='<tr><td colspan="16" style="text-align:center;color:var(--text3);padding:40px">No results found</td></tr>';}
  else{body.innerHTML=data.rows.map(function(r){
    var cls=r[5]==='Added'?'badge-green':'badge-red';
    var exclBadge=r[11]==='1'||r[11]===1?'<span class="badge badge-green">Yes</span>':'<span style="color:var(--text3)">No</span>';
    var origBadge=r[12]==='1'||r[12]===1?'<span class="badge badge-blue">Yes</span>':'<span style="color:var(--text3)">No</span>';
    var windowVal=r[15]!=null&&r[15]!==''?fmt(r[15])+' d':'<span style="color:var(--text3)">&mdash;</span>';
    return '<tr><td style="white-space:nowrap">'+esc(r[0])+'</td><td style="white-space:nowrap">'+esc(r[1])+'</td><td>'+esc(r[2])+'</td><td>'+esc(r[3])+'</td><td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc(r[4])+'</td><td><span class="badge '+cls+'">'+esc(r[5])+'</span></td><td><span class="badge badge-blue">'+esc(r[6])+'</span></td><td class="num">'+fmt(r[7])+'</td><td class="num" style="color:'+(parseFloat(r[8])>=7?'#4ade80':parseFloat(r[8])<5?'#f87171':'var(--text)')+'">'+fmt(r[8])+'</td><td class="num">'+fmt(r[9])+'</td><td class="num">'+fmt(r[10])+(r[10]!=null&&r[10]!==''?' min':'')+'</td><td>'+exclBadge+'</td><td>'+origBadge+'</td><td>'+esc(r[13]||'')+'</td><td class="num">'+esc(r[14]||'')+'</td><td class="num">'+windowVal+'</td></tr>';
  }).join('');}
  document.getElementById('pageInfo').textContent='Showing page '+data.page+' of '+data.pages+' ('+fmt(data.total)+' titles)';
  document.getElementById('prevBtn').disabled=data.page<=1;
  document.getElementById('nextBtn').disabled=data.page>=data.pages;
}
function applyFilter(chip){
  document.querySelectorAll('#filterChips .chip').forEach(function(c){c.classList.remove('active')});
  chip.classList.add('active');
  explorerState.contentType=chip.getAttribute('data-filter')||'';
  explorerState.status=chip.getAttribute('data-status')||'';
  explorerState.page=1;
  loadExplorer();
}
function changePage(dir){explorerState.page+=dir;if(explorerState.page<1)explorerState.page=1;loadExplorer();}

/* ── Genie Panel ── */
function openGenie(){document.getElementById('geniePanel').classList.add('open');document.getElementById('fabBtn').classList.add('hidden')}
function closeGenie(){document.getElementById('geniePanel').classList.remove('open');document.getElementById('fabBtn').classList.remove('hidden')}
function newConversation(){conversationId=null;var w=document.getElementById('chatMessages');w.querySelectorAll('.gp-msg').forEach(function(m){m.remove()});var wl=document.getElementById('genieWelcome');if(wl)wl.style.display=''}
function addGMsg(role,html){
  var w=document.getElementById('chatMessages'),wl=document.getElementById('genieWelcome');
  if(wl)wl.style.display='none';
  var d=document.createElement('div');d.className='gp-msg '+role;d.innerHTML='<div class="bubble">'+html+'</div>';w.appendChild(d);w.scrollTop=w.scrollHeight;
}
function addThinking(){var w=document.getElementById('chatMessages'),d=document.createElement('div');d.className='gp-msg assistant';d.id='gThink';d.innerHTML='<div class="bubble"><div class="thinking"><div class="dot"></div><div class="dot"></div><div class="dot"></div><span>Analyzing...</span></div></div>';w.appendChild(d);w.scrollTop=w.scrollHeight}
function removeThinking(){var e=document.getElementById('gThink');if(e)e.remove()}
function toggleSql(id){var b=document.getElementById(id);if(b)b.classList.toggle('visible')}

function formatGenieResult(data){
  var html='',uid=++chartCounter;
  if(data.text)html+='<div class="response-text">'+data.text.replace(/\\*\\*(.*?)\\*\\*/g,'<strong>$1</strong>').replace(/\\n/g,'<br>')+'</div>';
  if(data.query_description)html+='<div class="query-desc">'+esc(data.query_description)+'</div>';
  if(data.query_result&&data.query_result.columns&&data.query_result.columns.length){
    var qr=data.query_result,hasRows=qr.rows&&qr.rows.length;
    if(hasRows&&typeof Chart!=='undefined'){
      var ci=detectChart(qr.columns,qr.column_types||[],qr.rows);
      if(ci){var cid='gc-'+uid;html+='<div class="chart-container"><canvas id="'+cid+'"></canvas></div>';setTimeout(function(){renderGChart(cid,ci,qr.columns,qr.rows)},100)}
    }
    html+='<div class="table-wrap"><table class="result-table"><thead><tr>';
    qr.columns.forEach(function(c){html+='<th>'+esc(c)+'</th>'});
    html+='</tr></thead><tbody>';
    if(hasRows)qr.rows.forEach(function(row){html+='<tr>';row.forEach(function(cell){html+='<td>'+esc(fmt(cell))+'</td>'});html+='</tr>'});
    else html+='<tr><td colspan="'+qr.columns.length+'" style="text-align:center;color:var(--text3)">No data</td></tr>';
    html+='</tbody></table></div>';
  }
  if(data.query_error)html+='<div style="margin-top:6px;padding:8px;background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.15);border-radius:6px;font-size:11px;color:#fca5a5">'+esc(data.query_error)+'</div>';
  if(data.sql){var sid='sql-'+uid;html+='<div class="sql-toggle" onclick="toggleSql(\\''+sid+'\\')">SQL</div><div class="sql-block" id="'+sid+'">'+esc(data.sql)+'</div>'}
  if(data.follow_up_questions&&data.follow_up_questions.length){html+='<div class="follow-up-questions">';data.follow_up_questions.forEach(function(q){html+='<button class="follow-up-btn" onclick="askGenie(\\''+esc(q).replace(/'/g,"\\\\'")+'\\')">' +esc(q)+'</button>'});html+='</div>'}
  return html||'<span style="color:var(--text3)">No response.</span>';
}

function detectChart(columns,columnTypes,rows){
  if(!columns||columns.length<2||!rows||!rows.length)return null;
  var numCols=[],strCols=[],dateCols=[];
  columns.forEach(function(col,i){
    var t=(columnTypes[i]||'').toUpperCase(),s=rows[0]?rows[0][i]:null;
    var isNum=['INT','LONG','DOUBLE','FLOAT','DECIMAL','BIGINT'].some(function(n){return t.indexOf(n)>=0})||(s&&!isNaN(Number(s))&&s!=='');
    if(isNum)numCols.push(i);
    else if(t.indexOf('DATE')>=0||t.indexOf('TIMESTAMP')>=0||(s&&/^\\d{4}-\\d{2}/.test(s)))dateCols.push(i);
    else strCols.push(i);
  });
  if(dateCols.length>0&&numCols.length>0)return{type:'line',labelCol:dateCols[0],valueCols:numCols};
  if(strCols.length>0&&numCols.length>0){
    if(rows.length<=6&&numCols.length===1)return{type:'doughnut',labelCol:strCols[0],valueCols:numCols};
    return{type:'bar',labelCol:strCols[0],valueCols:numCols};
  }
  if(numCols.length>=2)return{type:'bar',labelCol:numCols[0],valueCols:numCols.slice(1)};
  return null;
}

function renderGChart(canvasId,ci,columns,rows){
  var labels=rows.map(function(r){return r[ci.labelCol]||''});
  var datasets=ci.valueCols.map(function(idx,di){
    var d=rows.map(function(r){return parseFloat(r[idx])||0});
    if(ci.type==='doughnut')return{label:columns[idx],data:d,backgroundColor:COLORS.slice(0,d.length),borderWidth:1};
    return{label:columns[idx],data:d,backgroundColor:COLORS[di%COLORS.length],borderColor:BORDERS[di%BORDERS.length],borderWidth:ci.type==='line'?2:1,fill:ci.type==='line'?false:undefined,tension:.3,pointRadius:3};
  });
  requestAnimationFrame(function(){
    var cv=document.getElementById(canvasId);if(!cv)return;
    try{new Chart(cv.getContext('2d'),{type:ci.type,data:{labels:labels,datasets:datasets},options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:datasets.length>1||ci.type==='doughnut',position:ci.type==='doughnut'?'right':'top',labels:{color:LABEL,font:{size:10}}},tooltip:TT},
      scales:ci.type==='doughnut'?{}:{x:{ticks:{color:TICK,font:{size:10}},grid:{color:GRID}},y:{ticks:{color:TICK,font:{size:10},callback:function(v){return fmt(v)}},grid:{color:GRID}}}
    }})}catch(e){console.error(e)}
  });
}

function sendGenie(){
  if(isLoading)return;
  var input=document.getElementById('chatInput'),q=input.value.trim();
  if(!q)return;
  input.value='';isLoading=true;document.getElementById('sendBtn').disabled=true;
  addGMsg('user',esc(q));addThinking();
  fetch('/api/genie/ask',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:q,conversation_id:conversationId})})
    .then(function(r){return r.json()})
    .then(function(data){
      removeThinking();
      if(data.error)addGMsg('assistant','<span style="color:#f87171">'+esc(data.error)+'</span>');
      else{conversationId=data.conversation_id;addGMsg('assistant',formatGenieResult(data))}
    })
    .catch(function(e){removeThinking();addGMsg('assistant','<span style="color:#f87171">Error: '+esc(e.message)+'</span>')})
    .finally(function(){isLoading=false;document.getElementById('sendBtn').disabled=false});
}
function askGenie(q){openGenie();document.getElementById('chatInput').value=q;sendGenie()}

/* ── Key Metrics ── */
var metricsLoaded = false;
function loadMetrics(){
  if(metricsLoaded) return;
  fetch('/api/metrics/streaming').then(function(r){return r.json()}).then(function(data){
    if(data.error){document.getElementById('metricsError').innerHTML='<div class="error-box">'+esc(data.error)+'</div>';return}
    renderMetrics(data);
    metricsLoaded=true;
  }).catch(function(e){document.getElementById('metricsError').innerHTML='<div class="error-box">'+esc(e.message)+'</div>'});
}
function renderMetrics(d){
  function g(ds,r,c){try{return d[ds]&&d[ds].rows&&d[ds].rows[r]?d[ds].rows[r][c]:null}catch(e){return null}}
  function setEl(id,v){var e=document.getElementById(id);if(e)e.textContent=v||'\\u2014'}

  /* KPI summary row */
  if(d.pageviews_by_service&&d.pageviews_by_service.rows){
    var totalPV=0;d.pageviews_by_service.rows.forEach(function(r){totalPV+=parseInt(r[1])||0});
    setEl('mkpi0',fmt(totalPV));
  }
  if(d.imdb_overview&&d.imdb_overview.rows&&d.imdb_overview.rows[0]){
    var io=d.imdb_overview.rows[0];
    setEl('mkpi1',io[0]+' / 10');
  }
  if(d.content_churn&&d.content_churn.rows){
    var td=0,ta=0;d.content_churn.rows.forEach(function(r){td+=parseInt(r[1])||0;ta+=parseInt(r[3])||0});
    setEl('mkpi2',(ta>0?(td*100/ta).toFixed(1):'0')+'%');
  }
  if(d.runtime_distribution&&d.runtime_distribution.rows&&d.runtime_distribution.rows[0]){
    var rt=d.runtime_distribution.rows[0];
    setEl('mkpi3',rt[0]+' min');
  }
  if(d.window_overall&&d.window_overall.rows&&d.window_overall.rows[0]){
    setEl('mkpi4',d.window_overall.rows[0][0]+' days');
  }

  /* ── Engagement ── */
  if(d.pageviews_by_service&&d.pageviews_by_service.rows&&d.pageviews_by_service.rows[0]){
    var top=d.pageviews_by_service.rows[0];
    setEl('mPeakTitle',top[0]);
    setEl('mPeakViews',fmt(top[4])+' views');
  }
  if(d.content_type_stats&&d.content_type_stats.rows){
    d.content_type_stats.rows.forEach(function(r){
      if(r[0]==='Series')setEl('mRtSeries',r[2]+' min/ep');
      if(r[0]==='Non-Series')setEl('mRtFilm',r[2]+' min');
    });
  }
  if(d.dropoff_added_vs_dropped&&d.dropoff_added_vs_dropped.rows){
    d.dropoff_added_vs_dropped.rows.forEach(function(r){
      if(r[0]==='Dropped')setEl('mDropViews',fmt(r[2]));
      if(r[0]==='Added')setEl('mAddViews',fmt(r[2]));
    });
  }
  if(d.imdb_overview&&d.imdb_overview.rows&&d.imdb_overview.rows[0]){
    var io=d.imdb_overview.rows[0];
    setEl('mHighRated',fmt(io[2])+' titles');
    setEl('mLowRated',fmt(io[3])+' titles');
  }

  /* ── QoE ── */
  if(d.dropoff_added_vs_dropped&&d.dropoff_added_vs_dropped.rows){
    d.dropoff_added_vs_dropped.rows.forEach(function(r){
      if(r[0]==='Added')setEl('mRatingAdded',r[3]+' / 10');
      if(r[0]==='Dropped')setEl('mRatingDropped',r[3]+' / 10');
    });
  }
  if(d.window_overall&&d.window_overall.rows&&d.window_overall.rows[0]){
    var wo=d.window_overall.rows[0];
    setEl('mWindowAvg',wo[0]+' days');
    setEl('mWindowMed',wo[1]+' days');
  }
  if(d.runtime_distribution&&d.runtime_distribution.rows&&d.runtime_distribution.rows[0]){
    var rt=d.runtime_distribution.rows[0];
    setEl('mLongForm',fmt(rt[6])+' titles');
    setEl('mShortForm',fmt(rt[3])+' titles');
  }
  if(d.dropoff_added_vs_dropped&&d.dropoff_added_vs_dropped.rows){
    var totalExcl=0,totalAll=0;
    d.dropoff_added_vs_dropped.rows.forEach(function(r){totalExcl+=parseInt(r[5])||0;totalAll+=parseInt(r[1])||0});
    setEl('mExclTotal',fmt(totalExcl));
    setEl('mExclRate',(totalAll>0?(totalExcl*100/totalAll).toFixed(1):'0')+'%');
  }

  /* ── Business ── */
  if(d.content_churn&&d.content_churn.rows){
    var td=0,ta=0;d.content_churn.rows.forEach(function(r){td+=parseInt(r[1])||0;ta+=parseInt(r[3])||0});
    setEl('mChurnRate',(ta>0?(td*100/ta).toFixed(1):'0')+'%');
    if(d.content_churn.rows[0])setEl('mChurnWorst',d.content_churn.rows[0][0]+' ('+d.content_churn.rows[0][4]+'%)');
  }
  if(d.avod_svod&&d.avod_svod.rows){
    d.avod_svod.rows.forEach(function(r){
      if(r[0]==='AVOD')setEl('mAvodTitles',fmt(r[1])+' ('+fmt(r[3])+' avg views)');
      if(r[0]==='SVOD')setEl('mSvodTitles',fmt(r[1])+' ('+fmt(r[3])+' avg views)');
    });
  }
  if(d.business_model&&d.business_model.rows){
    d.business_model.rows.forEach(function(r){
      if(r[0]==='AVOD')setEl('mOrigAvod',fmt(r[3])+' ('+r[4]+'%)');
      if(r[0]==='SVOD')setEl('mOrigSvod',fmt(r[3])+' ('+r[4]+'%)');
    });
  }
  if(d.geo_summary&&d.geo_summary.rows){
    d.geo_summary.rows.forEach(function(r){
      if(r[0]==='US')setEl('mGeoUS',fmt(r[1])+' titles');
      if(r[0]==='CA')setEl('mGeoCA',fmt(r[1])+' titles');
    });
  }

  /* ── Exclusivity table ── */
  if(d.exclusivity_by_service&&d.exclusivity_by_service.rows&&d.exclusivity_by_service.rows.length){
    document.getElementById('exclusivityBody').innerHTML=d.exclusivity_by_service.rows.map(function(r){
      var pct=parseFloat(r[3])||0,cls=pct>60?'badge-green':pct>30?'badge-blue':'badge-red';
      return '<tr><td>'+esc(r[0])+'</td><td class="num">'+fmt(r[1])+'</td><td class="num">'+fmt(r[2])+'</td><td class="num"><span class="badge '+cls+'">'+r[3]+'%</span></td><td class="num">'+fmt(r[4])+'</td></tr>';
    }).join('');
  } else {
    document.getElementById('exclusivityBody').innerHTML='<tr><td colspan="5" class="loading-msg">No data</td></tr>';
  }

  /* ── Charts ── */
  if(typeof Chart==='undefined') return;
  try{
    /* Page views by service */
    if(d.pageviews_by_service&&d.pageviews_by_service.rows&&d.pageviews_by_service.rows.length)
      makeHBar('chartPageviews',d.pageviews_by_service.rows.map(function(r){return r[0]}),d.pageviews_by_service.rows.map(function(r){return parseInt(r[1])||0}),'Total Page Views',0);
    /* Engagement by category */
    if(d.engagement_by_category&&d.engagement_by_category.rows&&d.engagement_by_category.rows.length)
      makeHBar('chartEngCategory',d.engagement_by_category.rows.map(function(r){return r[0]}),d.engagement_by_category.rows.map(function(r){return parseInt(r[2])||0}),'Avg Page Views',4);
    /* Window duration by service */
    if(d.window_by_service&&d.window_by_service.rows&&d.window_by_service.rows.length)
      makeHBar('chartWindowSvc',d.window_by_service.rows.map(function(r){return r[0]}),d.window_by_service.rows.map(function(r){return parseFloat(r[1])||0}),'Avg Days',2);
    /* Runtime distribution donut */
    if(d.runtime_distribution&&d.runtime_distribution.rows&&d.runtime_distribution.rows[0]){
      var rt=d.runtime_distribution.rows[0];
      makeDonut('chartRuntime',['Short (<10 min)','Mid (10-30 min)','Standard (30-90 min)','Long (>90 min)'],[parseInt(rt[3])||0,parseInt(rt[4])||0,parseInt(rt[5])||0,parseInt(rt[6])||0]);
    }
    /* Content churn by service */
    if(d.content_churn&&d.content_churn.rows&&d.content_churn.rows.length){
      var rows=d.content_churn.rows;
      var cv=document.getElementById('chartChurn');if(cv){
        var labels=rows.map(function(r){return r[0]});
        var values=rows.map(function(r){return parseFloat(r[4])||0});
        var barColors=values.map(function(v){return v>50?'rgba(239,68,68,.7)':v>30?'rgba(245,158,11,.7)':'rgba(34,197,94,.7)'});
        new Chart(cv.getContext('2d'),{type:'bar',data:{labels:labels,datasets:[{label:'Churn %',data:values,backgroundColor:barColors,borderColor:barColors.map(function(c){return c.replace('.7)','1)')}),borderWidth:1,borderRadius:4,maxBarThickness:28}]},
          options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false},tooltip:Object.assign({},TT,{callbacks:{label:function(ctx){return ctx.raw.toFixed(1)+'% titles dropped'}}})},
            scales:{x:{ticks:{color:TICK,font:{size:10},callback:function(v){return v+'%'}},grid:{color:GRID},max:100},y:{ticks:{color:LABEL,font:{size:11}},grid:{color:GRID}}}}});
      }
    }
    /* Origin countries */
    if(d.origin_country&&d.origin_country.rows&&d.origin_country.rows.length)
      makeHBar('chartOrigin',d.origin_country.rows.map(function(r){return r[0]}),d.origin_country.rows.map(function(r){return parseInt(r[1])||0}),'Titles',5);
  }catch(e){console.error('Metrics chart error:',e)}
}

/* ── Competitive Analysis ── */
var compLoaded = false;
function loadCompetitive(){
  if(compLoaded) return;
  fetch('/api/metrics/competitive').then(function(r){return r.json()}).then(function(data){
    if(data.error){document.getElementById('compError').innerHTML='<div class="error-box">'+esc(data.error)+'</div>';return}
    renderCompetitive(data);
    compLoaded=true;
  }).catch(function(e){document.getElementById('compError').innerHTML='<div class="error-box">'+esc(e.message)+'</div>'});
}
function renderCompetitive(d){
  /* 1. Platform Growth table */
  if(d.platform_growth&&d.platform_growth.rows&&d.platform_growth.rows.length){
    var rows=d.platform_growth.rows;
    document.getElementById('compGrowthBody').innerHTML=rows.map(function(r){
      var net=parseInt(r[3])||0,cls=net>0?'badge-green':net<0?'badge-red':'badge-blue',sign=net>0?'+':'';
      var churnCls=parseFloat(r[4])>50?'style="color:#f87171"':parseFloat(r[4])<35?'style="color:#4ade80"':'';
      return '<tr><td style="font-weight:600">'+esc(r[0])+'</td><td class="num" style="color:#4ade80">'+fmt(r[1])+'</td><td class="num" style="color:#f87171">'+fmt(r[2])+'</td><td class="num"><span class="badge '+cls+'">'+sign+fmt(net)+'</span></td><td class="num" '+churnCls+'>'+r[4]+'%</td><td class="num">'+fmt(r[5])+'</td><td class="num">'+fmt(r[6])+'</td><td class="num">'+fmt(r[7])+'</td><td class="num">'+r[8]+'%</td></tr>';
    }).join('');
    if(typeof Chart!=='undefined'){
      var labels=rows.map(function(r){return r[0]});
      var nets=rows.map(function(r){return parseInt(r[3])||0});
      var barColors=nets.map(function(v){return v>0?'rgba(34,197,94,.7)':'rgba(239,68,68,.7)'});
      var cv1=document.getElementById('chartCompNet');
      if(cv1) new Chart(cv1.getContext('2d'),{type:'bar',data:{labels:labels,datasets:[{label:'Net Change',data:nets,backgroundColor:barColors,borderColor:barColors.map(function(c){return c.replace('.7)','1)')}),borderWidth:1,borderRadius:4}]},
        options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false},tooltip:Object.assign({},TT,{callbacks:{label:function(ctx){return (ctx.raw>0?'+':'')+fmt(ctx.raw)+' titles'}}})},
          scales:{x:{ticks:{color:TICK,font:{size:10}},grid:{color:GRID}},y:{ticks:{color:LABEL,font:{size:11}},grid:{color:GRID}}}}});
      var exclPct=rows.map(function(r){return parseFloat(r[8])||0});
      var avgViews=rows.map(function(r){return parseInt(r[6])||0});
      var cv2=document.getElementById('chartCompExcl');
      if(cv2) new Chart(cv2.getContext('2d'),{type:'scatter',data:{datasets:[{label:'Platforms',data:labels.map(function(_,i){return{x:exclPct[i],y:avgViews[i]}}),backgroundColor:COLORS.slice(0,labels.length),pointRadius:8,pointHoverRadius:10}]},
        options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:Object.assign({},TT,{callbacks:{label:function(ctx){return labels[ctx.dataIndex]+': '+ctx.raw.x+'% excl, '+fmt(ctx.raw.y)+' avg views'}}})},
          scales:{x:{title:{display:true,text:'Exclusivity %',color:LABEL,font:{size:11}},ticks:{color:TICK,font:{size:10},callback:function(v){return v+'%'}},grid:{color:GRID}},y:{title:{display:true,text:'Avg Page Views',color:LABEL,font:{size:11}},ticks:{color:TICK,font:{size:10},callback:function(v){return fmt(v)}},grid:{color:GRID}}}}});
    }
  }
  /* 2. NBCU Content */
  if(d.nbcu_content&&d.nbcu_content.rows&&d.nbcu_content.rows.length){
    document.getElementById('compNbcuBody').innerHTML=d.nbcu_content.rows.map(function(r){
      var net=(parseInt(r[3])||0)-(parseInt(r[4])||0),cls=net>=0?'badge-green':'badge-red',sign=net>0?'+':'';
      return '<tr><td style="font-weight:600">'+esc(r[0])+'</td><td class="num">'+fmt(r[1])+'</td><td class="num">'+fmt(r[2])+'</td><td class="num" style="color:#4ade80">'+fmt(r[3])+'</td><td class="num" style="color:#f87171">'+fmt(r[4])+'</td><td class="num"><span class="badge '+cls+'">'+sign+fmt(net)+'</span></td></tr>';
    }).join('');
  } else {document.getElementById('compNbcuBody').innerHTML='<tr><td colspan="6" class="loading-msg">No data</td></tr>'}
  /* 3. Window comparison charts */
  if(typeof Chart!=='undefined'){
    if(d.window_comparison&&d.window_comparison.rows&&d.window_comparison.rows.length){
      var wr=d.window_comparison.rows;
      var wLabels=wr.map(function(r){return r[0]}),wVals=wr.map(function(r){return parseFloat(r[1])||0});
      var wColors=wLabels.map(function(l){return l.indexOf('Fandango')>=0?'rgba(245,158,11,.8)':'rgba(59,130,246,.6)'});
      var cv3=document.getElementById('chartCompWindowS');
      if(cv3) new Chart(cv3.getContext('2d'),{type:'bar',data:{labels:wLabels,datasets:[{label:'Avg Days',data:wVals,backgroundColor:wColors,borderColor:wColors.map(function(c){return c.replace(/\\.\\d\\)/,'1)')}),borderWidth:1,borderRadius:4,maxBarThickness:24}]},
        options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false},tooltip:Object.assign({},TT,{callbacks:{label:function(ctx){return fmt(ctx.raw)+' days'}}})},
          scales:{x:{ticks:{color:TICK,font:{size:10}},grid:{color:GRID}},y:{ticks:{color:LABEL,font:{size:11}},grid:{color:GRID}}}}});
    }
    if(d.window_nonseries&&d.window_nonseries.rows&&d.window_nonseries.rows.length){
      var nr=d.window_nonseries.rows;
      var nLabels=nr.map(function(r){return r[0]}),nVals=nr.map(function(r){return parseFloat(r[1])||0});
      var nColors=nLabels.map(function(l){return l.indexOf('Fandango')>=0?'rgba(245,158,11,.8)':'rgba(16,185,129,.6)'});
      var cv4=document.getElementById('chartCompWindowNS');
      if(cv4) new Chart(cv4.getContext('2d'),{type:'bar',data:{labels:nLabels,datasets:[{label:'Avg Days',data:nVals,backgroundColor:nColors,borderColor:nColors.map(function(c){return c.replace(/\\.\\d\\)/,'1)')}),borderWidth:1,borderRadius:4,maxBarThickness:24}]},
        options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false},tooltip:Object.assign({},TT,{callbacks:{label:function(ctx){return fmt(ctx.raw)+' days'}}})},
          scales:{x:{ticks:{color:TICK,font:{size:10}},grid:{color:GRID}},y:{ticks:{color:LABEL,font:{size:11}},grid:{color:GRID}}}}});
    }
  }
  /* 4. High-value drops */
  if(d.high_value_drops&&d.high_value_drops.rows&&d.high_value_drops.rows.length){
    document.getElementById('compDropsBody').innerHTML=d.high_value_drops.rows.map(function(r){
      return '<tr><td style="font-weight:600">'+esc(r[0])+'</td><td>'+esc(r[1])+'</td><td class="num" style="color:#fbbf24;font-weight:600">'+fmt(r[2])+'</td><td class="num">'+fmt(r[3])+'</td><td>'+esc(r[4])+'</td><td>'+(r[5]==='1'||r[5]===1?'<span class="badge badge-green">Yes</span>':'No')+'</td></tr>';
    }).join('');
  } else {document.getElementById('compDropsBody').innerHTML='<tr><td colspan="6" class="loading-msg">No data</td></tr>'}
  /* 5. Exclusive adds */
  if(d.high_value_adds&&d.high_value_adds.rows&&d.high_value_adds.rows.length){
    document.getElementById('compAddsBody').innerHTML=d.high_value_adds.rows.map(function(r){
      return '<tr><td style="font-weight:600">'+esc(r[0])+'</td><td>'+esc(r[1])+'</td><td class="num" style="color:#4ade80;font-weight:600">'+fmt(r[2])+'</td><td class="num">'+fmt(r[3])+'</td><td>'+esc(r[4])+'</td></tr>';
    }).join('');
  } else {document.getElementById('compAddsBody').innerHTML='<tr><td colspan="5" class="loading-msg">No data</td></tr>'}
  /* 6. Category engagement charts */
  if(typeof Chart!=='undefined'&&d.category_engagement&&d.category_engagement.rows&&d.category_engagement.rows.length){
    var cr=d.category_engagement.rows;
    makeHBar('chartCompCatEng',cr.map(function(r){return r[0]}),cr.map(function(r){return parseInt(r[2])||0}),'Avg Page Views',0);
    var catLabels=cr.map(function(r){return r[0]});
    var catAdded=cr.map(function(r){return parseInt(r[4])||0});
    var catDropped=cr.map(function(r){return parseInt(r[5])||0});
    makeGrouped('chartCompCatFlow',catLabels,catAdded,catDropped);
  }
  /* 7. Multi-platform overlap */
  if(d.multi_platform_titles&&d.multi_platform_titles.rows&&d.multi_platform_titles.rows.length){
    document.getElementById('compOverlapBody').innerHTML=d.multi_platform_titles.rows.map(function(r){
      return '<tr><td style="font-weight:600">'+esc(r[0])+'</td><td class="num"><span class="badge badge-red">'+r[1]+'</span></td><td style="font-size:11px;color:var(--text2)">'+esc(r[2])+'</td><td class="num">'+fmt(r[3])+'</td><td class="num">'+fmt(r[4])+'</td></tr>';
    }).join('');
  } else {document.getElementById('compOverlapBody').innerHTML='<tr><td colspan="5" class="loading-msg">No data</td></tr>'}
  /* 8. Network groups */
  if(d.network_groups&&d.network_groups.rows&&d.network_groups.rows.length){
    document.getElementById('compNetworkBody').innerHTML=d.network_groups.rows.map(function(r){
      var net=(parseInt(r[3])||0)-(parseInt(r[4])||0),cls=net>=0?'badge-green':'badge-red',sign=net>0?'+':'';
      var isVersant=r[0]==='NBCUniversal';
      return '<tr style="'+(isVersant?'background:rgba(245,158,11,.06)':'')+'"><td style="font-weight:600">'+(isVersant?'<span style="color:#fbbf24">&#x2605; </span>':'')+esc(r[0])+'</td><td class="num">'+fmt(r[1])+'</td><td class="num">'+fmt(r[2])+'</td><td class="num" style="color:#4ade80">'+fmt(r[3])+'</td><td class="num" style="color:#f87171">'+fmt(r[4])+'</td><td class="num"><span class="badge '+cls+'">'+sign+fmt(net)+'</span></td></tr>';
    }).join('');
  } else {document.getElementById('compNetworkBody').innerHTML='<tr><td colspan="6" class="loading-msg">No data</td></tr>'}
}

/* ── Search debounce ── */
var searchTimer;
document.getElementById('searchInput').addEventListener('input',function(){
  clearTimeout(searchTimer);
  searchTimer=setTimeout(function(){explorerState.search=document.getElementById('searchInput').value;explorerState.page=1;loadExplorer()},400);
});

/* ── Init ── */
loadMetrics();
</script>
</body>
</html>"""
