#!/usr/bin/env python3
"""
Add join specifications to Genie space 01f1142ce4e51e52908a9772dfa7cd23
"""
import json
import uuid
from databricks.sdk import WorkspaceClient

SPACE_ID = "01f1142ce4e51e52908a9772dfa7cd23"
CATALOG = "jay_mehta_catalog.stream_metrics"

JOIN_SPECS = [
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "vod_episode_availability_detail",
        "condition": "vod_title_availability_detail.series_id = vod_episode_availability_detail.series_id AND vod_title_availability_detail.service_name = vod_episode_availability_detail.service_name",
        "instruction": "Use to drill from title-level catalog data to episode-level detail. Match on series_id and service_name for accurate episode counts per title per service. Only series content has matching episode records (content_type = 'Series').",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "vod_title_changes_us_ca",
        "condition": "vod_title_availability_detail.series_id = vod_title_changes_us_ca.series_id AND vod_title_availability_detail.service_name = vod_title_changes_us_ca.service_name",
        "instruction": "Use to correlate the full title catalog with recent adds/drops. Match on series_id and service_name. The changes table has current_month_changes (Added/Dropped) and streaming start/end dates.",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "window_series_detail",
        "condition": "vod_title_availability_detail.series_id = window_series_detail.series_id AND vod_title_availability_detail.service_name = window_series_detail.service_name",
        "instruction": "Use to correlate VOD title availability with series streaming windows. Match on series_id and service_name. The window_series_detail table has up to 18 streaming windows with start/end dates and day counts.",
    },
    {
        "left_table": "vod_title_availability_detail",
        "right_table": "window_non_series_detail",
        "condition": "vod_title_availability_detail.program_id = window_non_series_detail.program_id AND vod_title_availability_detail.service_name = window_non_series_detail.service_name",
        "instruction": "Use to correlate VOD non-series (movie) availability with movie streaming windows. Match on program_id and service_name. Only for content_type = 'Non-series'.",
    },
    {
        "left_table": "vod_title_availability_summary",
        "right_table": "window_series_trends",
        "condition": "vod_title_availability_summary.service_name = window_series_trends.service_name AND vod_title_availability_summary.country = window_series_trends.country",
        "instruction": "Use to compare service-level catalog size with windowing duration trends. Match on service_name and country. Both tables have aggregated metrics per service.",
    },
    {
        "left_table": "vod_title_availability_summary",
        "right_table": "window_non_series_trends",
        "condition": "vod_title_availability_summary.service_name = window_non_series_trends.service_name AND vod_title_availability_summary.country = window_non_series_trends.country",
        "instruction": "Use to compare service-level catalog size with non-series windowing duration trends. Match on service_name and country.",
    },
    {
        "left_table": "vod_episode_availability_detail",
        "right_table": "window_series_seasons",
        "condition": "vod_episode_availability_detail.series_id = window_series_seasons.series_id AND vod_episode_availability_detail.season_number = window_series_seasons.season_number AND vod_episode_availability_detail.service_name = window_series_seasons.service_name",
        "instruction": "Use cautiously - both tables have many rows. Always pre-aggregate one side first. Useful for checking if episode availability aligns with season-level streaming windows. Match on series_id, season_number, and service_name.",
    },
]


def to_sql_format(condition):
    """Convert condition to backtick format for Genie: table.col -> `table`.`col`"""
    # Parse "a.col = b.col AND a.col2 = b.col2" -> "`a`.`col` = `b`.`col` AND `a`.`col2` = `b`.`col2`"
    import re
    # Match table.column pattern
    def repl(m):
        return f"`{m.group(1)}`.`{m.group(2)}`"
    return re.sub(r'(\w+)\.(\w+)', repl, condition)


def make_id():
    return uuid.uuid4().hex


def build_join_specs():
    specs = []
    for js in JOIN_SPECS:
        left_id = f"{CATALOG}.{js['left_table']}"
        right_id = f"{CATALOG}.{js['right_table']}"
        sql_condition = to_sql_format(js["condition"])
        specs.append({
            "id": make_id(),
            "left": {"identifier": left_id, "alias": js["left_table"]},
            "right": {"identifier": right_id, "alias": js["right_table"]},
            "sql": [sql_condition, "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"],
            "instruction": [js["instruction"]],
        })
    specs.sort(key=lambda x: x["id"])
    return specs


def main():
    w = WorkspaceClient()
    
    print("Fetching current Genie space...")
    resp = w.genie.get_space(space_id=SPACE_ID, include_serialized_space=True)
    data = json.loads(resp.serialized_space)
    
    join_specs = build_join_specs()
    if "instructions" not in data:
        data["instructions"] = {}
    data["instructions"]["join_specs"] = join_specs
    
    print(f"Adding {len(join_specs)} join specifications...")
    
    w.genie.update_space(
        space_id=SPACE_ID,
        serialized_space=json.dumps(data),
    )
    print("SUCCESS: Join specs added.")
    for i, js in enumerate(JOIN_SPECS, 1):
        print(f"  {i}. {js['left_table']} <-> {js['right_table']}")
    return 0


if __name__ == "__main__":
    exit(main())
