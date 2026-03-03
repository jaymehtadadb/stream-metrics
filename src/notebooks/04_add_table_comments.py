# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Add Table & Column Comments
# MAGIC
# MAGIC Adds detailed column-level comments to all Stream Metrics tables.
# MAGIC These comments power the Genie Space by providing context for natural language queries.
# MAGIC
# MAGIC **Why this matters:**
# MAGIC - Genie uses table/column comments to understand the data semantics
# MAGIC - Well-documented columns lead to more accurate SQL generation
# MAGIC - Comments serve as a living data dictionary

# COMMAND ----------

dbutils.widgets.text("catalog", "jay_mehta_catalog", "Catalog Name")
dbutils.widgets.text("schema", "stream_metrics", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Comment Definitions
# MAGIC
# MAGIC Comprehensive column descriptions sourced from the Stream Metrics data dictionary.

# COMMAND ----------

COLUMN_COMMENTS = {
    "vod_title_availability_detail": {
        "record_id": "Internal temporary identifier",
        "current_to": "The day, week, month, quarter or year the title is present. For a title to be present, it must be available within the following intervals: day (24 hours), week (7 days), month (30 days on average), quarter (90 days), year (365 days).",
        "current_to_id": "Numeric 'Current to' (airing month) identifier.",
        "country": "Country of the provider site services.",
        "business_model": "Primary revenue source for the streaming service.",
        "service_name": "Name of the provider service brand.",
        "offer_type": "Name of the offer type (e.g. AVOD, SVOD, TVOD, etc.).",
        "content_type": "Indicates whether the title is a Series or Non-series.",
        "programs_carried": "Total count of episodes available for that series.",
        "total_runtime_min": "Total series or non-series runtime for a provider.",
        "mom_changes": "Month Over Month Changes: 'added' or 'dropped' for the month.",
        "series_id": "Internal Stream Metrics ID for a series.",
        "program_id": "Internal Stream Metrics ID for an episode or non-series.",
        "title": "Title of a series or non-series (movie, event, other).",
        "alternate_title": "Alternate name, usually foreign language or descriptive.",
        "is_exclusive_avod_and_svod": "Flag (1,0) - Exclusive on one AVOD or SVOD service.",
        "is_theatrical": "Flag (1,0) - Movie released in theater first.",
        "media_type": "Tags a movie: Theatrical, TV, Web, Other.",
        "category": "Macro Genre #1 (9 total) - Animation, Anime, Documentary, Film, Scripted, Sport, Stage, Stand up, Unscripted.",
        "genre": "Micro Genre - one or more descriptive genres per title.",
        "consolidated_genre": "Macro Genre #2 (5 total) - Anime, Comedy, Drama, Kids/Animated, Unscript.",
        "unscripted_genre": "Macro Genre #3 (14 total) - emphasis on Reality sub-genres.",
        "is_scripted": "Flag (1,0) - Scripted=1, Unscripted=0.",
        "original_network": "Network the title premiered on.",
        "original_network_second": "2nd network the title premiered on.",
        "network_group": "Company owning the original network.",
        "network_group_second": "Company owning the 2nd network.",
        "distributor": "Company that owns distribution rights.",
        "distributor_group": "Parent company of the distributor.",
        "sm_originals": "Flag (1,0) - SM Original: premiered on this streaming platform first AND Network Name matches Provider Name.",
        "sm_originals_second": "SM Original flag for secondary network.",
        "is_off_net": "Flag (1,0) - Series premiered on linear first.",
        "is_off_net_second": "Flag for 2nd linear premiere.",
        "is_web": "Flag (1,0) - Series premiered on the web.",
        "is_us": "Flag (1,0) - Country of origin is the US.",
        "original_country": "Country of origin or production.",
        "original_language": "Primary language of the title.",
        "start_year": "Premiere year for the title.",
        "start_year_range": "Ranges: Current (2021+), Recent (2010-2020), Modern (2000-2009), Classic (before 2000).",
        "runtime_sec": "Duration of a program in seconds.",
        "is_short": "Flag (1,0) - Runtime less than 10 minutes.",
        "maturity_rating": "Age appropriateness rating (not fully populated).",
        "imdb_rating": "User rating from IMDb.com.",
        "imdb_id": "Unique identifier from IMDb.com.",
        "validated_through": "Month/Year the data was validated in Stream Metrics.",
        "report_updated": "Date the report is current to.",
    },
    "vod_title_availability_summary": {
        "record_id": "Internal temporary identifier",
        "report_interval": "Comparison time frame: Annual, Quarterly, Monthly, Weekly.",
        "current_to": "Period the data covers.",
        "current_to_id": "Numeric period identifier.",
        "country": "Country of the provider site services.",
        "business_model": "Primary revenue source for the streaming service.",
        "service_name": "Name of the provider service brand.",
        "offer_type": "Offer type (AVOD, SVOD, TVOD, etc.).",
        "matched_titles": "Total tagged titles available (series + non-series).",
        "series": "Total series titles available.",
        "movies_non_series": "Total movie titles available.",
        "other_non_series": "Total licensed non-movie programs (Stage, Stand Up, Unscripted).",
        "sm_originals": "Count of SM Originals.",
        "acquired_series": "Total licensed series titles.",
        "acquired_movies_non_series": "Total licensed movie titles.",
        "acquired_other_non_series": "Total licensed TV Special titles.",
        "scripted_series": "Total scripted series.",
        "unscripted_series": "Total unscripted series.",
        "off_net_series": "Total Off-net Series.",
        "theatrical_non_series": "Total theatrical movies.",
        "us": "US origin titles count.",
        "intl": "International (non-US) origin titles count.",
        "matched_titles_minutes": "Total runtime minutes for tagged titles.",
        "series_minutes": "Total runtime minutes of series.",
        "movies_non_series_minutes": "Total runtime minutes of movies.",
        "other_non_series_minutes": "Total runtime minutes of other programs.",
        "sm_originals_minutes": "Total runtime minutes for SM Originals.",
        "acquired_series_minutes": "Total runtime minutes of licensed series.",
        "acquired_movies_non_series_minutes": "Total runtime minutes of licensed movies.",
        "acquired_other_non_series_minutes": "Total runtime minutes of licensed TV Specials.",
        "scripted_series_minutes": "Total runtime minutes of scripted series.",
        "unscripted_series_minutes": "Total runtime minutes of unscripted series.",
        "off_net_series_minutes": "Total runtime minutes of Off-net Series.",
        "theatrical_non_series_minutes": "Total runtime minutes of theatrical movies.",
        "us_minutes": "Total runtime minutes of US titles.",
        "intl_minutes": "Total runtime minutes of non-US titles.",
        "validated_through": "Month/Year data was validated.",
        "report_updated": "Date the report is current to.",
    },
    "vod_title_changes_us_ca": {
        "record_id": "Internal temporary identifier",
        "current_to": "Period the data covers.",
        "country": "Country of the provider site services.",
        "business_model": "Primary revenue source for the streaming service.",
        "service_name": "Name of the provider service brand.",
        "offer_type": "Offer type (AVOD, SVOD, TVOD, etc.).",
        "current_month_changes": "Title was 'Added' or 'Dropped' comparing current month to previous.",
        "streaming_start_date": "Date the title was first carried by the service.",
        "streaming_end_date": "Date the title ceased streaming.",
        "content_type": "Series or Non-series.",
        "is_country_exclusive": "Exclusivity flag for the specific country across all tracked providers.",
        "series_id": "Internal Stream Metrics series ID.",
        "program_id": "Internal Stream Metrics program ID.",
        "title": "Title of the series or movie.",
        "alternate_title": "Alternate name for the title.",
        "is_theatrical": "Flag (1,0) - Theatrical release.",
        "category": "Macro Genre (9 categories).",
        "genre": "Micro Genre (detailed).",
        "consolidated_genre": "Macro Genre #2 (5 categories).",
        "unscripted_genre": "Macro Genre #3 (14 categories, Reality focus).",
        "original_network": "Original premiere network.",
        "network_group": "Company owning the original network.",
        "sm_originals": "SM Original flag.",
        "is_off_net": "Off-net flag.",
        "is_web": "Web premiere flag.",
        "start_year": "Premiere year.",
        "is_us": "US origin flag.",
        "original_country": "Country of origin.",
        "original_language": "Primary language.",
        "runtime_sec": "Duration in seconds.",
        "maturity_rating": "Age rating.",
        "imdb_id": "IMDb identifier.",
        "imdb_rating": "IMDb user rating.",
        "imdb_votes": "IMDb vote count.",
        "pageviews_views": "Total page views for the title during that month.",
        "pageviews_current_to": "Year/month the pageview data covers.",
        "pageviews_interval": "Pageview aggregation interval (Monthly).",
        "report_updated": "Date the report is current to.",
    },
    "vod_title_mom_changes": {
        "record_id": "Internal temporary identifier",
        "country": "Country of the provider site services.",
        "business_model": "Primary revenue source.",
        "service_name": "Provider service brand.",
        "offer_type": "Offer type.",
        "content_type": "Series or Non-series.",
        "change_type": "Monthly changes: Added or Dropped.",
        "validated_through": "Validation date.",
        "report_updated": "Report currency date.",
    },
    "vod_episode_availability_detail": {
        "record_id": "Internal temporary identifier",
        "current_to": "Period the data covers.",
        "current_to_id": "Numeric period identifier.",
        "country": "Country of the provider site services.",
        "provider_owner_group": "Company owning the streaming service.",
        "business_model": "Primary revenue source.",
        "service_name": "Provider service brand.",
        "offer_type": "Offer type.",
        "series_id": "Stream Metrics series ID.",
        "series_title": "Standardized series name.",
        "series_alternate_title": "Alternate series name.",
        "program_id": "Stream Metrics episode ID.",
        "season_number": "Season number.",
        "episode_number": "Episode number within the season.",
        "episode_title": "Name of the episode.",
        "category": "Macro Genre (9 categories).",
        "genre": "Micro Genre.",
        "consolidated_genre": "Macro Genre #2 (5 categories).",
        "unscripted_genre": "Macro Genre #3 (14 categories).",
        "provider_genre": "Genre as defined by the provider.",
        "is_scripted": "Scripted (1) or Unscripted (0).",
        "original_network": "Original premiere network.",
        "original_network_second": "2nd premiere network.",
        "network_group": "Parent company of original network.",
        "network_group_second": "Parent company of 2nd network.",
        "distributor": "Distribution rights holder.",
        "distributor_group": "Parent of distributor.",
        "sm_originals": "SM Original flag.",
        "sm_originals_second": "SM Original flag (secondary).",
        "is_off_net": "Off-net flag.",
        "is_off_net_second": "2nd off-net flag.",
        "is_web": "Web premiere flag.",
        "is_web_second": "2nd web premiere flag.",
        "is_us": "US origin flag.",
        "original_country": "Country of origin.",
        "original_language": "Primary language.",
        "start_year": "Premiere year.",
        "release_date": "Episode original air date.",
        "runtime_hrs": "Duration in hours.",
        "runtime_min": "Duration in minutes.",
        "runtime_sec": "Duration in seconds.",
        "series_imdb_id": "IMDb identifier for the series.",
        "validated_through": "Validation date.",
        "report_updated": "Report currency date.",
    },
    "window_series_trends": {
        "record_id": "Internal temporary identifier",
        "country": "Country of the provider site services.",
        "business_model": "Primary revenue source.",
        "service_name": "Provider service brand.",
        "offer_type": "Offer type.",
        "channel_name": "Standardized channel name.",
        "provider_channel_name": "Channel name per provider.",
        "window_type": "Window type: Linear, Movie, or Streaming.",
        "report_interval": "Time frame: Annual, Quarterly, Monthly.",
        "window_occurrence": "Streaming window occurrence number.",
        "current_to": "Period the data covers.",
        "avg_days": "Average window duration in days.",
        "avg_days_trend": "Quarterly trend for avg window.",
        "series": "Series count for trend calculation.",
        "avg_currently_shared_days": "Avg window for titles shared across providers.",
        "avg_currently_exclusive_days": "Avg window for exclusively held titles.",
        "report_updated": "Report currency date.",
    },
    "window_series_detail": {
        "record_id": "Internal temporary identifier",
        "country": "Country of provider.",
        "business_model": "Revenue model.",
        "service_name": "Provider brand.",
        "offer_type": "Offer type.",
        "series_id": "Stream Metrics series ID.",
        "series_title": "Standardized series name.",
        "currently_carried": "Flag - title still available on this provider.",
        "currently_carried_exclusive": "Flag - exclusively available on this provider.",
        "included_in_summaries": "Flag - included in trend summaries.",
        "category": "Macro Genre.",
        "sm_originals": "SM Original flag.",
        "is_off_net": "Off-net flag.",
        "pre_streaming_window_start_date": "Date title premiered on linear/other platform.",
        "pre_streaming_window_days": "Days between linear premiere and streaming start.",
        "report_updated": "Report currency date.",
    },
    "window_series_seasons": {
        "record_id": "Internal temporary identifier",
        "country": "Country of provider.",
        "business_model": "Revenue model.",
        "service_name": "Provider brand.",
        "offer_type": "Offer type.",
        "series_id": "Stream Metrics series ID.",
        "series_title": "Standardized series name.",
        "season_number": "Season number.",
        "currently_carried": "Flag - still available.",
        "season_episode_cadence": "SM Originals: avg days between episodes (0 = all dropped same day).",
        "sm_originals": "SM Original flag.",
        "report_updated": "Report currency date.",
    },
    "window_series_episodes": {
        "record_id": "Internal temporary identifier",
        "country": "Country of provider.",
        "business_model": "Revenue model.",
        "service_name": "Provider brand.",
        "offer_type": "Offer type.",
        "series_id": "Stream Metrics series ID.",
        "program_id": "Stream Metrics episode ID.",
        "series_title": "Standardized series name.",
        "season_number": "Season number.",
        "episode_number": "Episode number.",
        "title": "Episode title.",
        "currently_carried": "Flag - still available.",
        "sm_originals": "SM Original flag.",
        "report_updated": "Report currency date.",
    },
    "window_non_series_detail": {
        "record_id": "Internal temporary identifier",
        "country": "Country of provider.",
        "business_model": "Revenue model.",
        "service_name": "Provider brand.",
        "offer_type": "Offer type.",
        "program_id": "Stream Metrics program ID.",
        "non_series_title": "Standardized movie title.",
        "currently_carried": "Flag - still available.",
        "is_theatrical": "Flag - theatrical release.",
        "media_type": "Theatrical, TV, Web, Other.",
        "category": "Macro Genre.",
        "sm_originals": "SM Original flag.",
        "release_year": "Release year.",
        "runtime_sec": "Duration in seconds.",
        "report_updated": "Report currency date.",
    },
    "window_non_series_trends": {
        "record_id": "Internal temporary identifier",
        "country": "Country of provider.",
        "business_model": "Revenue model.",
        "service_name": "Provider brand.",
        "offer_type": "Offer type.",
        "window_type": "Window type: Linear, Movie, Streaming.",
        "report_interval": "Time frame.",
        "current_to": "Period the data covers.",
        "avg_days": "Average window duration in days.",
        "avg_days_trend": "Quarterly trend.",
        "non_series": "Movie count for trend calculation.",
        "avg_theatrical_days": "Avg window for theatrical movies.",
        "avg_non_theatrical_days": "Avg window for non-theatrical movies.",
        "report_updated": "Report currency date.",
    },
    "vod_title_availability_native": {
        "record_id": "Internal temporary identifier",
        "country": "Country of provider.",
        "business_model": "Revenue model.",
        "service_name": "Provider brand.",
        "offer_type": "Offer type.",
        "provider_content_type": "Content type from provider (Series, Non-series, Episode).",
        "provider_title": "Title as listed by the provider.",
        "current_month_changes": "Added or Dropped this month.",
        "provider_release_year": "Release year per provider.",
        "provider_genre": "Genre per provider.",
        "provider_is_original": "Original flag per provider.",
        "provider_runtime_sec": "Runtime in seconds per provider.",
        "report_updated": "Report currency date.",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Column Comments

# COMMAND ----------

success_count = 0
error_count = 0

for table_name, columns in COLUMN_COMMENTS.items():
    full_table = f"{catalog}.{schema}.{table_name}"
    print(f"\nApplying comments to {full_table}...")

    try:
        existing_cols = [c.name for c in spark.table(full_table).schema]
    except Exception as e:
        print(f"  SKIPPED - Table not found: {e}")
        continue

    applied = 0
    for col_name, comment in columns.items():
        if col_name in existing_cols:
            safe_comment = comment.replace("'", "\\'")
            try:
                spark.sql(f"ALTER TABLE {full_table} ALTER COLUMN `{col_name}` COMMENT '{safe_comment}'")
                applied += 1
            except Exception as e:
                print(f"  Warning: Could not comment {col_name}: {e}")
                error_count += 1

    print(f"  Applied {applied}/{len(columns)} column comments")
    success_count += applied

print(f"\nTotal: {success_count} comments applied, {error_count} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Comments on Sample Table

# COMMAND ----------

sample_table = f"{catalog}.{schema}.vod_title_changes_us_ca"
print(f"Sample column comments for {sample_table}:\n")

desc = spark.sql(f"DESCRIBE TABLE {sample_table}").collect()
for row in desc[:15]:
    if row["comment"]:
        print(f"  {row['col_name']:<30} {row['comment'][:80]}")
