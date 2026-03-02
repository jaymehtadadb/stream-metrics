# Versant Stream Metrics Explorer — Genie Space Configuration (Extracted)

**Space ID:** `01f113f62a2d125dba4cfd0de74c23ae`  
**Space Title:** Versant Stream Metrics Explorer  
**Source:** Reference Genie space at https://e2-demo-field-eng.cloud.databricks.com/genie/rooms/01f113f62a2d125dba4cfd0de74c23ae

---

## a. General Instructions / Text Instructions

The following text instructions are configured in the Genie space (from `instructions.text_instructions`):

---

### Domain & Abbreviations

This Genie Space contains streaming media distribution data from Stream Metrics, covering three domains: FAST TV, VOD availability, and content windowing.

**Key abbreviations and terms:**

- FAST = Free Ad-Supported Streaming Television (e.g., Pluto TV, Tubi, Roku Channel, Samsung TV Plus, Freevee)

- VOD = Video On Demand — content available to watch at any time

- SVOD = Subscription VOD (e.g., Netflix, Disney+, Hulu, HBO Max, Paramount+, Apple TV+, Amazon Prime Video)

- AVOD = Ad-supported VOD (e.g., Tubi, Pluto TV on-demand, Peacock free tier)

- TVOD = Transactional VOD — pay-per-view or rental (e.g., iTunes, Amazon Video rentals)

- EST = Electronic Sell-Through — digital purchase to own (e.g., buying a movie on Apple TV)

- PVOD = Premium VOD — early digital release window before standard VOD, typically $19.99-$29.99

- MoM = Month-over-Month — refers to changes between consecutive months in vod_title_mom_changes

- OTT = Over-The-Top — streaming content delivered directly over the internet, bypassing traditional cable/satellite

- Linear = Traditional scheduled broadcasting on a channel (as opposed to on-demand)

- Windowing = The sequential release strategy where content moves through exclusive distribution windows over time

- Window lifecycle = The progression path content follows: for series it is SVOD Exclusive → SVOD Shared → AVOD → FAST → Library; for movies it is Theatrical → PVOD → SVOD Exclusive → SVOD Shared → AVOD → FAST → Library

- Primetime = The peak viewing period, typically 8:00 PM to 11:00 PM local time. In programming_schedule, is_primetime = true indicates primetime airings

- Time block = Daypart classifications in programming_schedule: Early Morning, Morning, Afternoon, Prime Access, Primetime, Late Night, Overnight

- White-label channel = A channel branded and operated by a platform rather than a content owner; identified by is_white_label = true in the channels table

- Exclusive content = Content available on only one platform in a territory; in vod_title_availability, is_exclusive = true

- First-run = A title airing for the first time (is_repeat = false in programming_schedule), as opposed to a repeat airing

- Territory = A geographic region identified by ISO 2-letter country code (US, CA, UK, DE, FR, AU, JP, BR, MX, IN, KR, IT, ES, SE, NL)

**Platform name conventions:**

- Always use the exact platform names stored in the data: Netflix, Hulu, Disney+, Amazon Prime Video, HBO Max, Peacock, Paramount+, Apple TV+, Pluto TV, Tubi, Roku Channel, Samsung TV Plus, Crackle, Freevee, Xumo, Plex, Amazon Freevee

- When users say "Amazon" they likely mean Amazon Prime Video (SVOD) unless they specify Freevee (AVOD/FAST)

- When users say "Apple" they mean Apple TV+

- When users say "HBO" they mean HBO Max

- When users say "Paramount" they mean Paramount+

---

### Data Domain Guidance

**FAST TV tables (channels, providers, content_catalog, programming_schedule, channel_availability):**

- Use the channels table for questions about channel counts, genres, owners, or languages

- Use programming_schedule for questions about what airs when, primetime analysis, schedule density, repeat rates, or time-of-day analysis

- Use channel_availability for questions about distribution: which channels are on which providers, in which territories

- Use content_catalog for questions about the content library: title metadata, ratings, genres, production studios

- For "channel lineup" questions, join channels to channel_availability to providers

**VOD tables (vod_title_availability, vod_episode_availability, vod_title_mom_changes):**

- Use vod_title_availability for questions about what titles are available where, platform comparisons, exclusivity analysis, or content type breakdowns

- Use vod_episode_availability for episode-level drill-downs: how many episodes are available per season, episode completeness

- Use vod_title_mom_changes for trend analysis: what was added/removed each month, platform churn, window transitions

- The change_type field values are: Added, Removed, Renewed, Platform Switch, Window Change

- "Churn" means titles being Removed from a platform. "Net adds" = Added minus Removed in a given month

**Windowing tables (window_series, window_non_series):**

- Use window_series for series lifecycle tracking: how long series stay in each window, when they transition

- Use window_non_series for movie/special lifecycle: theatrical-to-library timeline analysis

- window_lifecycle_stage for series: SVOD Exclusive, SVOD Shared, AVOD, FAST, Library

- window_lifecycle_stage for non-series: Theatrical, PVOD, SVOD Exclusive, SVOD Shared, AVOD, FAST, Library

- window_start_date and window_end_date define the time bounds of each window stage

- window_duration_days shows how long content spent in that stage

- When users ask about "content lifecycle" or "release strategy," use the windowing tables

---

### Query Patterns & Business Logic

- When asked about "top platforms" or "biggest platforms," rank by number of available titles using vod_title_availability grouped by platform

- When asked about "content diversity," count distinct genres or content types

- When asked about "market coverage" or "reach," count distinct territories

- When asked about "schedule fill rate," calculate the ratio of scheduled hours to total hours per channel

- For "platform comparison" questions, always include a GROUP BY platform and consider filtering to a specific territory for fair comparison

- For "availability trends" over time, use vod_title_mom_changes with GROUP BY report_month and change_type

- When computing "exclusive content percentage," count titles WHERE is_exclusive = true divided by total titles per platform

- "Recently added" means change_type = 'Added' in the most recent report_month in vod_title_mom_changes

- "Content in the FAST window" means window_lifecycle_stage = 'FAST' and the window is currently active (window_end_date IS NULL or window_end_date >= CURRENT_DATE)

---

### Formatting & Presentation

- Always round percentages to 2 decimal places

- Format large numbers with commas for readability (e.g., use FORMAT_NUMBER)

- When showing date ranges, use YYYY-MM-DD format

- For duration analysis (window_duration_days), present averages rounded to whole days

- When showing top-N lists, default to top 10 unless the user specifies otherwise

- Order results by the most meaningful metric in descending order unless asked otherwise

- When presenting platform comparisons, always include the territory context

---

### Clarification Questions

When users ask about "availability" without specifying FAST vs VOD, ask: "Are you asking about FAST TV channel availability (which providers carry which channels) or VOD title availability (which titles are on which streaming platforms)?"

When users ask about "content" without specifying the domain, ask: "Would you like to analyze the FAST TV content catalog (titles airing on FAST channels), VOD title availability (titles on streaming platforms), or the content windowing lifecycle?"

When users ask about "trends" without a time range, ask: "What time period would you like to analyze? The MoM changes data covers the last 12 months. Please specify a date range or number of months."

When users ask about platform performance without specifying a territory, ask: "Which territory should I focus on? Platform availability varies significantly by region. Common territories: US, CA, UK, DE, FR, AU."

When users ask about "all content" or "everything," guide them: "The data spans 3 domains — FAST TV (channels/schedules), VOD availability (titles/episodes), and windowing (lifecycle tracking). Which domain would you like to explore first?"

---

### Instructions you must follow when providing summaries

- Include the total row count when applicable (e.g., "Across 5,508 title-availability records...")

- When summarizing platform data, always mention the territory scope

- Use bullet points for multi-part summaries

- Cite specific table and column names when the answer involves joins

- Highlight notable outliers or patterns (e.g., "Netflix has 3x more exclusive titles than the next platform")

- When showing time-based trends, note the direction of change (increasing, decreasing, stable)

---

### SQL Expression Definitions (Key Metrics)

Use these SQL expression definitions when users ask about these business metrics:

#### Measures (Aggregations)

**Total Active Channels**: COUNT(*) FROM channels WHERE status = 'active'
- Synonyms: channel count, number of channels, active channels

**Primetime Percentage**: ROUND(SUM(CASE WHEN is_primetime = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM programming_schedule
- Synonyms: primetime share, primetime ratio, prime time %

**First-Run Rate**: ROUND(SUM(CASE WHEN is_repeat = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM programming_schedule
- Synonyms: new content rate, original airing percentage, first run %

**Exclusive Content Percentage**: ROUND(SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT title_id), 2) FROM vod_title_availability
- Synonyms: exclusivity rate, exclusive %, exclusive content share

**Net Adds**: SUM(CASE WHEN change_type = 'Added' THEN 1 ELSE 0 END) - SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) FROM vod_title_mom_changes GROUP BY report_month
- Synonyms: net additions, net growth, title growth

**Content Churn Rate**: ROUND(SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM vod_title_mom_changes
- Synonyms: removal rate, attrition rate, churn %

**Average Window Duration**: ROUND(AVG(window_duration_days), 0) FROM window_series or window_non_series
- Synonyms: avg window length, typical window, mean duration

**Platform Library Size**: COUNT(DISTINCT title_id) FROM vod_title_availability WHERE is_currently_available = true GROUP BY platform
- Synonyms: catalog size, library count, content library, number of titles

#### Filters (Boolean Conditions)

**Currently Available**: is_currently_available = true
- Synonyms: active, live, available now, currently on

**Primetime Only**: is_primetime = true
- Synonyms: primetime, prime time, evening schedule, peak hours

**First-Run Only**: is_repeat = false
- Synonyms: new content, original, first airing, not a repeat

**Exclusive Only**: is_exclusive = true
- Synonyms: exclusive, platform exclusive, only on

**Active Window**: window_end_date IS NULL OR window_end_date >= CURRENT_DATE()
- Synonyms: current window, active stage, currently in window

**SVOD Content**: availability_type = 'SVOD'
- Synonyms: subscription, subscription streaming, paid streaming

**FAST Content**: availability_type = 'FAST'
- Synonyms: free streaming, ad-supported, free channels

**Recently Changed**: report_month = (SELECT MAX(report_month) FROM vod_title_mom_changes)
- Synonyms: latest month, most recent changes, recent updates

#### Dimensions (Grouping Attributes)

**Time Block**: time_block column in programming_schedule with values: Early Morning, Morning, Afternoon, Prime Access, Primetime, Late Night, Overnight
- Use for daypart analysis of programming schedules

**Content Type**: content_type column with values: Series, Movie, Special
- Use for breaking down analysis by content format

**Availability Type**: availability_type column with values: SVOD, AVOD, TVOD, EST, FAST
- Use for comparing distribution models

**Window Lifecycle Stage (Series)**: window_lifecycle_stage ordered: SVOD Exclusive, SVOD Shared, AVOD, FAST, Library
- Use for series lifecycle analysis. Always order by lifecycle progression, not alphabetically.

**Window Lifecycle Stage (Movies)**: window_lifecycle_stage ordered: Theatrical, PVOD, SVOD Exclusive, SVOD Shared, AVOD, FAST, Library
- Use for movie release strategy analysis. Always order by lifecycle progression, not alphabetically.

---

### Example SQL Queries for Common Questions

#### FAST TV Examples

**Example: How many active FAST channels are there by genre?**
```sql
SELECT genre, COUNT(*) AS channel_count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM jay_mehta_catalog.versant_stream_metrics.channels
WHERE status = 'active'
GROUP BY genre
ORDER BY channel_count DESC
```

**Example: Which providers carry the most channels?**
```sql
SELECT p.provider_name, p.parent_company, COUNT(DISTINCT ca.channel_id) AS channel_count, COUNT(DISTINCT ca.territory) AS territory_count
FROM jay_mehta_catalog.versant_stream_metrics.providers p
JOIN jay_mehta_catalog.versant_stream_metrics.channel_availability ca ON p.provider_id = ca.provider_id
WHERE ca.is_currently_available = true
GROUP BY p.provider_name, p.parent_company
ORDER BY channel_count DESC
```

**Example: What content airs during primetime on Comedy channels?**
```sql
SELECT cc.title, cc.content_type, COUNT(*) AS airing_count, ROUND(AVG(cc.imdb_rating), 1) AS avg_rating
FROM jay_mehta_catalog.versant_stream_metrics.programming_schedule ps
JOIN jay_mehta_catalog.versant_stream_metrics.channels c ON ps.channel_id = c.channel_id
JOIN jay_mehta_catalog.versant_stream_metrics.content_catalog cc ON ps.content_id = cc.content_id
WHERE c.genre = 'Comedy' AND ps.is_primetime = true
GROUP BY cc.title, cc.content_type
ORDER BY airing_count DESC
```

**Example: What is the repeat rate by time block across all channels?**
```sql
SELECT ps.time_block, COUNT(*) AS total_airings, SUM(CASE WHEN ps.is_repeat = true THEN 1 ELSE 0 END) AS repeats, ROUND(SUM(CASE WHEN ps.is_repeat = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repeat_pct
FROM jay_mehta_catalog.versant_stream_metrics.programming_schedule ps
GROUP BY ps.time_block
ORDER BY CASE ps.time_block WHEN 'Early Morning' THEN 1 WHEN 'Morning' THEN 2 WHEN 'Afternoon' THEN 3 WHEN 'Prime Access' THEN 4 WHEN 'Primetime' THEN 5 WHEN 'Late Night' THEN 6 WHEN 'Overnight' THEN 7 END
```

#### VOD Examples

**Example: Which platforms have the most exclusive VOD content in the US?**
```sql
SELECT platform, COUNT(DISTINCT title_id) AS total_titles, SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) AS exclusive_titles, ROUND(SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT title_id), 2) AS exclusive_pct
FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability
WHERE territory = 'US' AND is_currently_available = true
GROUP BY platform
ORDER BY exclusive_titles DESC
```

**Example: Show month-over-month title change trends**
```sql
SELECT report_month, change_type, COUNT(DISTINCT title_id) AS title_count
FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes
GROUP BY report_month, change_type
ORDER BY report_month, change_type
```

**Example: What is the net adds trend by month?**
```sql
SELECT report_month, SUM(CASE WHEN change_type = 'Added' THEN 1 ELSE 0 END) AS added, SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) AS removed, SUM(CASE WHEN change_type = 'Added' THEN 1 ELSE 0 END) - SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) AS net_adds
FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes
GROUP BY report_month
ORDER BY report_month
```

**Example: How many episodes are available per platform in the US?**
```sql
SELECT platform, COUNT(DISTINCT title_id) AS title_count, COUNT(*) AS total_episodes, SUM(CASE WHEN is_currently_available = true THEN 1 ELSE 0 END) AS available_episodes
FROM jay_mehta_catalog.versant_stream_metrics.vod_episode_availability
WHERE territory = 'US'
GROUP BY platform
ORDER BY total_episodes DESC
```

#### Windowing Examples

**Example: What is the average window duration by lifecycle stage for series?**
```sql
SELECT window_lifecycle_stage, COUNT(DISTINCT title_id) AS titles, ROUND(AVG(window_duration_days), 0) AS avg_days, MIN(window_duration_days) AS min_days, MAX(window_duration_days) AS max_days
FROM jay_mehta_catalog.versant_stream_metrics.window_series
GROUP BY window_lifecycle_stage
ORDER BY CASE window_lifecycle_stage WHEN 'SVOD Exclusive' THEN 1 WHEN 'SVOD Shared' THEN 2 WHEN 'AVOD' THEN 3 WHEN 'FAST' THEN 4 WHEN 'Library' THEN 5 END
```

**Example: How quickly do movies progress through release windows?**
```sql
SELECT window_lifecycle_stage, ROUND(AVG(window_duration_days), 0) AS avg_days, COUNT(DISTINCT title_id) AS title_count
FROM jay_mehta_catalog.versant_stream_metrics.window_non_series
GROUP BY window_lifecycle_stage
ORDER BY CASE window_lifecycle_stage WHEN 'Theatrical' THEN 1 WHEN 'PVOD' THEN 2 WHEN 'SVOD Exclusive' THEN 3 WHEN 'SVOD Shared' THEN 4 WHEN 'AVOD' THEN 5 WHEN 'FAST' THEN 6 WHEN 'Library' THEN 7 END
```

**Example: Which series are currently in the FAST window?**
```sql
SELECT title_id, title, genre, window_start_date, window_duration_days
FROM jay_mehta_catalog.versant_stream_metrics.window_series
WHERE window_lifecycle_stage = 'FAST' AND (window_end_date IS NULL OR window_end_date >= CURRENT_DATE())
ORDER BY window_start_date DESC
```

#### Cross-Domain Examples

**Example: Which titles are available on both FAST channels and VOD platforms?**
```sql
SELECT cc.title, cc.content_type, cc.genre, COUNT(DISTINCT ps.channel_id) AS fast_channels, COUNT(DISTINCT vta.platform) AS vod_platforms
FROM jay_mehta_catalog.versant_stream_metrics.content_catalog cc
JOIN jay_mehta_catalog.versant_stream_metrics.programming_schedule ps ON cc.content_id = ps.content_id
JOIN jay_mehta_catalog.versant_stream_metrics.vod_title_availability vta ON cc.title = vta.title
WHERE vta.territory = 'US' AND vta.is_currently_available = true
GROUP BY cc.title, cc.content_type, cc.genre
ORDER BY fast_channels + vod_platforms DESC
```

**Example: Compare platform presence across territories**
```sql
SELECT platform, territory, COUNT(DISTINCT title_id) AS title_count, SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) AS exclusive_count
FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability
WHERE is_currently_available = true
GROUP BY platform, territory
ORDER BY platform, title_count DESC
```

---

### Available SQL Functions

The following UC functions are registered and can be called directly for trusted metric calculations:

- jay_mehta_catalog.versant_stream_metrics.fast_channel_count_by_genre() — FAST channel distribution by genre
- jay_mehta_catalog.versant_stream_metrics.fast_provider_lineup_size() — Provider channel lineup comparison
- jay_mehta_catalog.versant_stream_metrics.fast_primetime_schedule_analysis() — Primetime vs non-primetime breakdown
- jay_mehta_catalog.versant_stream_metrics.fast_content_airtime_leaders() — Top 25 most-aired FAST titles
- jay_mehta_catalog.versant_stream_metrics.vod_platform_title_counts(territory) — Platform library sizes, default 'US'
- jay_mehta_catalog.versant_stream_metrics.vod_content_type_breakdown(territory) — Content type and genre breakdown
- jay_mehta_catalog.versant_stream_metrics.vod_mom_change_summary() — Month-over-month availability change trends
- jay_mehta_catalog.versant_stream_metrics.vod_episode_completeness(territory) — Episode availability per platform
- jay_mehta_catalog.versant_stream_metrics.window_series_lifecycle_summary() — Series windowing lifecycle stages
- jay_mehta_catalog.versant_stream_metrics.window_nonseries_lifecycle_summary() — Movie windowing lifecycle stages
- jay_mehta_catalog.versant_stream_metrics.cross_domain_title_distribution(territory) — Titles on both FAST and VOD
- jay_mehta_catalog.versant_stream_metrics.platform_territory_heatmap() — Platform presence across territories

---

## b. SQL Expression Instructions

The SQL expression instructions are embedded within the General Instructions (see section a above) under the heading **"## SQL Expression Definitions (Key Metrics)"**. They include:

- **Measures**: Total Active Channels, Primetime Percentage, First-Run Rate, Exclusive Content Percentage, Net Adds, Content Churn Rate, Average Window Duration, Platform Library Size
- **Filters**: Currently Available, Primetime Only, First-Run Only, Exclusive Only, Active Window, SVOD Content, FAST Content, Recently Changed
- **Dimensions**: Time Block, Content Type, Availability Type, Window Lifecycle Stage (Series), Window Lifecycle Stage (Movies)

Each has SQL definitions and synonyms as documented in section a.

---

## c. Joins / Table Relationships

The following join specs are configured in the Genie space (`instructions.join_specs`):

| # | Left Table | Right Table | Join Condition | Instruction |
|---|------------|-------------|----------------|-------------|
| 1 | channels | programming_schedule | `channels`.`channel_id` = `programming_schedule`.`channel_id` | Use when you need channel metadata (genre, owner, language) for scheduled programming. Always filter channels.status = 'active' for current channels. |
| 2 | channels | channel_availability | `channels`.`channel_id` = `channel_availability`.`channel_id` | Use to find which providers carry a channel. Combine with is_currently_available = true to see current distribution only. |
| 3 | providers | channel_availability | `providers`.`provider_id` = `channel_availability`.`provider_id` | Use to enrich provider details (parent company, HQ country) when analyzing channel distribution. The provider_name field already exists in channel_availability, so only join if you need parent_company or website. |
| 4 | content_catalog | programming_schedule | `content_catalog`.`content_id` = `programming_schedule`.`content_id` | Use to get title metadata (genre, rating, runtime, IMDb rating) for scheduled airings. Essential for analyzing what type of content airs in primetime vs off-peak. |
| 5 | vod_title_availability | vod_episode_availability | `vod_title_availability`.`title_id` = `vod_episode_availability`.`title_id` | Use to drill from title-level availability down to episode-level. Only series titles (title_id LIKE 'S%') have matching episode records. Filter vod_title_availability.content_type = 'series' before joining. |
| 6 | vod_title_availability | vod_title_mom_changes | `vod_title_availability`.`title_id` = `vod_title_mom_changes`.`title_id` | Use to see which currently-available titles had recent changes. Join with report_month filter to focus on a specific month. A single title can have multiple changes across different months/platforms. |
| 7 | vod_title_availability | window_series | `vod_title_availability`.`title_id` = `window_series`.`title_id` | Use to compare a series' current VOD availability against its windowing lifecycle. Filter title_id LIKE 'S%' since only series titles exist in window_series. Useful for understanding if a title's current platform matches its expected window stage. |
| 8 | vod_title_availability | window_non_series | `vod_title_availability`.`title_id` = `window_non_series`.`title_id` | Use to compare a movie's VOD availability against its theatrical-to-library lifecycle. Filter title_id LIKE 'M%' since only movies/specials exist in window_non_series. Useful for tracking if a movie has reached AVOD/FAST stage. |
| 9 | window_series | vod_episode_availability | `window_series`.`title_id` = `vod_episode_availability`.`title_id` AND `window_series`.`season_number` = `vod_episode_availability`.`season_number` | Use cautiously — both tables have many rows per title/season. Always pre-aggregate one side first. Useful for checking if episode availability aligns with the current window stage for a given season. |
| 10 | content_catalog | vod_title_availability | `content_catalog`.`title` = `vod_title_availability`.`title` | Use to bridge FAST content with VOD availability. This is a text-based join on title name (not an ID join), so be aware of possible mismatches. Use for exploratory analysis to see if FAST-scheduled content also appears in VOD catalogs. |

---

## d. Benchmark Questions / Curated Questions

The following benchmark questions are configured with expected SQL (`benchmarks.questions`):

| # | Question | Expected SQL |
|---|----------|--------------|
| 1 | How many total VOD titles are available across all platforms? | `SELECT COUNT(DISTINCT title_id) AS total_titles FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE is_currently_available = true` |
| 2 | Show month-over-month title adds and removes | `SELECT report_month, SUM(CASE WHEN change_type = 'Added' THEN 1 ELSE 0 END) AS added, SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) AS removed, SUM(CASE WHEN change_type = 'Added' THEN 1 ELSE 0 END) - SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) AS net_adds FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes GROUP BY report_month ORDER BY report_month` |
| 3 | Which series have the most episodes available on Netflix? | `SELECT title, COUNT(*) AS episode_count, COUNT(DISTINCT season_number) AS season_count FROM jay_mehta_catalog.versant_stream_metrics.vod_episode_availability WHERE platform = 'Netflix' AND territory = 'US' AND is_currently_available = true GROUP BY title ORDER BY episode_count DESC LIMIT 20` |
| 4 | How many titles are currently active in each windowing stage? | `SELECT window_lifecycle_stage, COUNT(DISTINCT title_id) AS active_titles FROM jay_mehta_catalog.versant_stream_metrics.window_series WHERE window_end_date IS NULL OR window_end_date >= CURRENT_DATE() GROUP BY window_lifecycle_stage ORDER BY CASE window_lifecycle_stage WHEN 'SVOD Exclusive' THEN 1 WHEN 'SVOD Shared' THEN 2 WHEN 'AVOD' THEN 3 WHEN 'FAST' THEN 4 WHEN 'Library' THEN 5 ELSE 6 END` |
| 5 | What percentage of programming is primetime by genre? | `SELECT c.genre, COUNT(*) AS total_airings, SUM(CASE WHEN ps.is_primetime = true THEN 1 ELSE 0 END) AS primetime_airings, ROUND(SUM(CASE WHEN ps.is_primetime = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS primetime_pct FROM jay_mehta_catalog.versant_stream_metrics.programming_schedule ps JOIN jay_mehta_catalog.versant_stream_metrics.channels c ON ps.channel_id = c.channel_id GROUP BY c.genre ORDER BY primetime_pct DESC` |
| 6 | How many titles changed availability last month? | `SELECT change_type, COUNT(DISTINCT title_id) AS title_count FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes WHERE report_month = (SELECT MAX(report_month) FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes) GROUP BY change_type ORDER BY title_count DESC` |
| 7 | Which territories have the most FAST channel coverage? | `SELECT territory, COUNT(DISTINCT channel_id) AS channel_count, COUNT(DISTINCT provider_id) AS provider_count FROM jay_mehta_catalog.versant_stream_metrics.channel_availability WHERE is_currently_available = true GROUP BY territory ORDER BY channel_count DESC` |
| 8 | Which titles were recently added or removed? | `SELECT title, change_type, platform, territory, change_date FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes WHERE report_month = (SELECT MAX(report_month) FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes) ORDER BY change_date DESC` |
| 9 | What is the average time from Theatrical to SVOD for movies? | `SELECT window_lifecycle_stage, ROUND(AVG(window_duration_days), 0) AS avg_days, COUNT(DISTINCT title_id) AS title_count FROM jay_mehta_catalog.versant_stream_metrics.window_non_series WHERE window_lifecycle_stage IN ('Theatrical', 'PVOD', 'SVOD Exclusive') GROUP BY window_lifecycle_stage ORDER BY CASE window_lifecycle_stage WHEN 'Theatrical' THEN 1 WHEN 'PVOD' THEN 2 WHEN 'SVOD Exclusive' THEN 3 END` |
| 10 | Show me a breakdown of active channels per genre | `SELECT genre, COUNT(*) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.channels WHERE status = 'active' GROUP BY genre ORDER BY channel_count DESC` |
| 11 | Which titles air most frequently on FAST channels? | `SELECT cc.title, cc.content_type, cc.genre, COUNT(*) AS airing_count, COUNT(DISTINCT ps.channel_id) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.programming_schedule ps JOIN jay_mehta_catalog.versant_stream_metrics.content_catalog cc ON ps.content_id = cc.content_id GROUP BY cc.title, cc.content_type, cc.genre ORDER BY airing_count DESC LIMIT 25` |
| 12 | What are the monthly content churn trends? | `SELECT report_month, change_type, COUNT(DISTINCT title_id) AS title_count FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes GROUP BY report_month, change_type ORDER BY report_month, change_type` |
| 13 | How long do series typically stay in each windowing stage? | `SELECT window_lifecycle_stage, COUNT(DISTINCT title_id) AS title_count, ROUND(AVG(window_duration_days), 0) AS avg_days, MIN(window_duration_days) AS min_days, MAX(window_duration_days) AS max_days FROM jay_mehta_catalog.versant_stream_metrics.window_series GROUP BY window_lifecycle_stage ORDER BY CASE window_lifecycle_stage WHEN 'SVOD Exclusive' THEN 1 WHEN 'SVOD Shared' THEN 2 WHEN 'AVOD' THEN 3 WHEN 'FAST' THEN 4 WHEN 'Library' THEN 5 ELSE 6 END` |
| 14 | Compare FAST platform lineup sizes | `SELECT p.provider_name, COUNT(DISTINCT ca.channel_id) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.providers p JOIN jay_mehta_catalog.versant_stream_metrics.channel_availability ca ON p.provider_id = ca.provider_id WHERE ca.is_currently_available = true GROUP BY p.provider_name ORDER BY channel_count DESC` |
| 15 | What is the average window duration by lifecycle stage for series? | (Same as #13) |
| 16 | How many channels does each provider have? | `SELECT p.provider_name, COUNT(DISTINCT ca.channel_id) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.providers p JOIN jay_mehta_catalog.versant_stream_metrics.channel_availability ca ON p.provider_id = ca.provider_id WHERE ca.is_currently_available = true GROUP BY p.provider_name ORDER BY channel_count DESC` |
| 17 | What genres have the most FAST channels? | `SELECT genre, COUNT(*) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.channels WHERE status = 'active' GROUP BY genre ORDER BY channel_count DESC` |
| 18 | Which platforms have the most exclusive content in the US? | `SELECT platform, COUNT(DISTINCT title_id) AS total_titles, SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) AS exclusive_titles, ROUND(SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT title_id), 2) AS exclusive_pct FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE territory = 'US' AND is_currently_available = true GROUP BY platform ORDER BY exclusive_titles DESC` |
| 19 | What genres have the most content across SVOD platforms? | `SELECT genre, COUNT(DISTINCT title_id) AS title_count, COUNT(DISTINCT platform) AS platform_count FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE availability_type = 'SVOD' AND is_currently_available = true GROUP BY genre ORDER BY title_count DESC` |
| 20 | How many titles does each platform have in the United States? | `SELECT platform, COUNT(DISTINCT title_id) AS total_titles FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE territory = 'US' AND is_currently_available = true GROUP BY platform ORDER BY total_titles DESC` |
| 21 | How many channels are available in each territory? | `SELECT territory, COUNT(DISTINCT channel_id) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.channel_availability WHERE is_currently_available = true GROUP BY territory ORDER BY channel_count DESC` |
| 22 | How many episodes are available per platform in the US? | `SELECT platform, COUNT(DISTINCT title_id) AS title_count, COUNT(*) AS total_episodes, SUM(CASE WHEN is_currently_available = true THEN 1 ELSE 0 END) AS available_episodes FROM jay_mehta_catalog.versant_stream_metrics.vod_episode_availability WHERE territory = 'US' GROUP BY platform ORDER BY total_episodes DESC` |
| 23 | Which platforms have the most VOD titles in the US? | `SELECT platform, COUNT(DISTINCT title_id) AS total_titles FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE territory = 'US' AND is_currently_available = true GROUP BY platform ORDER BY total_titles DESC` |
| 24 | Compare streaming platform library sizes in the US | `SELECT platform, COUNT(DISTINCT title_id) AS total_titles FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE territory = 'US' AND is_currently_available = true GROUP BY platform ORDER BY total_titles DESC` |
| 25 | Break down VOD content by type - series vs movies vs specials | `SELECT content_type, COUNT(DISTINCT title_id) AS title_count, COUNT(DISTINCT platform) AS platform_count, COUNT(DISTINCT territory) AS territory_count FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE is_currently_available = true GROUP BY content_type ORDER BY title_count DESC` |
| 26 | What is the repeat rate by time block? | `SELECT time_block, COUNT(*) AS total_airings, SUM(CASE WHEN is_repeat = true THEN 1 ELSE 0 END) AS repeats, ROUND(SUM(CASE WHEN is_repeat = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repeat_pct FROM jay_mehta_catalog.versant_stream_metrics.programming_schedule GROUP BY time_block ORDER BY CASE time_block WHEN 'Early Morning' THEN 1 WHEN 'Morning' THEN 2 WHEN 'Afternoon' THEN 3 WHEN 'Prime Access' THEN 4 WHEN 'Primetime' THEN 5 WHEN 'Late Night' THEN 6 WHEN 'Overnight' THEN 7 END` |
| 27 | How does VOD availability in the US compare to the UK? | `SELECT territory, COUNT(DISTINCT title_id) AS total_titles, COUNT(DISTINCT platform) AS platform_count, SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) AS exclusive_titles FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE territory IN ('US', 'UK') AND is_currently_available = true GROUP BY territory ORDER BY total_titles DESC` |
| 28 | How quickly do movies move through release windows? | `SELECT window_lifecycle_stage, COUNT(DISTINCT title_id) AS title_count, ROUND(AVG(window_duration_days), 0) AS avg_days FROM jay_mehta_catalog.versant_stream_metrics.window_non_series GROUP BY window_lifecycle_stage ORDER BY CASE window_lifecycle_stage WHEN 'Theatrical' THEN 1 WHEN 'PVOD' THEN 2 WHEN 'SVOD Exclusive' THEN 3 WHEN 'SVOD Shared' THEN 4 WHEN 'AVOD' THEN 5 WHEN 'FAST' THEN 6 WHEN 'Library' THEN 7 ELSE 8 END` |
| 29 | Show me the series windowing lifecycle summary | `SELECT window_lifecycle_stage, COUNT(DISTINCT title_id) AS title_count, ROUND(AVG(window_duration_days), 0) AS avg_days, MIN(window_duration_days) AS min_days, MAX(window_duration_days) AS max_days FROM jay_mehta_catalog.versant_stream_metrics.window_series GROUP BY window_lifecycle_stage ORDER BY CASE window_lifecycle_stage WHEN 'SVOD Exclusive' THEN 1 WHEN 'SVOD Shared' THEN 2 WHEN 'AVOD' THEN 3 WHEN 'FAST' THEN 4 WHEN 'Library' THEN 5 ELSE 6 END` |
| 30 | Which series are currently in the FAST window? | `SELECT title_id, title, genre, window_start_date, window_duration_days FROM jay_mehta_catalog.versant_stream_metrics.window_series WHERE window_lifecycle_stage = 'FAST' AND (window_end_date IS NULL OR window_end_date >= CURRENT_DATE()) ORDER BY window_start_date DESC` |
| 31 | Show the primetime vs non-primetime split for each channel genre | `SELECT c.genre, COUNT(*) AS total_airings, SUM(CASE WHEN ps.is_primetime = true THEN 1 ELSE 0 END) AS primetime_airings, ROUND(SUM(CASE WHEN ps.is_primetime = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS primetime_pct FROM jay_mehta_catalog.versant_stream_metrics.programming_schedule ps JOIN jay_mehta_catalog.versant_stream_metrics.channels c ON ps.channel_id = c.channel_id GROUP BY c.genre ORDER BY primetime_pct DESC` |
| 32 | Show me the exclusivity rate by platform for the US market | `SELECT platform, COUNT(DISTINCT title_id) AS total_titles, SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) AS exclusive_titles, ROUND(SUM(CASE WHEN is_exclusive = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT title_id), 2) AS exclusive_pct FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE territory = 'US' AND is_currently_available = true GROUP BY platform ORDER BY exclusive_titles DESC` |
| 33 | Show platform availability across territories | `SELECT platform, territory, COUNT(DISTINCT title_id) AS title_count FROM jay_mehta_catalog.versant_stream_metrics.vod_title_availability WHERE is_currently_available = true GROUP BY platform, territory ORDER BY platform, title_count DESC` |
| 34 | What is the net adds trend by month? | `SELECT report_month, SUM(CASE WHEN change_type = 'Added' THEN 1 ELSE 0 END) AS added, SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) AS removed, SUM(CASE WHEN change_type = 'Added' THEN 1 ELSE 0 END) - SUM(CASE WHEN change_type = 'Removed' THEN 1 ELSE 0 END) AS net_adds FROM jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes GROUP BY report_month ORDER BY report_month` |
| 35 | How many active FAST channels are there by genre? | `SELECT genre, COUNT(*) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.channels WHERE status = 'active' GROUP BY genre ORDER BY channel_count DESC` |
| 36 | Which providers carry the most channels? | `SELECT p.provider_name, COUNT(DISTINCT ca.channel_id) AS channel_count FROM jay_mehta_catalog.versant_stream_metrics.providers p JOIN jay_mehta_catalog.versant_stream_metrics.channel_availability ca ON p.provider_id = ca.provider_id WHERE ca.is_currently_available = true GROUP BY p.provider_name ORDER BY channel_count DESC` |
| 37 | Which titles are available on both FAST channels and VOD platforms? | `SELECT cc.title, cc.content_type, cc.genre, COUNT(DISTINCT ps.channel_id) AS fast_channels, COUNT(DISTINCT vta.platform) AS vod_platforms FROM jay_mehta_catalog.versant_stream_metrics.content_catalog cc JOIN jay_mehta_catalog.versant_stream_metrics.programming_schedule ps ON cc.content_id = ps.content_id JOIN jay_mehta_catalog.versant_stream_metrics.vod_title_availability vta ON cc.title = vta.title WHERE vta.territory = 'US' AND vta.is_currently_available = true GROUP BY cc.title, cc.content_type, cc.genre ORDER BY fast_channels + vod_platforms DESC` |
| 38 | What percentage of FAST programming is repeats? | `SELECT ROUND(SUM(CASE WHEN is_repeat = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repeat_pct FROM jay_mehta_catalog.versant_stream_metrics.programming_schedule` |

---

## e. Sample Questions (Suggested Questions Shown to Users)

The following sample questions appear in the chat UI (`config.sample_questions`):

1. Compare the number of titles available on Tubi vs Pluto TV by genre
2. Which production studios have content in the most FAST channels?
3. What is the typical gap between windows for movies moving from SVOD to AVOD?
4. How many episodes of Drama series are currently available on Hulu?
5. What genres have the most content available across SVOD platforms?
6. Show month-over-month title change trends by change type
7. Which platforms have the most exclusive VOD content?
8. What is the average window duration by lifecycle stage for series content?
9. How many titles were added vs removed each month in 2025?
10. Which titles are exclusively available on Netflix in the US?
11. What percentage of programming schedule slots are repeats vs first-run?
12. Show me the top 10 channel owners by number of channels
13. What content airs during primetime on Comedy channels this week?
14. Which providers carry the most channels?
15. How many active FAST channels are there by genre?

---

## f. Space Description (from API)

Stream Metrics data explorer for Versant — analyze FAST TV channels, VOD title/episode availability, and content windowing across streaming platforms.

This space covers three key data domains from the Stream Metrics API:

1. **FAST TV (Free Ad-Supported TV):**
   - providers: 15 FAST platforms (Pluto TV, Tubi, Samsung TV Plus, Roku Channel, etc.)
   - channels: 200 FAST channels with genre, owner, language, and library size
   - content_catalog: 500 titles (series, movies, specials) with ratings and licensing
   - programming_schedule: 283K scheduled airings over 90 days with primetime/time block flags
   - channel_availability: 712 channel-provider distribution records across territories

2. **VOD Title & Episode Availability:**
   - vod_title_availability: 5,508 title-platform-territory availability records (SVOD, AVOD, TVOD, EST, FAST)
   - vod_episode_availability: 17,419 episode-level availability records across platforms
   - vod_title_mom_changes: 2,730 month-over-month availability changes (adds, removes, renewals, platform switches)

3. **Content Windowing:**
   - window_series: 64,367 series windowing records tracking lifecycle from SVOD Exclusive to AVOD to FAST
   - window_non_series: 2,744 movie/special windowing records from Theatrical to PVOD to SVOD to FAST to Library

**TABLE JOINS AND RELATIONSHIPS:**

FAST TV Domain:
- channels.channel_id = programming_schedule.channel_id (1:Many) — what airs on each channel, schedule density by genre, primetime patterns
- channels.channel_id = channel_availability.channel_id (1:Many) — which providers carry each channel, territory coverage
- content_catalog.content_id = programming_schedule.content_id (1:Many) — how frequently titles air, airtime by content type
- providers.provider_id = channel_availability.provider_id (1:Many) — platform channel lineups, provider reach
- 3-table: channels -> channel_availability -> providers (Many:Many) — full channel-provider distribution by territory
- 3-table: content_catalog -> programming_schedule -> channels (Many:Many) — which content airs on which channels

VOD Domain:
- vod_title_availability.title_id = vod_episode_availability.title_id (1:Many) — title vs episode availability, completeness analysis. Also match on platform and territory.
- vod_title_availability.title_id = vod_title_mom_changes.title_id (1:Many) — track availability changes over time. Also match on platform.

Cross-Domain (VOD to Windowing):
- vod_title_availability.title_id = window_series.title_id — correlate VOD availability with series windowing lifecycle (filter content_type = Series)
- vod_title_availability.title_id = window_non_series.title_id — correlate VOD availability with movie windowing lifecycle (filter content_type IN Movie, Special)
- vod_episode_availability.title_id = window_series.title_id — episode availability by windowing stage
- vod_title_mom_changes.title_id = window_series.title_id — validate MoM changes align with window transitions
- vod_title_mom_changes.title_id = window_non_series.title_id — movie availability changes with windowing lifecycle

Cross-Domain (FAST to VOD):
- content_catalog.title = vod_title_availability.title (name-based match) — titles in both FAST and VOD ecosystems

**Key values:**
- Territories: ISO 2-letter codes (US, CA, UK, DE, FR, AU, JP, BR, MX, IN, KR, IT, ES, SE, NL)
- Platforms: Netflix, Hulu, Disney+, Amazon Prime Video, HBO Max, Peacock, Paramount+, Apple TV+, Pluto TV, Tubi, Roku Channel, Crackle, Freevee
- availability_type: SVOD, AVOD, TVOD, EST, FAST
- window_lifecycle_stage (series): SVOD Exclusive, SVOD Shared, AVOD, FAST, Library
- window_lifecycle_stage (non-series): Theatrical, PVOD, SVOD Exclusive, SVOD Shared, AVOD, FAST, Library
- change_type: Added, Removed, Renewed, Platform Switch, Window Change

---

## g. Configured Tables

- jay_mehta_catalog.versant_stream_metrics.channel_availability
- jay_mehta_catalog.versant_stream_metrics.channels
- jay_mehta_catalog.versant_stream_metrics.content_catalog
- jay_mehta_catalog.versant_stream_metrics.programming_schedule
- jay_mehta_catalog.versant_stream_metrics.providers
- jay_mehta_catalog.versant_stream_metrics.vod_episode_availability
- jay_mehta_catalog.versant_stream_metrics.vod_title_availability
- jay_mehta_catalog.versant_stream_metrics.vod_title_mom_changes
- jay_mehta_catalog.versant_stream_metrics.window_non_series
- jay_mehta_catalog.versant_stream_metrics.window_series

---

*Extracted via Databricks Genie API (get_space with include_serialized_space=true) and browser navigation to the Genie configuration page.*
