# Versant Content Intelligence Platform — Detailed App Description

**URL:** https://versant-content-intel-1533418539107699.aws.databricksapps.com/

**Screenshots saved to:** `/Users/jay.mehta/Desktop/stream-metrics/`
- `screenshot_1_dashboard_loading.png` — Dashboard initial load
- `screenshot_2_dashboard_full.png` — Full dashboard with all sections
- `screenshot_3_articles.png` — Articles page (loading state)
- `screenshot_4_articles_loaded.png` — Articles page with data table
- `screenshot_5_architecture.png` — Technical Architecture page
- `screenshot_6_ask_assistant.png` — Genie AI assistant panel (Ask Assistant)

---

## 1. Overall Layout Structure

### Navigation Pattern
- **Left sidebar (complementary)**: Fixed vertical navigation
  - **VERSANT** — Brand/logo at top
  - **Dashboard** — Main analytics view (default)
  - **Articles** — Content explorer / article list
  - **Architecture** — Technical documentation
  - **jay.mehta@databricks.com** — User email at bottom

- **Main content area**: Scrollable, takes remaining width
- **Ask Assistant button**: Floating button (likely bottom-right) — opens Genie AI chat panel overlay/sidebar

### Section Organization
- **Single-page scroll** within each view (Dashboard, Articles, Architecture)
- **No top nav bar** — all navigation is in the left sidebar
- **No breadcrumbs** — direct sidebar links

---

## 2. Dashboard — Content Intelligence Dashboard

### Page Header
- **H1**: "Content Intelligence Dashboard"
- **Subtitle**: "AI-extracted themes and performance analytics for MS NOW (January 2026)"

### KPI Cards (6 metrics in a row)
| Metric | Value | Display |
|--------|-------|---------|
| ARTICLES | 1,315 | Large number |
| AVG VIEWS / ARTICLE | 23,075 | Large number |
| UNIQUE THEMES | 4,056 | Large number |
| UNIQUE ENTITIES | 8,455 | Large number |
| AUTHORS | 260 | Large number |
| NEGATIVE SENTIMENT | 77.6% | Percentage |

**KPI card styling**: Card-style layout, metric label above value, prominent typography

### Section 1: Top Articles by Page Views
- **Title**: "Top Articles by Page Views"
- **Subtitle**: "Highest-performing content in January 2026"
- **Visualization**: Table
- **Columns**: # | HEADLINE | SECTION | VIEWS | SENTIMENT
- **Sample rows**: Top 10 articles with headlines, section (News, Opinion, Maddow Blog), view counts (e.g., 305,748, 271,950), sentiment (negative/neutral)
- **Data**: MS NOW (formerly MSNBC) article performance data

### Section 2: Theme Performance
- **Title**: "Theme Performance"
- **Subtitle**: "Average page views by AI-extracted theme (min 3 articles)"
- **Visualization**: Horizontal bar chart
- **X-axis**: 0 to 100,000 (avg page views)
- **Y-axis**: Themes — trade agreements, self-defense claims, media manipulation, activism, war powers, video evidence, presidential response, law enforcement, reform, civil rights violations, state vs federal jurisdiction
- **Purpose**: Shows which AI-extracted storylines/themes drive the most engagement

### Section 3: Sentiment Distribution
- **Title**: "Sentiment Distribution"
- **Subtitle**: "1,315 articles analyzed"
- **Visualization**: Donut/pie chart (or stacked bar)
- **Legend**: Critical | Mixed | Negative | Neutral | Positive
- **Data**: Sentiment breakdown across all articles (78% Negative, 19% other visible in snapshot)

### Section 4: Weekly Theme Trends
- **Title**: "Weekly Theme Trends"
- **Subtitle**: "Top 5 themes by article volume per week"
- **Visualization**: Multi-line chart
- **X-axis**: Week dates (11-10, 11-17, 11-24, 12-01, 12-15, 12-29, 01-12, 01-26)
- **Y-axis**: 0–140 (article count)
- **Series**: Politics, Trump administration, civil rights, immigration enforcement, international relations
- **Purpose**: Shows which themes dominate article volume over time

### Section 5: Traffic Source by Theme
- **Title**: "Traffic Source by Theme"
- **Subtitle**: "Average traffic source % per theme (Adobe data)"
- **Visualization**: Stacked horizontal bar chart
- **Legend**: App | Search (and possibly Cover, Partner, AMP)
- **Y-axis**: Same themes as Theme Performance (trade agreements, self-defense claims, etc.)
- **X-axis**: 0%–36% (percentage)
- **Purpose**: Which themes get traffic from Search vs App vs other sources

### Section 6: Entity Trends Over Time
- **Title**: "Entity Trends Over Time"
- **Subtitle**: "Weekly mention frequency for top people and organizations"
- **Visualization**: Multi-line chart
- **X-axis**: Week dates (11-10 through 01-26)
- **Y-axis**: 0–160 (mention frequency)
- **Series**: Byron Donalds, Donald Trump, Ja'han Jones, Justice Department, MS NOW, Starbucks, Steve Benen, White House
- **Purpose**: Tracks how often key entities are mentioned week-over-week

### Section 7: Top Entities
- **Title**: "Top Entities"
- **Subtitle**: "Most mentioned people and organizations across MS NOW"
- **Visualization**: Horizontal bar chart
- **Entities**: Donald Trump, White House, Steve Benen, MS NOW, Ja'han Jones, Byron Donalds, Starbucks, Justice Department, Maddowblog, Congress
- **X-axis**: 0–1000 (mention count)
- **Filter**: Person | Organization (toggle or legend)

---

## 3. Articles Page — Content Explorer

### Page Header
- **H1**: "Content Explorer"
- **Subtitle**: "Browse MS NOW articles with AI-extracted metadata and performance data"

### Filters
- **Search**: Text input "Search articles..."
- **Section filters**: All | News | Opinion | Maddow Blog | Deadline: Legal

### Data Table
- **Columns**: Title | Section | Author | Views | Sentiment | Published
- **Pagination**: "Showing 1–20 of 1,315 articles", Page 1 of 66, Previous/Next
- **Sample data**: Article headlines, section, author(s), view counts, sentiment, publish date (YYYY-MM-DD)

---

## 4. Architecture Page — Technical Architecture

### Page Header
- **H1**: "Technical Architecture"
- **Subtitle**: "IE Agent Brick + SDP Pipeline + Genie Space + AI Insights"

### Medallion Pipeline (SDP) Diagram
- **Bronze**: MS NOW Articles + Adobe Metrics
- **IE Agent Brick via ai_query()**
- **Silver**: Entities, Topics, Metadata
- **Gold**: Theme Performance, Weekly Trends, Traffic
- **App / Genie / AI Insights**

### Narrative
- "MS NOW articles with Adobe performance metrics (page views, traffic sources) are processed through the IE Agent Brick for entity extraction, topic classification, and sentiment analysis. Gold layer connects AI-extracted themes to performance data — answering 'which storylines are winning?'"
- **Future**: Competitive stream metrics, real-time article ingestion

### Agent Bricks
- **IE Agent Brick**: Extracts entities (PERSON, ORGANIZATION, LOCATION, EVENT), topics, sentiment, key facts via ai_query(). Processes ~1,315 articles from January 2026. Endpoint: kie-be879940-endpoint
- **Genie Space**: Natural language SQL over content intelligence tables — theme performance, traffic source analysis, entity trends, sentiment breakdowns. Status: "Genie Space (to be configured)"

### Unity Catalog Tables
- **Catalog**: my_demo_env_catalog.versant_intel
- **Bronze**: articles (~1,315), articles_for_ie (~1,315)
- **Silver**: ie_extraction_results (~1,315), entity_mentions (~15K), topic_classifications (~10K), article_metadata (~1,315)
- **Gold**: gold_entity_trends (~10K), gold_theme_performance (~200), gold_weekly_trends (~500), gold_traffic_by_theme (~200), gold_entity_connections (~50K), gold_extraction_metrics (~100)

### Data Sources
- **MS NOW (formerly MSNBC)**: 1,315 top articles from January 2026. Sections: News, Opinion, Maddow Blog, Deadline: Legal
- **Adobe Analytics**: Page views and traffic source breakdowns (% Search, % App, % Cover, % Partner, % AMP)

---

## 5. Ask Assistant (Genie AI Panel)

### Trigger
- **Button**: "Ask Assistant" — floating, likely bottom-right

### Panel Content
- **Header**: "Genie AI"
- **Actions**: Conversation history | New conversation
- **Subtitle**: "Ask questions about content performance, themes, traffic sources, and trends."

### Suggested Questions (chips)
- Which themes drive the most page views?
- Top 5 articles by engagement this month
- Traffic source mix — search vs social vs direct
- Sentiment risk areas by theme
- Which content categories have highest reader retention?
- Revenue-per-article breakdown by brand

### Input
- **Textbox**: "Ask about your content..."
- **Send button**: Disabled until input

---

## 6. Color Scheme and Visual Hierarchy

### Inferred from Structure
- **Background**: Dark (typical Databricks Apps dark theme)
- **Sidebar**: Dark with accent for active nav
- **KPI cards**: Card backgrounds with contrasting borders
- **Charts**: Multi-color series (Politics, Trump administration, civil rights, etc. each have distinct colors)
- **Tables**: Alternating row styling, header row distinct
- **Sentiment**: Color-coded (Critical, Mixed, Negative, Neutral, Positive)

### Typography
- **Headings**: H1 for page title, H3 for section titles
- **Subtitle**: Muted, smaller font
- **Data**: Numeric values formatted with commas (e.g., 305,748)

---

## 7. How Insights/Commentary Are Presented

- **Section subtitles**: Each chart/table has a descriptive subtitle (e.g., "Average page views by AI-extracted theme (min 3 articles)")
- **Context**: "1,315 articles analyzed" under Sentiment Distribution
- **Architecture page**: Narrative paragraphs explaining the pipeline and data flow
- **No standalone insight cards** — insights are embedded in chart titles and subtitles

---

## 8. Navigation Pattern Summary

| From | To | Method |
|------|-----|--------|
| Any page | Dashboard | Sidebar link "Dashboard" |
| Any page | Articles | Sidebar link "Articles" |
| Any page | Architecture | Sidebar link "Architecture" |
| Any page | Genie Chat | Click "Ask Assistant" button |
| Articles | Filter by section | Click All/News/Opinion/Maddow Blog/Deadline: Legal |
| Articles | Search | Type in "Search articles..." |
| Articles | Next page | Click "Next" button |

---

## 9. Visualization Types Summary

| Section | Type | Data Shown |
|---------|------|------------|
| KPI Cards | 6 cards | Articles, Avg Views, Themes, Entities, Authors, Negative Sentiment % |
| Top Articles | Table | Headline, Section, Views, Sentiment |
| Theme Performance | Horizontal bar chart | Avg page views by theme |
| Sentiment Distribution | Donut/pie | Sentiment breakdown (Critical, Mixed, Negative, Neutral, Positive) |
| Weekly Theme Trends | Multi-line chart | Top 5 themes by article volume per week |
| Traffic Source by Theme | Stacked bar chart | App vs Search % by theme |
| Entity Trends Over Time | Multi-line chart | Entity mention frequency by week |
| Top Entities | Horizontal bar chart | Mention count by entity |

---

*Document generated from browser screenshots and accessibility tree snapshots on March 2, 2025.*
