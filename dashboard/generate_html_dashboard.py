import pandas as pd
import plotly.express as px
import plotly.io as pio
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
BASE_DIR = Path("/home/pingh/Brazilian-E-Commerce-Public-Dataset-by-Olist")
DATA_PATH = BASE_DIR / "dashboard" / "fact_orders_with_sentiment.csv"
OUTPUT_HTML = BASE_DIR / "dashboard" / "olist_ml_business_dashboard.html"

# 🔧 Update these from your ML notebook if they change
XGB_RMSE = 9.36
XGB_MAE = 3.90
XGB_R2 = 0.6334

# -----------------------------
# LOAD DATA
# -----------------------------
df = pd.read_csv(DATA_PATH)

# Ensure timestamp & month
df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
df["order_date"] = df["order_purchase_timestamp"].dt.date
df["year_month"] = df["order_purchase_timestamp"].dt.to_period("M").astype(str)

# If sentiment_label not present, derive from review_score
if "sentiment_label" not in df.columns and "review_score" in df.columns:
    def score_to_sentiment(s):
        if s >= 4:
            return "positive"
        elif s == 3:
            return "neutral"
        else:
            return "negative"
    df["sentiment_label"] = df["review_score"].apply(score_to_sentiment)

# -----------------------------
# KPI METRICS
# -----------------------------
total_revenue = float(df["payment_value"].sum())
total_orders = int(df["order_id"].nunique())
avg_order_value = float(df["payment_value"].mean())
avg_review = float(df["review_score"].mean())
avg_freight = float(df["freight_value"].mean())

sent_counts = df["sentiment_label"].value_counts()
positive_ratio = sent_counts.get("positive", 0) / sent_counts.sum() * 100
negative_ratio = sent_counts.get("negative", 0) / sent_counts.sum() * 100

# -----------------------------
# BUILD PLOTLY FIGURES
# -----------------------------

# 1) Revenue trend
monthly = df.groupby("year_month").agg(
    revenue=("payment_value", "sum"),
    orders=("order_id", "nunique")
).reset_index()

fig_rev = px.line(
    monthly,
    x="year_month",
    y="revenue",
    title="Monthly Revenue Trend",
    labels={"year_month": "Month", "revenue": "Revenue (BRL)"}
)
fig_rev.update_layout(margin=dict(l=10, r=10, t=40, b=10))

# 2) Sentiment share over time
sent_time = (
    df.groupby(["year_month", "sentiment_label"])
    .size()
    .reset_index(name="count")
)
sent_time["share"] = sent_time.groupby("year_month")["count"].transform(
    lambda x: x / x.sum()
)

fig_sent_time = px.area(
    sent_time,
    x="year_month",
    y="share",
    color="sentiment_label",
    title="Sentiment Share Over Time",
    labels={"year_month": "Month", "share": "Share of Reviews"}
)
fig_sent_time.update_layout(margin=dict(l=10, r=10, t=40, b=10))

# 3) Revenue by state
if "customer_state" in df.columns:
    rev_state = (
        df.groupby("customer_state")["payment_value"]
        .sum()
        .reset_index()
        .sort_values("payment_value", ascending=False)
    )

    fig_rev_state = px.bar(
        rev_state,
        x="customer_state",
        y="payment_value",
        title="Revenue by State",
        labels={"customer_state": "State", "payment_value": "Revenue (BRL)"}
    )
    fig_rev_state.update_layout(margin=dict(l=10, r=10, t=40, b=60))
else:
    fig_rev_state = None

# 4) Sentiment by state
if "customer_state" in df.columns:
    state_sent = (
        df.groupby(["customer_state", "sentiment_label"])
        .size()
        .reset_index(name="count")
    )
    state_sent["share"] = state_sent.groupby("customer_state")["count"].transform(
        lambda x: x / x.sum()
    )

    fig_sent_state = px.bar(
        state_sent,
        x="customer_state",
        y="share",
        color="sentiment_label",
        title="Sentiment Mix by State",
        labels={"customer_state": "State", "share": "Share of Reviews"},
        barmode="stack"
    )
    fig_sent_state.update_layout(margin=dict(l=10, r=10, t=40, b=60))
else:
    fig_sent_state = None

# 5) Freight vs review score
fig_freight_review = px.box(
    df,
    x="review_score",
    y="freight_value",
    points="outliers",
    title="Freight Cost vs Review Score",
    labels={"review_score": "Review Score", "freight_value": "Freight Value"}
)
fig_freight_review.update_layout(margin=dict(l=10, r=10, t=40, b=10))

# -----------------------------
# CONVERT FIGURES TO HTML SNIPPETS
# -----------------------------
def fig_to_div(fig):
    return pio.to_html(fig, include_plotlyjs=False, full_html=False)

rev_div = fig_to_div(fig_rev)
sent_time_div = fig_to_div(fig_sent_time)
freight_review_div = fig_to_div(fig_freight_review)
rev_state_div = fig_to_div(fig_rev_state) if fig_rev_state else ""
sent_state_div = fig_to_div(fig_sent_state) if fig_sent_state else ""

# -----------------------------
# HTML TEMPLATE
# -----------------------------
html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Brazilian E-Commerce ML & Sentiment Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.33.0.min.js"></script>
  <style>
    :root {{
      --bg: #020617;
      --bg-soft: #0b1120;
      --card: #020617;
      --accent: #38bdf8;
      --accent-soft: rgba(56, 189, 248, 0.15);
      --text: #e5e7eb;
      --muted: #9ca3af;
      --danger: #f97373;
      --success: #4ade80;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: radial-gradient(circle at top, #082f49, #020617 60%);
      color: var(--text);
    }}

    .page {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px 20px 40px;
    }}

    header {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 16px;
      margin-bottom: 24px;
    }}

    .title-block h1 {{
      margin: 0 0 4px;
      font-size: 26px;
      letter-spacing: 0.02em;
    }}

    .title-block p {{
      margin: 0;
      color: var(--muted);
      font-size: 13px;
      max-width: 640px;
    }}

    .chip {{
      padding: 6px 12px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.5);
      font-size: 11px;
      color: var(--muted);
      background: rgba(15, 23, 42, 0.7);
    }}

    .kpi-row {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}

    .kpi {{
      background: radial-gradient(circle at top, #020617, #020617 60%);
      border-radius: 16px;
      padding: 10px 12px;
      border: 1px solid rgba(148, 163, 184, 0.25);
      box-shadow: 0 15px 40px rgba(15, 23, 42, 0.7);
    }}

    .kpi-label {{
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--muted);
      margin-bottom: 6px;
    }}

    .kpi-value {{
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 4px;
    }}

    .kpi-sub {{
      font-size: 11px;
      color: var(--muted);
    }}

    .kpi-value.positive {{
      color: var(--success);
    }}

    .kpi-value.negative {{
      color: var(--danger);
    }}

    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 18px;
    }}

    .card {{
      background: rgba(15, 23, 42, 0.94);
      border-radius: 18px;
      padding: 14px 16px;
      border: 1px solid rgba(148, 163, 184, 0.35);
      box-shadow: 0 18px 40px rgba(15, 23, 42, 0.85);
    }}

    .card h2 {{
      margin: 0 0 4px;
      font-size: 15px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }}

    .card h2 span.tag {{
      font-size: 10px;
      padding: 2px 7px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.6);
      color: var(--muted);
    }}

    .card p {{
      margin: 0 0 6px;
      font-size: 12px;
      color: var(--muted);
    }}

    .insights {{
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr);
      gap: 16px;
      margin-top: 8px;
    }}

    .bullet-list {{
      font-size: 12px;
      color: var(--muted);
      padding-left: 18px;
    }}

    .bullet-list li {{
      margin-bottom: 4px;
    }}

    @media (max-width: 1024px) {{
      .kpi-row {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .grid {{
        grid-template-columns: minmax(0, 1fr);
      }}
      .insights {{
        grid-template-columns: minmax(0, 1fr);
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header>
      <div class="title-block">
        <h1>Brazilian E-Commerce ML & Sentiment Dashboard</h1>
        <p>
          End-to-end pipeline: Kaggle → Meltano → BigQuery → dbt (star schema) → GX → XGBoost → HTML dashboard.
          This view combines business KPIs, customer sentiment and model performance.
        </p>
      </div>
      <div class="chip">
        XGBoost Freight Model &middot; RMSE {XGB_RMSE:.2f} &middot; R² {XGB_R2:.3f}
      </div>
    </header>

    <section class="kpi-row">
      <div class="kpi">
        <div class="kpi-label">Total Revenue</div>
        <div class="kpi-value">R$ {total_revenue:,.0f}</div>
        <div class="kpi-sub">Across {total_orders:,} unique orders</div>
      </div>

      <div class="kpi">
        <div class="kpi-label">Average Order Value</div>
        <div class="kpi-value">R$ {avg_order_value:,.2f}</div>
        <div class="kpi-sub">Average freight: R$ {avg_freight:,.2f}</div>
      </div>

      <div class="kpi">
        <div class="kpi-label">Customer Satisfaction</div>
        <div class="kpi-value">{avg_review:.2f} / 5</div>
        <div class="kpi-sub">Review score (1–5)</div>
      </div>

      <div class="kpi">
        <div class="kpi-label">Positive Sentiment</div>
        <div class="kpi-value positive">{positive_ratio:.1f}%</div>
        <div class="kpi-sub">Negative: {negative_ratio:.1f}%</div>
      </div>

      <div class="kpi">
        <div class="kpi-label">Model Error</div>
        <div class="kpi-value">{XGB_RMSE:.2f}</div>
        <div class="kpi-sub">RMSE (freight_value prediction)</div>
      </div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Revenue Over Time <span class="tag">Business Trend</span></h2>
        <p>Track monthly GMV and seasonality patterns. Drops aligned with negative sentiment may indicate service or logistics issues.</p>
        {rev_div}
      </div>

      <div class="card">
        <h2>Sentiment Share Over Time <span class="tag">Customer Voice</span></h2>
        <p>Stacked sentiment mix shows whether improvements are shifting customers from negative to neutral or positive feedback.</p>
        {sent_time_div}
      </div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Revenue by State <span class="tag">Geo Performance</span></h2>
        <p>Identify top-performing regions where growth initiatives should focus, and underperforming states needing attention.</p>
        {rev_state_div}
      </div>

      <div class="card">
        <h2>Sentiment Mix by State <span class="tag">Geo Sentiment</span></h2>
        <p>States with high revenue but weak sentiment are prime candidates for improvement in last-mile delivery or customer support.</p>
        {sent_state_div}
      </div>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Freight Cost vs Review Score <span class="tag">ML Signal</span></h2>
        <p>Box plot shows how high freight costs correlate with lower review scores, a key driver used by the XGBoost model.</p>
        {freight_review_div}
      </div>

      <div class="card">
        <h2>ML & Business Insights <span class="tag">Summary</span></h2>
        <div class="insights">
          <ul class="bullet-list">
            <li><strong>Model quality:</strong> XGBoost achieves RMSE {XGB_RMSE:.2f}, MAE {XGB_MAE:.2f}, R² {XGB_R2:.3f} on freight_value.</li>
            <li><strong>Key drivers:</strong> Features such as payment_value, distance proxies and customer_state are important predictors of freight.</li>
            <li><strong>Customer impact:</strong> Orders with high freight_value show a higher proportion of 1–2 star reviews (negative sentiment).</li>
            <li><strong>Risk regions:</strong> States with strong revenue but weak sentiment should be prioritised for operational fixes.</li>
          </ul>
          <ul class="bullet-list">
            <li><strong>Action 1:</strong> Review freight pricing and SLA in states with both high revenue and high negative sentiment.</li>
            <li><strong>Action 2:</strong> Use the model to flag orders with predicted high freight and proactively communicate with customers.</li>
            <li><strong>Action 3:</strong> Monitor this dashboard monthly after changes (pricing, carriers, promotions) to validate uplift.</li>
          </ul>
        </div>
      </div>
    </section>
  </div>
</body>
</html>
"""

# -----------------------------
# WRITE FILE
# -----------------------------
OUTPUT_HTML.write_text(html_template, encoding="utf-8")
print("✅ Dashboard written to:", OUTPUT_HTML)



