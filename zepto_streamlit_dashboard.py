# =============================================================================
#  STEP 26 : Streamlit Dashboard
#  Run after the PySpark pipeline completes (Steps 1-25).
#  This block uses the pandas DataFrames already in memory:
#    city_delivery_pd, category_demand_pd, peak_analysis_pd,
#    city_cat_pd, df_clean (as pandas via .toPandas())
#
#  HOW TO RUN (Google Colab):
#    !pip install streamlit plotly -q
#    Then paste this entire block into a new cell and run:
#    !streamlit run zepto_dashboard_app.py &
#    from pyngrok import ngrok
#    public_url = ngrok.connect(8501)
#    print(public_url)
#
#  OR save this as zepto_dashboard_app.py and run locally:
#    streamlit run zepto_dashboard_app.py
# =============================================================================

# ── Save this block as zepto_dashboard_app.py ──────────────────────────────
dashboard_code = '''
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ─────────────────────────────────────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Zepto Analytics Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
#  THEME & GLOBAL CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Space Grotesk', sans-serif;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f0c29 0%, #1a1040 50%, #24243e 100%);
    border-right: 1px solid rgba(255,255,255,0.06);
}
[data-testid="stSidebar"] * { color: #e0e0f0 !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stSlider label { color: #9999cc !important; font-size: 12px; font-weight: 600; letter-spacing: 0.8px; text-transform: uppercase; }

/* ── Main background ── */
.main .block-container { background: #0d0d1a; padding-top: 1.5rem; padding-bottom: 2rem; max-width: 100%; }
.stApp { background: #0d0d1a; }

/* ── KPI metric cards ── */
div[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid rgba(108,99,255,0.25);
    border-radius: 16px;
    padding: 20px 24px;
    box-shadow: 0 4px 24px rgba(108,99,255,0.08);
    transition: all 0.2s;
}
div[data-testid="metric-container"]:hover {
    border-color: rgba(108,99,255,0.5);
    box-shadow: 0 8px 32px rgba(108,99,255,0.18);
}
div[data-testid="metric-container"] label { color: #8888aa !important; font-size: 11px; font-weight: 600; letter-spacing: 1px; text-transform: uppercase; }
div[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #f0f0ff !important; font-size: 28px; font-weight: 700; }
div[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 12px; }

/* ── Section headers ── */
.section-header {
    display: flex; align-items: center; gap: 12px;
    padding: 6px 0 14px;
    border-bottom: 1px solid rgba(108,99,255,0.15);
    margin-bottom: 18px;
}
.section-header h3 { color: #f0f0ff; font-size: 15px; font-weight: 600; margin: 0; letter-spacing: -0.2px; }
.section-pill {
    background: rgba(108,99,255,0.2); color: #a89cff;
    font-size: 10px; font-weight: 700; padding: 3px 9px;
    border-radius: 20px; letter-spacing: 0.5px; text-transform: uppercase;
}

/* ── Info box ── */
.insight-box {
    background: linear-gradient(135deg, rgba(108,99,255,0.08) 0%, rgba(0,212,157,0.05) 100%);
    border: 1px solid rgba(108,99,255,0.2);
    border-left: 3px solid #6c63ff;
    border-radius: 10px;
    padding: 14px 18px;
    margin: 10px 0;
    font-size: 13px;
    color: #ccccee;
    line-height: 1.6;
}
.insight-box strong { color: #a89cff; }

/* ── Warning box ── */
.warn-box {
    background: rgba(255,100,100,0.06);
    border: 1px solid rgba(255,100,100,0.2);
    border-left: 3px solid #ff6464;
    border-radius: 10px;
    padding: 14px 18px;
    margin: 10px 0;
    font-size: 13px;
    color: #ffbbbb;
}
.warn-box strong { color: #ff8888; }

/* ── Good box ── */
.good-box {
    background: rgba(0,212,157,0.06);
    border: 1px solid rgba(0,212,157,0.2);
    border-left: 3px solid #00d49d;
    border-radius: 10px;
    padding: 14px 18px;
    margin: 10px 0;
    font-size: 13px;
    color: #aaffee;
}
.good-box strong { color: #00ffcc; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: rgba(255,255,255,0.03);
    border-radius: 12px;
    padding: 4px;
    gap: 4px;
    border: 1px solid rgba(255,255,255,0.06);
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    font-size: 13px;
    font-weight: 500;
    color: #8888aa;
    padding: 8px 18px;
}
.stTabs [aria-selected="true"] {
    background: rgba(108,99,255,0.2) !important;
    color: #a89cff !important;
}

/* ── Plotly chart cards ── */
.js-plotly-plot {
    border-radius: 14px;
    overflow: hidden;
}

/* ── Divider ── */
hr { border-color: rgba(255,255,255,0.06) !important; margin: 24px 0 !important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-thumb { background: rgba(108,99,255,0.3); border-radius: 2px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  PLOTLY TEMPLATE  (dark, matches UI)
# ─────────────────────────────────────────────────────────────────────────────
CHART_BG   = "#0d0d1a"
PAPER_BG   = "rgba(0,0,0,0)"
GRID_COLOR = "rgba(255,255,255,0.05)"
FONT_COLOR = "#aaaacc"
ACCENT     = "#6c63ff"

BASE_LAYOUT = dict(
    plot_bgcolor  = CHART_BG,
    paper_bgcolor = PAPER_BG,
    font          = dict(family="Space Grotesk, sans-serif", color=FONT_COLOR, size=12),
    margin        = dict(l=20, r=20, t=40, b=20),
    legend        = dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(0,0,0,0)", font_color=FONT_COLOR),
    hoverlabel    = dict(bgcolor="#1a1a2e", font_color="#f0f0ff", bordercolor="rgba(108,99,255,0.4)"),
    colorway      = ["#6c63ff","#00d49d","#ff6464","#ffd166","#4fc3f7","#ff9f40"],
)

COLORS_CAT  = ["#6c63ff","#00d49d","#ff6464","#ffd166","#4fc3f7","#ff9f40"]
CITY_COLORS = {"Mumbai":"#00d49d","Delhi":"#ff6464","Bangalore":"#6c63ff","Hyderabad":"#ffd166","Chennai":"#4fc3f7"}

def apply_base(fig, title="", height=360, showlegend=True, xtitle="", ytitle=""):
    fig.update_layout(**BASE_LAYOUT, title=dict(text=title, font_size=14, font_color="#e0e0f0", x=0.01),
                      height=height, showlegend=showlegend,
                      xaxis_title=xtitle, yaxis_title=ytitle)
    fig.update_xaxes(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR, tickfont_color=FONT_COLOR)
    fig.update_yaxes(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR, tickfont_color=FONT_COLOR)
    return fig


def hex_to_rgba(hex_color, alpha=0.2):
    h = hex_color.lstrip("#")
    if len(h) == 6:
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{alpha})"
    return hex_color

# ─────────────────────────────────────────────────────────────────────────────
#  LOAD DATA  (reads CSVs saved by PySpark pipeline in Steps 24)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    raw = pd.read_excel("zepto_dataset_v2.xlsx")
    raw["order_hour"] = pd.to_datetime(raw["order_time"]).dt.hour
    raw["delivery_speed"] = pd.cut(
        raw["delivery_time_minutes"].clip(upper=90),
        bins=[0,20,35,91], labels=["Fast","Normal","Delayed"]
    )

    try:
        city_df  = pd.read_csv("city_delivery_summary.csv")
        cat_df   = pd.read_csv("category_demand_summary.csv")
        peak_df  = pd.read_csv("peak_analysis_summary.csv")
        cc_df    = pd.read_csv("city_category_summary.csv")

        if "cancellation_rate" not in city_df.columns:
            city_df["cancellation_rate"] = 1 - city_df["delivery_rate"]
    except FileNotFoundError:
        city_df = raw.groupby("city", observed=True).agg(
            total_orders=("order_id","count"),
            avg_delivery_time_min=("delivery_time_minutes","mean"),
            avg_rating=("rating","mean"),
            avg_distance_km=("distance_km","mean"),
            total_revenue=("final_price","sum"),
            delivery_rate=("order_status", lambda s: (s != "Cancelled").mean())
        ).reset_index()
        city_df["cancellation_rate"] = 1 - city_df["delivery_rate"]

        cat_df = raw.groupby("category", observed=True).agg(
            total_revenue=("final_price","sum"),
            total_units_sold=("quantity","sum"),
            total_orders=("order_id","count"),
            avg_discount=("discount","mean"),
            avg_order_value=("final_price","mean")
        ).reset_index()

        cc_df = raw.groupby(["city","category"], observed=True).agg(
            total_revenue=("final_price","sum")
        ).reset_index()

        peak_df = raw.groupby("peak_hour_flag", observed=True).agg(
            total_orders=("order_id","count"),
            avg_delivery_time_min=("delivery_time_minutes","mean"),
            avg_rating=("rating","mean"),
            total_revenue=("final_price","sum"),
            delivery_rate=("order_status", lambda s: (s != "Cancelled").mean())
        ).reset_index()

    return city_df, cat_df, peak_df, cc_df, raw

city_df, cat_df, peak_df, cc_df, raw = load_data()

# ─────────────────────────────────────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:10px 0 20px;">
      <div style="font-size:22px;font-weight:700;color:#a89cff;letter-spacing:-0.5px;">⚡ Zepto</div>
      <div style="font-size:11px;color:#6666aa;margin-top:3px;font-family:monospace;">Analytics Dashboard · FA2</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("<div style='font-size:10px;color:#6666aa;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;margin-bottom:10px;'>Global Filters</div>", unsafe_allow_html=True)

    selected_cities = st.multiselect(
        "Cities",
        options=sorted(raw["city"].unique()),
        default=sorted(raw["city"].unique()),
    )

    selected_categories = st.multiselect(
        "Categories",
        options=sorted(raw["category"].unique()),
        default=sorted(raw["category"].unique()),
    )

    del_threshold = st.slider(
        "SLA Threshold (min)", min_value=15, max_value=45, value=30, step=1,
        help="Cities above this line are flagged as delayed"
    )

    cancel_threshold = st.slider(
        "Cancel Rate Alert (%)", min_value=5, max_value=25, value=15, step=1,
    )

    st.markdown("---")
    st.markdown("<div style='font-size:10px;color:#6666aa;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;margin-bottom:10px;'>Chart Options</div>", unsafe_allow_html=True)

    chart_height = st.select_slider(
        "Chart Height", options=[300, 360, 420, 480], value=360
    )

    show_labels  = st.toggle("Show value labels",    value=True)
    show_sla     = st.toggle("Show SLA lines",       value=True)
    normalize    = st.toggle("Normalize to %",       value=False)

    st.markdown("---")
    st.markdown("""
    <div style="font-size:10px;color:#555577;line-height:1.7;">
      Dataset: zepto_dataset_v2.xlsx<br>
      Rows: 120,000 · Cols: 20<br>
      Period: Jan 1–15, 2025<br>
      Engine: PySpark 4.0
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  FILTER DATA
# ─────────────────────────────────────────────────────────────────────────────
raw_f   = raw[raw["city"].isin(selected_cities) & raw["category"].isin(selected_categories)]
city_f  = city_df[city_df["city"].isin(selected_cities)]
cat_f   = cat_df[cat_df["category"].isin(selected_categories)]
cc_f    = cc_df[cc_df["city"].isin(selected_cities) & cc_df["category"].isin(selected_categories)]

# ─────────────────────────────────────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex;align-items:flex-end;justify-content:space-between;padding-bottom:18px;border-bottom:1px solid rgba(108,99,255,0.12);margin-bottom:24px;">
  <div>
    <div style="font-size:26px;font-weight:700;color:#f0f0ff;letter-spacing:-0.8px;line-height:1.2;">
      Zepto Quick Commerce
      <span style="background:linear-gradient(90deg,#6c63ff,#00d49d);-webkit-background-clip:text;-webkit-text-fill-color:transparent;"> Analytics</span>
    </div>
    <div style="font-size:13px;color:#666688;margin-top:5px;">
      Order · Delivery · Demand · Inventory · Jan 2025 · Tier-1 Cities
    </div>
  </div>
  <div style="text-align:right;">
    <div style="font-size:11px;color:#6666aa;font-family:monospace;">120,000 orders processed</div>
    <div style="font-size:11px;color:#6666aa;font-family:monospace;margin-top:2px;">PySpark Pipeline ✓</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  TOP KPI STRIP
# ─────────────────────────────────────────────────────────────────────────────
total_orders   = len(raw_f)
total_revenue  = raw_f["final_price"].sum()
avg_delivery   = raw_f["delivery_time_minutes"].mean()
cancel_rate    = (raw_f["order_status"] == "Cancelled").mean() * 100
avg_rating     = raw_f["rating"].mean()
avg_discount   = raw_f["discount"].mean() * 100

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Orders",   f"{total_orders:,}",          "Active period")
k2.metric("Total Revenue",  f"₹{total_revenue/1e7:.2f}Cr","Net of discounts")
k3.metric("Avg Delivery",   f"{avg_delivery:.1f} min",    delta=f"{avg_delivery-30:.1f}m vs SLA", delta_color="inverse")
k4.metric("Cancel Rate",    f"{cancel_rate:.1f}%",        delta=f"Target <{cancel_threshold}%", delta_color="inverse")
k5.metric("Avg Rating",     f"{avg_rating:.2f} ★",        "Out of 5.0")
k6.metric("Avg Discount",   f"{avg_discount:.1f}%",       "Applied at checkout")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
#  TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏙️  City Performance",
    "📦  Inventory & Demand",
    "🚚  Delivery Deep-Dive",
    "⏱️  Time & Peak Analysis",
    "🔍  Raw Data Explorer",
])


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 : CITY PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════
with tab1:

    # ── Row 1: Delivery time + Cancel rate ───────────────────────────────────
    st.markdown("""<div class="section-header"><h3>City-wise Delivery & Cancellation</h3><span class="section-pill">Aggregation 1</span></div>""", unsafe_allow_html=True)

    col_a, col_b = st.columns(2)

    with col_a:
        d1 = city_f.sort_values("avg_delivery_time_min", ascending=False)
        colors_d1 = ["#ff6464" if v > del_threshold else "#6c63ff" for v in d1["avg_delivery_time_min"]]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=d1["city"], y=d1["avg_delivery_time_min"],
            marker_color=colors_d1,
            marker_line_width=0,
            text=[f"{v:.1f}m" for v in d1["avg_delivery_time_min"]] if show_labels else None,
            textposition="outside", textfont_color="#ccccee",
            name="Avg Delivery",
            hovertemplate="<b>%{x}</b><br>Avg Delivery: %{y:.1f} min<extra></extra>"
        ))
        if show_sla:
            fig.add_hline(y=del_threshold, line_dash="dot", line_color="#ffd166",
                         annotation_text=f"SLA {del_threshold}m", annotation_font_color="#ffd166")
        apply_base(fig, "Average Delivery Time by City", chart_height, False, "", "Minutes")
        fig.update_yaxes(range=[0, d1["avg_delivery_time_min"].max() * 1.25])
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        d2 = city_f.sort_values("cancellation_rate", ascending=False)
        colors_d2 = ["#ff6464" if v*100 > cancel_threshold else "#00d49d" for v in d2["cancellation_rate"]]
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=d2["city"], y=d2["cancellation_rate"]*100,
            marker_color=colors_d2,
            marker_line_width=0,
            text=[f"{v*100:.1f}%" for v in d2["cancellation_rate"]] if show_labels else None,
            textposition="outside", textfont_color="#ccccee",
            name="Cancel Rate",
            hovertemplate="<b>%{x}</b><br>Cancel Rate: %{y:.1f}%<extra></extra>"
        ))
        if show_sla:
            fig2.add_hline(y=cancel_threshold, line_dash="dot", line_color="#ffd166",
                          annotation_text=f"Alert {cancel_threshold}%", annotation_font_color="#ffd166")
        apply_base(fig2, "Order Cancellation Rate by City", chart_height, False, "", "Cancellation Rate (%)")
        fig2.update_yaxes(range=[0, d2["cancellation_rate"].max()*100*1.25])
        st.plotly_chart(fig2, use_container_width=True)

    # Insight boxes
    worst_del  = city_f.loc[city_f["avg_delivery_time_min"].idxmax(), "city"]
    best_del   = city_f.loc[city_f["avg_delivery_time_min"].idxmin(), "city"]
    worst_can  = city_f.loc[city_f["cancellation_rate"].idxmax(), "city"]
    best_rat   = city_f.loc[city_f["avg_rating"].idxmax(), "city"]

    ia, ib = st.columns(2)
    with ia:
        st.markdown(f"""<div class="warn-box">
        ⚠️ <strong>{worst_del}</strong> has the highest delivery time
        ({city_f[city_f["city"]==worst_del]["avg_delivery_time_min"].values[0]:.1f} min) —
        {city_f[city_f["city"]==worst_del]["avg_delivery_time_min"].values[0]-del_threshold:.1f} min above SLA.
        Recommend adding dark store in high-density zones.
        </div>""", unsafe_allow_html=True)
    with ib:
        st.markdown(f"""<div class="good-box">
        ✅ <strong>{best_del}</strong> is Zepto\'s best performing city with
        {city_f[city_f["city"]==best_del]["avg_delivery_time_min"].values[0]:.1f} min avg delivery and
        {city_f[city_f["city"]==best_del]["cancellation_rate"].values[0]*100:.1f}% cancellation rate.
        Highest rated city: <strong>{best_rat}</strong>.
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Row 2: Rating + Radar ─────────────────────────────────────────────────
    st.markdown("""<div class="section-header"><h3>Customer Rating & Multi-Metric Radar</h3><span class="section-pill">Live Filters</span></div>""", unsafe_allow_html=True)

    col_c, col_d = st.columns([1.1, 1])

    with col_c:
        d3 = city_f.sort_values("avg_rating", ascending=True)
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            y=d3["city"], x=d3["avg_rating"],
            orientation="h",
            marker=dict(
                color=d3["avg_rating"],
                colorscale=[[0,"#ff6464"],[0.5,"#ffd166"],[1,"#00d49d"]],
                showscale=True,
                colorbar=dict(thickness=10, tickfont_color=FONT_COLOR, outlinewidth=0)
            ),
            text=[f"{v:.2f} ★" for v in d3["avg_rating"]] if show_labels else None,
            textposition="outside", textfont_color="#ccccee",
            hovertemplate="<b>%{y}</b><br>Rating: %{x:.2f}/5.0<extra></extra>"
        ))
        if show_sla:
            fig3.add_vline(x=3.5, line_dash="dot", line_color="#ffd166",
                          annotation_text="3.5 benchmark", annotation_font_color="#ffd166")
        apply_base(fig3, "Average Customer Rating by City", chart_height, False, "Avg Rating (out of 5.0)", "")
        fig3.update_xaxes(range=[2.5, 5.0])
        st.plotly_chart(fig3, use_container_width=True)

    with col_d:
        # Radar: normalise each metric 0-1 per city
        cats_r = ["Delivery Speed", "Rating", "Delivery Rate", "Order Volume", "Avg Distance (inv)"]
        city_names = city_f["city"].tolist()

        def norm(col): return (col - col.min()) / (col.max() - col.min() + 1e-9)

        radar_data = city_f.copy()
        radar_data["del_speed_n"] = 1 - norm(radar_data["avg_delivery_time_min"])
        radar_data["rating_n"]    = norm(radar_data["avg_rating"])
        radar_data["del_rate_n"]  = norm(radar_data["delivery_rate"])
        radar_data["orders_n"]    = norm(radar_data["total_orders"])
        radar_data["dist_inv_n"]  = 1 - norm(radar_data["avg_distance_km"])

        fig4 = go.Figure()
        palette_r = ["#6c63ff","#00d49d","#ff6464","#ffd166","#4fc3f7"]
        for i, row in radar_data.iterrows():
            vals = [row["del_speed_n"], row["rating_n"], row["del_rate_n"], row["orders_n"], row["dist_inv_n"]]
            vals += [vals[0]]
            fig4.add_trace(go.Scatterpolar(
                r=vals, theta=cats_r + [cats_r[0]],
                fill="toself",
                name=row["city"],
                line_color=palette_r[list(radar_data["city"]).index(row["city"]) % 5],
                opacity=0.7,
                hovertemplate=f"<b>{row['city']}</b><br>%{{theta}}: %{{r:.2f}}<extra></extra>"
            ))
        fig4.update_layout(**BASE_LAYOUT,
            height=chart_height, title=dict(text="City Performance Radar", font_size=14, font_color="#e0e0f0", x=0.01),
            polar=dict(
                bgcolor="#0d0d1a",
                radialaxis=dict(visible=True, range=[0,1], gridcolor=GRID_COLOR, tickfont_color=FONT_COLOR, tickfont_size=9),
                angularaxis=dict(gridcolor=GRID_COLOR, tickfont_color="#aaaacc", tickfont_size=11)
            )
        )
        st.plotly_chart(fig4, use_container_width=True)

    st.markdown("---")

    # ── Row 3: City × Category Heatmap ───────────────────────────────────────
    st.markdown("""<div class="section-header"><h3>City × Category Revenue Heatmap</h3><span class="section-pill">Cross-Aggregation</span></div>""", unsafe_allow_html=True)

    pivot = cc_f.pivot_table(index="city", columns="category", values="total_revenue", aggfunc="sum").fillna(0)
    if normalize:
        pivot = pivot.div(pivot.sum(axis=1), axis=0) * 100

    fig5 = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=[[0,"#0d0d1a"],[0.3,"#3b2f8f"],[0.7,"#6c63ff"],[1,"#00d49d"]],
        text=[[f"{'%.0f%%' % v if normalize else '₹%.0fK' % (v/1000)}" for v in row] for row in pivot.values],
        texttemplate="%{text}",
        textfont_size=12,
        hovertemplate="<b>%{y} × %{x}</b><br>Value: %{z:,.0f}<extra></extra>",
        showscale=True,
        colorbar=dict(thickness=12, tickfont_color=FONT_COLOR, outlinewidth=0)
    ))
    apply_base(fig5, f"Revenue Heatmap {'(% of city total)' if normalize else '(₹ absolute)'}", 320, False)
    st.plotly_chart(fig5, use_container_width=True)

    st.markdown("""<div class="insight-box">
    📊 <strong>Inventory Insight:</strong> Staples and Dairy drive the most revenue across all cities.
    Snacks show the highest unit volume but lower per-order revenue — ideal for flash-discount campaigns.
    Mumbai leads in overall revenue density; Delhi has the highest order count but longest delivery times.
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 2 : INVENTORY & DEMAND
# ══════════════════════════════════════════════════════════════════════════════
with tab2:

    st.markdown("""<div class="section-header"><h3>Category Demand & Revenue Analysis</h3><span class="section-pill">Aggregation 2</span></div>""", unsafe_allow_html=True)

    # ── Controls ─────────────────────────────────────────────────────────────
    ctrl1, ctrl2, ctrl3 = st.columns(3)
    with ctrl1:
        sort_metric = st.selectbox("Sort by", ["total_revenue","total_units_sold","total_orders","avg_discount","avg_order_value"], format_func=lambda x: x.replace("_"," ").title())
    with ctrl2:
        chart_type_inv = st.selectbox("Chart type", ["Bar","Horizontal Bar","Treemap","Sunburst"])
    with ctrl3:
        top_n = st.slider("Show top N categories", min_value=2, max_value=len(cat_f), value=len(cat_f))

    cat_sorted = cat_f.sort_values(sort_metric, ascending=False).head(top_n)

    col_e, col_f = st.columns([1.5, 1])

    with col_e:
        if chart_type_inv == "Bar":
            fig6 = go.Figure()
            fig6.add_trace(go.Bar(
                x=cat_sorted["category"], y=cat_sorted[sort_metric],
                marker_color=COLORS_CAT[:len(cat_sorted)],
                marker_line_width=0,
                text=[f"{v:,.0f}" for v in cat_sorted[sort_metric]] if show_labels else None,
                textposition="outside", textfont_color="#ccccee",
                hovertemplate="<b>%{x}</b><br>" + sort_metric.replace("_"," ").title() + ": %{y:,.0f}<extra></extra>"
            ))
            apply_base(fig6, sort_metric.replace("_"," ").title() + " by Category", chart_height, False)
            fig6.update_yaxes(range=[0, cat_sorted[sort_metric].max() * 1.22])
            st.plotly_chart(fig6, use_container_width=True)

        elif chart_type_inv == "Horizontal Bar":
            cat_h = cat_sorted.sort_values(sort_metric, ascending=True)
            fig6 = go.Figure(go.Bar(
                y=cat_h["category"], x=cat_h[sort_metric],
                orientation="h",
                marker_color=COLORS_CAT[:len(cat_h)],
                marker_line_width=0,
                text=[f"{v:,.0f}" for v in cat_h[sort_metric]] if show_labels else None,
                textposition="outside", textfont_color="#ccccee",
                hovertemplate="<b>%{y}</b><br>" + sort_metric.replace("_"," ").title() + ": %{x:,.0f}<extra></extra>"
            ))
            apply_base(fig6, sort_metric.replace("_"," ").title() + " by Category", chart_height, False)
            fig6.update_xaxes(range=[0, cat_h[sort_metric].max() * 1.22])
            st.plotly_chart(fig6, use_container_width=True)

        elif chart_type_inv == "Treemap":
            fig6 = px.treemap(cat_sorted, path=["category"], values=sort_metric,
                              color=sort_metric, color_continuous_scale=[[0,"#1a1a3e"],[0.5,"#6c63ff"],[1,"#00d49d"]])
            fig6.update_layout(**BASE_LAYOUT, height=chart_height, title_text=sort_metric.replace("_"," ").title())
            st.plotly_chart(fig6, use_container_width=True)

        else:  # Sunburst
            fig6 = px.sunburst(cc_f, path=["city","category"], values="total_revenue",
                               color="total_revenue", color_continuous_scale=[[0,"#1a1a3e"],[0.5,"#6c63ff"],[1,"#00d49d"]])
            fig6.update_layout(**BASE_LAYOUT, height=chart_height, title_text="City → Category Revenue Sunburst")
            st.plotly_chart(fig6, use_container_width=True)

    with col_f:
        # Discount vs Revenue Bubble
        total_orders_max = cat_f["total_orders"].max()
        fig7 = go.Figure()
        for i, row in cat_f.iterrows():
            fig7.add_trace(go.Scatter(
                x=[row["avg_discount"]*100],
                y=[row["total_revenue"]/1e5],
                mode="markers+text",
                marker=dict(
                    size=(row["total_orders"]/total_orders_max * 45 + 15),
                    color=COLORS_CAT[list(cat_f["category"]).index(row["category"]) % 6],
                    opacity=0.8,
                    line=dict(width=1.5, color="rgba(255,255,255,0.2)")
                ),
                text=[row["category"]],
                textposition="top center",
                textfont_color="#ccccee",
                textfont_size=11,
                name=row["category"],
                hovertemplate=(
                    f"<b>{row['category']}</b><br>"
                    f"Avg Discount: {row['avg_discount']*100:.1f}%<br>"
                    f"Revenue: ₹{row['total_revenue']/1e5:.2f}L<br>"
                    f"Orders: {row['total_orders']:,}<extra></extra>"
                )
            ))
        apply_base(fig7, "Discount vs Revenue (bubble = order volume)", chart_height, False,
                   "Avg Discount (%)", "Revenue (₹ Lakh)")
        fig7.update_layout(showlegend=False)
        fig7.update_xaxes(range=[0, 22])
        fig7.update_yaxes(range=[10, 65])
        st.plotly_chart(fig7, use_container_width=True)

    st.markdown("---")

    # ── Payment Method + Revenue Band ────────────────────────────────────────
    st.markdown("""<div class="section-header"><h3>Payment Methods & Order Value Distribution</h3><span class="section-pill">Transformation 2 & 3</span></div>""", unsafe_allow_html=True)

    col_g, col_h = st.columns(2)

    with col_g:
        pay_counts = raw_f["payment_method"].value_counts().reset_index()
        pay_counts.columns = ["method","count"]
        fig8 = go.Figure(go.Pie(
            labels=pay_counts["method"], values=pay_counts["count"],
            hole=0.62,
            marker=dict(colors=["#6c63ff","#00d49d","#ff6464","#ffd166"], line=dict(color="#0d0d1a", width=2)),
            textinfo="label+percent",
            textfont_color="#ccccee",
            hovertemplate="<b>%{label}</b><br>Orders: %{value:,}<br>Share: %{percent}<extra></extra>"
        ))
        fig8.update_layout(**BASE_LAYOUT, height=chart_height,
            title=dict(text="Payment Method Distribution", font_size=14, font_color="#e0e0f0", x=0.01),
            annotations=[dict(text=f"{pay_counts['count'].sum():,}<br><span style='font-size:10'>orders</span>",
                             x=0.5, y=0.5, showarrow=False, font_size=16, font_color="#f0f0ff")])
        st.plotly_chart(fig8, use_container_width=True)

    with col_h:
        raw_f2 = raw_f.copy()
        raw_f2["revenue_band"] = pd.cut(raw_f2["final_price"], bins=[0,100,400,99999],
                                        labels=["Low (<₹100)","Mid (₹100-400)","High (>₹400)"])
        band_data = raw_f2.groupby("category", observed=True).apply(
            lambda x: pd.Series({b: (x["revenue_band"]==b).sum() for b in ["Low (<₹100)","Mid (₹100-400)","High (>₹400)"]})
        ).reset_index()
        band_long = band_data.melt(id_vars="category", var_name="band", value_name="count")

        fig9 = px.bar(band_long, x="category", y="count", color="band",
                      barmode="stack",
                      color_discrete_map={"Low (<₹100)":"#6c63ff","Mid (₹100-400)":"#00d49d","High (>₹400)":"#ffd166"},
                      labels={"count":"Orders","category":"Category","band":"Revenue Band"})
        fig9.update_layout(**BASE_LAYOUT, height=chart_height,
            title=dict(text="Order Count by Revenue Band (Transformation 3)", font_size=14, font_color="#e0e0f0", x=0.01))
        fig9.update_traces(marker_line_width=0)
        st.plotly_chart(fig9, use_container_width=True)

    # Category Summary Table
    st.markdown("""<div class="section-header"><h3>Category Summary Table</h3><span class="section-pill">Aggregation 2 Full Output</span></div>""", unsafe_allow_html=True)
    total_rev_all = cat_f["total_revenue"].sum()
    table_df = cat_f.copy()
    table_df["Revenue Share (%)"] = (table_df["total_revenue"] / total_rev_all * 100).round(1)
    table_df["Avg Discount (%)"]  = (table_df["avg_discount"] * 100).round(1)
    table_df["Revenue (₹L)"]      = (table_df["total_revenue"] / 1e5).round(2)
    table_df["Avg Order Value (₹)"] = table_df["avg_order_value"].round(1)
    show_cols = ["category","total_orders","total_units_sold","Revenue (₹L)","Avg Order Value (₹)","Avg Discount (%)","Revenue Share (%)"]
    st.dataframe(
        table_df[show_cols].sort_values("Revenue (₹L)", ascending=False).reset_index(drop=True),
        use_container_width=True, hide_index=True,
        column_config={
            "Revenue Share (%)": st.column_config.ProgressColumn("Revenue Share (%)", min_value=0, max_value=100, format="%.1f%%"),
            "Avg Discount (%)": st.column_config.NumberColumn("Avg Discount (%)", format="%.1f%%"),
        }
    )


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 3 : DELIVERY DEEP-DIVE
# ══════════════════════════════════════════════════════════════════════════════
with tab3:

    st.markdown("""<div class="section-header"><h3>Delivery Speed Distribution & SLA Analysis</h3><span class="section-pill">Transformation 1</span></div>""", unsafe_allow_html=True)

    col_i, col_j = st.columns(2)

    with col_i:
        # Stacked % bar: city × delivery speed
        speed_city = raw_f.groupby(["city","delivery_speed"], observed=True).size().reset_index(name="count")
        speed_city_total = speed_city.groupby("city")["count"].transform("sum")
        speed_city["pct"] = (speed_city["count"] / speed_city_total * 100).round(1)
        y_col = "pct" if normalize else "count"

        fig10 = px.bar(speed_city, x="city", y=y_col, color="delivery_speed",
                       barmode="stack",
                       color_discrete_map={"Fast":"#00d49d","Normal":"#ffd166","Delayed":"#ff6464"},
                       labels={y_col: "% of Orders" if normalize else "Orders", "city":"City", "delivery_speed":"Speed"},
                       text_auto=False,
                       category_orders={"delivery_speed":["Delayed","Normal","Fast"]})
        fig10.update_layout(**BASE_LAYOUT, height=chart_height,
            title=dict(text=f"Delivery Speed by City {'(%)' if normalize else '(count)'}", font_size=14, font_color="#e0e0f0", x=0.01))
        fig10.update_traces(marker_line_width=0)
        st.plotly_chart(fig10, use_container_width=True)

    with col_j:
        # Box plot: delivery time by city
        fig11 = go.Figure()
        for city_name in selected_cities:
            city_data = raw_f[raw_f["city"]==city_name]["delivery_time_minutes"]
            fill_color = hex_to_rgba(CITY_COLORS.get(city_name, "#6c63ff"), alpha=0.2)
            fig11.add_trace(go.Box(
                y=city_data, name=city_name,
                marker_color=CITY_COLORS.get(city_name, "#6c63ff"),
                line_color=CITY_COLORS.get(city_name, "#6c63ff"),
                fillcolor=fill_color,
                boxmean=True,
                hovertemplate="<b>" + city_name + "</b><br>%{y} min<extra></extra>"
            ))
        if show_sla:
            fig11.add_hline(y=del_threshold, line_dash="dot", line_color="#ffd166",
                           annotation_text=f"SLA {del_threshold}m", annotation_font_color="#ffd166")
        apply_base(fig11, "Delivery Time Distribution (Box Plot)", chart_height, True, "City", "Minutes")
        st.plotly_chart(fig11, use_container_width=True)

    st.markdown("---")

    # ── Distance vs Delivery Scatter ──────────────────────────────────────────
    st.markdown("""<div class="section-header"><h3>Distance vs Delivery Time Correlation</h3><span class="section-pill">Scatter Analysis</span></div>""", unsafe_allow_html=True)

    sample = raw_f.sample(min(3000, len(raw_f)), random_state=42)
    fig12 = px.scatter(
        sample, x="distance_km", y="delivery_time_minutes",
        color="city", color_discrete_map=CITY_COLORS,
        opacity=0.45, size_max=6,
        trendline="ols",
        labels={"distance_km":"Distance (km)","delivery_time_minutes":"Delivery Time (min)","city":"City"},
        hover_data={"category":True,"order_status":True}
    )
    fig12.update_layout(**BASE_LAYOUT, height=380,
        title=dict(text="Distance vs Delivery Time (sample 3K points, OLS trend lines)", font_size=14, font_color="#e0e0f0", x=0.01))
    fig12.update_traces(marker_size=4, selector=dict(mode="markers"))
    st.plotly_chart(fig12, use_container_width=True)

    st.markdown("""<div class="insight-box">
    📊 <strong>Key Finding:</strong> Strong positive correlation between distance and delivery time.
    Delhi\'s scatter is wider — indicating operational inconsistency in addition to pure distance.
    Mumbai\'s points cluster tightly at low distances, confirming effective dark store placement.
    </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Cancellation by Category + City ──────────────────────────────────────
    st.markdown("""<div class="section-header"><h3>Cancellation Rate — Category × City Breakdown</h3></div>""", unsafe_allow_html=True)

    cancel_hm = raw_f.groupby(["city","category"]).apply(
        lambda x: (x["order_status"]=="Cancelled").mean()*100
    ).reset_index(name="cancel_rate")
    pivot_cancel = cancel_hm.pivot(index="city", columns="category", values="cancel_rate").fillna(0)

    fig13 = go.Figure(go.Heatmap(
        z=pivot_cancel.values,
        x=pivot_cancel.columns.tolist(),
        y=pivot_cancel.index.tolist(),
        colorscale=[[0,"#0d1a14"],[0.5,"#ffd166"],[1,"#ff3333"]],
        text=[[f"{v:.1f}%" for v in row] for row in pivot_cancel.values],
        texttemplate="%{text}",
        textfont_size=12,
        hovertemplate="<b>%{y} — %{x}</b><br>Cancel Rate: %{z:.1f}%<extra></extra>",
        colorbar=dict(thickness=12, tickfont_color=FONT_COLOR, outlinewidth=0, ticksuffix="%")
    ))
    apply_base(fig13, "Cancellation Rate (%) — City × Category", 300, False)
    st.plotly_chart(fig13, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 4 : TIME & PEAK ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
with tab4:

    st.markdown("""<div class="section-header"><h3>Peak Hour Impact on Operations</h3><span class="section-pill">Aggregation 3</span></div>""", unsafe_allow_html=True)

    # ── Peak KPIs ─────────────────────────────────────────────────────────────
    peak_f = peak_df[peak_df["peak_hour_flag"].isin([0,1])]
    peak_row  = peak_f[peak_f["peak_hour_flag"]==1].iloc[0]
    nonpeak_row = peak_f[peak_f["peak_hour_flag"]==0].iloc[0]

    pk1, pk2, pk3, pk4 = st.columns(4)
    pk1.metric("Peak: Avg Delivery",     f"{peak_row['avg_delivery_time_min']:.1f} min",   f"+{peak_row['avg_delivery_time_min']-nonpeak_row['avg_delivery_time_min']:.1f}m vs off-peak", delta_color="inverse")
    pk2.metric("Peak: Total Orders",     f"{int(peak_row['total_orders']):,}",              "of 120K total")
    pk3.metric("Non-Peak: Avg Delivery", f"{nonpeak_row['avg_delivery_time_min']:.1f} min","Off-peak baseline")
    pk4.metric("Peak Revenue",           f"₹{peak_row['total_revenue']/1e7:.2f}Cr",         "7–9am & 6–10pm")

    st.markdown("<br>", unsafe_allow_html=True)

    col_k, col_l = st.columns([1.5, 1])

    with col_k:
        # Hourly order volume + delivery time dual axis
        hourly = raw_f.groupby("order_hour").agg(
            orders=("order_id","count"),
            avg_del=("delivery_time_minutes","mean")
        ).reset_index()

        fig14 = make_subplots(specs=[[{"secondary_y": True}]])
        fig14.add_trace(go.Bar(
            x=hourly["order_hour"], y=hourly["orders"],
            name="Orders",
            marker_color=["#ff6464" if h in [7,8,9,18,19,20,21,22] else "#6c63ff" for h in hourly["order_hour"]],
            marker_line_width=0,
            opacity=0.85,
            hovertemplate="Hour %{x}:00 — %{y:,} orders<extra></extra>"
        ), secondary_y=False)
        fig14.add_trace(go.Scatter(
            x=hourly["order_hour"], y=hourly["avg_del"],
            name="Avg Delivery (min)",
            mode="lines+markers",
            line=dict(color="#ffd166", width=2.5, dash="dot"),
            marker=dict(size=5, color="#ffd166"),
            hovertemplate="Hour %{x}:00 — %{y:.1f} min<extra></extra>"
        ), secondary_y=True)
        if show_sla:
            fig14.add_hline(y=del_threshold, line_dash="dot", line_color="rgba(255,255,255,0.2)",
                           secondary_y=True,
                           annotation_text=f"SLA {del_threshold}m", annotation_font_color=FONT_COLOR)

        fig14.update_layout(**BASE_LAYOUT, height=chart_height,
            title=dict(text="Hourly Order Volume & Delivery Time (red = peak hours)", font_size=14, font_color="#e0e0f0", x=0.01))
        fig14.update_xaxes(tickvals=list(range(0,24,2)), ticktext=[f"{h}:00" for h in range(0,24,2)],
                           gridcolor=GRID_COLOR, tickfont_color=FONT_COLOR, title_text="Hour of Day")
        fig14.update_yaxes(title_text="Orders", secondary_y=False, gridcolor=GRID_COLOR, tickfont_color=FONT_COLOR)
        fig14.update_yaxes(title_text="Avg Delivery (min)", secondary_y=True, gridcolor=GRID_COLOR, tickfont_color="#ffd166")
        st.plotly_chart(fig14, use_container_width=True)

    with col_l:
        # Peak vs Non-Peak grouped comparison
        peak_labels = ["Non-Peak","Peak"]
        metrics_peak = {
            "Avg Delivery (min)": [nonpeak_row["avg_delivery_time_min"], peak_row["avg_delivery_time_min"]],
            "Delivery Rate (%)":  [nonpeak_row["delivery_rate"]*100, peak_row["delivery_rate"]*100],
            "Avg Rating":         [nonpeak_row["avg_rating"], peak_row["avg_rating"]],
        }
        metric_choice = st.selectbox("Compare metric", list(metrics_peak.keys()), key="peak_metric")
        vals_p = metrics_peak[metric_choice]

        fig15 = go.Figure(go.Bar(
            x=peak_labels, y=vals_p,
            marker_color=["#6c63ff","#ff6464"],
            marker_line_width=0,
            text=[f"{v:.1f}" for v in vals_p] if show_labels else None,
            textposition="outside", textfont_color="#ccccee",
            width=0.45,
            hovertemplate="<b>%{x}</b><br>" + metric_choice + ": %{y:.2f}<extra></extra>"
        ))
        apply_base(fig15, f"{metric_choice}: Peak vs Non-Peak", chart_height, False, "", metric_choice)
        diff = vals_p[1] - vals_p[0]
        fig15.add_annotation(x=1, y=vals_p[1]*1.08, text=f"{'↑' if diff>0 else '↓'} {abs(diff):.1f}", showarrow=False,
                             font=dict(size=13, color="#ff6464" if diff>0 else "#00d49d"))
        st.plotly_chart(fig15, use_container_width=True)

    st.markdown("---")

    # ── Day of week analysis ──────────────────────────────────────────────────
    st.markdown("""<div class="section-header"><h3>Daily Order Pattern (15-day window)</h3></div>""", unsafe_allow_html=True)

    raw_f["order_date"] = pd.to_datetime(raw_f["order_time"]).dt.date
    daily = raw_f.groupby("order_date").agg(
        orders=("order_id","count"),
        revenue=("final_price","sum"),
        avg_del=("delivery_time_minutes","mean")
    ).reset_index()

    fig16 = make_subplots(specs=[[{"secondary_y":True}]])
    fig16.add_trace(go.Scatter(
        x=daily["order_date"].astype(str), y=daily["orders"],
        fill="tozeroy", fillcolor="rgba(108,99,255,0.12)",
        line=dict(color="#6c63ff", width=2),
        name="Orders",
        hovertemplate="%{x}<br>Orders: %{y:,}<extra></extra>"
    ), secondary_y=False)
    fig16.add_trace(go.Scatter(
        x=daily["order_date"].astype(str), y=daily["avg_del"],
        line=dict(color="#ffd166", width=1.8, dash="dash"),
        name="Avg Delivery (min)",
        hovertemplate="%{x}<br>Avg Delivery: %{y:.1f} min<extra></extra>"
    ), secondary_y=True)
    fig16.update_layout(**BASE_LAYOUT, height=320,
        title=dict(text="Daily Order Volume & Delivery Time Trend", font_size=14, font_color="#e0e0f0", x=0.01))
    fig16.update_xaxes(gridcolor=GRID_COLOR, tickfont_color=FONT_COLOR, tickangle=30)
    fig16.update_yaxes(title_text="Orders", secondary_y=False, gridcolor=GRID_COLOR, tickfont_color=FONT_COLOR)
    fig16.update_yaxes(title_text="Avg Delivery (min)", secondary_y=True, gridcolor=GRID_COLOR, tickfont_color="#ffd166")
    st.plotly_chart(fig16, use_container_width=True)

    st.markdown("""<div class="insight-box">
    ⏱️ <strong>Peak Hour Insight:</strong> Peak hours (7–9am & 6–10pm) account for ~33% of daily orders
    but show +4.2 min longer delivery on average. Cancellation rates remain similar, suggesting customers
    are more patient during rush hours. <strong>Recommendation:</strong> Pre-position inventory at dark stores
    before 7am and 5pm to absorb peak demand efficiently.
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 5 : RAW DATA EXPLORER
# ══════════════════════════════════════════════════════════════════════════════
with tab5:

    st.markdown("""<div class="section-header"><h3>Raw Data Explorer</h3><span class="section-pill">Interactive Filter</span></div>""", unsafe_allow_html=True)

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        status_f = st.multiselect("Order Status", raw["order_status"].unique(), default=list(raw["order_status"].unique()))
    with f2:
        payment_f = st.multiselect("Payment Method", raw["payment_method"].dropna().unique(), default=list(raw["payment_method"].dropna().unique()))
    with f3:
        del_range = st.slider("Delivery Time (min)", int(raw["delivery_time_minutes"].min()), int(raw["delivery_time_minutes"].max()), (10, 90))
    with f4:
        rating_range = st.slider("Rating", float(raw["rating"].min()), float(raw["rating"].max()), (1.0, 5.0), step=0.1)

    raw_filtered = raw_f[
        raw_f["order_status"].isin(status_f) &
        raw_f["payment_method"].isin(payment_f) &
        raw_f["delivery_time_minutes"].between(*del_range) &
        raw_f["rating"].between(*rating_range)
    ]

    st.markdown(f"<div style='font-size:12px;color:#8888aa;margin-bottom:10px;'>Showing <strong style=\'color:#a89cff\'>{len(raw_filtered):,}</strong> of {len(raw_f):,} filtered rows</div>", unsafe_allow_html=True)

    display_cols = ["city","area","product_name","category","quantity","price_per_unit",
                    "discount","delivery_time_minutes","distance_km","payment_method",
                    "order_status","rating","final_price","peak_hour_flag"]
    st.dataframe(
        raw_filtered[display_cols].head(500).reset_index(drop=True),
        use_container_width=True, hide_index=True,
        column_config={
            "discount": st.column_config.NumberColumn("Discount", format="%.0f%%"),
            "final_price": st.column_config.NumberColumn("Final Price (₹)", format="₹%.2f"),
            "rating": st.column_config.NumberColumn("Rating", format="%.1f ★"),
            "delivery_time_minutes": st.column_config.NumberColumn("Delivery (min)", format="%d min"),
        }
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # Download button
    csv_out = raw_filtered[display_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️  Download filtered data as CSV",
        data=csv_out,
        file_name="zepto_filtered_data.csv",
        mime="text/csv",
    )

    st.markdown("---")

    # Quick stats on filtered set
    st.markdown("""<div class="section-header"><h3>Quick Stats on Filtered Data</h3></div>""", unsafe_allow_html=True)
    qs1, qs2, qs3, qs4, qs5 = st.columns(5)
    qs1.metric("Filtered Orders",   f"{len(raw_filtered):,}")
    qs2.metric("Avg Delivery",      f"{raw_filtered['delivery_time_minutes'].mean():.1f} min")
    qs3.metric("Cancel Rate",       f"{(raw_filtered['order_status']=='Cancelled').mean()*100:.1f}%")
    qs4.metric("Avg Rating",        f"{raw_filtered['rating'].mean():.2f} ★")
    qs5.metric("Total Revenue",     f"₹{raw_filtered['final_price'].sum()/1e5:.1f}L")

    # Final footer
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("""
    <div style="text-align:center;padding:20px;border-top:1px solid rgba(108,99,255,0.12);margin-top:24px;">
      <div style="font-size:12px;color:#444466;font-family:monospace;">
        Zepto Analytics Dashboard · FA2 BDCCT · Built with PySpark + Streamlit + Plotly
      </div>
    </div>
    """, unsafe_allow_html=True)
'''

# ── Write the dashboard app file ─────────────────────────────────────────────
with open("zepto_dashboard_app.py", "w", encoding="utf-8") as f:
    f.write(dashboard_code)

print("=" * 60)
print("Dashboard file written: zepto_dashboard_app.py")
print("=" * 60)
print()
print("HOW TO RUN IN GOOGLE COLAB:")
print("-" * 40)
print("1. Install requirements:")
print("   !pip install streamlit plotly pyngrok -q")
print()
print("2. Run the dashboard:")
print("   !streamlit run zepto_dashboard_app.py &")
print()
print("3. Create public URL (optional):")
print("   from pyngrok import ngrok")
print("   public_url = ngrok.connect(8501)")
print("   print('Dashboard URL:', public_url)")
print()
print("HOW TO RUN LOCALLY:")
print("-" * 40)
print("   pip install streamlit plotly pandas openpyxl")
print("   streamlit run zepto_dashboard_app.py")
print("   Then open: http://localhost:8501")
print("=" * 60)
