import pandas as pd
import streamlit as st
from supabase import create_client

def priority_order(value):
    order = {
        "high": 0,
        "medium": 1,
        "low": 2,
        "none": 3
    }
    return order.get(str(value).lower(), 99)

def style_priority(val):
    val = str(val).lower()

    if val == "high":
        return "background-color: #FDECEC; color: #A61B1B; font-weight: 700;"
    elif val == "medium":
        return "background-color: #FFF4E5; color: #9A5B00; font-weight: 700;"
    elif val == "low":
        return "background-color: #EAF4FF; color: #1D4F91; font-weight: 700;"
    elif val == "none":
        return "background-color: #F3F4F6; color: #4B5563; font-weight: 600;"
    return ""

def detail_box(label, value, bg="#F8FAFC", border="#E5E7EB"):
    return f"""
    <div style="
        background: {bg};
        border: 1px solid {border};
        border-radius: 10px;
        padding: 12px 14px;
        min-height: 68px;
        margin-bottom: 8px;
    ">
        <div style="
            font-size: 0.8rem;
            color: #6B7280;
            margin-bottom: 6px;
        ">{label}</div>
        <div style="
            font-size: 1rem;
            font-weight: 600;
            color: #111827;
            line-height: 1.35;
        ">{value}</div>
    </div>
    """

st.set_page_config(page_title="Credit Early Warning Dashboard", layout="wide")

st.title("Credit Early Warning Dashboard")
st.subheader("Frühwarnsystem für Kreditanalysten")

supabase = create_client(
    st.secrets["SUPABASE_URL"],
    st.secrets["SUPABASE_KEY"]
)

customers_response = (
    supabase.table("customers")
    .select("id, customer_name, sector, ticker, track_price")
    .execute()
)

alerts_response = (
    supabase.table("alerts")
    .select(
        "id, customer_id, alert_date, alert_priority, alert_status, alert_type, "
        "max_abs_price_change_pct, alert_reason"
    )
    .execute()
)

prices_response = ( 
    supabase.table("price_snapshots") 
    .select( 
        "id, customer_id, trading_date, close_price, prev_close_price, pct_change, " 
        "price_alert_level" 
    ) 
    .execute()
)

news_response = (
    supabase.table("news_events")
    .select(
        "id, customer_id, ingestion_date, published_at, source_name, headline, "
        "signal_type, sentiment, relevance, llm_summary"
    )
    .execute()
)

customers = pd.DataFrame(customers_response.data)
alerts = pd.DataFrame(alerts_response.data)
prices = pd.DataFrame(prices_response.data)
news = pd.DataFrame(news_response.data)

if customers.empty:
    st.error("Keine Kunden gefunden.")
    st.stop()

customer_count = len(customers)
alert_count = len(alerts) if not alerts.empty else 0
high_alert_count = len(alerts[alerts["alert_priority"] == "high"]) if not alerts.empty else 0
price_move_count = len(prices[prices["pct_change"].abs() >= 3]) if not prices.empty else 0

st.subheader("Überblick")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Kunden im Portfolio", customer_count)

with col2:
    st.metric("Offene Alerts", alert_count)

with col3:
    st.metric("High Alerts", high_alert_count)

with col4:
    st.metric("Kursbewegung > 3%", price_move_count)

st.divider()

sector_options = ["Alle"] + sorted(customers["sector"].dropna().unique().tolist())
selected_sector = st.selectbox("Branche filtern", sector_options)

st.divider() 

left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("Alerts des Tages")

    if alerts.empty:
        st.warning("Keine Alerts gefunden.")
    else:
        alerts_view = alerts.merge(
            customers,
            left_on="customer_id",
            right_on="id",
            how="left"
        )

        if selected_sector != "Alle":
            alerts_view = alerts_view[alerts_view["sector"] == selected_sector]

        alert_dates = sorted(alerts_view["alert_date"].dropna().astype(str).unique().tolist())

        if len(alert_dates) == 1:
            st.markdown(f"**Stand Alerts:** {alert_dates[0]}")
        elif len(alert_dates) > 1:
            st.markdown(f"**Stand Alerts:** {', '.join(alert_dates)}")
        
        alerts_view["priority_rank"] = alerts_view["alert_priority"].apply(priority_order)
        alerts_view = alerts_view.sort_values(
            by=["priority_rank", "customer_name"],
            ascending=[True, True]
        )
      
        st.metric("Anzahl Alerts", len(alerts_view))

        alerts_table = alerts_view[
            [
                "customer_name",
                "sector",
                "alert_priority",
                "alert_type",
                "max_abs_price_change_pct",
                "alert_status",
                "alert_reason",
            ]
        ].rename(
            columns={
                "customer_name": "Kunde",
                "sector": "Branche",
                "alert_priority": "Priorität",
                "alert_type": "Alert-Typ",
                "max_abs_price_change_pct": "Max. Kursbewegung %",
                "alert_status": "Status",
                "alert_reason": "Begründung",
            }
        )

        styled_alerts = alerts_table.style.map(style_priority, subset=["Priorität"])

        st.dataframe(
            styled_alerts,
            use_container_width=True,
            hide_index=True,
        )

with right_col:
    st.subheader("Kundendetail")

    if selected_sector != "Alle":
        filtered_customers = customers[customers["sector"] == selected_sector].copy()
    else:
        filtered_customers = customers.copy()

    customer_options = filtered_customers["customer_name"].sort_values().tolist()
    selected_customer = st.selectbox("Kunde auswählen", customer_options)

    selected_row = filtered_customers[
        filtered_customers["customer_name"] == selected_customer
    ].iloc[0]
    selected_customer_id = selected_row["id"]

    detail_alerts = (
        alerts[alerts["customer_id"] == selected_customer_id]
        if not alerts.empty
        else pd.DataFrame()
    )

    detail_prices = (
        prices[prices["customer_id"] == selected_customer_id]
        if not prices.empty
        else pd.DataFrame()
    )

    detail_news = (
        news[news["customer_id"] == selected_customer_id]
        if not news.empty
        else pd.DataFrame()
    )

    st.markdown("#### Überblick")

    if not detail_alerts.empty:
        latest_alert = detail_alerts.sort_values("alert_date").iloc[-1]
        alert_priority_value = latest_alert["alert_priority"]
        alert_type_value = latest_alert["alert_type"]
        reason_value = latest_alert["alert_reason"]
    else:
        alert_priority_value = "none"
        alert_type_value = "kein Alert"
        reason_value = "Kein Alert vorhanden."
    
    if not detail_prices.empty:
        latest_price = detail_prices.sort_values("trading_date").iloc[-1]
        last_price_value = f"{latest_price['close_price']:.2f}"
        pct_change_value = f"{latest_price['pct_change']:.2f} %"
    else:
        last_price_value = "n/a"
        pct_change_value = "n/a"
    
    priority_bg = "#F3F4F6"
    priority_border = "#D1D5DB"
    
    if str(alert_priority_value).lower() == "high":
        priority_bg = "#FDECEC"
        priority_border = "#F5C2C7"
    elif str(alert_priority_value).lower() == "medium":
        priority_bg = "#FFF4E5"
        priority_border = "#F3D19C"
    elif str(alert_priority_value).lower() == "low":
        priority_bg = "#EAF4FF"
        priority_border = "#B9D6F2"
    
    top1, top2, top3 = st.columns([1.4, 1.2, 0.8])
    
    with top1:
        st.markdown(detail_box("Kunde", selected_row["customer_name"]), unsafe_allow_html=True)
    
    with top2:
        st.markdown(detail_box("Branche", selected_row["sector"]), unsafe_allow_html=True)
    
    with top3:
        st.markdown(detail_box("Ticker", selected_row["ticker"]), unsafe_allow_html=True)
    
    mid1, mid2, mid3, mid4 = st.columns(4)
    
    with mid1:
        st.markdown(
            detail_box("Alert-Priorität", alert_priority_value, bg=priority_bg, border=priority_border),
            unsafe_allow_html=True
        )
    
    with mid2:
        st.markdown(detail_box("Alert-Typ", alert_type_value), unsafe_allow_html=True)
    
    with mid3:
        st.markdown(detail_box("Letzter Kurs", last_price_value), unsafe_allow_html=True)
    
    with mid4:
        st.markdown(detail_box("Veränderung zum Vortag", pct_change_value), unsafe_allow_html=True)
    
    st.markdown(
        f"""
        <div style="
            background: #F8FAFC;
            border: 1px solid #E5E7EB;
            border-radius: 10px;
            padding: 12px 14px;
            margin-top: 6px;
            margin-bottom: 12px;
        ">
            <div style="
                font-size: 0.8rem;
                color: #6B7280;
                margin-bottom: 6px;
            ">Begründung</div>
            <div style="
                font-size: 0.96rem;
                color: #111827;
                line-height: 1.45;
            ">{reason_value}</div>
        </div>
        """,
        unsafe_allow_html=True
    ) 

    st.markdown("#### Neueste Meldungen")

    if not detail_news.empty:
        detail_news = detail_news.sort_values("published_at", ascending=False).head(3)

        news_table = detail_news[
            [
                "published_at",
                "source_name",
                "headline",
                "signal_type",
                "relevance",
            ]
        ].rename(
            columns={
                "published_at": "Zeitpunkt",
                "source_name": "Quelle",
                "headline": "Überschrift",
                "signal_type": "Signalart",
                "relevance": "Relevanz",
            }
        )

        st.dataframe(news_table, use_container_width=True, hide_index=True)

        for _, row in detail_news.iterrows():
            if pd.notnull(row.get("llm_summary")) and str(row["llm_summary"]).strip():
                st.caption(f"{row['source_name']}: {row['llm_summary']}")
    else:
        st.write("Keine Meldungen vorhanden.")
        
st.subheader("Portfolioübersicht") 

portfolio_view = customers.copy() 

if not prices.empty: 
    latest_prices = prices.sort_values("trading_date").drop_duplicates( 
        subset=["customer_id"], 
        keep="last" 
    ) 
    
    portfolio_view = portfolio_view.merge( 
        latest_prices[["customer_id", "close_price", "pct_change"]], 
        left_on="id", 
        right_on="customer_id", 
        how="left" 
    ) 
    
if not alerts.empty: 
    latest_alerts = alerts.sort_values("alert_date").drop_duplicates( 
        subset=["customer_id"], 
        keep="last" 
    ) 
    
    portfolio_view = portfolio_view.merge( 
        latest_alerts[["customer_id", "alert_priority"]], 
        left_on="id", 
        right_on="customer_id", 
        how="left" 
    ) 
    
    portfolio_view["alert_priority"] = portfolio_view["alert_priority"].fillna("none") 

    sector_view = portfolio_view.copy()

    sector_view["has_alert"] = sector_view["alert_priority"].apply(
        lambda x: 0 if x == "none" else 1
    )

    sector_view["has_high_alert"] = sector_view["alert_priority"].apply(
        lambda x: 1 if x == "high" else 0
    )

    sector_overview = (
        sector_view.groupby("sector", dropna=False)
        .agg(
            kunden_anzahl=("customer_name", "count"),
            alerts_anzahl=("has_alert", "sum"),
            high_alerts=("has_high_alert", "sum"),
            durchschnitt_kursveraenderung=("pct_change", "mean"),
        )
        .reset_index()
    )

    st.markdown("Branchenüberblick")

    st.dataframe(
        sector_overview.rename(
            columns={
                "sector": "Branche",
                "kunden_anzahl": "Anzahl Kunden",
                "alerts_anzahl": "Anzahl Alerts",
                "high_alerts": "High Alerts",
                  "durchschnitt_kursveraenderung": "Ø Kursveränderung %",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("Kundenübersicht")
    
    portfolio_table = portfolio_view[
        [
            "customer_name",
            "sector",
            "ticker",
            "track_price",
            "close_price",
            "pct_change",
            "alert_priority",
        ]

    ].rename(
        columns={
            "customer_name": "Kunde",
            "sector": "Branche",
            "ticker": "Ticker",
            "track_price": "Kurse tracken",
            "close_price": "Letzter Kurs",
            "pct_change": "Veränderung zum Vortag %",
            "alert_priority": "Heutiger Alert",
        }
    )

    styled_portfolio = portfolio_table.style.map(style_priority, subset=["Heutiger Alert"])

    st.dataframe(
        styled_portfolio,
        use_container_width=True,
        hide_index=True,
    )





    
