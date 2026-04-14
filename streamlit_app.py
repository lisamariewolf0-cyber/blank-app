import pandas as pd
import streamlit as st
from supabase import create_client

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

if alerts.empty:
    st.warning("Keine Alerts gefunden.")

else: alerts_view = alerts.merge( 
    customers, 
    left_on="customer_id", 
    right_on="id", 
    how="left" 
) 
    
st.metric("Anzahl Alerts", len(alerts_view)) 

st.dataframe( 
    alerts_view[ 
        [ 
            "customer_name", 
            "sector", 
            "alert_date", 
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
            "alert_date": "Datum", 
            "alert_priority": "Priorität", 
            "alert_type": "Alert-Typ", 
            "max_abs_price_change_pct": "Max. Kursbewegung %", 
            "alert_status": "Status", 
            "alert_reason": "Begründung", 
        } 
    ), 
    use_container_width=True, 
    hide_index=True, 
) 

st.divider() 
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
    
    st.dataframe( 
        portfolio_view[ 
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
    ), 
    use_container_width=True, 
    hide_index=True, 
)

st.divider() 
st.subheader("Kundendetail") 

customer_options = customers["customer_name"].sort_values().tolist() 
selected_customer = st.selectbox("Kunde auswählen", customer_options) 

selected_row = customers[customers["customer_name"] == selected_customer].iloc[0] 
selected_customer_id = selected_row["id"] 

detail_alerts = alerts[alerts["customer_id"] == selected_customer_id] if not alerts.empty else pd.DataFrame() 
detail_prices = prices[prices["customer_id"] == selected_customer_id] if not prices.empty else pd.DataFrame() 
detail_news = (
    news[news["customer_id"] == selected_customer_id]
    if not news.empty
    else pd.DataFrame()
)

col1, col2 = st.columns(2)

with col1: 
    st.markdown("**Stammdaten**") 
    st.write(f"Kunde: {selected_row['customer_name']}") 
    st.write(f"Branche: {selected_row['sector']}") 
    st.write(f"Ticker: {selected_row['ticker']}") 
    st.write(f"Kurse tracken: {selected_row['track_price']}") 

with col2: 
    st.markdown("**Aktueller Status**") 

    if not detail_alerts.empty: 
        latest_alert = detail_alerts.sort_values("alert_date").iloc[-1] 
        st.write(f"Alert-Priorität: {latest_alert['alert_priority']}") 
        st.write(f"Alert-Typ: {latest_alert['alert_type']}") 
        st.write(f"Begründung: {latest_alert['alert_reason']}") 
    else: 
        st.write("Kein Alert vorhanden.") 

    if not detail_prices.empty: 
        latest_price = detail_prices.sort_values("trading_date").iloc[-1] 
        st.write(f"Letzter Kurs: {latest_price['close_price']}") 
        st.write(f"Veränderung zum Vortag %: {latest_price['pct_change']}") 
    else: 
        st.write("Kein Kursdatensatz vorhanden.")

st.markdown("**Neueste Meldungen**")

if not detail_news.empty:
    detail_news = detail_news.sort_values("published_at", ascending=False)

    st.dataframe(
        detail_news[
            [
                "published_at",
                "source_name",
                "headline",
                "signal_type",
                "sentiment",
                "relevance",
                "llm_summary",
            ]
        ].rename(
            columns={
                "published_at": "Zeitpunkt",
                "source_name": "Quelle",
                "headline": "Überschrift",
                "signal_type": "Signalart",
                "sentiment": "Sentiment",
                "relevance": "Relevanz",
                "llm_summary": "Zusammenfassung",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.write("Keine Meldungen vorhanden.")




    
