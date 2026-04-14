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
customers = pd.DataFrame(customers_response.data)
alerts = pd.DataFrame(alerts_response.data)
prices = pd.DataFrame(prices_response.data)

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







    
