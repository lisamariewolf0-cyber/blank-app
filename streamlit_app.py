import pandas as pd
import streamlit as st
from supabase import create_client

st.set_page_config(page_title="Credit Early Warning Dashboard", layout="wide")

st.title("Credit Early Warning Dashboard")
st.subheader("Alerts des Tages")

supabase = create_client(
    st.secrets["SUPABASE_URL"],
    st.secrets["SUPABASE_KEY"]
)

customers_response = (
    supabase.table("customers")
    .select("id, customer_name, sector")
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

customers = pd.DataFrame(customers_response.data)
alerts = pd.DataFrame(alerts_response.data)

if customers.empty:
    st.error("Keine Kunden gefunden.")
    st.stop()

if alerts.empty:
    st.warning("Keine Alerts gefunden.")
    st.stop()

alerts_view = alerts.merge(
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
