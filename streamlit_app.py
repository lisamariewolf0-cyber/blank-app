import pandas as pd
import streamlit as st
from supabase import create_client

st.set_page_config(page_title="Credit Early Warning Dashboard", layout="wide")

st.title("Credit Early Warning Dashboard")
st.subheader("Verbindungstest zu Supabase")

supabase = create_client(
    st.secrets["SUPABASE_URL"],
    st.secrets["SUPABASE_KEY"]
)

response = (
    supabase.table("customers")
    .select("id, customer_name, legal_name, ticker, sector, track_price")
    .execute()
)

customers = pd.DataFrame(response.data)

st.metric("Anzahl Kunden", len(customers))

st.write("Kundentabelle aus Supabase:")
st.dataframe(customers, use_container_width=True, hide_index=True)
