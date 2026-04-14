import streamlit as st

st.set_page_config(page_title="Credit Early Warning Dashboard", layout="wide")

st.title("Credit Early Warning Dashboard")
st.subheader("Frühwarnsystem für Kreditanalysten")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Offene Alerts", "3")

with col2:
    st.metric("High Alerts", "2")

with col3:
    st.metric("Kunden mit Kursbewegung > 3%", "2")

with col4:
    st.metric("Neue Meldungen", "5")

st.divider()

st.subheader("Alerts des Tages")

st.table([
    {
        "Kunde": "Bayer",
        "Priorität": "high",
        "Alert-Typ": "combined",
        "Begründung": "Gewinnwarnung und Kursrückgang über 5 Prozent"
    },
    {
        "Kunde": "BMW",
        "Priorität": "medium",
        "Alert-Typ": "news_only",
        "Begründung": "Relevanter Managementwechsel ohne starke Kursreaktion"
    },
    {
        "Kunde": "Volkswagen",
        "Priorität": "high",
        "Alert-Typ": "combined",
        "Begründung": "Negative Presse kombiniert mit deutlicher Kursbewegung"
    }
])

st.divider()

st.subheader("Portfolioübersicht")

st.table([
    {
        "Kunde": "BASF",
        "Branche": "Chemie & Life Sciences",
        "Letzter Kurs": "48.30",
        "Veränderung zum Vortag %": "0.63",
        "Heutiger Alert": "none"
    },
    {
        "Kunde": "Bayer",
        "Branche": "Chemie & Life Sciences",
        "Letzter Kurs": "24.50",
        "Veränderung zum Vortag %": "-5.77",
        "Heutiger Alert": "high"
    },
    {
        "Kunde": "BMW",
        "Branche": "Automotive",
        "Letzter Kurs": "101.00",
        "Veränderung zum Vortag %": "-1.94",
        "Heutiger Alert": "medium"
    }
])

st.divider()

st.subheader("KI-Agent")
user_question = st.text_input("Frage zum Portfolio", placeholder="Was ist heute bei Bayer passiert?")

if user_question:
    st.write("Antwort des KI-Agenten erscheint hier im nächsten Schritt.")
