import os
import json
import time

import google.generativeai as genai
from supabase import create_client

genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel("gemini-1.5-flash")

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)

TARGET_DATE = os.environ.get("INGEST_DATE")

def classification_prompt(item: dict) -> str:
    return f"""Du klassifizierst externe Unternehmensmeldungen für ein Frühwarnsystem für Kreditanalysten.
Gib NUR ein JSON-Objekt zurück, keine Erklärungen, keine Markdown-Backticks.

Regeln:
- "profit_warning" bei Gewinnwarnung, Guidance-Senkung oder finanzieller Verschlechterung
- "management_change" bei relevantem Wechsel im Top-Management
- "negative_press" bei negativer Berichterstattung mit möglicher Kreditrelevanz
- "price_related_news" nur wenn die Nachricht direkt kursbezogen ist
- sonst "other"

Relevanz:
- high: direkt kreditrelevant oder deutliche finanzielle Verschlechterung
- medium: relevant, aber nicht unmittelbar kritisch
- low: dokumentationswürdig, aber kein Warnsignal

Meldung:
Kunde-ID: {item["customer_id"]}
Quelle: {item["source_name"]}
Titel: {item["headline"]}
Kurztext: {item["summary"]}
Volltext: {item["raw_text"]}

Antworte ausschließlich mit diesem JSON-Schema:
{{
  "signal_type": "negative_press|management_change|profit_warning|price_related_news|other",
  "sentiment": "negative|neutral|positive",
  "relevance": "high|medium|low",
  "triggers_alert_candidate": true|false,
  "classification_reason": "kurze Begründung",
  "llm_summary": "Kurzzusammenfassung der Meldung"
}}"""


def classify_item(item: dict, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            response = model.generate_content(classification_prompt(item))
            text = response.text.strip()

            # Markdown-Backticks entfernen falls vorhanden
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()

            return json.loads(text)

        except Exception as e:
            wait = 2 ** attempt
            print(f"  Versuch {attempt + 1}/{retries} fehlgeschlagen: {e}")
            if attempt < retries - 1:
                print(f"  Warte {wait}s ...")
                time.sleep(wait)
            else:
                raise


def already_exists(source_external_id: str, customer_id: int) -> bool:
    resp = (
        supabase.table("news_events")
        .select("id")
        .eq("source_external_id", source_external_id)
        .eq("customer_id", customer_id)
        .limit(1)
        .execute()
    )
    return len(resp.data or []) > 0


def save_item(item: dict, cls: dict):
    row = {
        "customer_id":              item["customer_id"],
        "source_name":              item["source_name"],
        "source_type":              item["source_type"],
        "source_url":               item["source_url"],
        "source_external_id":       item["source_external_id"],
        "published_at":             item["published_at"],
        "ingestion_date":           item["ingestion_date"],
        "headline":                 item["headline"],
        "summary":                  item["summary"],
        "raw_text":                 item["raw_text"],
        "language":                 item["language"],
        "matched_alias":            item["matched_alias"],
        "signal_type":              cls["signal_type"],
        "sentiment":                cls["sentiment"],
        "relevance":                cls["relevance"],
        "relevance_score":          None,
        "is_duplicate":             False,
        "dedupe_key":               f'{item["customer_id"]}_{item["source_external_id"]}',
        "triggers_alert_candidate": cls["triggers_alert_candidate"],
        "classification_reason":    cls["classification_reason"],
        "llm_summary":              cls["llm_summary"],
    }
    return supabase.table("news_events").insert(row).execute()
