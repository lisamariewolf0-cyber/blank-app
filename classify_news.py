import os
import json
import time

from openai import OpenAI
from supabase import create_client

client = OpenAI(
    api_key=os.environ["GROQ_API_KEY"],
    base_url="https://api.groq.com/openai/v1",
)

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)

TARGET_DATE = os.environ.get("INGEST_DATE")

def classification_messages(item):
    system_msg = {
        "role": "system",
        "content": "Du klassifizierst externe Unternehmensmeldungen fuer ein Fruehwarnsystem fuer Kreditanalysten. Gib nur JSON zurueck."
    }
    user_content = "Kunde-ID: " + str(item["customer_id"]) + "\n"
    user_content += "Quelle: " + str(item["source_name"]) + "\n"
    user_content += "Titel: " + str(item["headline"]) + "\n"
    user_content += "Kurztext: " + str(item["summary"]) + "\n"
    user_content += "Volltext: " + str(item["raw_text"]) + "\n\n"
    user_content += "Gib JSON zurueck mit: signal_type (negative_press|management_change|profit_warning|price_related_news|other), "
    user_content += "sentiment (negative|neutral|positive), relevance (high|medium|low), "
    user_content += "triggers_alert_candidate (true|false), classification_reason (string), llm_summary (string)"
    user_msg = {"role": "user", "content": user_content}
    return [system_msg, user_msg]

def classify_item(item, retries=3):
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=classification_messages(item),
                response_format={"type": "json_object"},
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            wait = 2 ** attempt
            print("  Versuch " + str(attempt + 1) + "/" + str(retries) + " fehlgeschlagen: " + str(e))
            if attempt < retries - 1:
                print("  Warte " + str(wait) + "s ...")
                time.sleep(wait)
            else:
                raise

def already_exists(source_external_id, customer_id):
    resp = (
        supabase.table("news_events")
        .select("id")
        .eq("source_external_id", source_external_id)
        .eq("customer_id", customer_id)
        .limit(1)
        .execute()
    )
    return len(resp.data or []) > 0

def save_item(item, cls):
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
        "dedupe_key":               str(item["customer_id"]) + "_" + str(item["source_external_id"]),
        "triggers_alert_candidate": cls["triggers_alert_candidate"],
        "classification_reason":    cls["classification_reason"],
        "llm_summary":              cls["llm_summary"],
    }
    return supabase.table("news_events").insert(row).execute()
