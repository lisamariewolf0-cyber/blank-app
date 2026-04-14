import os
from datetime import datetime, date, timezone

from openai import OpenAI

client = OpenAI(
    api_key=os.environ["GROQ_API_KEY"],
    base_url="https://api.groq.com/openai/v1",
)
from supabase import create_client


openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)

# Testmeldung fuer v1
raw_news = {
    "customer_id": 6,  # Bayer
    "source_name": "Reuters",
    "source_type": "newswire",
    "source_url": "",
    "source_external_id": "REU-TEST-20260414-100",
    "published_at": "2026-04-14T08:15:00+02:00",
    "ingestion_date": "2026-04-14",
    "headline": "Bayer senkt Gewinnprognose",
    "summary": "Unternehmen passt den Finanzausblick fuer das laufende Jahr nach unten an.",
    "raw_text": "Bayer hat heute mitgeteilt, dass der Finanzausblick fuer das laufende Jahr gesenkt wird. Das Management verweist auf anhaltenden Ergebnisdruck.",
    "language": "de",
    "matched_alias": "Bayer",
}


schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "news_classification",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "signal_type": {
                    "type": "string",
                    "enum": [
                        "negative_press",
                        "management_change",
                        "profit_warning",
                        "price_related_news",
                        "other",
                    ],
                },
                "sentiment": {
                    "type": "string",
                    "enum": ["negative", "neutral", "positive"],
                },
                "relevance": {
                    "type": "string",
                    "enum": ["high", "medium", "low"],
                },
                "triggers_alert_candidate": {
                    "type": "boolean"
                },
                "classification_reason": {
                    "type": "string"
                },
                "llm_summary": {
                    "type": "string"
                },
            },
            "required": [
                "signal_type",
                "sentiment",
                "relevance",
                "triggers_alert_candidate",
                "classification_reason",
                "llm_summary",
            ],
        },
    },
}

prompt = f"""
Du klassifizierst externe Unternehmensmeldungen fuer ein Fruehwarnsystem fuer Kreditanalysten.

Gib nur strukturierte Daten gemaess Schema zurueck.

Regeln:
- "profit_warning" bei Gewinnwarnung, Guidance-Senkung oder vergleichbarer Verschlechterung
- "management_change" bei relevantem Wechsel im Top-Management
- "negative_press" bei negativer Berichterstattung mit moeglicher Kreditrelevanz
- "price_related_news" nur wenn die Nachricht selbst direkt kursbezogen ist
- sonst "other"

Relevanz:
- high: direkt kreditrelevant oder deutliche finanzielle Verschlechterung
- medium: relevant, aber nicht unmittelbar kritisch
- low: dokumentationswuerdig, aber kein Warnsignal

Meldung:
Kunde-ID: {raw_news["customer_id"]}
Quelle: {raw_news["source_name"]}
Titel: {raw_news["headline"]}
Kurztext: {raw_news["summary"]}
Volltext: {raw_news["raw_text"]}
"""

response = openai_client.responses.create(
    model="gpt-4.1",
    input=prompt,
    text={"format": schema},
)

result = response.output[0].content[0].parsed

news_row = {
    "customer_id": raw_news["customer_id"],
    "source_name": raw_news["source_name"],
    "source_type": raw_news["source_type"],
    "source_url": raw_news["source_url"],
    "source_external_id": raw_news["source_external_id"],
    "published_at": raw_news["published_at"],
    "ingestion_date": raw_news["ingestion_date"],
    "headline": raw_news["headline"],
    "summary": raw_news["summary"],
    "raw_text": raw_news["raw_text"],
    "language": raw_news["language"],
    "matched_alias": raw_news["matched_alias"],
    "signal_type": result["signal_type"],
    "sentiment": result["sentiment"],
    "relevance": result["relevance"],
    "relevance_score": None,
    "is_duplicate": False,
    "dedupe_key": f'{raw_news["customer_id"]}_{raw_news["source_external_id"]}',
    "triggers_alert_candidate": result["triggers_alert_candidate"],
    "classification_reason": result["classification_reason"],
    "llm_summary": result["llm_summary"],
}

insert_result = supabase.table("news_events").insert(news_row).execute()

print("Klassifikation:")
print(result)
print("In Supabase geschrieben:")
print(insert_result.data)
