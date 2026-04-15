import os
import json

from openai import OpenAI
from supabase import create_client
from sources.eqs_symrise import fetch_symrise_eqs_items

client = OpenAI(
    api_key=os.environ["GROQ_API_KEY"],
    base_url="https://api.groq.com/openai/v1",
)

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)

TARGET_DATE = os.environ.get("INGEST_DATE")

def classification_messages(item: dict):
    return [
        {
            "role": "system",
            "content": (
                "Du klassifizierst externe Unternehmensmeldungen für ein Frühwarnsystem "
                "für Kreditanalysten. Gib nur JSON zurück, exakt passend zum Schema."
            ),
        },
        {
            "role": "user",
            "content": f"""
Regeln:
- "profit_warning" bei Gewinnwarnung, Guidance-Senkung oder ähnlicher finanzieller Verschlechterung
- "management_change" bei relevantem Wechsel im Top-Management
- "negative_press" bei negativer Berichterstattung mit möglicher Kreditrelevanz
- "price_related_news" nur wenn die Nachricht selbst direkt kursbezogen ist
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
""",
        },
    ]


def classify_item(item: dict) -> dict:
    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=classification_messages(item),
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "news_classification",
                "strict": True,
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
                        "triggers_alert_candidate": {"type": "boolean"},
                        "classification_reason": {"type": "string"},
                        "llm_summary": {"type": "string"},
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
        },
    )

    return json.loads(response.choices[0].message.content)


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
        "customer_id": item["customer_id"],
        "source_name": item["source_name"],
        "source_type": item["source_type"],
        "source_url": item["source_url"],
        "source_external_id": item["source_external_id"],
        "published_at": item["published_at"],
        "ingestion_date": item["ingestion_date"],
        "headline": item["headline"],
        "summary": item["summary"],
        "raw_text": item["raw_text"],
        "language": item["language"],
        "matched_alias": item["matched_alias"],
        "signal_type": cls["signal_type"],
        "sentiment": cls["sentiment"],
        "relevance": cls["relevance"],
        "relevance_score": None,
        "is_duplicate": False,
        "dedupe_key": f'{item["customer_id"]}_{item["source_external_id"]}',
        "triggers_alert_candidate": cls["triggers_alert_candidate"],
        "classification_reason": cls["classification_reason"],
        "llm_summary": cls["llm_summary"],
    }
    return supabase.table("news_events").insert(row).execute()


def main():
    items = fetch_symrise_eqs_items(target_date=TARGET_DATE)
    print(f"Gefundene Symrise-EQS-Meldungen: {len(items)}")

    inserted = 0
    skipped = 0

    for item in items:
        if already_exists(item["source_external_id"], item["customer_id"]):
            skipped += 1
            continue

        classification = classify_item(item)
        save_item(item, classification)
        inserted += 1

        print("Neu klassifiziert:")
        print(item["headline"])
        print(classification)

    print(f"Neu geschrieben: {inserted}")
    print(f"Übersprungen: {skipped}")


if __name__ == "__main__":
    main()
print("Klassifikation:")
print(result)
print("In Supabase geschrieben:")
print(insert_result.data)
