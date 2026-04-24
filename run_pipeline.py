import os
from datetime import datetime, timezone

from supabase import create_client

from fetch_market_data import fetch_and_store_prices
from sources.google_news_fetch import fetch_all_customers_news
from classify_news import classify_item, save_item, already_exists
from build_alerts import main as build_alerts

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)

TARGET_DATE = os.environ.get("INGEST_DATE") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

def run():
    print("=" * 50)
    print(f"Pipeline gestartet - {TARGET_DATE}")
    print("=" * 50)

    print("\n>>> SCHRITT 1/4: Boersenkurse holen")
    fetch_and_store_prices(target_date=TARGET_DATE)

    print("\n>>> SCHRITT 2/4: News holen")
    news_items = fetch_all_customers_news(supabase, target_date=TARGET_DATE)
    print(f"Gesamt: {len(news_items)} Artikel gefunden")

    print("\n>>> SCHRITT 3/4: KI-Klassifikation")
    inserted = 0
    skipped  = 0
    errors   = 0

    for item in news_items:
        try:
            if already_exists(item["source_external_id"], item["customer_id"]):
                skipped += 1
                continue

            classification = classify_item(item)
            save_item(item, classification)

            signal    = classification["signal_type"]
            relevance = classification["relevance"]
            print(f"  OK  [{relevance}] [{signal}] {item['headline'][:80]}")
            inserted += 1

        except Exception as e:
            print(f"  ERR {item.get('headline', '?')[:60]}: {e}")
            errors += 1

    print(f"\nKlassifikation - Neu: {inserted} | Uebersprungen: {skipped} | Fehler: {errors}")

    print("\n>>> SCHRITT 4/4: Alerts generieren")
    try:
        build_alerts()
    except Exception as e:
        print(f"  ERR Alert-Generierung fehlgeschlagen: {e}")

    print("\n" + "=" * 50)
    print("Pipeline abgeschlossen")
    print(f"  Datum:       {TARGET_DATE}")
    print(f"  News neu:    {inserted}")
    print(f"  News skip:   {skipped}")
    print(f"  Fehler:      {errors}")
    print("=" * 50)

if __name__ == "__main__":
    run()
