import os
import hashlib
from datetime import datetime, timezone
from newsapi import NewsApiClient

newsapi = NewsApiClient(api_key=os.environ["NEWSAPI_KEY"])

def fetch_news_for_customer(customer: dict, target_date: str = None) -> list[dict]:
    """
    Holt aktuelle News für einen Kunden.
    customer = {"id": 1, "name": "Symrise AG", "ticker": "SY1.DE", "isin": "..."}
    Gibt eine Liste von items zurück, die exakt dem Format deiner Pipeline entsprechen.
    """
    # Suchanfrage: Firmenname (präziser als Ticker für News)
    query = customer["name"]
    
    try:
        response = newsapi.get_everything(
            q=query,
            language="de",          # deutsche Artikel zuerst
            sort_by="publishedAt",
            page_size=10,
        )
    except Exception as e:
        print(f"NewsAPI Fehler für {customer['name']}: {e}")
        return []

    items = []
    today = target_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for article in response.get("articles", []):
        # Eindeutige ID aus URL erzeugen (wie deine source_external_id)
        url = article.get("url", "")
        external_id = hashlib.md5(url.encode()).hexdigest()

        # Nur Artikel von heute (oder target_date)
        published_raw = article.get("publishedAt", "")
        from datetime import datetime, timezone, timedelta

        cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
        article_date = published_raw[:10]  # nur "YYYY-MM-DD" nehmen
        if article_date < cutoff:
           continue

        headline = article.get("title") or ""
        description = article.get("description") or ""
        content = article.get("content") or description

        items.append({
            "customer_id":        customer["id"],
            "source_name":        article.get("source", {}).get("name", "NewsAPI"),
            "source_type":        "news_api",
            "source_url":         url,
            "source_external_id": external_id,
            "published_at":       published_raw,
            "ingestion_date":     today,
            "headline":           headline,
            "summary":            description,
            "raw_text":           content,
            "language":           "de",
            "matched_alias":      customer["name"],
        })

    return items


def fetch_all_customers_news(supabase_client, target_date: str = None) -> list[dict]:
    """
    Lädt alle Kunden aus Supabase und holt News für jeden.
    """
    response = supabase_client.table("customers").select("id, name, ticker, isin").execute()
    customers = response.data

    all_items = []
    for customer in customers:
        items = fetch_news_for_customer(customer, target_date)
        print(f"{customer['name']}: {len(items)} neue Artikel heute")
        all_items.extend(items)

    return all_items
