import os
import hashlib
from datetime import datetime, timedelta, timezone
from newsapi import NewsApiClient

newsapi = NewsApiClient(api_key=os.environ["NEWSAPI_KEY"])

def fetch_news_for_customer(customer: dict, target_date: str = None) -> list[dict]:
    query = customer["name"]

    try:
        response = newsapi.get_everything(
            q=query,
            language="de",
            sort_by="publishedAt",
            page_size=10,
        )
    except Exception as e:
        print(f"NewsAPI Fehler für {customer['name']}: {e}")
        return []

    items = []
    today = target_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")

    for article in response.get("articles", []):
        url = article.get("url", "")
        external_id = hashlib.md5(url.encode()).hexdigest()

        published_raw = article.get("publishedAt", "")
        article_date = published_raw[:10]
        if article_date < cutoff:
            continue

        headline    = article.get("title") or ""
        description = article.get("description") or ""
        content     = article.get("content") or description

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
    response = supabase_client.table("customers").select("id, name, ticker, isin").execute()
    customers = response.data

    all_items = []
    for customer in customers:
        items = fetch_news_for_customer(customer, target_date)
        print(f"{customer['name']}: {len(items)} neue Artikel heute")
        all_items.extend(items)

    return all_items
