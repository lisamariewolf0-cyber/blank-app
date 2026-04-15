import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.bayer.com"
INVESTOR_NEWS_URL = "https://www.bayer.com/en/investors/investor-news"

MONTH_DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CreditEarlyWarningBot/1.0)"
}


def _clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _extract_date_from_text(text: str):
    match = MONTH_DATE_RE.search(text or "")
    if not match:
        return None

    try:
        dt = datetime.strptime(match.group(0), "%B %d, %Y")
        return dt.date().isoformat()
    except ValueError:
        return None


def fetch_bayer_ir_items(target_date=None, customer_id=6):
    response = requests.get(INVESTOR_NEWS_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    items = []
    seen_urls = set()

    # Investor-News-Detailseiten laufen typischerweise über /media/en-us/...
    for a in soup.select('a[href*="/media/en-us/"]'):
        title = _clean(a.get_text(" ", strip=True))

        if not title or title.upper() == "READ MORE" or len(title) < 20:
            continue

        url = urljoin(BASE_URL, a.get("href", ""))
        if not url or url in seen_urls:
            continue

        # Wir suchen das Datum im umgebenden Card-/Container-Text
        container = a
        container_text = ""
        for _ in range(5):
            container = container.parent
            if container is None:
                break
            container_text = _clean(container.get_text(" ", strip=True))
            published = _extract_date_from_text(container_text)
            if published:
                break
        else:
            published = None

        if not published:
            continue

        if target_date and published != target_date:
            continue

        source_external_id = url.rstrip("/").split("/")[-1]

        items.append(
            {
                "customer_id": customer_id,
                "source_name": "Bayer IR",
                "source_type": "ir",
                "source_url": url,
                "source_external_id": f"bayer-ir-{source_external_id}",
                "published_at": f"{published}T08:00:00+01:00",
                "ingestion_date": published,
                "headline": title,
                "summary": title,
                "raw_text": title,
                "language": "en",
                "matched_alias": "Bayer",
            }
        )

        seen_urls.add(url)

    return items


def fetch_ir_items(target_date=None):
    return fetch_bayer_ir_items(target_date=target_date)
