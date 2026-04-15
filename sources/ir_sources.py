import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.bayer.com"
BAYER_MEDIA_URL = "https://www.bayer.com/media/en-us/?h=1&t=Investor+News"

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
    response = requests.get(BAYER_MEDIA_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    items = []
    seen_urls = set()

    for a in soup.select('a[href*="/media/en-us/"]'):
        href = a.get("href", "")
        url = urljoin(BASE_URL, href)
        title = _clean(a.get_text(" ", strip=True))

        if not url or url in seen_urls:
            continue

        if not title or title.upper() == "READ MORE" or len(title) < 15:
            continue

        container = a
        published = None

        for _ in range(6):
            container = container.parent
            if container is None:
                break
            container_text = _clean(container.get_text(" ", strip=True))
            published = _extract_date_from_text(container_text)
            if published:
                break

        if not published:
            continue

        if target_date and published != target_date:
            continue

        slug = url.rstrip("/").split("/")[-1]

        items.append(
            {
                "customer_id": customer_id,
                "source_name": "Bayer IR",
                "source_type": "ir",
                "source_url": url,
                "source_external_id": f"bayer-ir-{slug}",
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
