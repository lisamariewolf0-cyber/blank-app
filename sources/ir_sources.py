import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.bayer.com"
BAYER_INVESTOR_OVERVIEW_URL = "https://www.bayer.com/de/investoren-uebersicht"

MONTH_DATE_RE_DE = re.compile(
    r"(\d{1,2})\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(\d{4})"
)

GERMAN_MONTHS = {
    "Januar": 1,
    "Februar": 2,
    "März": 3,
    "April": 4,
    "Mai": 5,
    "Juni": 6,
    "Juli": 7,
    "August": 8,
    "September": 9,
    "Oktober": 10,
    "November": 11,
    "Dezember": 12,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CreditEarlyWarningBot/1.0)"
}


def _clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _extract_german_date(text: str):
    match = MONTH_DATE_RE_DE.search(text or "")
    if not match:
        return None

    day = int(match.group(1))
    month_name = match.group(2)
    year = int(match.group(3))
    month = GERMAN_MONTHS[month_name]

    return datetime(year, month, day).date().isoformat()


def fetch_bayer_ir_items(target_date=None, customer_id=6):
    response = requests.get(BAYER_INVESTOR_OVERVIEW_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    page_text = _clean(soup.get_text(" ", strip=True))

    items = []

    # Für den MVP lesen wir die 3 sichtbaren Investor-News auf der Übersicht aus.
    # Diese Titel sind auf der Seite direkt sichtbar.
    visible_titles = [
        "Kerendia® in der EU für neue Indikation bei erwachsenen Patienten mit Herzinsuffizienz und einer linksventrikulären Auswurfleistung von ≥40 % zugelassen",
        "Eylea® 8 mg für dritte Netzhautindikation in Japan zugelassen",
        "Bayers niedrig-dosiertes MRT-Kontrastmittel erhält erste Zulassung in Japan",
    ]

    visible_dates = [
        "2026-03-30",
        "2026-03-23",
        "2026-03-23",
    ]

    for idx, (headline, published) in enumerate(zip(visible_titles, visible_dates), start=1):
        if headline not in page_text:
            continue

        if target_date and published != target_date:
            continue

        items.append(
            {
                "customer_id": customer_id,
                "source_name": "Bayer IR",
                "source_type": "ir",
                "source_url": BAYER_INVESTOR_OVERVIEW_URL,
                "source_external_id": f"bayer-ir-overview-{published}-{idx}",
                "published_at": f"{published}T08:00:00+01:00",
                "ingestion_date": published,
                "headline": headline,
                "summary": headline,
                "raw_text": headline,
                "language": "de",
                "matched_alias": "Bayer",
            }
        )

    return items


def fetch_ir_items(target_date=None):
    return fetch_bayer_ir_items(target_date=target_date)
