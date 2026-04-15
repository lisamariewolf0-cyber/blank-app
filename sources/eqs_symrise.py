import re
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CreditEarlyWarningBot/1.0)"
}

SYMRISE_EQS_URLS = [
    "https://live.deutsche-boerse.com/nachrichten/EQS-Adhoc-Symrise-AG-beschliesst-Aktienrueckkaufprogramm-in-Hoehe-von-EUR400-Mio-deutsch-b1affe30-5aba-4978-aea1-e4955bb4e149",
    "https://live.deutsche-boerse.com/nachrichten/EQS-News-Symrise-begibt-Anleihe-in-Hoehe-von-800-Mio-EUR-deutsch-78cfa810-e2a4-408d-9ef0-56f8f6dcb685",
    "https://live.deutsche-boerse.com/news/EQS-News-Kapitalmarkttag-von-Symrise-Unleashing-the-full-beauty-of-ONE-Symrise-das-Motto-des-Strategie-Updates-deutsch-07076e11-5260-472b-9312-45d340b35cf5",
]

DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")


def _clean(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _to_iso_date(text: str):
    match = DATE_RE.search(text or "")
    if not match:
        return None

    day, month, year = match.groups()
    return datetime(int(year), int(month), int(day)).date().isoformat()


def _extract_headline(lines):
    for line in lines:
        if line.startswith("EQS-News:") or line.startswith("EQS-Adhoc:"):
            return line
    return lines[0] if lines else None


def _extract_date(lines):
    for line in lines[:30]:
        iso = _to_iso_date(line)
        if iso:
            return iso
    return None


def _extract_body(lines, headline):
    if not lines:
        return ""

    start_idx = 0
    if headline and headline in lines:
        start_idx = lines.index(headline) + 1

    body_lines = []
    stop_markers = [
        "Originalinhalt anzeigen:",
        "Sprache:",
        "Unternehmen:",
        "Ende der Mitteilung EQS News-Service",
        "EQS News ID:",
    ]

    for line in lines[start_idx:]:
        if any(marker in line for marker in stop_markers):
            break
        if len(line) < 2:
            continue
        body_lines.append(line)
        if len(body_lines) >= 40:
            break

    return " ".join(body_lines).strip()


def fetch_symrise_eqs_items(target_date=None, customer_id=2):
    items = []

    for url in SYMRISE_EQS_URLS:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        raw_lines = [_clean(x) for x in soup.get_text("\n", strip=True).splitlines()]
        lines = [x for x in raw_lines if x]

        headline = _extract_headline(lines)
        published = _extract_date(lines)
        body = _extract_body(lines, headline)

        if not headline or not published:
            continue

        if target_date and published != target_date:
            continue

        source_external_id = urlparse(url).path.rstrip("/").split("/")[-1]

        items.append(
            {
                "customer_id": customer_id,
                "source_name": "EQS / Deutsche Börse",
                "source_type": "eqs",
                "source_url": url,
                "source_external_id": f"symrise-eqs-{source_external_id}",
                "published_at": f"{published}T08:00:00+01:00",
                "ingestion_date": published,
                "headline": headline,
                "summary": body[:300] if body else headline,
                "raw_text": body if body else headline,
                "language": "de",
                "matched_alias": "Symrise",
            }
        )

    return items
