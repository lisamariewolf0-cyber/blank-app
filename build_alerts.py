import os
from collections import defaultdict
from datetime import date, datetime

from supabase import create_client


supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)


def relevance_rank(value: str) -> int:
    order = {"low": 1, "medium": 2, "high": 3}
    return order.get(str(value).lower(), 0)


def get_max_relevance(values):
    if not values:
        return None
    ranked = sorted(values, key=relevance_rank, reverse=True)
    return ranked[0]


def parse_date(value):
    if not value:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def build_alert_reason(
    has_profit_warning: bool,
    has_negative_press: bool,
    has_management_change: bool,
    abs_price_change: float | None,
    priority: str,
) -> str:
    reasons = []

    if has_profit_warning:
        reasons.append("Gewinnwarnung")
    if has_negative_press:
        reasons.append("negative Presse")
    if has_management_change:
        reasons.append("Managementwechsel")
    if abs_price_change is not None and abs_price_change >= 3:
        reasons.append(f"Kursbewegung von {abs_price_change:.2f} %")

    if not reasons:
        if priority == "high":
            return "Hohe Priorität aufgrund der Regelkombination"
        if priority == "medium":
            return "Mittlere Priorität aufgrund der Regelkombination"
        return "Alert durch Regelwerk erzeugt"

    if len(reasons) == 1:
        return reasons[0]

    return " + ".join(reasons)


def determine_alert(news_rows, price_row):
    signal_types = {str(row.get("signal_type", "")).lower() for row in news_rows}
    relevances = [str(row.get("relevance", "")).lower() for row in news_rows if row.get("relevance")]
    has_profit_warning = any(
        str(row.get("signal_type", "")).lower() == "profit_warning"
        and str(row.get("relevance", "")).lower() == "high"
        for row in news_rows
    )
    has_negative_press_high = any(
        str(row.get("signal_type", "")).lower() == "negative_press"
        and str(row.get("relevance", "")).lower() == "high"
        for row in news_rows
    )
    has_negative_press_medium = any(
        str(row.get("signal_type", "")).lower() == "negative_press"
        and str(row.get("relevance", "")).lower() == "medium"
        for row in news_rows
    )
    has_negative_press_any = any(
        str(row.get("signal_type", "")).lower() == "negative_press"
        for row in news_rows
    )
    has_management_change = any(
        str(row.get("signal_type", "")).lower() == "management_change"
        and str(row.get("relevance", "")).lower() in {"medium", "high"}
        for row in news_rows
    )

    pct_change = None
    abs_change = None
    if price_row and price_row.get("pct_change") is not None:
        pct_change = float(price_row["pct_change"])
        abs_change = abs(pct_change)

    has_strong_price_move = abs_change is not None and abs_change >= 3.0
    has_very_strong_price_move = abs_change is not None and abs_change >= 5.0
    has_price_plus_negative_news = (
        abs_change is not None and 3.0 <= abs_change < 5.0 and has_negative_press_any
    )

    priority = None

    if has_profit_warning or has_negative_press_high or has_very_strong_price_move or has_price_plus_negative_news:
        priority = "high"
    elif has_management_change or has_negative_press_medium or (abs_change is not None and 3.0 <= abs_change < 5.0):
        priority = "medium"

    if priority is None:
        return None

    if news_rows and has_strong_price_move:
        alert_type = "combined"
    elif news_rows:
        alert_type = "news_only"
    else:
        alert_type = "price_only"

    trigger_count = len(news_rows)
    if has_strong_price_move:
        trigger_count += 1

    max_relevance = get_max_relevance(relevances)
    reason = build_alert_reason(
        has_profit_warning=has_profit_warning,
        has_negative_press=has_negative_press_any,
        has_management_change=has_management_change,
        abs_price_change=abs_change,
        priority=priority,
    )

    return {
        "alert_priority": priority,
        "alert_status": "open",
        "alert_type": alert_type,
        "trigger_count": trigger_count,
        "has_negative_press": has_negative_press_any,
        "has_management_change": has_management_change,
        "has_profit_warning": has_profit_warning,
        "has_strong_price_move": has_strong_price_move,
        "max_relevance": max_relevance,
        "max_abs_price_change_pct": abs_change,
        "alert_reason": reason,
        "analyst_comment": None,
    }


def get_target_date():
    env_date = os.environ.get("ALERT_DATE")
    if env_date:
        return env_date

    news_resp = (
        supabase.table("news_events")
        .select("ingestion_date")
        .order("ingestion_date", desc=True)
        .limit(1)
        .execute()
    )
    if news_resp.data:
        return parse_date(news_resp.data[0]["ingestion_date"])

    prices_resp = (
        supabase.table("price_snapshots")
        .select("trading_date")
        .order("trading_date", desc=True)
        .limit(1)
        .execute()
    )
    if prices_resp.data:
        return parse_date(prices_resp.data[0]["trading_date"])

    raise RuntimeError("Kein Datum gefunden. news_events und price_snapshots sind leer.")


def main():
    target_date = get_target_date()
    print(f"Baue Alerts für Datum: {target_date}")

    news_resp = (
        supabase.table("news_events")
        .select(
            "id, customer_id, ingestion_date, signal_type, relevance, "
            "triggers_alert_candidate, headline"
        )
        .eq("ingestion_date", target_date)
        .execute()
    )

    prices_resp = (
        supabase.table("price_snapshots")
        .select("id, customer_id, trading_date, pct_change")
        .eq("trading_date", target_date)
        .execute()
    )

    existing_alerts_resp = (
        supabase.table("alerts")
        .select("id, customer_id, alert_date")
        .eq("alert_date", target_date)
        .execute()
    )

    news_rows = news_resp.data or []
    price_rows = prices_resp.data or []
    existing_alerts = existing_alerts_resp.data or []

    existing_customer_ids = {row["customer_id"] for row in existing_alerts}

    news_by_customer = defaultdict(list)
    for row in news_rows:
        if row.get("triggers_alert_candidate"):
            news_by_customer[row["customer_id"]].append(row)

    price_by_customer = {row["customer_id"]: row for row in price_rows}

    customer_ids = set(news_by_customer.keys()) | set(price_by_customer.keys())

    created_alerts = []
    skipped_existing = []

    for customer_id in sorted(customer_ids):
        if customer_id in existing_customer_ids:
            skipped_existing.append(customer_id)
            continue

        customer_news = news_by_customer.get(customer_id, [])
        customer_price = price_by_customer.get(customer_id)

        alert_payload = determine_alert(customer_news, customer_price)
        if not alert_payload:
            continue

        row = {
            "customer_id": customer_id,
            "alert_date": target_date,
            **alert_payload,
        }

        insert_resp = supabase.table("alerts").insert(row).execute()
        created_alerts.extend(insert_resp.data or [])

    print(f"Neue Alerts geschrieben: {len(created_alerts)}")
    if skipped_existing:
        print(f"Übersprungen, weil bereits vorhanden: {skipped_existing}")

    for row in created_alerts:
        print(row)


if __name__ == "__main__":
    main()
