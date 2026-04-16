# fetch_market_data.py
import os
from datetime import datetime, timezone
import yfinance as yf
from supabase import create_client

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
)

TARGET_DATE = os.environ.get("INGEST_DATE") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Schwellenwerte für price_alert_level
ALERT_THRESHOLDS = {
    "critical": 5.0,   # >= 5% Bewegung → critical
    "warning":  2.0,   # >= 2% Bewegung → warning
    # sonst          → none
}

def get_alert_level(pct_change: float) -> tuple[bool, str]:
    abs_pct = abs(pct_change)
    if abs_pct >= ALERT_THRESHOLDS["critical"]:
        return True, "critical"
    elif abs_pct >= ALERT_THRESHOLDS["warning"]:
        return True, "warning"
    return False, "none"

def already_exists(customer_id: int, trading_date: str) -> bool:
    resp = (
        supabase.table("price_snapshots")
        .select("id")
        .eq("customer_id", customer_id)
        .eq("trading_date", trading_date)
        .limit(1)
        .execute()
    )
    return len(resp.data or []) > 0

def fetch_and_store_prices(target_date: str = TARGET_DATE):
    # Alle Kunden aus Supabase laden
    customers = supabase.table("customers").select("id, name, ticker").execute().data
    print(f"Verarbeite {len(customers)} Kunden für {target_date} ...")

    inserted = 0
    skipped = 0
    errors = 0

    for customer in customers:
        customer_id = customer["id"]
        ticker      = customer["ticker"]
        name        = customer["name"]

        if already_exists(customer_id, target_date):
            print(f"  SKIP {name} ({ticker}) – bereits vorhanden")
            skipped += 1
            continue

        try:
            stock = yf.Ticker(ticker)
            # 5 Handelstage holen → letzter = heute, vorletzter = prev_close
            hist = stock.history(period="5d")

            if hist.empty or len(hist) < 1:
                print(f"  WARN {name} ({ticker}) – keine Daten von yfinance")
                errors += 1
                continue

            latest = hist.iloc[-1]
            prev   = hist.iloc[-2] if len(hist) >= 2 else None

            close_price      = round(float(latest["Close"]), 4)
            open_price       = round(float(latest["Open"]), 4)
            high_price       = round(float(latest["High"]), 4)
            low_price        = round(float(latest["Low"]), 4)
            volume           = int(latest["Volume"]) if latest["Volume"] else None
            prev_close_price = round(float(prev["Close"]), 4) if prev is not None else None

            if prev_close_price:
                abs_change = round(close_price - prev_close_price, 4)
                pct_change = round((abs_change / prev_close_price) * 100, 4)
            else:
                abs_change = None
                pct_change = None

            is_alert, alert_level = get_alert_level(pct_change) if pct_change is not None else (False, "none")

            row = {
                "customer_id":            customer_id,
                "trading_date":           target_date,
                "currency":               "EUR",
                "open_price":             open_price,
                "high_price":             high_price,
                "low_price":              low_price,
                "close_price":            close_price,
                "prev_close_price":       prev_close_price,
                "abs_change":             abs_change,
                "pct_change":             pct_change,
                "volume":                 volume,
                "is_price_alert_candidate": is_alert,
                "price_alert_level":      alert_level,
                "source_name":            "yahoo_finance",
            }

            supabase.table("price_snapshots").insert(row).execute()

            arrow = "▲" if (pct_change or 0) >= 0 else "▼"
            print(f"  OK  {name} ({ticker}): {close_price} EUR {arrow} {pct_change:+.2f}%  [{alert_level}]")
            inserted += 1

        except Exception as e:
            print(f"  ERR {name} ({ticker}): {e}")
            errors += 1

    print(f"\nFertig – Neu: {inserted} | Übersprungen: {skipped} | Fehler: {errors}")

if __name__ == "__main__":
    fetch_and_store_prices()
