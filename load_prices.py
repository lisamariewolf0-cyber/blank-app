import os 
import requests
from datetime import datetime 
 
from supabase import create_client 
 
supabase = create_client( 
  os.environ["SUPABASE_URL"], 
  os.environ["SUPABASE_KEY"], 
) 

API_KEY = os.environ["ALPHAVANTAGE_API_KEY"] 
BASE_URL = "https://www.alphavantage.co/query"

# Startmapping fuer den MVP. 
# Wenn ein Symbol leer zurueckkommt, passen wir es morgen gezielt an. 

CANDIDATE_SYMBOLS = {
    7: {"customer_name": "BMW", "symbols": ["BMW.DEX", "BMW3.FRK"]},
}
  
TARGET_DATE = os.environ.get("PRICE_DATE") 
# optional, z. B. 2026-04-15 

def compute_alert_level(pct_change): 
  if pct_change is None: 
    return False, "none" 
  
    abs_change = abs(pct_change) 
  
  if abs_change >= 5: 
    return True, "high" 
  if abs_change >= 3: 
    return True, "medium" 
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

def search_symbol(keyword: str):
    params = {
        "function": "SYMBOL_SEARCH",
        "keywords": keyword,
        "apikey": API_KEY,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "Note" in data:
        raise RuntimeError(f"Alpha Vantage Limit erreicht: {data['Note']}")

    matches = data.get("bestMatches", [])
    if not matches:
        raise RuntimeError(f"Keine Symbolsuche-Treffer fuer {keyword}")

    return matches

def resolve_symbol(company_name: str):
    matches = search_symbol(company_name)

    preferred_regions = ["XETRA", "Frankfurt"]

    for region in preferred_regions:
        for match in matches:
            match_region = str(match.get("4. region", "")).strip()
            symbol = str(match.get("1. symbol", "")).strip()
            name = str(match.get("2. name", "")).strip()

            if match_region == region and symbol:
                return symbol, name, match_region

    # Fallback: erster Treffer mit Symbol
    for match in matches:
        symbol = str(match.get("1. symbol", "")).strip()
        name = str(match.get("2. name", "")).strip()
        region = str(match.get("4. region", "")).strip()
        if symbol:
            return symbol, name, region

    raise RuntimeError(f"Kein verwendbares Symbol fuer {company_name}")
 
def fetch_daily_series(symbol: str): 
    params = { 
        "function": "TIME_SERIES_DAILY_ADJUSTED", 
        "symbol": symbol, 
        "apikey": API_KEY, 
        "outputsize": "compact", 
    } 
    
    response = requests.get(BASE_URL, params=params, timeout=30) 
    response.raise_for_status() 
    data = response.json() 
    
    if "Error Message" in data: 
        raise RuntimeError(f"Alpha Vantage Fehler fuer {symbol}: {data['Error Message']}") 
    
    if "Note" in data: 
        raise RuntimeError(f"Alpha Vantage Limit erreicht fuer {symbol}: {data['Note']}") 
        
    series = data.get("Time Series (Daily)")
    if not series: 
        raise RuntimeError(f"Keine Daily-Daten fuer {symbol}") 
        
    return series 

def get_latest_two_days(symbol: str):
    series = fetch_daily_series(symbol)
    dates = sorted(series.keys())

    if TARGET_DATE:
        dates = [d for d in dates if d <= TARGET_DATE]

    if len(dates) < 2:
        raise RuntimeError(f"Zu wenig Historie fuer {symbol}")

    prev_date = dates[-2]
    last_date = dates[-1]

    prev_row = series[prev_date]
    last_row = series[last_date]

    return prev_date, prev_row, last_date, last_row
    
def build_row(customer_id: int, symbol: str): 
    prev_date, prev_row, last_date, last_row = get_latest_two_days(symbol) 
    
    close_price = float(last_row["4. close"]) 
    prev_close_price = float(prev_row["4. close"]) 
    abs_change = close_price - prev_close_price 
    pct_change = round((abs_change / prev_close_price) * 100, 2) if prev_close_price else None 
    is_candidate, level = compute_alert_level(pct_change) 
    
    volume = int(last_row["6. volume"]) if last_row.get("6. volume") else None 
    
    return { 
        "customer_id": customer_id, 
        "trading_date": last_date, 
        "currency": "EUR", 
        "open_price": round(float(last_row["1. open"]), 2), 
        "high_price": round(float(last_row["2. high"]), 2), 
        "low_price": round(float(last_row["3. low"]), 2), 
        "close_price": round(close_price, 2), 
        "prev_close_price": round(prev_close_price, 2), 
        "abs_change": round(abs_change, 2), 
        "pct_change": pct_change, 
        "volume": volume, 
        "is_price_alert_candidate": is_candidate, 
        "price_alert_level": level, 
        "source_name": f"Alpha Vantage ({symbol})",
    } 

def build_row_from_candidates(customer_id: int, symbols: list[str]):
    errors = []

    for symbol in symbols:
        try:
            row = build_row(customer_id, symbol)
            return row, symbol
        except Exception as e:
            errors.append(f"{symbol}: {e}")

    raise RuntimeError(" | ".join(errors))

def main():
    inserted = 0
    skipped = 0
    failed = []

    for customer_id, info in CANDIDATE_SYMBOLS.items():
        try:
            row, used_symbol = build_row_from_candidates(customer_id, info["symbols"])

            if already_exists(customer_id, row["trading_date"]):
                skipped += 1
                continue

            supabase.table("price_snapshots").insert(row).execute()
            inserted += 1
            print(
                f"Eingefuegt: {info['customer_name']} | "
                f"{used_symbol} | {row['trading_date']} | {row['pct_change']} %"
            )

        except Exception as e:
            failed.append((customer_id, info["customer_name"], str(e)))

    print(f"\nNeu geschrieben: {inserted}")
    print(f"Uebersprungen: {skipped}")

    if failed:
        print("\nFehler / keine Daten:")
        for item in failed:
            print(item)


if __name__ == "__main__":
    main()
