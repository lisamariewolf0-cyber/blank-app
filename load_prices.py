import os 
from datetime import datetime 

import pandas as pd 
import yfinance as yf 
from supabase import create_client 

supabase = create_client( 
  os.environ["SUPABASE_URL"], 
  os.environ["SUPABASE_KEY"], 
) 

# Startmapping fuer den MVP. 
# Wenn ein Symbol leer zurueckkommt, passen wir es morgen gezielt an. 

TICKER_MAP = { 
  5: {"customer_name": "BASF", "ticker": "BAS.DE"}, 
  6: {"customer_name": "Bayer", "ticker": "BAYN.DE"}, 
  7: {"customer_name": "BMW", "ticker": "BMW.DE"}, 
  8: {"customer_name": "Volkswagen", "ticker": "VOW3.DE"}, 
  4: {"customer_name": "AMAG", "ticker": "AMAG.VI"}, 
  1: {"customer_name": "MTU", "ticker": "MTX.DE"}, 
  2: {"customer_name": "Symrise", "ticker": "SY1.DE"}, 
  3: {"customer_name": "Henkel", "ticker": "HEN3.DE"}, 
  9: {"customer_name": "TUI", "ticker": "TUI1.DE"}, 

  #10 Telefonica Deutschland bewusst raus: delisted / kein aktives Kursmonitoring 
} 

TARGET_DATE = os.environ.get("PRICE_DATE") 
# optional, z.B. 2026-04-15 

def compute_alert_level(pct_change: float | None): 
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

def fetch_latest_two_days(symbol: str) -> pd.DataFrame:
    # 10 Tage Puffer, damit Feiertage/Wochenenden kein Problem sind
    df = yf.download(symbol, period="10d", interval="1d", auto_adjust=False, progress=False)

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()

    # yfinance liefert je nach Version unterschiedliche Spaltennamen
    col_map = {}
    for col in df.columns:
        if str(col).lower() == "date":
            col_map[col] = "Date"
        elif str(col).lower() == "open":
            col_map[col] = "Open"
        elif str(col).lower() == "high":
            col_map[col] = "High"
        elif str(col).lower() == "low":
            col_map[col] = "Low"
        elif str(col).lower() == "close":
            col_map[col] = "Close"
        elif str(col).lower() == "volume":
            col_map[col] = "Volume"

    df = df.rename(columns=col_map)

    required = {"Date", "Open", "High", "Low", "Close"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame()

    df = df.sort_values("Date")

    if TARGET_DATE:
        df["trade_date_str"] = pd.to_datetime(df["Date"]).dt.date.astype(str)
        df = df[df["trade_date_str"] <= TARGET_DATE]

    if len(df) < 2:
        return pd.DataFrame()

    return df.tail(2).copy() 

def build_row(customer_id: int, symbol: str, df_two_days: pd.DataFrame): 
  prev_row = df_two_days.iloc[0] 
  last_row = df_two_days.iloc[1] 

  trading_date = pd.to_datetime(last_row["Date"]).date().isoformat() 
  close_price = float(last_row["Close"]) 
  prev_close_price = float(prev_row["Close"]) 
  abs_change = close_price - prev_close_price 
  pct_change = round((abs_change / prev_close_price) * 100, 2) if prev_close_price else None 
  is_candidate, level = compute_alert_level(pct_change) 

  volume = None 
  if "Volume" in df_two_days.columns and pd.notnull(last_row["Volume"]): 
    volume = int(last_row["Volume"]) 
    
  return { 
    "customer_id": customer_id, 
    "trading_date": trading_date, 
    "currency": "EUR", 
    "open_price": round(float(last_row["Open"]), 2), 
    "high_price": round(float(last_row["High"]), 2), 
    "low_price": round(float(last_row["Low"]), 2), 
    "close_price": round(close_price, 2), 
    "prev_close_price": round(prev_close_price, 2), 
    "abs_change": round(abs_change, 2), 
    "pct_change": pct_change, 
    "volume": volume, 
    "is_price_alert_candidate": is_candidate, 
    "price_alert_level": level, 
    "source_name": f"Yahoo Finance ({symbol})", 
  } 

def main(): 
  inserted = 0 
  skipped = 0 
  failed = [] 
  
  for customer_id, info in TICKER_MAP.items(): 
    symbol = info["ticker"] 
    
    try: 
      df_two_days = fetch_latest_two_days(symbol) 
      
      if df_two_days.empty: 
        failed.append((customer_id, info["customer_name"], symbol, "keine Daten")) 
        continue 
        
      row = build_row(customer_id, symbol, df_two_days) 
      
      if already_exists(customer_id, row["trading_date"]): 
        skipped += 1 
        continue 
        
      supabase.table("price_snapshots").insert(row).execute() 
      inserted += 1 
      print(f"Eingefuegt: {info['customer_name']} | {symbol} | {row['trading_date']} | {row['pct_change']} %") 
    
    except Exception as e: 
      failed.append((customer_id, info["customer_name"], symbol, str(e))) 
      
  print(f"\nNeu geschrieben: {inserted}") 
  print(f"Uebersprungen: {skipped}") 
  
  if failed: 
    print("\nFehler / keine Daten:") 
    for item in failed: 
      print(item) 
      
if __name__ == "__main__": 
  main()
