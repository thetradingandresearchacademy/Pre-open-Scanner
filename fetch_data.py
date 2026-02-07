import yfinance as yf
import pandas as pd
import requests
import io
import time
from datetime import datetime

def get_nifty500_symbols():
    """Fetch live Nifty 500 list from NSE."""
    print("⏳ Connecting to NSE...")
    try:
        url = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        s = requests.get(url, headers=headers, timeout=10).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        symbols = [x + ".NS" for x in df['Symbol'].tolist()]
        print(f"✅ FOUND: {len(symbols)} Stocks in Nifty 500.")
        return symbols
    except:
        # Emergency Fallback (Top 100) if NSE site is down
        print("⚠️ NSE Link Failed. Using Backup Nifty 100.")
        return [
            "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS", "ITC.NS", 
            "SBIN.NS", "BHARTIARTL.NS", "LICI.NS", "LT.NS", "HINDUNILVR.NS", "KOTAKBANK.NS",
            "AXISBANK.NS", "HCLTECH.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS", 
            "TITAN.NS", "BAJFINANCE.NS", "ULTRACEMCO.NS", "TATASTEEL.NS", "NTPC.NS", 
            "M&M.NS", "JSWSTEEL.NS", "ADANIENT.NS", "POWERGRID.NS", "ONGC.NS", "COALINDIA.NS"
            # (Add remaining top 100 manually if needed, but usually NSE link works)
        ]

def harvest_data():
    symbols = get_nifty500_symbols()
    all_data = []
    
    print(f"🚀 STARTING HARVEST: {len(symbols)} Stocks...")
    
    # Batch download to avoid blocking (25 stocks per batch)
    batch_size = 25
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        print(f"   📦 Batch {i//batch_size + 1}: {batch[0]} ... {batch[-1]}")
        
        try:
            # Download with threads
            data = yf.download(batch, period="1y", group_by='ticker', threads=True, progress=False)
            
            # Process
            for sym in batch:
                try:
                    if len(batch) == 1: df = data.copy()
                    else: df = data[sym].copy() if sym in data else pd.DataFrame()
                    
                    if df.empty: continue
                    
                    df = df.reset_index()
                    df['SYMBOL'] = sym.replace(".NS", "")
                    
                    # Fix Column Names
                    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
                    df = df.rename(columns={"Date":"TIMESTAMP", "Open":"OPEN", "High":"HIGH", "Low":"LOW", "Close":"CLOSE", "Volume":"TOTTRDQTY"})
                    
                    # Essential Columns Only
                    cols = ['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']
                    if all(c in df.columns for c in cols):
                        all_data.append(df[cols])
                        
                except: continue
            
            # PAUSE to be polite to Yahoo server
            time.sleep(2) 
            
        except Exception as e:
            print(f"   ⚠️ Batch Error: {e}")

    if all_data:
        final_df = pd.concat(all_data)
        # Force Numeric
        for c in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']:
            final_df[c] = pd.to_numeric(final_df[c], errors='coerce')
            
        final_df.to_csv("smart_db.csv", index=False)
        print(f"🎉 SUCCESS: Harvested {len(final_df)} rows. Saved to smart_db.csv")
    else:
        print("❌ FAILED: No data harvested.")
        exit(1)

if __name__ == "__main__":
    harvest_data()
