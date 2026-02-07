import yfinance as yf
import pandas as pd
import datetime
import time

# --- CONFIGURATION ---
# We hardcode the Top 20 NSE Stocks to ensure it ALWAYS has something to download
SYMBOLS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "KOTAKBANK.NS", "LICI.NS",
    "LT.NS", "AXISBANK.NS", "HCLTECH.NS", "ASIANPAINT.NS", "MARUTI.NS",
    "SUNPHARMA.NS", "TITAN.NS", "BAJFINANCE.NS", "ULTRACEMCO.NS", "TATASTEEL.NS"
]

def harvest_data():
    print(f"🚀 TARA HARVEST STARTED: Scanning {len(SYMBOLS)} Stocks...")
    
    all_data = []
    
    for symbol in SYMBOLS:
        try:
            print(f"   Downloading: {symbol}...")
            # Fetch 1 year of data
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="1y")
            
            if df.empty:
                print(f"   ⚠️ No data for {symbol}")
                continue
            
            # Reset index to make Date a column
            df = df.reset_index()
            
            # CLEANUP: Rename columns to match TARA Engine (UPPERCASE)
            # yfinance gives: Date, Open, High, Low, Close, Volume
            # We need: TIMESTAMP, OPEN, HIGH, LOW, CLOSE, TOTTRDQTY
            df = df.rename(columns={
                "Date": "TIMESTAMP",
                "Open": "OPEN",
                "High": "HIGH",
                "Low": "LOW",
                "Close": "CLOSE",
                "Volume": "TOTTRDQTY"
            })
            
            # Add Symbol Column
            df['SYMBOL'] = symbol.replace(".NS", "") # Remove .NS for cleaner UI
            
            # Keep only relevant columns
            cols = ['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']
            df = df[cols]
            
            all_data.append(df)
            
        except Exception as e:
            print(f"   ❌ Error fetching {symbol}: {e}")
            
    if all_data:
        # Combine all stocks into one big table
        final_df = pd.concat(all_data)
        
        # Save to CSV
        final_df.to_csv("smart_db.csv", index=False)
        print(f"✅ SUCCESS: Saved {len(final_df)} rows to 'smart_db.csv'")
    else:
        print("❌ CRITICAL FAILURE: No data downloaded.")
        exit(1) # Force the workflow to crash so we see the red X

if __name__ == "__main__":
    harvest_data()
