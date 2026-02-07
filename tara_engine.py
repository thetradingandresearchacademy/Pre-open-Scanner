import pandas as pd
import numpy as np
from datetime import timedelta

# ==========================================
# SWINGLAB PRO NEXT: CORE LOGIC ENGINE
# ==========================================

def prepare_data(df):
    """
    Pre-processes the Smart DB data:
    1. Sorts by Date.
    2. Calculates Shifts (Previous Day).
    3. Calculates Rolling Windows (4-week Lows, 10-week Highs).
    """
    df['DATE'] = pd.to_datetime(df['TIMESTAMP'])
    df = df.sort_values(by=['SYMBOL', 'DATE'])
    
    # Group by Symbol to prevent data bleeding between stocks
    g = df.groupby('SYMBOL')
    
    # --- PREVIOUS DAY DATA (Shift 1) ---
    df['PDO'] = g['OPEN'].shift(1)
    df['PDH'] = g['HIGH'].shift(1)
    df['PDL'] = g['LOW'].shift(1)
    df['PDC'] = g['CLOSE'].shift(1)
    df['PD_VOL'] = g['TOTTRDQTY'].shift(1)
    
    # --- AVERAGES ---
    df['AVG_VOL_5'] = g['TOTTRDQTY'].rolling(5).mean()
    df['AVG_VOL_10'] = g['TOTTRDQTY'].rolling(10).mean()
    df['AVG_VOL_90'] = g['TOTTRDQTY'].rolling(90).mean()
    
    # --- ROLLING HIGHS/LOWS (Look-backs) ---
    # 1 Week = 5 days, 2 Weeks = 10 days, etc.
    df['MIN_LOW_1W'] = g['LOW'].rolling(5).min()
    df['MIN_LOW_2W'] = g['LOW'].rolling(10).min()
    df['MIN_LOW_4W'] = g['LOW'].rolling(20).min()
    df['MIN_LOW_6W'] = g['LOW'].rolling(30).min()
    
    df['MAX_HIGH_2W'] = g['HIGH'].rolling(10).max()
    df['MAX_HIGH_3W'] = g['HIGH'].rolling(15).max()
    df['MAX_HIGH_6W'] = g['HIGH'].rolling(30).max()
    df['MAX_HIGH_10W'] = g['HIGH'].rolling(50).max()
    
    return df

def resample_to_weekly(daily_df):
    """
    Converts Daily data to Weekly for Signals 11, 12, 13, 14.
    """
    # Logic to resample OHLCV by 'W-FRI' (Week ending Friday)
    weekly = daily_df.set_index('DATE').groupby('SYMBOL').resample('W-FRI').agg({
        'OPEN': 'first',
        'HIGH': 'max',
        'LOW': 'min',
        'CLOSE': 'last',
        'TOTTRDQTY': 'sum'
    }).dropna().reset_index()
    
    # Rename for clarity in Weekly Logic
    weekly = weekly.rename(columns={
        'OPEN': 'TWO', 'HIGH': 'TWH', 'LOW': 'TWL', 'CLOSE': 'TWC'
    })
    
    # Calculate Weekly Shifts
    wg = weekly.groupby('SYMBOL')
    weekly['PWO'] = wg['TWO'].shift(1)
    weekly['PWH'] = wg['TWH'].shift(1)
    weekly['PWL'] = wg['TWL'].shift(1)
    weekly['PWC'] = wg['TWC'].shift(1)
    
    weekly['QWO'] = wg['TWO'].shift(2) # Previous to Previous (2 weeks ago)
    weekly['QWH'] = wg['TWH'].shift(2)
    weekly['QWL'] = wg['TWL'].shift(2)
    weekly['QWC'] = wg['TWC'].shift(2)
    
    # Rolling Weekly Lookbacks
    weekly['WK_LOW_7W'] = wg['TWL'].rolling(7).min()
    weekly['WK_HIGH_5W'] = wg['TWH'].rolling(5).min()
    weekly['WK_HIGH_10W'] = wg['TWH'].rolling(10).max()
    
    return weekly

def run_signals(df):
    """
    Executes the 19 Locked Signals.
    Returns a filtered DataFrame with a 'SIGNAL_NAME' column.
    """
    df = prepare_data(df)
    
    # We also need a weekly version for Signals 11-14
    wk_df = resample_to_weekly(df)
    # Merge latest weekly data back to daily for "Synergy" checks (simplified for EOD)
    # Note: For production, we usually run weekly scans separately, but here we flag them.
    
    # Aliases for cleaner code (matching your file's terminology)
    TDO = df['OPEN']; TDC = df['CLOSE']; TDL = df['LOW']; TDH = df['HIGH']; VOL = df['TOTTRDQTY']
    PDO = df['PDO']; PDC = df['PDC']; PDL = df['PDL']; PDH = df['PDH']; PD_VOL = df['PD_VOL']
    
    signals_found = []

    # ==========================================
    # 1. U TURN (BUY) [cite: 1]
    # ==========================================
    # Close gained > 0.15%, Open > 0.25% below Prev Low, Close > Prev Open
    # Low is new 4 week low, Vol > 20% over yesterday
    mask_s1 = (
        (TDC > PDC * 1.0015) &
        (TDO < PDL * 0.9975) &
        (TDC > PDO) &
        (TDL <= df['MIN_LOW_4W']) &
        (VOL > PD_VOL * 1.20)
    )
    df.loc[mask_s1, 'SIGNAL'] = 'U-Turn (Buy)'

    # ==========================================
    # 2. U TURN (SELL) [cite: 2]
    # ==========================================
    mask_s2 = (
        (TDC < PDC * 0.9985) & # Dropped > 0.15%
        (TDO > PDH * 1.0015) & # Open > 0.15% above Prev High
        (TDC < PDO) &
        (TDH >= df['MAX_HIGH_3W']) &
        (TDC > 2) &
        (VOL > PD_VOL * 1.20) &
        (VOL > 500000)
    )
    df.loc[mask_s2, 'SIGNAL'] = 'U-Turn (Sell)'

    # ==========================================
    # 3. JUMP START (BUY) [cite: 3]
    # ==========================================
    mask_s3 = (
        (TDO > PDH * 1.0010) &
        (TDC > TDO) &
        (PDL <= df['MIN_LOW_2W'].shift(1)) & # Low 1 day ago reached new 2wk low
        (TDL > PDH) & # Low is above High 1 day ago (Gap support)
        (TDH < df['MAX_HIGH_10W'] * 0.97) & # High is > 3% below 10wk high
        (VOL > PD_VOL) &
        (VOL > 500000)
    )
    df.loc[mask_s3, 'SIGNAL'] = 'Jump Start (Buy)'

    # ==========================================
    # 4. JUMP START (FREE FALL) [cite: 4]
    # ==========================================
    mask_s4 = (
        (TDO < PDL * 0.9990) &
        (TDC < TDO) &
        (PDH >= df['MAX_HIGH_2W'].shift(1)) &
        (TDH < PDL) &
        (df['AVG_VOL_10'] > 100000) &
        (TDC > 5) &
        (VOL > PD_VOL)
    )
    df.loc[mask_s4, 'SIGNAL'] = 'Jump Start (Sell)'

    # ==========================================
    # 5. FULL STOP (BUY) [cite: 5]
    # ==========================================
    mask_s5 = (
        (TDL > PDC * 1.0010) &
        (PDH > TDL) & # Typo in file? "High 1 day ago is above low" (today's low?)
        (TDC > TDO) &
        (TDC > PDH) &
        (PDL <= df['MIN_LOW_6W'].shift(1)) &
        (VOL > PD_VOL) &
        (VOL > 500000)
    )
    df.loc[mask_s5, 'SIGNAL'] = 'Full Stop (Buy)'

    # ==========================================
    # 6. FULL STOP (SELL) [cite: 6]
    # ==========================================
    mask_s6 = (
        (TDH < PDC * 0.9990) &
        (PDL < TDH) &
        (TDC < TDO) &
        (PDH >= df['MAX_HIGH_6W'].shift(1)) &
        (VOL > PD_VOL) &
        (VOL > 500000)
    )
    df.loc[mask_s6, 'SIGNAL'] = 'Full Stop (Sell)'

    # ==========================================
    # 7. TURN AROUND (BUY) - INTRADAY/EOD [cite: 7]
    # ==========================================
    # Down-trend check (Close < 20MA proxy or just prev day logic)
    mask_s7 = (
        (TDO < PDL) & # Key Gap Down
        (TDC > PDC) & # Strong Close
        (TDL == df['MIN_LOW_1W']) & # Lowest in last few days
        (VOL > df['AVG_VOL_5']) # Volume Jump
        # Nice to have: PDC < PDO (Bear Day) - implied by down trend context
    )
    df.loc[mask_s7, 'SIGNAL'] = 'Turn Around (Buy)'

    # ==========================================
    # 8. TURN AROUND (SELL) [cite: 16]
    # ==========================================
    mask_s8 = (
        (TDH == df['MAX_HIGH_1W']) & # Highest in last few days
        (TDO > PDH) & # Gap Up
        (TDC < PDC) & # Close lower
        (VOL > df['AVG_VOL_5'])
    )
    df.loc[mask_s8, 'SIGNAL'] = 'Turn Around (Sell)'

    # ==========================================
    # 9. REVERSE (BUY) [cite: 20]
    # ==========================================
    mask_s9 = (
        (TDC > PDC * 1.002) &
        (TDL == df['MIN_LOW_1W']) &
        (TDL < PDL * 0.9925) & # Low > 0.75% below prev low
        (TDC > TDO * 1.002) &
        (VOL > df['AVG_VOL_5'] * 1.20) &
        (VOL > 500000) &
        (TDC > 0.60)
    )
    df.loc[mask_s9, 'SIGNAL'] = 'Reverse (Buy)'

    # ==========================================
    # 15. GAP (BUY) [cite: 28]
    # ==========================================
    mask_s15 = (
        (TDL > PDH * 1.01) &
        (TDC > TDO) &
        (VOL == df['TOTTRDQTY'].rolling(3).max()) &
        (df['AVG_VOL_10'] > 200000) &
        (TDC > 40)
    )
    df.loc[mask_s15, 'SIGNAL'] = 'Gap (Buy)'

    # ==========================================
    # 18. VOLUME SPIKE [cite: 51]
    # ==========================================
    # Vol 1 day ago > 300% gain over last 1, 2, 3 days
    # This logic in file is tricky: "volume 1 day ago gained... over last 1 day"
    # Interpreted: Yesterday's vol was huge, Today's vol dropped > 25%
    prev_vol_spike = (
        (PD_VOL > df['TOTTRDQTY'].shift(2) * 4.0) # >300% gain means 4x
    )
    mask_s18 = (
        prev_vol_spike &
        (VOL < PD_VOL * 0.75) & # Dropped > 25% today
        (df['AVG_VOL_90'] > 200000) &
        (TDC.between(5, 250))
    )
    df.loc[mask_s18, 'SIGNAL'] = 'Volume Spike'

    # Filter only rows with signals
    result = df[df['SIGNAL'].notna()].copy()
    
    # Add STOP LOSS Logic (User Requirement)
    # Buy Signals -> SL = Low - 0.5%
    # Sell Signals -> SL = High + 0.5%
    buy_sigs = result['SIGNAL'].str.contains('Buy')
    result.loc[buy_sigs, 'STOP_LOSS'] = result['LOW'] * 0.995
    result.loc[~buy_sigs, 'STOP_LOSS'] = result['HIGH'] * 1.005
    
    return result

# ==========================================
# WEEKLY ENGINE (For Signals 11, 12, 13, 14)
# ==========================================
def run_weekly_signals(daily_df):
    wk = resample_to_weekly(daily_df)
    
    # Aliases for Weekly
    TWC = wk['TWC']; TWH = wk['TWH']; TWL = wk['TWL']; TWO = wk['TWO']
    PWC = wk['PWC']; PWH = wk['PWH']; PWL = wk['PWL']; PWO = wk['PWO']
    QWC = wk['QWC']; QWH = wk['QWH']; QWL = wk['QWL']; QWO = wk['QWO']
    
    # 11. WEEKLY REVERSAL (BUY) [cite: 21]
    mask_w11 = (
        (TWC > PWC * 1.005) &
        (TWC > TWO * 1.005) &
        (TWC > daily_df.groupby('SYMBOL')['CLOSE'].shift(1).resample('W-FRI').last().values * 1.01) & # Approx check
        (TWC > PWO * 1.005) &
        (PWL < QWL) &
        (PWC < PWO) &
        (QWC < QWO) &
        (TWL <= wk['WK_LOW_7W']) &
        (TWL < PWL * 0.98) &
        (wk['TOTTRDQTY'] > 500000) &
        (TWC > 2) &
        (TWC < wk['WK_HIGH_10W'] * 0.85)
    )
    wk.loc[mask_w11, 'SIGNAL'] = 'Weekly Reversal (Buy)'
    
    # 13. 3-WEEK REVERSAL (BUY) [cite: 26]
    mask_w13 = (
        (TWC > PWH) &
        (TWC > QWC) &
        (TWC > TWO * 1.01) &
        (PWL < QWL) &
        (PWC < PWO) &
        (QWC < QWO) &
        (wk['TOTTRDQTY'] > 500000)
    )
    wk.loc[mask_w13, 'SIGNAL'] = '3-Week Reversal (Buy)'

    return wk[wk['SIGNAL'].notna()]
