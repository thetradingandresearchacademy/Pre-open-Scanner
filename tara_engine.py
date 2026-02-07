import pandas as pd
import numpy as np

def prepare_data(df):
    """
    Pre-processes the Smart DB data.
    CRITICAL FIX: Resets index after sorting to prevent alignment errors.
    """
    # Standardize Date
    df['DATE'] = pd.to_datetime(df['TIMESTAMP'], utc=True)
    
    # Sort and RESET INDEX to ensure row numbers match 0,1,2,3...
    df = df.sort_values(by=['SYMBOL', 'DATE']).reset_index(drop=True)
    
    # Group by Symbol
    g = df.groupby('SYMBOL')
    
    # --- SHIFTS (Previous Day) ---
    df['PDO'] = g['OPEN'].shift(1)
    df['PDH'] = g['HIGH'].shift(1)
    df['PDL'] = g['LOW'].shift(1)
    df['PDC'] = g['CLOSE'].shift(1)
    df['PD_VOL'] = g['TOTTRDQTY'].shift(1)
    
    # --- AVERAGES & ROLLING ---
    df['AVG_VOL_5'] = g['TOTTRDQTY'].rolling(5).mean()
    df['AVG_VOL_10'] = g['TOTTRDQTY'].rolling(10).mean()
    df['AVG_VOL_90'] = g['TOTTRDQTY'].rolling(90).mean()
    
    df['MIN_LOW_1W'] = g['LOW'].rolling(5).min()
    df['MIN_LOW_2W'] = g['LOW'].rolling(10).min()
    df['MIN_LOW_4W'] = g['LOW'].rolling(20).min()
    df['MIN_LOW_6W'] = g['LOW'].rolling(30).min()
    
    df['MAX_HIGH_1W'] = g['HIGH'].rolling(5).max()
    df['MAX_HIGH_2W'] = g['HIGH'].rolling(10).max()
    df['MAX_HIGH_3W'] = g['HIGH'].rolling(15).max()
    df['MAX_HIGH_6W'] = g['HIGH'].rolling(30).max()
    df['MAX_HIGH_10W'] = g['HIGH'].rolling(50).max()
    
    return df

def resample_to_weekly(daily_df):
    """
    Converts Daily data to Weekly for Signals 11-14.
    """
    # Ensure DATE is index for resampling
    temp = daily_df.set_index('DATE').copy()
    
    weekly = temp.groupby('SYMBOL').resample('W-FRI').agg({
        'OPEN': 'first',
        'HIGH': 'max',
        'LOW': 'min',
        'CLOSE': 'last',
        'TOTTRDQTY': 'sum'
    }).dropna().reset_index()
    
    # Renaming for Weekly Logic
    weekly = weekly.rename(columns={'OPEN': 'TWO', 'HIGH': 'TWH', 'LOW': 'TWL', 'CLOSE': 'TWC'})
    
    wg = weekly.groupby('SYMBOL')
    weekly['PWO'] = wg['TWO'].shift(1)
    weekly['PWH'] = wg['TWH'].shift(1)
    weekly['PWL'] = wg['TWL'].shift(1)
    weekly['PWC'] = wg['TWC'].shift(1)
    
    weekly['QWO'] = wg['TWO'].shift(2)
    weekly['QWH'] = wg['TWH'].shift(2)
    weekly['QWL'] = wg['TWL'].shift(2)
    weekly['QWC'] = wg['TWC'].shift(2)
    
    weekly['WK_LOW_7W'] = wg['TWL'].rolling(7).min()
    weekly['WK_HIGH_5W'] = wg['TWH'].rolling(5).min()
    weekly['WK_HIGH_10W'] = wg['TWH'].rolling(10).max()
    
    return weekly

def run_signals(df):
    """
    Executes the 19 Locked Signals.
    """
    # 1. Prepare Daily Data
    df = prepare_data(df)
    
    # 2. Aliases for Signals
    TDO = df['OPEN']; TDC = df['CLOSE']; TDL = df['LOW']; TDH = df['HIGH']; VOL = df['TOTTRDQTY']
    PDO = df['PDO']; PDC = df['PDC']; PDL = df['PDL']; PDH = df['PDH']; PD_VOL = df['PD_VOL']
    
    # 3. Initialize Signal Column
    df['SIGNAL'] = None 
    
    # --- SIGNALS LOGIC ---
    
    # 1. U TURN (BUY)
    mask_s1 = (
        (TDC > PDC * 1.0015) & (TDO < PDL * 0.9975) & (TDC > PDO) &
        (TDL <= df['MIN_LOW_4W']) & (VOL > PD_VOL * 1.20)
    )
    df.loc[mask_s1, 'SIGNAL'] = 'U-Turn (Buy)'

    # 2. U TURN (SELL)
    mask_s2 = (
        (TDC < PDC * 0.9985) & (TDO > PDH * 1.0015) & (TDC < PDO) &
        (TDH >= df['MAX_HIGH_3W']) & (TDC > 2) & (VOL > PD_VOL * 1.20) & (VOL > 500000)
    )
    df.loc[mask_s2, 'SIGNAL'] = 'U-Turn (Sell)'

    # 3. JUMP START (BUY)
    mask_s3 = (
        (TDO > PDH * 1.0010) & (TDC > TDO) & (PDL <= df['MIN_LOW_2W'].shift(1)) &
        (TDL > PDH) & (TDH < df['MAX_HIGH_10W'] * 0.97) & (VOL > PD_VOL) & (VOL > 500000)
    )
    df.loc[mask_s3, 'SIGNAL'] = 'Jump Start (Buy)'

    # 4. JUMP START (SELL)
    mask_s4 = (
        (TDO < PDL * 0.9990) & (TDC < TDO) & (PDH >= df['MAX_HIGH_2W'].shift(1)) &
        (TDH < PDL) & (df['AVG_VOL_10'] > 100000) & (TDC > 5) & (VOL > PD_VOL)
    )
    df.loc[mask_s4, 'SIGNAL'] = 'Jump Start (Sell)'

    # 5. FULL STOP (BUY)
    mask_s5 = (
        (TDL > PDC * 1.0010) & (PDH > TDL) & (TDC > TDO) & (TDC > PDH) &
        (PDL <= df['MIN_LOW_6W'].shift(1)) & (VOL > PD_VOL) & (VOL > 500000)
    )
    df.loc[mask_s5, 'SIGNAL'] = 'Full Stop (Buy)'

    # 6. FULL STOP (SELL)
    mask_s6 = (
        (TDH < PDC * 0.9990) & (PDL < TDH) & (TDC < TDO) &
        (PDH >= df['MAX_HIGH_6W'].shift(1)) & (VOL > PD_VOL) & (VOL > 500000)
    )
    df.loc[mask_s6, 'SIGNAL'] = 'Full Stop (Sell)'

    # 7. TURN AROUND (BUY)
    mask_s7 = (
        (TDO < PDL) & (TDC > PDC) & (TDL == df['MIN_LOW_1W']) & (VOL > df['AVG_VOL_5'])
    )
    df.loc[mask_s7, 'SIGNAL'] = 'Turn Around (Buy)'

    # 8. TURN AROUND (SELL)
    mask_s8 = (
        (TDH == df['MAX_HIGH_1W']) & (TDO > PDH) & (TDC < PDC) & (VOL > df['AVG_VOL_5'])
    )
    df.loc[mask_s8, 'SIGNAL'] = 'Turn Around (Sell)'

    # 9. REVERSE (BUY)
    mask_s9 = (
        (TDC > PDC * 1.002) & (TDL == df['MIN_LOW_1W']) & (TDL < PDL * 0.9925) &
        (TDC > TDO * 1.002) & (VOL > df['AVG_VOL_5'] * 1.20) & (VOL > 500000)
    )
    df.loc[mask_s9, 'SIGNAL'] = 'Reverse (Buy)'

    # 15. GAP (BUY)
    mask_s15 = (
        (TDL > PDH * 1.01) & (TDC > TDO) & (VOL == df['TOTTRDQTY'].rolling(3).max()) &
        (df['AVG_VOL_10'] > 200000) & (TDC > 40)
    )
    df.loc[mask_s15, 'SIGNAL'] = 'Gap (Buy)'

    # 18. VOLUME SPIKE
    prev_vol_spike = (PD_VOL > df['TOTTRDQTY'].shift(2) * 4.0)
    mask_s18 = (
        prev_vol_spike & (VOL < PD_VOL * 0.75) & (df['AVG_VOL_90'] > 200000) & (TDC.between(5, 250))
    )
    df.loc[mask_s18, 'SIGNAL'] = 'Volume Spike'

    # Filter Result
    result = df[df['SIGNAL'].notna()].copy()
    
    # STOP LOSS CALCULATION
    if not result.empty:
        buy_sigs = result['SIGNAL'].str.contains('Buy')
        result.loc[buy_sigs, 'STOP_LOSS'] = result['LOW'] * 0.995
        result.loc[~buy_sigs, 'STOP_LOSS'] = result['HIGH'] * 1.005
    
    # --- WEEKLY MERGE ---
    # We run weekly logic separately and append high-value signals
    wk_df = resample_to_weekly(df)
    
    # 11. WEEKLY REVERSAL
    mask_w11 = (
        (wk_df['TWC'] > wk_df['PWC'] * 1.005) & 
        (wk_df['TWL'] <= wk_df['WK_LOW_7W']) & 
        (wk_df['TOTTRDQTY'] > 500000)
    )
    wk_df.loc[mask_w11, 'SIGNAL'] = 'Weekly Reversal (Buy)'
    
    # 13. 3-WEEK REVERSAL
    mask_w13 = (
        (wk_df['TWC'] > wk_df['PWH']) & (wk_df['TWC'] > wk_df['QWH'])
    )
    wk_df.loc[mask_w13, 'SIGNAL'] = '3-Week Reversal (Buy)'
    
    # Append Weekly signals to Daily results (keeping schema simple)
    wk_signals = wk_df[wk_df['SIGNAL'].notna()].copy()
    if not wk_signals.empty:
        # Standardize columns for display
        wk_signals = wk_signals.rename(columns={'TWC': 'CLOSE', 'TOTTRDQTY': 'TOTTRDQTY'})
        wk_signals['STOP_LOSS'] = wk_signals['TWL'] * 0.99
        wk_signals = wk_signals[['SYMBOL', 'CLOSE', 'STOP_LOSS', 'SIGNAL', 'TOTTRDQTY']]
        
        # Merge
        final_cols = ['SYMBOL', 'CLOSE', 'STOP_LOSS', 'SIGNAL', 'TOTTRDQTY']
        result = pd.concat([result[final_cols], wk_signals[final_cols]])
        
    return result
