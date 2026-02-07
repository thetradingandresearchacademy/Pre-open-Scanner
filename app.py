import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

# Import your locked logic engine
try:
    from tara_engine import run_signals
except ImportError:
    st.error("CRITICAL: 'tara_engine.py' not found in repository. Please upload the Engine file.")
    st.stop()

# ==========================================
# CONFIGURATION & PAGE SETUP
# ==========================================
st.set_page_config(
    page_title="SwingLab Pro Next by TARA",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Replace with your GitHub details to fetch the self-harvested data
GITHUB_USER = "YourGitHubUsername" # <--- CHANGE THIS
GITHUB_REPO = "SwingLab-Pro-Next"  # <--- CHANGE THIS
DATA_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/main/smart_db.csv"

# ==========================================
# DATA LOADING (THE SMART HARVEST)
# ==========================================
@st.cache_data(ttl=3600) # Cache for 1 hour
def load_and_process_data():
    try:
        # Load the "Smart DB" harvested by GitHub Actions
        df = pd.read_csv(DATA_URL)
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        
        # Run the 19 Locked Signals
        processed_df = run_signals(df)
        return processed_df
    except Exception as e:
        return None

# ==========================================
# UI: SIDEBAR (FREEMIUM CONTROLLER)
# ==========================================
with st.sidebar:
    st.header("⚙️ TARA Control")
    user_tier = "FREE"
    
    # The Gatekeeper
    api_key = st.text_input("Enter Elite Key", type="password")
    if api_key == "TARA2026": # Simple hardcoded key for now
        user_tier = "ELITE"
        st.success("🔓 ELITE MODE ACTIVE")
    else:
        st.info("🔒 Free Tier Active")
    
    st.divider()
    
    # Filter Controls
    min_vol = st.slider("Min Volume", 100000, 1000000, 500000, step=50000)
    
    st.markdown("---")
    st.caption(f"Last Harvest: {datetime.now().strftime('%d-%b %H:%M')}")

# ==========================================
# MAIN DASHBOARD: HEADER & KPI
# ==========================================
st.title("SwingLab Pro Next `v2.0`")
st.markdown("### *Profit from Prices - Technical Market Scan*")

# Load Data
df = load_and_process_data()

if df is None:
    st.warning("⚠️ Waiting for first GitHub Harvest... Data will appear after 7:45 PM IST.")
    st.stop()

# Filter by user volume preference
df = df[df['TOTTRDQTY'] > min_vol]

# Top Metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Stocks Scanned", f"{len(df['SYMBOL'].unique())}")
col2.metric("Bullish Signals", f"{len(df[df['SIGNAL'].str.contains('Buy')])}")
col3.metric("Bearish Signals", f"{len(df[df['SIGNAL'].str.contains('Sell')])}")
col4.metric("User Tier", user_tier, delta="Active" if user_tier=="ELITE" else None)

# ==========================================
# MODULE 1: MONDAY MORNING & EXPIRY
# ==========================================
today = datetime.now()
is_monday = today.weekday() == 0
is_thursday = today.weekday() == 3

if is_monday:
    st.info("🔔 **Monday Morning Protocol:** Market is stabilizing. Wait 30 mins before entry. Watch MO vs FC.")

if is_thursday:
    st.warning("📅 **Expiry Day Alert:** Check Signal 19. Watch for High/Low breakout of the last 2 days.")

# ==========================================
# MODULE 2: SIGNAL FEED (THE ENGINE OUTPUT)
# ==========================================
tab_bull, tab_bear, tab_elite = st.tabs(["🐂 Bullish Setups", "🐻 Bearish Setups", "💎 Elite Synergy"])

def style_dataframe(data):
    return st.dataframe(
        data,
        use_container_width=True,
        column_config={
            "SYMBOL": "Ticker",
            "CLOSE": st.column_config.NumberColumn("Price", format="₹%.2f"),
            "STOP_LOSS": st.column_config.NumberColumn("Stop Loss (0.5%)", format="₹%.2f"),
            "SIGNAL": "Pattern Detected",
            "TOTTRDQTY": st.column_config.NumberColumn("Volume", format="%d")
        },
        hide_index=True
    )

with tab_bull:
    # Filter Bullish Signals
    bull_df = df[df['SIGNAL'].str.contains('Buy')][['SYMBOL', 'CLOSE', 'STOP_LOSS', 'SIGNAL', 'TOTTRDQTY']]
    if not bull_df.empty:
        style_dataframe(bull_df)
    else:
        st.write("No Bullish Signals found today.")

with tab_bear:
    # Filter Bearish Signals
    bear_df = df[df['SIGNAL'].str.contains('Sell')][['SYMBOL', 'CLOSE', 'STOP_LOSS', 'SIGNAL', 'TOTTRDQTY']]
    if not bear_df.empty:
        style_dataframe(bear_df)
    else:
        st.write("No Bearish Signals found today.")

with tab_elite:
    if user_tier == "FREE":
        st.error("🔒 Upgrade to ELITE to view 'Synergy Signals' (U-Turn + 3-Week Reversal).")
        st.image("https://placehold.co/600x200?text=Locked+Content", use_column_width=True)
    else:
        # Show specific high-value signals (Signal 13 + Signal 1)
        elite_mask = df['SIGNAL'].isin(['3-Week Reversal (Buy)', 'Weekly Reversal (Buy)'])
        elite_df = df[elite_mask][['SYMBOL', 'CLOSE', 'STOP_LOSS', 'SIGNAL', 'TOTTRDQTY']]
        
        st.success("💎 **High Conviction Setups Detected**")
        if not elite_df.empty:
            style_dataframe(elite_df)
        else:
            st.info("No Weekly Reversals detected. Market is consolidating.")

# ==========================================
# BRANDED SEBI SAFE FOOTER
# ==========================================
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; font-size: 12px;'>
    <b>SwingLab Pro Next by TARA</b> <br>
    System Status: ✅ LIVE | Source: GitHub Smart DB (EOD) <br>
    <i>Disclaimer: This tool generates algorithmic matches based on the 'Profit from Prices' logic. 
    It is for educational research only. TARA is not a SEBI registered investment advisor. 
    <b>Always adhere to the 0.5% Stop-Loss rule.</b></i>
    </div>
    """,
    unsafe_allow_html=True
)
