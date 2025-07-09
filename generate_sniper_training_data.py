import pandas as pd
from datetime import timedelta

# Load token risk analysis
try:
    risk_df = pd.read_csv('token_risk_analysis.csv')
    if risk_df.empty:
        raise ValueError
except Exception:
    risk_df = pd.read_csv('token_risk_analysis.csv.bak')

# Load trades with explicit columns
trade_cols = [
    'timestamp','token_name','mint_address','reason',
    'buy_price','sell_price','percent','result'
]
trades = pd.read_csv('trades.csv', names=trade_cols, header=0)
trades['timestamp'] = pd.to_datetime(trades['timestamp'])

# Load sniperx results if available
try:
    sniper_df = pd.read_csv('sniperx_results_1m.csv')
    sniper_df.rename(columns={'Address': 'mint_address'}, inplace=True)
    sniper_df['timestamp'] = pd.to_datetime(sniper_df.get('timestamp'))
    if sniper_df.empty:
        sniper_df = None
except Exception:
    sniper_df = None

# Rename address column for merge
risk_df.rename(columns={'Address': 'mint_address'}, inplace=True)

# Calculate cumulative buy and sell counts per token
trades['buy_flag'] = trades['buy_price'].notna() & (trades['buy_price'] != '')
trades['sell_flag'] = trades['sell_price'].notna() & (trades['sell_price'] != '')
trades.sort_values('timestamp', inplace=True)
trades['buys'] = trades.groupby('mint_address')['buy_flag'].cumsum()
trades['sells'] = trades.groupby('mint_address')['sell_flag'].cumsum()

# Merge risk data into trades
merged = trades.merge(risk_df, on='mint_address', how='left')

# Merge sniper results if present
if sniper_df is not None:
    merged = merged.merge(sniper_df, on=['mint_address','timestamp'], how='left', suffixes=('', '_sniper'))

# Convert percent column to numeric
merged['percent_value'] = pd.to_numeric(merged['percent'].str.rstrip('%'), errors='coerce')

# Determine dump detection
merged['dump_detected'] = False
for mint, group in merged.groupby('mint_address'):
    group = group.sort_values('timestamp')
    pct = group['percent_value']
    times = group['timestamp']
    for i in range(1, len(group)):
        if pd.notna(pct.iloc[i]) and pct.iloc[i] <= -10:
            if (times.iloc[i] - times.iloc[i-1]) <= timedelta(minutes=3):
                merged.loc[group.index[i], 'dump_detected'] = True

# Holders and price impact from risk data
merged['holders'] = pd.to_numeric(merged.get('Cluster_Token_Amount_Est'), errors='coerce')
merged['price_impact'] = pd.to_numeric(merged.get('Price_Impact_Cluster_Sell_Percent'), errors='coerce')

# Save final dataset
merged.to_csv('sniper_training_data.csv', index=False)
print('Saved sniper_training_data.csv with', len(merged), 'rows')
