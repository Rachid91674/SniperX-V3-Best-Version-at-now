import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "sniperx_tracker.db"

conn = sqlite3.connect(DB_PATH)

stages = pd.read_sql_query("SELECT * FROM tokens_stage_log", conn)
risks = pd.read_sql_query("SELECT * FROM risk_analysis", conn)
trades = pd.read_sql_query("SELECT * FROM trades", conn)

if trades.empty:
    print("No trades logged yet.")
    exit()

merged = trades.merge(risks, left_on="token_address", right_on="address", how="left")

wins = merged[merged["pnl_percent"] > 0]
losses = merged[merged["pnl_percent"] <= 0]

metrics = ["liquidity_usd", "volume_usd", "price_impact_percent", "cluster_dominance_percent", "dump_risk_ratio"]

print("Average metrics for winning trades:")
print(wins[metrics].mean())
print("\nAverage metrics for losing trades:")
print(losses[metrics].mean())

suggestions = []
if not wins.empty and not losses.empty:
    for m in metrics:
        win_avg = wins[m].mean()
        lose_avg = losses[m].mean()
        if pd.notna(win_avg) and pd.notna(lose_avg) and lose_avg > win_avg:
            if m == "price_impact_percent":
                suggestions.append("reduce price impact threshold")
            if m == "dump_risk_ratio":
                suggestions.append("tighten LP/cluster ratio")

if suggestions:
    print("\nSuggestions:")
    for s in set(suggestions):
        print("-", s)
else:
    print("\nNo suggestions - insufficient data.")
