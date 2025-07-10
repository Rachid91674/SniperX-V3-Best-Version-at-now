import asyncio
import websockets
import json
import aiohttp
import csv
import os
from pathlib import Path
import requests
import subprocess
import sys
from collections import deque, OrderedDict
import time
from datetime import datetime
import logging

# --- Configuration ---
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_CSV_FILE = os.environ.get(
    "TOKEN_RISK_ANALYSIS_CSV",
    str(SCRIPT_DIR / "token_risk_analysis.csv"),
)
SOL_PRICE_UPDATE_INTERVAL_SECONDS = 15
TRADE_LOGIC_INTERVAL_SECONDS = 0.5
CSV_CHECK_INTERVAL_SECONDS = 10

MIN_LIQUIDITY_USD = 10000.0
PRICE_IMPACT_THRESHOLD_MONITOR = 80.0

# Trading Parameters
TRAILING_STOP_THRESHOLD_PERCENT = 0.80
TRAILING_STOP_ACTIVATION_PERCENT = 1.01
TAKE_PROFIT_THRESHOLD_PERCENT = 1.15
STOP_LOSS_THRESHOLD_PERCENT = 0.80
STAGNATION_PRICE_THRESHOLD_PERCENT = 0.80
STAGNATION_TIMEOUT_SECONDS = 180
NO_BUY_SIGNAL_TIMEOUT_SECONDS = 180

# Volume-based buy signal configuration
MIN_VOLUME_RATIO = 1.2
MIN_TX_RATIO = 1.1
MIN_BUY_VOLUME_USD = 500
MIN_BUY_TXS = 10
MIN_SELL_VOLUME_USD = 100
BUY_SIGNAL_PRICE_INCREASE_PERCENT = 1.01

# --- Global State Variables ---
g_last_known_sol_price = 155.0
g_latest_trade_data = deque(maxlen=100)
g_last_dex_price_fetch_time = 0.0
g_current_mint_address = None
g_token_name = None
g_baseline_price_usd = None
g_buy_price_usd = None
g_highest_price_usd = None
g_token_monitor_start_time = 0
g_trade_status = 'monitoring'

# --- Helper Functions ---
def log_trade_result(token_name, mint_address, reason, buy_price=None, sell_price=None):
    log_file = os.path.join(SCRIPT_DIR, 'trades.csv')
    file_exists = os.path.isfile(log_file)
    gain_loss_pct = None
    if buy_price is not None and sell_price is not None and buy_price > 0:
        gain_loss_pct = ((sell_price - buy_price) / buy_price) * 100.0
    
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        fieldnames = ['timestamp', 'token_name', 'mint_address', 'reason', 'buy_price', 'sell_price', 'gain_loss_pct', 'result']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists: writer.writeheader()
        
        trade_result = ''
        if gain_loss_pct is not None: trade_result = 'PROFIT' if gain_loss_pct > 0 else 'LOSS'
            
        writer.writerow({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'token_name': token_name,
            'mint_address': mint_address, 'reason': reason,
            'buy_price': f"{buy_price:.9f}" if buy_price is not None else '',
            'sell_price': f"{sell_price:.9f}" if sell_price is not None else '',
            'gain_loss_pct': f"{gain_loss_pct:.2f}%" if gain_loss_pct is not None else '',
            'result': trade_result
        })
    pnl_val = gain_loss_pct or 0
    print(f"\n📊 Trade Result: {'PROFIT' if pnl_val > 0 else 'LOSS' if pnl_val < 0 else 'EVENT'} | Gain/Loss: {pnl_val:+.2f}% | Reason: {reason}\n")

def remove_token_from_csv(token_address, csv_file_path):
    if not os.path.exists(csv_file_path): return False
    
    rows_map = OrderedDict()
    fieldnames = []
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            for row in reader:
                if 'Address' in row:
                    rows_map[row['Address']] = row
    except Exception as e:
        print(f"Error reading {csv_file_path} for removal: {e}")
        return False

    if token_address in rows_map:
        del rows_map[token_address]
        try:
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(list(rows_map.values()))
            print(f"✅ Removed token {token_address} from {os.path.basename(csv_file_path)}")
            return True
        except Exception as e:
            print(f"Error writing back to {csv_file_path} after removal: {e}")
            return False
    return False

def load_token_from_csv(csv_file_path):
    if not os.path.exists(csv_file_path): return None, None
    print(f"\n=== Checking for Low Risk tokens in {os.path.basename(csv_file_path)} ===")
    try:
        with open(csv_file_path, mode='r', newline='', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                token_name = row.get('Name', '').strip() or row.get('Address', 'Unknown')
                risk_status = row.get('Overall_Risk_Status', '').strip()
                
                print(f"--- Checking token: {token_name} | Risk Status: '{risk_status}' ---")
                
                if risk_status == "Low Risk":
                    liquidity_str = row.get('DexScreener_Liquidity_USD', row.get('Liquidity(1m)', '0')).replace(',', '').strip()
                    price_impact_str = row.get('Price_Impact_Cluster_Sell_Percent', '100').strip()
                    try:
                        liquidity_val = float(liquidity_str)
                        price_impact_val = float(price_impact_str)
                    except (ValueError, TypeError):
                        print(f"❌ Invalid liquidity/price impact data for {token_name}. Skipping.")
                        continue
                    if liquidity_val >= MIN_LIQUIDITY_USD and price_impact_val < PRICE_IMPACT_THRESHOLD_MONITOR:
                        print(f"✅ Found valid Low Risk token: {token_name}")
                        return row.get('Address'), token_name
                    else:
                        print(f"❌ Low Risk token {token_name} failed final sanity check.")
                        if liquidity_val < MIN_LIQUIDITY_USD: print(f"   Liquidity: ${liquidity_val:,.2f} (Required >= ${MIN_LIQUIDITY_USD})")
                        if price_impact_val >= PRICE_IMPACT_THRESHOLD_MONITOR: print(f"   Price Impact: {price_impact_val:.2f}% (Required < {PRICE_IMPACT_THRESHOLD_MONITOR}%)")
                else:
                    print(f"ℹ️ Skipping token {token_name} (risk status is '{risk_status}')")
            print("No 'Low Risk' tokens found that meet monitoring criteria.")
            return None, None
    except Exception as e:
        print(f"Error reading token CSV: {e}")
        return None, None

def reset_token_specific_state():
    global g_latest_trade_data, g_baseline_price_usd, g_buy_price_usd, \
           g_token_monitor_start_time, g_highest_price_usd, g_trade_status
    g_latest_trade_data.clear()
    g_baseline_price_usd, g_buy_price_usd, g_highest_price_usd = None, None, None
    g_token_monitor_start_time = time.time()
    g_trade_status = 'monitoring'
    print(f"Token-specific state reset. New monitoring started at {time.strftime('%Y-%m-%d %H:%M:%S')}.")

async def periodic_sol_price_updater():
    global g_last_known_sol_price
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = data.get("solana", {}).get("usd")
                        if price and price != g_last_known_sol_price:
                            print(f"\n🔄 SOL/USD updated: {price:.2f} USD (was {g_last_known_sol_price:.2f} USD)")
                            g_last_known_sol_price = price
                await asyncio.sleep(SOL_PRICE_UPDATE_INTERVAL_SECONDS)
            except Exception as e:
                print(f"Error updating SOL price: {e}")
                await asyncio.sleep(SOL_PRICE_UPDATE_INTERVAL_SECONDS)

def get_dexscreener_data(token_address: str):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        pairs = data.get("pairs", [])
        if not pairs: return None
        pairs.sort(key=lambda p: p.get("liquidity", {}).get("usd", 0), reverse=True)
        for pair_data in pairs:
            try:
                timeframe = "h1" if pair_data.get("txns", {}).get("h1") else "m5" if pair_data.get("txns", {}).get("m5") else None
                if not timeframe or not pair_data.get("priceUsd"): continue
                
                txns, volume = pair_data["txns"][timeframe], pair_data["volume"]
                buy_txns, sell_txns = int(txns.get("buys", 0)), int(txns.get("sells", 0))
                total_vol, total_txns = float(volume.get(timeframe, 0)), buy_txns + sell_txns
                buy_vol = (total_vol * (buy_txns / total_txns)) if total_txns > 0 else 0
                return {"price": float(pair_data["priceUsd"]), "buy_volume": buy_vol, "sell_volume": total_vol - buy_vol, "buys": buy_txns, "sells": sell_txns, "timeframe": timeframe}
            except (KeyError, ValueError, TypeError): continue
    except Exception as e:
        print(f"Error getting dexscreener data: {e}")
    return None

async def listen_for_trades(mint_address, token_name):
    uri = "wss://pumpportal.fun/api/data"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint_address]}))
                print(f"📡 Subscribed to trades for: {token_name}")
                async for message in websocket:
                    data = json.loads(message)
                    if data.get("mint") == mint_address:
                        g_latest_trade_data.append(data)
        except asyncio.CancelledError:
            print(f"WebSocket listener for {token_name} cancelled.")
            raise
        except Exception as e:
            print(f"WebSocket Error for {token_name}: {e}. Reconnecting...")
            await asyncio.sleep(5)

async def trade_logic_and_price_display_loop(mint_address, token_name):
    global g_baseline_price_usd, g_buy_price_usd, g_highest_price_usd, \
           g_token_monitor_start_time, g_trade_status, g_last_dex_price_fetch_time

    stagnation_timer_start = None
    
    while True:
        try: # --- FIX: Added try/except block to make the loop resilient ---
            await asyncio.sleep(TRADE_LOGIC_INTERVAL_SECONDS)
            current_time = time.time()
            
            usd_price_per_token = None
            dex_data = None
            
            if g_latest_trade_data:
                trade = g_latest_trade_data[-1]
                if trade.get("tokenAmount", 0) > 0:
                    usd_price_per_token = (trade["solAmount"] / trade["tokenAmount"]) * g_last_known_sol_price
            
            if (current_time - g_last_dex_price_fetch_time) > 1.0:
                loop = asyncio.get_running_loop()
                dex_data = await loop.run_in_executor(None, get_dexscreener_data, mint_address)
                if dex_data and dex_data.get("price") and usd_price_per_token is None:
                    usd_price_per_token = dex_data["price"]
                g_last_dex_price_fetch_time = current_time

            if usd_price_per_token is None:
                print(f"Waiting for first trade or Dexscreener data for {token_name}...", end='\r')
                continue

            if g_baseline_price_usd is None:
                g_baseline_price_usd = usd_price_per_token
                print(f"\n💰 Initial Baseline Price for {token_name}: ${g_baseline_price_usd:.9f}")

            pnl_pct_str = f"PnL: {(((usd_price_per_token / g_buy_price_usd) - 1) * 100):+.2f}%" if g_buy_price_usd else "PnL: N/A"
            
            # Clear previous output with spaces and return to start of line
            print('\r' + ' ' * 200, end='\r')
            
            # Print current status
            print(f"🔍 {token_name} | ${usd_price_per_token:.9f} | {pnl_pct_str} | Status: {g_trade_status.upper()}")
            
            if dex_data:
                # Calculate condition statuses
                price_cond = usd_price_per_token > g_baseline_price_usd * BUY_SIGNAL_PRICE_INCREASE_PERCENT
                vol_cond = dex_data.get('buy_volume', 0) >= MIN_BUY_VOLUME_USD
                tx_cond = dex_data.get('buys', 0) >= MIN_BUY_TXS
                
                vol_ratio = dex_data['buy_volume'] / max(dex_data['sell_volume'], MIN_SELL_VOLUME_USD) if dex_data['sell_volume'] > 0 else float('inf')
                tx_ratio = dex_data['buys'] / max(dex_data['sells'], 1)
                
                vol_ratio_cond = vol_ratio >= MIN_VOLUME_RATIO
                tx_ratio_cond = tx_ratio >= MIN_TX_RATIO
                
                # Print conditions
                print("\n📊 Trade Conditions Status:")
                print(f"  • Price Increase: {'✅' if price_cond else '❌'} (Current: {usd_price_per_token:.9f} | Needed: {g_baseline_price_usd * BUY_SIGNAL_PRICE_INCREASE_PERCENT:.9f})")
                print(f"  • Buy Volume: {'✅' if vol_cond else '❌'} (Current: ${dex_data.get('buy_volume', 0):.2f} | Needed: ${MIN_BUY_VOLUME_USD:.2f})")
                print(f"  • Buy TXs: {'✅' if tx_cond else '❌'} (Current: {dex_data.get('buys', 0)} | Needed: {MIN_BUY_TXS})")
                print(f"  • Volume Ratio: {'✅' if vol_ratio_cond else '❌'} (Current: {vol_ratio:.2f}x | Needed: {MIN_VOLUME_RATIO:.1f}x)")
                print(f"  • TX Ratio: {'✅' if tx_ratio_cond else '❌'} (Current: {tx_ratio:.2f}x | Needed: {MIN_TX_RATIO:.1f}x)")
                
                # Print additional market data
                print("\n📈 Market Data:")
                print(f"  • 5m Volume: ${dex_data.get('volume_5m', 0):.2f} | 1h Volume: ${dex_data.get('volume_1h', 0):.2f}")
                print(f"  • Liquidity: ${dex_data.get('liquidity', 0):.2f}")
                print(f"  • Price Impact: {dex_data.get('price_impact', 0):.2f}%")
                
                # Print time until timeout
                time_elapsed = current_time - g_token_monitor_start_time
                time_remaining = max(0, NO_BUY_SIGNAL_TIMEOUT_SECONDS - time_elapsed)
                print(f"\n⏱️  Time Remaining: {int(time_remaining // 60)}m {int(time_remaining % 60)}s")
                
                # Move cursor up to overwrite on next update
                print("\033[12A", end='')

            if g_trade_status == 'monitoring':
                if (current_time - g_token_monitor_start_time) > NO_BUY_SIGNAL_TIMEOUT_SECONDS:
                    print(f"\n⏳ Timeout: No buy signal for {token_name}. Ending monitoring.")
                    log_trade_result(token_name, mint_address, "No buy signal timeout", sell_price=usd_price_per_token)
                    return # Exit the task

                price_cond = usd_price_per_token > g_baseline_price_usd * BUY_SIGNAL_PRICE_INCREASE_PERCENT
                if price_cond and dex_data:
                    vol_ratio = dex_data['buy_volume'] / max(dex_data['sell_volume'], MIN_SELL_VOLUME_USD) if dex_data['sell_volume'] > 0 else float('inf')
                    tx_ratio = dex_data['buys'] / max(dex_data['sells'], 1)
                    
                    if all([dex_data.get('buy_volume', 0) >= MIN_BUY_VOLUME_USD, dex_data.get('buys', 0) >= MIN_BUY_TXS, vol_ratio >= MIN_VOLUME_RATIO, tx_ratio >= MIN_TX_RATIO]):
                        g_buy_price_usd = usd_price_per_token
                        g_highest_price_usd = g_buy_price_usd
                        g_trade_status = 'bought'
                        print(f"\n🚨 BUY SIGNAL DETECTED for {token_name} at ${g_buy_price_usd:.9f}")

            elif g_trade_status == 'bought' and g_buy_price_usd is not None:
                if usd_price_per_token > g_highest_price_usd: g_highest_price_usd = usd_price_per_token
                
                if usd_price_per_token >= g_buy_price_usd * TAKE_PROFIT_THRESHOLD_PERCENT:
                    print(f"\n✅ TAKE PROFIT for {token_name} at ${usd_price_per_token:.9f}")
                    log_trade_result(token_name, mint_address, "Take profit", g_buy_price_usd, usd_price_per_token)
                    return
                if usd_price_per_token <= g_buy_price_usd * STOP_LOSS_THRESHOLD_PERCENT:
                    print(f"\n🛑 STOP LOSS for {token_name} at ${usd_price_per_token:.9f}")
                    log_trade_result(token_name, mint_address, "Stop loss", g_buy_price_usd, usd_price_per_token)
                    return
                if g_highest_price_usd > g_buy_price_usd * TRAILING_STOP_ACTIVATION_PERCENT and usd_price_per_token <= g_highest_price_usd * TRAILING_STOP_THRESHOLD_PERCENT:
                    print(f"\n🛑 TRAILING STOP for {token_name} at ${usd_price_per_token:.9f} (Peak: ${g_highest_price_usd:.9f})")
                    log_trade_result(token_name, mint_address, "Trailing stop", g_buy_price_usd, usd_price_per_token)
                    return
                if usd_price_per_token <= g_baseline_price_usd * STAGNATION_PRICE_THRESHOLD_PERCENT:
                    if stagnation_timer_start is None: stagnation_timer_start = current_time
                    elif (current_time - stagnation_timer_start) > STAGNATION_TIMEOUT_SECONDS:
                        print(f"\n⏳ STAGNATION TIMEOUT for {token_name}")
                        log_trade_result(token_name, mint_address, "Stagnation timeout", g_buy_price_usd, usd_price_per_token)
                        return
                else:
                    stagnation_timer_start = None
        except Exception as e:
            print(f"\nError in trade logic loop for {token_name}: {e}. Continuing...")
            await asyncio.sleep(1) # Brief pause after an error

async def main():
    global g_current_mint_address, g_token_name
    sol_price_task = asyncio.create_task(periodic_sol_price_updater())
    
    try:
        while True:
            # This is the main control loop. It will only ever have one token active at a time.
            mint_address, token_name = load_token_from_csv(INPUT_CSV_FILE)
            
            if not mint_address:
                await asyncio.sleep(CSV_CHECK_INTERVAL_SECONDS)
                continue

            g_current_mint_address = mint_address
            g_token_name = token_name
            print(f"\n🚀 Starting monitoring for: {g_token_name} ({g_current_mint_address})")
            reset_token_specific_state()
            
            listener_task = asyncio.create_task(listen_for_trades(g_current_mint_address, g_token_name))
            trader_task = asyncio.create_task(trade_logic_and_price_display_loop(g_current_mint_address, g_token_name))
            
            # --- FIX: New, simpler control flow ---
            # Wait for the trader task to finish. It is the master task for a token's lifecycle.
            await trader_task
            
            # Once the trader task is done, the token's lifecycle is over. Clean up.
            print(f"Token lifecycle for {g_token_name} complete. Cleaning up...")
            listener_task.cancel() # Cancel the websocket listener
            await asyncio.gather(listener_task, return_exceptions=True)

            remove_token_from_csv(g_current_mint_address, INPUT_CSV_FILE)
            
            g_current_mint_address, g_token_name = None, None
            print("\n" + "="*50)
            print("Cycle complete. Ready for next token.")
            print("="*50 + "\n")
            await asyncio.sleep(2) # Small delay before checking for the next token

    except KeyboardInterrupt:
        print("\n📉 Monitoring stopped by user.")
    finally:
        print("Shutting down all tasks...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Critical error in main execution: {e}")