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
from collections import deque
import time
from datetime import datetime

# --- Configuration ---
# Path to the CSV file used for monitoring. The location can be overridden with
# the `TOKEN_RISK_ANALYSIS_CSV` environment variable. When not set the
# `token_risk_analysis.csv` file located in the same directory as this script is
# used. This keeps the script portable across platforms.
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_CSV_FILE = os.environ.get(
    "TOKEN_RISK_ANALYSIS_CSV",
    str(SCRIPT_DIR / "token_risk_analysis.csv"),
)
SOL_PRICE_UPDATE_INTERVAL_SECONDS = 30
TRADE_LOGIC_INTERVAL_SECONDS = 1
CSV_CHECK_INTERVAL_SECONDS = 10  # How often to check the CSV for changes
DEXSCREENER_PRICE_UPDATE_INTERVAL_SECONDS = 1  # How often to fetch price from Dexscreener when no trades
# STATUS_PRINT_INTERVAL_SECONDS removed (status now prints every loop iteration)

# --- Global State Variables ---
g_last_known_sol_price = 155.0
g_latest_trade_data = deque(maxlen=100)
# Timestamp (epoch seconds) of the last Dexscreener price request
g_last_dex_price_fetch_time = 0.0

g_current_mint_address = None
g_token_name = None
g_baseline_price_usd = None
g_trade_status = 'monitoring'  # Initial status
g_buy_price_usd = None

g_current_tasks = []  # Holds tasks like listener, trader, csv_checker for cancellation
g_processing_token = False  # Tracks if we're currently processing a token

# Custom exception for signaling restart
class RestartRequired(Exception):
    """Custom exception to signal a required restart of monitoring tasks."""
    pass

def log_trade_result(token_name, mint_address, reason, buy_price=None, sell_price=None):
    """
    Log trade results to a CSV file.
    
    Args:
        token_name (str): Name of the token
        mint_address (str): Token mint address
        reason (str): Reason for trade completion
        buy_price (float, optional): Buy price of the token
        sell_price (float, optional): Sell price of the token
    """
    log_file = 'trades.csv'
    file_exists = os.path.isfile(log_file)
    
    # Calculate gain/loss percentage if both buy and sell prices are available
    gain_loss_pct = None
    if buy_price is not None and sell_price is not None and buy_price > 0:
        gain_loss_pct = ((sell_price - buy_price) / buy_price) * 100.0
    
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        fieldnames = [
            'timestamp', 'token_name', 'mint_address', 'reason',
            'buy_price', 'sell_price', 'gain_loss_pct', 'result'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        
        if not file_exists:
            writer.writeheader()
        
        # Determine trade result
        trade_result = ''
        if gain_loss_pct is not None:
            trade_result = 'PROFIT' if gain_loss_pct > 0 else 'LOSS' if gain_loss_pct < 0 else 'BREAKEVEN'
            
        writer.writerow({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'token_name': token_name,
            'mint_address': mint_address,
            'reason': reason,
            'buy_price': f"{buy_price:.9f}" if buy_price is not None else '',
            'sell_price': f"{sell_price:.9f}" if sell_price is not None else '',
            'gain_loss_pct': f"{gain_loss_pct:.2f}%" if gain_loss_pct is not None else '',
            'result': trade_result
        })
    
    # Also print to console for immediate feedback
    if gain_loss_pct is not None:
        result = "PROFIT" if gain_loss_pct > 0 else "LOSS" if gain_loss_pct < 0 else "BREAKEVEN"
        print(f"\n📊 Trade Result: {result} | Gain/Loss: {gain_loss_pct:+.2f}% | "
              f"Buy: {buy_price:.9f} | Sell: {sell_price:.9f} | {token_name} | {mint_address}\n")

def restart_sniperx_v2():
    """Restart the SniperX V2 script and exit current process.
    
    Returns:
        bool: True if restart was initiated successfully, False otherwise
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, 'SniperX V2.py')
    
    if not os.path.exists(script_path):
        print(f"❌ SniperX V2.py not found at {script_path}")
        return False
        
    # Clean up lock file if it exists
    lock_file = os.path.join(script_dir, 'monitoring_active.lock')
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
            print(f"🔓 Removed lock file: {lock_file}")
        except Exception as e:
            print(f"⚠️ Could not remove lock file: {e}")
    
    # Start new process
    python_executable = sys.executable
    print(f"🔄 Restarting SniperX V2 script...")
    
    try:
        # Start the new process
        if os.name == 'nt':  # Windows
            subprocess.Popen(
                [python_executable, script_path],
                creationflags=subprocess.CREATE_NEW_CONSOLE,
                cwd=script_dir,  # Set working directory
                close_fds=True  # Close all file descriptors
            )
        else:  # Unix/Linux/Mac
            subprocess.Popen(
                [python_executable, script_path],
                start_new_session=True,
                cwd=script_dir,  # Set working directory
                close_fds=True   # Close all file descriptors
            )
        
        print("✅ Successfully started new SniperX V2 process")
        return True
        
    except Exception as e:
        print(f"❌ Failed to start new SniperX V2 process: {e}")
        return False

def remove_token_from_csv(token_address, csv_file_path):
    """Remove a token from the CSV file by its address."""
    try:
        # Read all rows from the CSV
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames
        
        # Filter out the token to be removed
        new_rows = [row for row in rows if row.get('Address', '').strip() != token_address]
        
        # If the token was found and removed, write the updated rows back to the file
        if len(new_rows) < len(rows):
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(new_rows)
            print(f"✅ Removed token {token_address} from {os.path.basename(csv_file_path)}")
            return True
        else:
            print(f"ℹ️ Token {token_address} not found in {os.path.basename(csv_file_path)}")
            return False
    except Exception as e:
        print(f"❌ Error removing token {token_address} from CSV: {e}")
        return False

class TokenProcessingComplete(Exception):
    """Signals that the current token's monitoring/trading lifecycle is complete."""
    def __init__(self, mint_address, reason, buy_price=None, sell_price=None):
        self.mint_address = mint_address
        self.reason = reason
        self.buy_price = buy_price
        self.sell_price = sell_price

        super().__init__(f"Token processing complete for {mint_address}: {reason}")

        # Log the trade result once the token lifecycle finishes
        log_trade_result(g_token_name, mint_address, reason, buy_price, sell_price)

        # Inform the main loop to continue processing without exiting
        print(f"🔄 Token processing complete for {mint_address}. Ready for next token.")

# --- Token Lifecycle Configuration ---
TAKE_PROFIT_THRESHOLD_PERCENT = 1.10  # e.g., 10% profit
STOP_LOSS_THRESHOLD_PERCENT = 0.95    # e.g., 5% loss
STAGNATION_PRICE_THRESHOLD_PERCENT = 0.80 # e.g., price is 20% below baseline
NO_BUY_SIGNAL_TIMEOUT_SECONDS = 180   # 3 minutes
STAGNATION_TIMEOUT_SECONDS = 180      # 3 minutes
BUY_SIGNAL_PRICE_INCREASE_PERCENT = 1.01 # 1% increase from baseline to consider it a buy signal
PRICE_IMPACT_THRESHOLD_MONITOR = 80.0  # Maximum price impact percentage to monitor (exclusive)

# --- Global State Variables (Token Lifecycle Specific) ---
g_token_start_time = None
g_buy_signal_detected = False
g_stagnation_timer_start = None # Tracks when price first fell below stagnation threshold

# --- CSV Loader ---
def load_token_from_csv(csv_file_path):
    """
    Loads the token from the CSV file.
    It prioritizes the *last* valid token entry in the CSV.
    A valid token entry must have a non-empty 'Address' field.
    """
    latest_mint_address = None
    latest_token_name = None
    print(f"\n=== DEBUG: Loading tokens from {csv_file_path} ===")
    print(f"DEBUG: PRICE_IMPACT_THRESHOLD_MONITOR = {PRICE_IMPACT_THRESHOLD_MONITOR}%")
    
    try:
        with open(csv_file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            if not reader.fieldnames:
                print("DEBUG: CSV is empty or has no headers")
                return None, None

            actual_headers = [header.strip() for header in reader.fieldnames]
            print(f"DEBUG: Found headers: {actual_headers}")
            
            if 'Address' not in actual_headers:
                print(f"ERROR: CSV missing 'Address' header. Found: {actual_headers}")
                return None, None

            for row_idx, row in enumerate(reader, 1):
                mint_address = row.get('Address', '').strip()
                print(f"\n--- DEBUG: Processing row {row_idx} ---")
                print(f"DEBUG: Token: {row.get('Name', 'N/A')} | Address: {mint_address}")

                # Get price impact from either column name variant
                price_impact_str = row.get('Price_Impact_Cluster_Sell_Percent', 
                                         row.get('Price_Impact_Cluster_Sell_Percent_', '')).strip()
                
                print(f"DEBUG: Raw price impact string: '{price_impact_str}'")
                
                # Handle 'N/A' or other non-numeric values
                price_impact_val = PRICE_IMPACT_THRESHOLD_MONITOR + 1  # Default to exclude if invalid
                if price_impact_str and price_impact_str.upper() != 'N/A':
                    try:
                        price_impact_val = float(price_impact_str)
                        print(f"DEBUG: Parsed price impact: {price_impact_val}%")
                    except (ValueError, TypeError) as e:
                        print(f"WARNING: Could not parse price impact value '{price_impact_str}': {e}")
                else:
                    print(f"WARNING: Empty or N/A price impact value")

                print(f"DEBUG: Checking if token passes filters...")
                print(f"DEBUG: - Has address: {bool(mint_address)}")
                print(f"DEBUG: - Price impact {price_impact_val}% < {PRICE_IMPACT_THRESHOLD_MONITOR}%: {price_impact_val < PRICE_IMPACT_THRESHOLD_MONITOR}")

                if mint_address and price_impact_val < PRICE_IMPACT_THRESHOLD_MONITOR:
                    token_name_from_row = row.get('Name', '').strip()
                    latest_mint_address = mint_address
                    latest_token_name = token_name_from_row if token_name_from_row else mint_address
                    print(f"✅ DEBUG: Token SELECTED for monitoring: {latest_token_name} ({latest_mint_address}) with price impact {price_impact_val}%")
                else:
                    reason = []
                    if not mint_address:
                        reason.append("missing address")
                    if price_impact_val >= PRICE_IMPACT_THRESHOLD_MONITOR:
                        reason.append(f"price impact {price_impact_val}% >= {PRICE_IMPACT_THRESHOLD_MONITOR}%")
                    print(f"❌ DEBUG: Token REJECTED: {', '.join(reason)}")
            
            if latest_mint_address:
                # This print can be verbose if called every CSV_CHECK_INTERVAL_SECONDS by checker
                # Consider logging it only when a change is detected or at startup.
                # print(f"Loaded latest token from CSV: {latest_token_name} ({latest_mint_address})")
                return latest_mint_address, latest_token_name
            else:
                # print(f"INFO: No valid token with an 'Address' found in '{csv_file_path}'. No token loaded.")
                return None, None
                
    except FileNotFoundError:
        print(f"Error: Input CSV file '{csv_file_path}' not found. No token loaded.")
def remove_token_from_csv(mint_address_to_remove: str, csv_file_path: str) -> bool:
    """
    Removes a token's row from the CSV file based on its mint address.
    
    Args:
        mint_address_to_remove: The token mint address to remove
        csv_file_path: Path to the CSV file
        
    Returns:
        bool: True if token was found and removed, False otherwise
    """
    if not mint_address_to_remove:
        print("ERROR: No mint address provided for removal.")
        return False

    print(f"ℹ️ Attempting to remove token {mint_address_to_remove} from {csv_file_path}")
    
    # Normalize the address for comparison
    mint_address_to_remove = mint_address_to_remove.strip().lower()
    
    # Create a backup of the original file
    backup_path = f"{csv_file_path}.bak"
    
    try:
        # Read all lines first to handle potential malformed CSV
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if not content.strip():
            print(f"INFO: CSV file '{csv_file_path}' is empty.")
            return False
            
        lines = content.splitlines()
        if not lines:
            print(f"INFO: No lines found in '{csv_file_path}'.")
            return False
            
        # Get headers from first line
        headers = [h.strip('"\' ') for h in lines[0].strip().split(',')]
        if 'Address' not in headers:
            print(f"ERROR: CSV '{csv_file_path}' missing 'Address' header. Cannot remove token.")
            print(f"Available headers: {headers}")
            return False
            
        address_index = headers.index('Address')
        rows_to_keep = [lines[0]]  # Keep headers
        found = False
        
        # Process each line (skip header)
        for line in lines[1:]:
            if not line.strip():
                continue  # Skip empty lines
                
            # Handle quoted values that might contain commas
            values = []
            in_quotes = False
            current_value = []
            
            for c in line:
                if c == '"':
                    in_quotes = not in_quotes
                elif c == ',' and not in_quotes:
                    values.append(''.join(current_value).strip())
                    current_value = []
                    continue
                current_value.append(c)
            if current_value:  # Add the last value
                values.append(''.join(current_value).strip())
            
            if len(values) > address_index:
                current_address = values[address_index].strip('"\' ').lower()
                if current_address == mint_address_to_remove:
                    print(f"✅ Found token {mint_address_to_remove} in CSV. Marking for removal.")
                    found = True
                    continue  # Skip this row (don't add to rows_to_keep)
            
            # Reconstruct the original line to preserve formatting
            rows_to_keep.append(line)
        
        if not found:
            print(f"ℹ️ Token {mint_address_to_remove} not found in '{csv_file_path}'. No changes made.")
            return False
            
        # Create backup
        import shutil
        try:
            shutil.copy2(csv_file_path, backup_path)
            print(f"ℹ️ Created backup at: {backup_path}")
        except Exception as e:
            print(f"⚠️ Could not create backup: {e}")
        
        # Write the filtered content back to the file
        with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
            f.write('\n'.join(rows_to_keep))
            
        print(f"✅ Successfully removed token {mint_address_to_remove} from '{csv_file_path}'.")
        return True
        
    except FileNotFoundError:
        print(f"❌ ERROR: CSV file '{csv_file_path}' not found.")
        return False
    except Exception as e:
        print(f"❌ ERROR: Failed to process CSV file '{csv_file_path}': {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def reset_token_specific_state():
    """Resets global variables specific to the currently monitored token."""
    global g_latest_trade_data, g_baseline_price_usd, g_trade_status, g_buy_price_usd, \
           g_token_start_time, g_buy_signal_detected, g_stagnation_timer_start
    
    g_latest_trade_data.clear()
    g_baseline_price_usd = None
    g_trade_status = 'monitoring' # Reset to initial status
    g_buy_price_usd = None
    g_token_start_time = time.time()
    g_buy_signal_detected = False
    g_stagnation_timer_start = None
    print(f"Token-specific state reset. New token monitoring started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(g_token_start_time))}.")

# --- SOL/USD Price Fetcher ---
async def periodic_sol_price_updater():
    """Periodically refresh the global SOL/USD price.

    Primary source: Pump.Fun.  Fallback: Coingecko simple price API.
    The function keeps the *same* update interval and preserves the previous
    behaviour if both endpoints fail – it just leaves the old cached value.
    """
    global g_last_known_sol_price

    primary_url = "https://frontend-api-v3.pump.fun/sol-price"
    secondary_url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"

    async def fetch_json(session, url):
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception:
            return None

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                source_used = "Pump.Fun"
                data = await fetch_json(session, primary_url)
                price = None

                if data and isinstance(data, dict):
                    price = float(data.get("solPrice", 0)) or None

                # Fallback if primary failed or gave 0/None
                if not price:
                    source_used = "Coingecko"
                    data = await fetch_json(session, secondary_url)
                    if data and isinstance(data, dict):
                        price = float(data.get("solana", {}).get("usd", 0)) or None

                if price and price > 0:
                    if g_last_known_sol_price != price:
                        print(f"🔄 SOL/USD updated ({source_used}): {price:.2f} USD (was {g_last_known_sol_price:.2f} USD)")
                        g_last_known_sol_price = price
                else:
                    print(f"⚠️ Could not refresh SOL/USD price from either source, keeping last known: {g_last_known_sol_price:.2f} USD")

                await asyncio.sleep(SOL_PRICE_UPDATE_INTERVAL_SECONDS)

            except asyncio.CancelledError:
                print("🔄 SOL/USD price updater task cancelled.")
                raise
            except Exception as e:
                print(f"❌ Unexpected error in SOL/USD price updater: {e}. Retrying in {SOL_PRICE_UPDATE_INTERVAL_SECONDS}s")
                await asyncio.sleep(SOL_PRICE_UPDATE_INTERVAL_SECONDS)

# --- Dexscreener Fallback Data Fetch ---
def get_dexscreener_data(token_address: str):
    """Fetch token price & volume info from Dexscreener.

    It first tries the legacy token-pairs endpoint and, if that fails or returns
    no useful data, falls back to the newer `latest/dex/tokens` endpoint.
    Returns dict with keys: price, buy_volume, sell_volume, buys, sells or None
    on failure.
    """
    endpoints = [
        # Legacy (sometimes still works for Solana)
        f"https://api.dexscreener.com/token-pairs/v1/solana/{token_address}",
        # Newer universal endpoint
        f"https://api.dexscreener.com/latest/dex/tokens/{token_address}",
    ]

    for idx, url in enumerate(endpoints, 1):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 404:
                continue  # Not found on this endpoint, try next
            res.raise_for_status()
            data = res.json()

            # Normalise to list of pairs
            if isinstance(data, dict):
                pairs = data.get("pairs") or []
            elif isinstance(data, list):
                pairs = data
            else:
                print(f"Dexscreener endpoint {idx} returned unexpected type {type(data)} for {token_address}")
                continue

            if not pairs:
                continue  # No pools here, try next endpoint

            for pair_data in pairs:
                try:
                    if not (pair_data.get("txns") and pair_data["txns"].get("h1") and \
                            pair_data.get("volume") and pair_data["volume"].get("h1") and \
                            pair_data.get("priceUsd")):
                        continue

                    buy_txns = int(pair_data["txns"]["h1"].get("buys", 0))
                    sell_txns = int(pair_data["txns"]["h1"].get("sells", 0))
                    total_hourly_volume_usd = float(pair_data["volume"]["h1"])
                    total_txns = buy_txns + sell_txns

                    buy_volume_estimation = sell_volume_estimation = 0.0
                    if total_txns > 0:
                        buy_share = buy_txns / total_txns
                        sell_share = sell_txns / total_txns
                        buy_volume_estimation = total_hourly_volume_usd * buy_share
                        sell_volume_estimation = total_hourly_volume_usd * sell_share

                    return {
                        "price": float(pair_data["priceUsd"]),
                        "buy_volume": buy_volume_estimation,
                        "sell_volume": sell_volume_estimation,
                        "buys": buy_txns,
                        "sells": sell_txns,
                    }
                except (KeyError, ValueError, TypeError):
                    continue

            # If we got here, endpoint had pairs but none usable
        except requests.RequestException as e_req:
            print(f"❌ Dexscreener request error (endpoint {idx}) for {token_address}: {e_req}")
        except json.JSONDecodeError:
            print(f"❌ Dexscreener response not valid JSON (endpoint {idx}) for {token_address}.")
        except Exception as e:
            print(f"❌ Unexpected Dexscreener error (endpoint {idx}) for {token_address}: {e}")

    # None of the endpoints produced usable data
    return None

# --- WebSocket Listener ---
async def listen_for_trades(mint_address_to_monitor):
    global g_latest_trade_data, g_token_name
    uri = "wss://pumpportal.fun/api/data"

    # Preserve the token name for logging in case globals change during a restart
    current_task_token_name = g_token_name

    while True:
        try:
            async with websockets.connect(uri) as websocket:
                subscribe_payload = {
                    "method": "subscribeTokenTrade",
                    "keys": [mint_address_to_monitor],
                }
                await websocket.send(json.dumps(subscribe_payload))
                print(
                    f"📡 Subscribed to trades for token: {current_task_token_name} ({mint_address_to_monitor})"
                )

                async for message in websocket:
                    data = json.loads(message)
                    if data.get("mint") != mint_address_to_monitor:
                        continue

                    if "tokenAmount" in data and "solAmount" in data:
                        try:
                            trade = {
                                "amount": float(data["tokenAmount"]),
                                "solAmount": float(data["solAmount"]),
                                "side": data.get("side", "").lower(),
                                "wallet": data.get("wallet", ""),
                            }
                            g_latest_trade_data.append(trade)
                        except ValueError:
                            print(
                                f"❌ WebSocket received non-numeric trade data for {current_task_token_name}: {data}"
                            )
        except websockets.exceptions.ConnectionClosed as e:
            print(
                f"🔌 WebSocket connection closed for {current_task_token_name}: {e}. Reconnecting in 5 seconds..."
            )
        except asyncio.CancelledError:
            print(f"📡 WebSocket listener for {current_task_token_name} cancelled.")
            raise
        except Exception as e:
            print(
                f"❌ WebSocket Error for {current_task_token_name}: {e}. Reconnecting in 5 seconds..."
            )

        # Any exception besides cancellation triggers a short delay before reconnecting
        await asyncio.sleep(5)


# --- Trade Logic ---
async def trade_logic_and_price_display_loop():
    global g_baseline_price_usd, g_trade_status, g_buy_price_usd, g_token_name, g_current_mint_address, \
           g_token_start_time, g_buy_signal_detected, g_stagnation_timer_start, g_last_dex_price_fetch_time, g_processing_token

    current_task_token_name = g_token_name
    current_task_mint_address = g_current_mint_address
    print(f"📈 Starting trade logic for {current_task_token_name or current_task_mint_address}")
    
    # Ensure processing flag is set when we start processing
    g_processing_token = True


    try:
        while True:
            if g_current_mint_address != current_task_mint_address:
                print(f"📈 Trade logic for old token {current_task_token_name} is stale. Exiting task.")
                return

            current_time = time.time()
            usd_price_per_token = None
            
            # --------------- Price Determination ---------------
            # 1) Preferred: price from the most recent trade (if any)
            if g_latest_trade_data:
                recent_trade = g_latest_trade_data[-1]
                token_amount = recent_trade["amount"]
                sol_amount = recent_trade["solAmount"]
                if token_amount > 0:
                    sol_price_per_token = sol_amount / token_amount
                    usd_price_per_token = sol_price_per_token * g_last_known_sol_price

            # 2) Fallback: query Dexscreener if we still have no price and enough time passed
            if usd_price_per_token is None and (current_time - g_last_dex_price_fetch_time) > DEXSCREENER_PRICE_UPDATE_INTERVAL_SECONDS:
                loop = asyncio.get_running_loop()
                dex_data = await loop.run_in_executor(None, get_dexscreener_data, current_task_mint_address)
                if dex_data and dex_data.get("price"):
                    usd_price_per_token = float(dex_data["price"])
                g_last_dex_price_fetch_time = current_time

            # --- Initial Baseline Price Setting ---
            if usd_price_per_token is not None and g_baseline_price_usd is None:
                g_baseline_price_usd = usd_price_per_token
                print(f"💰 Initial Baseline Price for {current_task_token_name}: {g_baseline_price_usd:.9f} USD")

            # --- Token Lifecycle Checks (only if baseline is set) ---
            if g_baseline_price_usd is not None:
                # 1. No Buy Signal Timeout Check
                if not g_buy_signal_detected:
                    if usd_price_per_token is not None and usd_price_per_token > g_baseline_price_usd * BUY_SIGNAL_PRICE_INCREASE_PERCENT:
                        g_buy_signal_detected = True
                        g_buy_price_usd = usd_price_per_token # Set buy price at the point of signal
                        g_trade_status = 'bought' # Update status
                        print(f"🚨 BUY SIGNAL DETECTED for {current_task_token_name} at {g_buy_price_usd:.9f} USD (Baseline: {g_baseline_price_usd:.9f} USD)")
                    elif (current_time - g_token_start_time) > NO_BUY_SIGNAL_TIMEOUT_SECONDS:
                        print(f"⏳ NO BUY SIGNAL timeout for {current_task_token_name} after {NO_BUY_SIGNAL_TIMEOUT_SECONDS}s. Baseline: {g_baseline_price_usd:.9f} USD.")
                        raise TokenProcessingComplete(
                            current_task_mint_address, 
                            "No buy signal timeout",
                            sell_price=usd_price_per_token
                        )

                # 2. Take Profit / Stop Loss Checks (only if buy signal was detected)
                if g_buy_signal_detected and g_buy_price_usd is not None and usd_price_per_token is not None:
                    if usd_price_per_token >= g_buy_price_usd * TAKE_PROFIT_THRESHOLD_PERCENT:
                        print(f"✅ TAKE PROFIT for {current_task_token_name} at {usd_price_per_token:.9f} USD (Target: {g_buy_price_usd * TAKE_PROFIT_THRESHOLD_PERCENT:.9f}, Buy: {g_buy_price_usd:.9f} USD)")
                        g_processing_token = False  # Mark processing as complete before raising
                        raise TokenProcessingComplete(
                            current_task_mint_address, 
                            "Take profit",
                            buy_price=g_buy_price_usd,
                            sell_price=usd_price_per_token
                        )
                    
                    if usd_price_per_token <= g_buy_price_usd * STOP_LOSS_THRESHOLD_PERCENT:
                        print(f"🛑 STOP LOSS for {current_task_token_name} at {usd_price_per_token:.9f} USD (Target: {g_buy_price_usd * STOP_LOSS_THRESHOLD_PERCENT:.9f}, Buy: {g_buy_price_usd:.9f} USD)")
                        g_processing_token = False  # Mark processing as complete before raising
                        raise TokenProcessingComplete(
                            current_task_mint_address, 
                            "Stop loss",
                            buy_price=g_buy_price_usd,
                            sell_price=usd_price_per_token
                        )

                # 3. Stagnation Check (only if buy signal was detected but no significant price movement)
                if g_buy_signal_detected and usd_price_per_token is not None and \
                   usd_price_per_token <= g_baseline_price_usd * STAGNATION_PRICE_THRESHOLD_PERCENT:
                    if g_stagnation_timer_start is None:
                        g_stagnation_timer_start = current_time
                    elif (current_time - g_stagnation_timer_start) > STAGNATION_TIMEOUT_SECONDS:
                        print(f"⏳ STAGNATION TIMEOUT for {current_task_token_name} at {usd_price_per_token:.9f} USD (Below {STAGNATION_PRICE_THRESHOLD_PERCENT*100:.0f}% of baseline: {g_baseline_price_usd:.9f} USD)")
                        g_processing_token = False  # Mark processing as complete before raising
                        raise TokenProcessingComplete(
                            current_task_mint_address, 
                            "Stagnation timeout",
                            buy_price=g_buy_price_usd,
                            sell_price=usd_price_per_token
                        )
                else:
                    g_stagnation_timer_start = None  # Reset timer if price recovers

            # Status Display (every iteration, but throttled in the loop)
            if usd_price_per_token is not None:
                status_line = f"🔍 {current_task_token_name or 'Loading...'} | "
                status_line += f"${usd_price_per_token:.9f} | "
                if g_buy_price_usd is not None:
                    pnl_pct = ((usd_price_per_token - g_buy_price_usd) / g_buy_price_usd) * 100
                    status_line += f"PnL: {pnl_pct:+.2f}% | "
                status_line += f"Status: {g_trade_status.upper()}"
                print(status_line)

            # Small delay to prevent CPU overuse
            await asyncio.sleep(0.1)  # 100ms delay between iterations
            
    except asyncio.CancelledError:
        print(f"📈 Trade logic task for {current_task_token_name} was cancelled.")
        g_processing_token = False  # Ensure we reset the flag on cancellation
        raise
    except TokenProcessingComplete as e:
        # The TokenProcessingComplete exception already handles logging and cleanup
        g_processing_token = False  # Ensure we reset the flag when processing is complete
        raise
    except Exception as e:
        print(f"❌ Error in trade logic for {current_task_token_name}: {e}")
        g_processing_token = False  # Ensure we reset the flag on error
        raise

# --- CSV Checker and Restart Trigger ---
async def periodic_csv_checker():
    global g_current_mint_address, g_token_name, g_current_tasks, g_processing_token
    
    address_being_monitored_by_this_task = g_current_mint_address 
    name_being_monitored_by_this_task = g_token_name

    try:
        while True:
            await asyncio.sleep(CSV_CHECK_INTERVAL_SECONDS)
            
            # Check if this checker instance is still relevant
            if g_current_mint_address != address_being_monitored_by_this_task:
                print(f"📋 CSV checker for old token {name_being_monitored_by_this_task} is stale. Exiting task.")
                return  # Exit if this checker is for a token no longer actively monitored
                
            # Skip checking if we're currently processing a token
            if g_processing_token:
                print(f"ℹ️ Currently processing {name_being_monitored_by_this_task}. Skipping CSV check...")
                continue

            new_target_mint_address, new_target_token_name = load_token_from_csv(INPUT_CSV_FILE)

            if new_target_mint_address:
                if new_target_mint_address != address_being_monitored_by_this_task:
                    print(f"ℹ️ New token detected: '{new_target_token_name}'. "
                          f"Will process after current token '{name_being_monitored_by_this_task}' is complete.")
                    # Don't trigger restart, just log the detection
            else: 
                if address_being_monitored_by_this_task is not None:
                    print(f"ℹ️ No target token in CSV. Continuing with current token '{name_being_monitored_by_this_task}'.")

    except asyncio.CancelledError:
        print(f"📋 CSV checker for {name_being_monitored_by_this_task} cancelled.")
        raise

# ... (rest of the code remains the same)
# --- Main Runner ---
async def main():
    global g_current_mint_address, g_token_name, g_current_tasks

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    LOCK_FILE_PATH = os.path.join(SCRIPT_DIR, "monitoring_active.lock")

    # Initial check: if lock file exists and we are trying to start, it's an issue.
    # This check is done *before* attempting to create our own lock file.
    # Note: The startup logic in `if __name__ == "__main__"` also tries to clear stale locks.
    # This check here is a secondary defense or for cases where that might not have run.
    if os.path.exists(LOCK_FILE_PATH):
        pid_str = ""
        try:
            with open(LOCK_FILE_PATH, 'r') as f_lock_read:
                pid_str = f_lock_read.read().strip()
            # Simple check: if it exists, it's problematic unless it's an empty file from a crash
            print(f"ERROR: Lock file {LOCK_FILE_PATH} exists (PID in file: '{pid_str}'). Another instance of Monitoring.py might be running or it's a stale lock.")
            print("The script will attempt to run, but if issues persist, manually remove the lock file.")
            # Not returning here to allow the startup stale lock removal to take precedence if it can.
        except Exception as e_lock_read:
            print(f"ERROR: Lock file {LOCK_FILE_PATH} exists but could not read PID: {e_lock_read}. It might be a stale lock.")
            # Not returning, let the script attempt to manage it.

    sol_price_task = None
    lock_created_by_this_instance = False
    try:
        # Attempt to create the lock file for this instance
        # This action signifies that this instance is now intending to be active.
        try:
            with open(LOCK_FILE_PATH, 'w') as f_lock:
                f_lock.write(str(os.getpid()))
            lock_created_by_this_instance = True
            print(f"INFO: Lock file {LOCK_FILE_PATH} created/taken by Monitoring.py (PID: {os.getpid()}).")
        except Exception as e:
            print(f"ERROR: Failed to create lock file: {e}")
            return

        sol_price_task = asyncio.create_task(periodic_sol_price_updater())

        while True:
            loaded_mint_address, loaded_token_name = load_token_from_csv(INPUT_CSV_FILE)

            if not loaded_mint_address:
                if g_current_mint_address is not None: # If we were monitoring something and it disappeared
                    print(f"INFO: No token currently specified in '{INPUT_CSV_FILE}'. Ceasing monitoring of '{g_token_name}'.")
                
                # If no token is active, this instance should not hold the lock.
                if lock_created_by_this_instance and os.path.exists(LOCK_FILE_PATH):
                    # print(f"INFO: No token active, ensuring lock file {LOCK_FILE_PATH} (owned by PID {os.getpid()}) is removed.")
                    try:
                        # Before removing, ideally verify it's still our lock (e.g. check PID in file)
                        # For now, if this instance created it and it exists, remove it.
                        current_pid_in_lock = ""
                        try:
                            with open(LOCK_FILE_PATH, 'r') as f_check: current_pid_in_lock = f_check.read().strip()
                        except: pass
                        if current_pid_in_lock == str(os.getpid()):
                            os.remove(LOCK_FILE_PATH)
                            print(f"🔑 Lock file {LOCK_FILE_PATH} removed as no token is being monitored by this instance.")
                        elif os.path.exists(LOCK_FILE_PATH): # Lock exists but not ours
                             print(f"INFO: No token active, but lock file {LOCK_FILE_PATH} (PID: {current_pid_in_lock}) is not from this instance. Leaving it.")
                    except OSError as e:
                        print(f"ERROR: Could not remove own lock file {LOCK_FILE_PATH} during no-token state: {e}")
                
                print(f"INFO: No token found in '{INPUT_CSV_FILE}' to monitor. Will check again in {CSV_CHECK_INTERVAL_SECONDS}s.")
                
                g_current_mint_address = None 
                g_token_name = None
                
                if g_current_tasks:
                    for task in g_current_tasks:
                        if not task.done(): task.cancel()
                    await asyncio.gather(*g_current_tasks, return_exceptions=True)
                    g_current_tasks = []

                await asyncio.sleep(CSV_CHECK_INTERVAL_SECONDS)
                continue

            # A token IS loaded. Ensure lock file exists if this instance is supposed to hold it.
            if lock_created_by_this_instance and not os.path.exists(LOCK_FILE_PATH):
                print(f"INFO: Token '{loaded_token_name}' is active. Re-asserting lock file {LOCK_FILE_PATH} for PID {os.getpid()}.")
                try:
                    with open(LOCK_FILE_PATH, 'w') as f_lock:
                        f_lock.write(str(os.getpid()))
                    print(f"INFO: Lock file {LOCK_FILE_PATH} re-created/taken by Monitoring.py (PID: {os.getpid()}).")
                except Exception as e_recreate_lock:
                    print(f"ERROR: Could not re-create lock file {LOCK_FILE_PATH} for active token: {e_recreate_lock}")
            
            # --- Token Monitoring Logic --- 
            if g_current_mint_address != loaded_mint_address or not g_current_tasks:
                if g_current_tasks: # Clean up tasks for old token or if tasks ended prematurely
                    # print(f"INFO: Token changed or tasks ended. Cleaning up for '{g_token_name or 'previous'}'.")
                    for task in g_current_tasks:
                        if not task.done(): task.cancel()
                    await asyncio.gather(*g_current_tasks, return_exceptions=True)
                
                g_current_mint_address = loaded_mint_address
                g_token_name = loaded_token_name
                g_processing_token = True  # Set flag when starting to process a token
                
                print(f"🚀 Initializing/Re-initializing monitoring for: {g_token_name} ({g_current_mint_address})")
                reset_token_specific_state()

                listener_task = asyncio.create_task(listen_for_trades(g_current_mint_address))
                trader_task = asyncio.create_task(trade_logic_and_price_display_loop())
                csv_checker_task = asyncio.create_task(periodic_csv_checker())
                
                g_current_tasks = [listener_task, trader_task, csv_checker_task]

            # Await tasks and handle their outcomes
            token_processing_outcome = None # Can be 'completed', 'restart_required', 'error', or None
            processed_token_mint_address = g_current_mint_address # Capture before tasks might alter globals
            processed_token_name = g_token_name
            
            if g_current_tasks:
                done, pending = await asyncio.wait(
                    g_current_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )

                for task in pending:
                    task.cancel()

                await asyncio.gather(*pending, return_exceptions=True)

                for finished_task in done:
                    task_name_for_log = finished_task.get_name() if hasattr(finished_task, 'get_name') else 'Task'
                    if finished_task.cancelled():
                        result = asyncio.CancelledError()
                    else:
                        exc = finished_task.exception()
                        result = exc if exc is not None else finished_task.result()

                    if isinstance(result, TokenProcessingComplete):
                        print(f"✅ Token processing complete for {result.mint_address or processed_token_name}: {result.reason}")
                        remove_token_from_csv(result.mint_address or processed_token_mint_address, INPUT_CSV_FILE)
                        token_processing_outcome = 'completed'
                        break
                    elif isinstance(result, RestartRequired):
                        print(f"🔄 RestartRequired signal received from task '{task_name_for_log}' for token {processed_token_name}.")
                        token_processing_outcome = 'restart_required'
                        break
                    elif isinstance(result, asyncio.CancelledError):
                        # Expected if another task triggered completion
                        pass
                    elif isinstance(result, Exception):
                        print(f"💥 Unexpected error in task '{task_name_for_log}' for token {processed_token_name}: {result}")
                        token_processing_outcome = 'error'
                        break
            
            # Cleanup tasks for the token that was just processed (or attempted)
            active_tasks_to_await_cleanup = []
            for task in g_current_tasks:
                if task and not task.done():
                    task.cancel()
                    active_tasks_to_await_cleanup.append(task)
            if active_tasks_to_await_cleanup:
                await asyncio.gather(*active_tasks_to_await_cleanup, return_exceptions=True)
            
            g_current_tasks = [] # Clear tasks list for the next iteration
            
            # Handle token processing outcome
            if token_processing_outcome == 'completed' or token_processing_outcome == 'error':
                reason = token_processing_outcome if token_processing_outcome else "unknown reason"
                # processed_token_name and processed_token_mint_address are assumed to be captured earlier in main()
                # e.g., processed_token_mint_address = g_current_mint_address
                #       processed_token_name = g_token_name
                
                print(f"ℹ️ Token processing complete with status: {{reason}} for {{processed_token_name or processed_token_mint_address or 'N/A'}}")
                
                removal_successful = False # Initialize
                if processed_token_mint_address:
                    print(f"🗑️ Attempting to remove processed token {{processed_token_name or processed_token_mint_address}} from CSV...")
                    # INPUT_CSV_FILE is assumed to be defined
                    removal_successful = remove_token_from_csv(processed_token_mint_address, INPUT_CSV_FILE)
                
                # Reset global state before restarting
                g_current_mint_address = None
                g_token_name = None
                reset_token_specific_state()
                
                print("\n" + "="*50)
                print(f"ℹ️  Token processing complete for {processed_token_mint_address}")
                print("="*50 + "\n")
                
                # Small delay to ensure all resources are released
                await asyncio.sleep(2)
                
                # Reset state and continue to next token
                print("🔄 Resetting state for next token...")
                g_current_mint_address = None
                g_token_name = None
                reset_token_specific_state()
                
                # Small delay before checking for next token
                await asyncio.sleep(2)
                continue
                
            # Reset state for next iteration
            current_token = None
            g_processing_token = False
            reset_token_specific_state()
            
            # Check for new tokens more frequently when idle
            print("ℹ️ No active token to process. Checking for new tokens...")
            await asyncio.sleep(2)  # Reduced delay for more responsive checking

    except KeyboardInterrupt:
        print("\n📉 Monitoring stopped by user (KeyboardInterrupt).")
    except asyncio.CancelledError:
        print("\n🌀 Main task was cancelled. Shutting down.")
    except Exception as e_main:
        print(f"\n💥 An unexpected error occurred in main: {e_main}")
        import traceback
        traceback.print_exc()
    finally:
        print("INFO: Shutting down Monitoring.py...")
        if sol_price_task and not sol_price_task.done():
            sol_price_task.cancel()
            try: await sol_price_task
            except asyncio.CancelledError: pass
            except Exception as e_sol_cancel: print(f"ERROR cancelling SOL price task: {e_sol_cancel}")

        # Final cleanup for any g_current_tasks during shutdown
        final_shutdown_tasks = []
        for task in g_current_tasks:
            if task and not task.done(): task.cancel(); final_shutdown_tasks.append(task)
        if final_shutdown_tasks: await asyncio.gather(*final_shutdown_tasks, return_exceptions=True)

        if lock_created_by_this_instance and os.path.exists(LOCK_FILE_PATH):
            try:
                # Verify it's our lock file by checking PID before removing
                current_pid_in_lock_on_exit = ""
                try: 
                    with open(LOCK_FILE_PATH, 'r') as f_check_exit: current_pid_in_lock_on_exit = f_check_exit.read().strip()
                except: pass
                if current_pid_in_lock_on_exit == str(os.getpid()):
                    os.remove(LOCK_FILE_PATH)
                    print(f"INFO: Lock file {LOCK_FILE_PATH} (PID: {os.getpid()}) removed by Monitoring.py upon exit.")
                elif os.path.exists(LOCK_FILE_PATH): # Lock exists but not ours
                    print(f"INFO: Lock file {LOCK_FILE_PATH} (PID: {current_pid_in_lock_on_exit}) was not removed as it's not owned by this instance (PID: {os.getpid()}).")
            except Exception as e_lock_remove:
                print(f"ERROR: Monitoring.py could not remove its lock file {LOCK_FILE_PATH} on exit: {e_lock_remove}")

if __name__ == "__main__":
    # Define SCRIPT_DIR and LOCK_FILE_PATH here as well for the initial stale check
    # This is important because main() might not be called if there's an early exit.
    _SCRIPT_DIR_MAIN = os.path.dirname(os.path.abspath(__file__))
    _LOCK_FILE_PATH_MAIN = os.path.join(_SCRIPT_DIR_MAIN, "monitoring_active.lock")

    if os.path.exists(_LOCK_FILE_PATH_MAIN):
        try:
            # Simple removal, no PID check here. If it's there, it's considered stale at this point.
            print(f"INFO: Stale lock file {_LOCK_FILE_PATH_MAIN} found on script startup. Attempting removal.")
            os.remove(_LOCK_FILE_PATH_MAIN)
            print(f"🔑 Stale lock file {_LOCK_FILE_PATH_MAIN} removed successfully.")
        except OSError as e:
            print(f"WARNING: Could not remove stale lock file {_LOCK_FILE_PATH_MAIN} on startup: {e}. Manual check may be needed if script fails to start.")

    try:
        asyncio.run(main())
    except Exception as e_run:
        print(f"\n💥 An critical error occurred at asyncio.run level: {e_run}")
        import traceback
        traceback.print_exc()
    finally:
        print("INFO: Monitoring.py has shut down completely.")