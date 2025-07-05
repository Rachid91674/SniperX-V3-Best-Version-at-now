#!/usr/bin/env python3

import os
import requests
import asyncio
import aiohttp
import datetime # Added for marker file timestamp
import dateparser
import csv      # Added for CSV writing
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import time
from dotenv import load_dotenv
import subprocess
import logging
import signal
load_dotenv()

# --- Global Configurations & Constants ---
TEST_MODE = len(sys.argv) > 1 and sys.argv[1] == "--test"
MORALIS_API_KEYS = [os.getenv(f"MORALIS_API_KEY_{i}", "").split('#')[0].strip() for i in range(1,6)]
MORALIS_API_KEYS = [k for k in MORALIS_API_KEYS if k]
if not MORALIS_API_KEYS:
    print("[ERROR] No Moralis API keys configured in .env. Exiting.")
    sys.exit(1)
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "pumpfun")
FETCH_LIMIT = 100
MORALIS_API_URL = f"https://solana-gateway.moralis.io/token/mainnet/exchange/{EXCHANGE_NAME}/graduated?limit={FETCH_LIMIT}"
DEFAULT_MAX_WORKERS = 1 # <<<< SET TO 1 AS PER USER REQUEST >>>>
DEXSCREENER_CHAIN_ID = "solana"
raw_snipe_age_env = os.getenv("SNIPE_GRADUATED_DELTA_MINUTES", "60")
SNIPE_GRADUATED_DELTA_MINUTES_FLOAT = float(raw_snipe_age_env.split('#')[0].strip()) if raw_snipe_age_env else 60.0
raw_wt = os.getenv("WHALE_TRAP_WINDOW_MINUTES", "1,5")
raw_wt = raw_wt.split('#')[0].strip()
parts = raw_wt.split(',')
WINDOW_MINS = []
for part in parts:
    try:
        val = int(part.strip())
        if val > 0 : WINDOW_MINS.append(val)
    except: pass
if not WINDOW_MINS: WINDOW_MINS = [1, 5]

raw_pl = os.getenv("PRELIM_LIQUIDITY_THRESHOLD", "5000").split('#')[0].strip()
PRELIM_LIQUIDITY_THRESHOLD = float(raw_pl) if raw_pl else 5000.0
raw_ppmin = os.getenv("PRELIM_MIN_PRICE_USD", "0.00001").split('#')[0].strip()
PRELIM_MIN_PRICE_USD = float(raw_ppmin) if raw_ppmin else 0.00001
raw_ppmax = os.getenv("PRELIM_MAX_PRICE_USD", "0.0004").split('#')[0].strip()
PRELIM_MAX_PRICE_USD = float(raw_ppmax) if raw_ppmax else 0.0004
raw_age = os.getenv("PRELIM_AGE_DELTA_MINUTES", "120").split('#')[0].strip()
PRELIM_AGE_DELTA_MINUTES = float(raw_age) if raw_age else 120.0
raw_wp = os.getenv("WHALE_PRICE_UP_PCT", "0.0").split('#')[0].strip()
WHALE_PRICE_UP_PCT = float(raw_wp) if raw_wp else 0.0
raw_wlq = os.getenv("WHALE_LIQUIDITY_UP_PCT", "0.0").split('#')[0].strip()
WHALE_LIQUIDITY_UP_PCT = float(raw_wlq) if raw_wlq else 0.0
raw_wvd = os.getenv("WHALE_VOLUME_DOWN_PCT", "0.0").split('#')[0].strip()
WHALE_VOLUME_DOWN_PCT = float(raw_wvd) if raw_wvd else 0.0
raw_sl1 = os.getenv("SNIPE_LIQUIDITY_MIN_PCT_1M","0.1").split('#')[0].strip()
SNIPE_LIQUIDITY_MIN_PCT_1M = float(raw_sl1) if raw_sl1 else 0.1
raw_sm1 = os.getenv("SNIPE_LIQUIDITY_MULTIPLIER_1M","1.5").split('#')[0].strip()
SNIPE_LIQUIDITY_MULTIPLIER_1M = float(raw_sm1) if raw_sm1 else 1.5
raw_sup = os.getenv("SNIPE_LIQUIDITY_UP_PCT", "0.30").split('#')[0].strip()
SNIPE_LIQUIDITY_UP_PCT_CONFIG = float(raw_sup) if raw_sup else 0.30
raw_sl5 = os.getenv("SNIPE_LIQUIDITY_MIN_PCT_5M", str(SNIPE_LIQUIDITY_UP_PCT_CONFIG)).split('#')[0].strip()
SNIPE_LIQUIDITY_MIN_PCT_5M = float(raw_sl5) if raw_sl5 else SNIPE_LIQUIDITY_UP_PCT_CONFIG
raw_sm5 = os.getenv("SNIPE_LIQUIDITY_MULTIPLIER_5M","5").split('#')[0].strip()
SNIPE_LIQUIDITY_MULTIPLIER_5M = float(raw_sm5) if raw_sm5 else 5.0
raw_gv1 = os.getenv("GHOST_VOLUME_MIN_PCT_1M", "0.5").split('#')[0].strip()
GHOST_VOLUME_MIN_PCT_1M = float(raw_gv1) if raw_gv1 else 0.5
raw_gv5 = os.getenv("GHOST_VOLUME_MIN_PCT_5M", "0.5").split('#')[0].strip()
GHOST_VOLUME_MIN_PCT_5M = float(raw_gv5) if raw_gv5 else 0.5
raw_gpr = os.getenv("GHOST_PRICE_REL_MULTIPLIER", "2").split('#')[0].strip()
GHOST_PRICE_REL_MULTIPLIER = float(raw_gpr) if raw_gpr else 2.0


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def get_graduated_tokens():
    print(f"[INFO] Fetching graduated tokens from '{EXCHANGE_NAME}'...")
    for idx, key in enumerate(MORALIS_API_KEYS, start=1):
        print(f"[INFO] Trying Moralis API key {idx}/{len(MORALIS_API_KEYS)}")
        headers = {"Accept": "application/json", "Authorization": f"Bearer {key}"}
        try:
            resp = requests.get(MORALIS_API_URL, headers=headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            tokens = data.get('result') or []
            print(f"[INFO] Retrieved {len(tokens)} tokens with key {idx}.")
            return [t for t in tokens if isinstance(t, dict)] 
        except requests.exceptions.HTTPError as err:
            status = getattr(err.response, 'status_code', None)
            print(f"[WARN] Key {idx} HTTP {status} error: {err}. Trying next key.")
        except requests.exceptions.RequestException as err:
            print(f"[WARN] Request error with key {idx}: {err}. Trying next key.")
        except ValueError:
            print(f"[WARN] Invalid JSON with key {idx}. Trying next key.")
    print("[ERROR] All Moralis API keys failed; please check Moralis API quota.")
    return []

def filter_preliminary(tokens):
    now = datetime.datetime.now(datetime.timezone.utc)
    filtered = []
    for token in tokens:
        if not isinstance(token, dict): continue
        raw_liq_data = token.get("liquidity", {})
        raw_liq_val = raw_liq_data.get("usd") if isinstance(raw_liq_data, dict) else token.get("liquidity")
        try: liquidity = float(raw_liq_val if raw_liq_val is not None else 0)
        except (ValueError, TypeError): liquidity = 0.0
        try: price_usd = float(token.get("priceUsd", 0))
        except (ValueError, TypeError): price_usd = 0.0
        grad_str = token.get("graduatedAt")
        minutes_diff = float('inf')
        if grad_str:
            try:
                grad = dateparser.parse(grad_str)
                if isinstance(grad, datetime.datetime):
                    grad = grad.replace(tzinfo=datetime.timezone.utc) if grad.tzinfo is None else grad.astimezone(datetime.timezone.utc)
                    minutes_diff = (now - grad).total_seconds() / 60
            except Exception as e: logging.debug(f"GraduatedAt parse error for {token.get('tokenAddress')}: {e}")
        if (liquidity >= PRELIM_LIQUIDITY_THRESHOLD and
            PRELIM_MIN_PRICE_USD <= price_usd <= PRELIM_MAX_PRICE_USD and
            minutes_diff <= PRELIM_AGE_DELTA_MINUTES):
            filtered.append(token)
    logging.info(f"{len(filtered)} tokens passed preliminary filters.")
    return filtered

async def fetch_token_data(session, token_address):
    url = f"https://api.dexscreener.com/tokens/v1/{DEXSCREENER_CHAIN_ID}/{token_address}"
    try:
        async with session.get(url, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("pairs") if isinstance(data, dict) else data if isinstance(data, list) else None
    except Exception:
        return None

async def fetch_all_token_data(token_addresses):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_token_data(session, addr) for addr in token_addresses]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    data_map = {}
    for addr, res in zip(token_addresses, results):
        data_map[addr] = res if not isinstance(res, Exception) else None
    return data_map

def get_token_metrics(token_pair_data_list):
    if not token_pair_data_list or not isinstance(token_pair_data_list, list): return 0.0, 0.0, 0.0
    token_data = token_pair_data_list[0] if token_pair_data_list else {}
    if not isinstance(token_data, dict): return 0.0, 0.0, 0.0
    try: price = float(token_data.get('priceUsd', token_data.get('price', 0)))
    except: price = 0.0
    liq_raw = token_data.get('liquidity', {})
    liq_val = liq_raw.get('usd', 0) if isinstance(liq_raw, dict) else liq_raw
    try: liquidity = float(liq_val if liq_val is not None else 0)
    except: liquidity = 0.0
    vol_data = token_data.get('volume', {})
    try: volume = float(vol_data.get('m5', 0) if isinstance(vol_data, dict) else 0)
    except: volume = 0.0
    return price, liquidity, volume

def whale_trap_avoidance(token_address, first_snap, second_snap):
    price1, liquidity1, volume1 = first_snap
    price2, liquidity2, volume2 = second_snap
    price_change_pct = (price2 - price1) / price1 if price1 else float('inf') if price2 > 0 else 0
    volume_change_pct = (volume2 - volume1) / volume1 if volume1 else float('inf') if volume2 > 0 else 0
    liq_change_pct = (liquidity2 - liquidity1) / liquidity1 if liquidity1 else float('inf') if liquidity2 > 0 else 0
    if price_change_pct > WHALE_PRICE_UP_PCT and liq_change_pct > WHALE_LIQUIDITY_UP_PCT and volume_change_pct < WHALE_VOLUME_DOWN_PCT:
        logging.warning(f"[WARN] Whale trap for {token_address}: Price↑, Liquidity↑, Vol Δ {volume_change_pct:.2%}")
        return False
    if price_change_pct > WHALE_PRICE_UP_PCT and liq_change_pct > WHALE_LIQUIDITY_UP_PCT and volume_change_pct >= WHALE_VOLUME_DOWN_PCT:
        return True
    return False

def apply_whale_trap(tokens, first_snaps, second_snaps):
    passed = []
    max_workers_for_whale_trap = DEFAULT_MAX_WORKERS if tokens else 1 # Adheres to global DEFAULT_MAX_WORKERS
    
    with ThreadPoolExecutor(max_workers=max_workers_for_whale_trap) as executor:
        futures = {executor.submit(whale_trap_avoidance, t.get('tokenAddress'), first_snaps.get(t.get('tokenAddress'), (0,0,0)), second_snaps.get(t.get('tokenAddress'), (0,0,0))): t for t in tokens if t.get('tokenAddress')}
        for future in as_completed(futures):
            if future.result(): passed.append(futures[future])
    logging.info(f"{len(passed)} tokens passed Whale Trap Avoidance.")
    return passed

def sanitize_name(name, fallback_name=None):
    if not name or name in ('None',): return fallback_name or 'Unknown'
    name_str = str(name)
    normalized_name = unicodedata.normalize('NFKC', name_str)
    sanitized = ''.join(ch for ch in normalized_name if ch.isprintable() and unicodedata.category(ch)[0] not in ('C', 'S'))
    if not sanitized: sanitized = ''.join(ch for ch in name_str if ch.isalnum() or ch in ' _-()[]{}<>')
    return sanitized[:30].strip() or (fallback_name or 'Unknown')

def load_existing_tokens(csv_filepath):
    """Load existing token addresses from CSV file"""
    existing_tokens = set()
    if os.path.exists(csv_filepath):
        try:
            with open(csv_filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('Address'):
                        existing_tokens.add(row['Address'].strip())
        except Exception as e:
            logging.error(f"Error reading existing tokens from {csv_filepath}: {e}")
    return existing_tokens

def process_window(win_minutes, prelim_tokens, script_dir_path):
    sleep_seconds = win_minutes * 60
    abs_csv_filepath = os.path.join(script_dir_path, f"sniperx_results_{win_minutes}m.csv")
    
    # Load existing tokens to avoid duplicates
    existing_tokens = load_existing_tokens(abs_csv_filepath)
    # Create a backup of the existing tokens to track new additions in this run
    existing_tokens_at_start = set(existing_tokens)

    logging.info(f"=== Running filters for {win_minutes}m window ({sleep_seconds}s) ===")
    token_addresses = [t.get('tokenAddress') for t in prelim_tokens if t.get('tokenAddress')]
    if not token_addresses: return {'whale': [], 'snipe': [], 'ghost': []}
    
    first_data = asyncio.run(fetch_all_token_data(token_addresses))
    first_snaps = {addr: get_token_metrics(first_data.get(addr)) for addr in token_addresses}
    logging.info(f"Captured t0 for {len(first_snaps)} tokens in {win_minutes}m window. Waiting {sleep_seconds}s...")
    time.sleep(sleep_seconds)
    second_data = asyncio.run(fetch_all_token_data(token_addresses))
    second_snaps = {addr: get_token_metrics(second_data.get(addr)) for addr in token_addresses}
    logging.info(f"Captured t1 for {len(second_snaps)} tokens in {win_minutes}m window.")
    
    passed_whale_trap = apply_whale_trap(prelim_tokens, first_snaps, second_snaps)
    snipe_candidates, ghost_buyer_candidates = [], []
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    for token_info in passed_whale_trap:
        grad_str = token_info.get('graduatedAt')
        if not grad_str: continue
        grad_dt = dateparser.parse(grad_str)
        if not isinstance(grad_dt, datetime.datetime): continue
        grad_dt = grad_dt.replace(tzinfo=datetime.timezone.utc) if grad_dt.tzinfo is None else grad_dt.astimezone(datetime.timezone.utc)
        age_minutes = (now_utc - grad_dt).total_seconds() / 60
        if age_minutes > SNIPE_GRADUATED_DELTA_MINUTES_FLOAT: continue
        addr = token_info.get('tokenAddress')
        p1,l1,v1 = first_snaps.get(addr,(0,0,0)); p2,l2,v2 = second_snaps.get(addr,(0,0,0))
        liq_chg = (l2-l1)/l1 if l1 else float('inf') if l2 else 0
        vol_chg = (v2-v1)/v1 if v1 else float('inf') if v2 else 0
        prc_chg = (p2-p1)/p1 if p1 else float('inf') if p2 else 0
        if vol_chg > 0.01 and prc_chg > 0.01 and liq_chg >= 0.05 and liq_chg > vol_chg and liq_chg > prc_chg:
            snipe_candidates.append(token_info)
        ghost_vol_min = GHOST_VOLUME_MIN_PCT_1M if win_minutes == 1 else GHOST_VOLUME_MIN_PCT_5M
        if vol_chg > ghost_vol_min and abs(prc_chg) < (vol_chg * GHOST_PRICE_REL_MULTIPLIER):
            ghost_buyer_candidates.append(token_info)
            
    final_results_for_csv = snipe_candidates + [t for t in ghost_buyer_candidates if t not in snipe_candidates]
    
    # Filter out tokens that were already in the file at the start
    new_tokens = [t for t in final_results_for_csv 
                 if t.get('tokenAddress') and t['tokenAddress'] not in existing_tokens_at_start]
    
    if not new_tokens:
        logging.info(f"No new tokens to add to {abs_csv_filepath}")
        return {'whale': [], 'snipe': [], 'ghost': []}
    
    # Write header if file doesn't exist
    write_header = not os.path.exists(abs_csv_filepath)
    
    try:
        with open(abs_csv_filepath, 'a' if os.path.exists(abs_csv_filepath) else 'w', 
                 newline='', encoding='utf-8') as csvfile:
            
            fieldnames = ['Address','Name','Price USD',f'Liquidity({win_minutes}m)',
                        f'Volume({win_minutes}m)',f'{win_minutes}m Change',
                        'Open Chart','Snipe','Ghost Buyer']
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            
            rows_added = 0
            for t_data in new_tokens:
                addr = t_data.get('tokenAddress')
                if not addr or addr in existing_tokens:
                    continue
                    
                p1, l1, v1 = first_snaps.get(addr, (0, 0, 0))
                p2, l2, v2 = second_snaps.get(addr, (0, 0, 0))
                
                writer.writerow({
                    'Address': addr, 
                    'Name': sanitize_name(t_data.get('name'), t_data.get('symbol')),
                    'Price USD': f"{p2:.8f}", 
                    f'Liquidity({win_minutes}m)': f"{l2:.2f}",
                    f'Volume({win_minutes}m)': f"{max(0, v2-v1):.2f}", 
                    f'{win_minutes}m Change': f"{((p2/p1-1)*100 if p1 else 0):.2f}",
                    'Open Chart': f'=HYPERLINK(\"https://dexscreener.com/{DEXSCREENER_CHAIN_ID}/{addr}\",\"Open Chart\")',
                    'Snipe': 'Yes' if t_data in snipe_candidates else '', 
                    'Ghost Buyer': 'Yes' if t_data in ghost_buyer_candidates else ''
                })
                existing_tokens.add(addr)
                rows_added += 1
                
        if rows_added > 0:
            logging.info(f"Successfully added {rows_added} new tokens to {abs_csv_filepath}")
        else:
            logging.info("No new tokens were added to the CSV file")
            
    except IOError as e: 
        logging.error(f"Error writing to {abs_csv_filepath}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in process_window: {e}")
    return {'whale': passed_whale_trap, 'snipe': snipe_candidates, 'ghost': ghost_buyer_candidates}

def initialize_all_files_once(script_dir_path):
    # --- START: First-Run Reset Logic ---
    logging.info("Checking for first run...")
    first_run_marker_file = os.path.join(script_dir_path, ".sniperx_first_run_complete")

    results_file_1m_to_reset_name = "sniperx_results_1m.csv"
    opened_tokens_file_to_reset_name = "opened_tokens.txt"
    token_risk_analysis_csv_name = "token_risk_analysis.csv"
    
    token_risk_analysis_header = [
        'Address','Name','Price USD','Liquidity(1m)','Volume(1m)','1m Change','Open Chart','Snipe','Ghost Buyer',
        'Global_Cluster_Percentage','Highest_Risk_Reason_Cluster','DexScreener_Pair_Address',
        'DexScreener_Liquidity_USD','DexScreener_Token_Price_USD','DexScreener_Token_Name',
        'LP_Percent_Supply','Cluster_Token_Amount_Est','Pool_Project_Token_Amount_Est',
        'Dump_Risk_LP_vs_Cluster_Ratio','Price_Impact_Cluster_Sell_Percent',
        'Overall_Risk_Status','Risk_Warning_Details'
    ]

    if not os.path.exists(first_run_marker_file):
        logging.info("First run detected. Resetting specified files.")
        
        # Delete sniperx_results_1m.csv
        results_file_1m_path = os.path.join(script_dir_path, results_file_1m_to_reset_name)
        try:
            if os.path.exists(results_file_1m_path):
                os.remove(results_file_1m_path)
                logging.info(f"Deleted {results_file_1m_to_reset_name} as part of first-run reset.")
        except Exception as e:
            logging.error(f"Error deleting {results_file_1m_to_reset_name} during first-run reset: {e}")

        # Delete opened_tokens.txt
        opened_tokens_file_path = os.path.join(script_dir_path, opened_tokens_file_to_reset_name)
        try:
            if os.path.exists(opened_tokens_file_path):
                os.remove(opened_tokens_file_path)
                logging.info(f"Deleted {opened_tokens_file_to_reset_name} as part of first-run reset.")
        except Exception as e:
            logging.error(f"Error deleting {opened_tokens_file_to_reset_name} during first-run reset: {e}")

        # Reset token_risk_analysis.csv to its header
        token_risk_file_path = os.path.join(script_dir_path, token_risk_analysis_csv_name)
        try:
            # Delete it first, then recreate with header.
            if os.path.exists(token_risk_file_path):
                os.remove(token_risk_file_path)
            with open(token_risk_file_path, 'w', newline='', encoding='utf-8') as f_csv:
                writer = csv.writer(f_csv)
                writer.writerow(token_risk_analysis_header)
            logging.info(f"Reset {token_risk_analysis_csv_name} to header as part of first-run reset.")
        except Exception as e:
            logging.error(f"Error resetting {token_risk_analysis_csv_name} to header during first-run reset: {e}")
        
        # Create the marker file
        try:
            with open(first_run_marker_file, 'w') as f_marker:
                f_marker.write(datetime.datetime.now(datetime.timezone.utc).isoformat())
            logging.info(f"Created first-run marker file: {first_run_marker_file}")
        except Exception as e:
            logging.error(f"Error creating first-run marker file: {e}")
    else:
        logging.info("Not the first run, skipping specific file reset.")
    # --- END: First-Run Reset Logic ---

    # --- START: Comprehensive File Initialization ---
    logging.info("Proceeding with standard file initialization checks...")
    
    files_to_initialize = {
        results_file_1m_to_reset_name: ['Address','Name','Price USD','Liquidity(1m)','Volume(1m)','1m Change','Open Chart','Snipe','Ghost Buyer'],
        # Files below have been disabled as per user request
        # "sniperx_results_5m.csv": ['Address','Name','Price USD','Liquidity(5m)','Volume(5m)','5m Change','Open Chart','Snipe','Ghost Buyer'],
        # "sniperx_prelim_filtered.csv": ['Address','Name','Price USD','Liquidity','Volume','Age (Minutes)','Created At','Open Chart'],
        # "sniperx_whale_trap_1m.csv": ['Address','Name','Price USD','Liquidity(1m)','Volume(1m)','1m Change','Open Chart'],
        # "sniperx_whale_trap_5m.csv": ['Address','Name','Price USD','Liquidity(5m)','Volume(5m)','5m Change','Open Chart'],
        # "sniperx_ghost_buyer_1m.csv": ['Address','Name','Price USD','Liquidity(1m)','Volume(1m)','1m Change','Open Chart'],
        # "sniperx_ghost_buyer_5m.csv": ['Address','Name','Price USD','Liquidity(5m)','Volume(5m)','5m Change','Open Chart'],
        "processed_tokens.txt": [], 
        opened_tokens_file_to_reset_name: [], 
        # Bubblemaps files disabled as per user request
        # "bubblemaps_processed.txt": [],
        # "bubblemaps_failed.txt": [],
        # "bubblemaps_cluster_summary.csv": ['Token Address', 'Cluster ID', 'Holder Count', 'Token Amount', 'Percentage of Supply', 'USD Value', 'Highest Risk Reason'],
        token_risk_analysis_csv_name: token_risk_analysis_header,
    }

    sniperx_config_env_name = "sniperx_config.env"
    sniperx_config_env_content = (
        "MORALIS_API_KEY_1=\"YOUR_MORALIS_API_KEY_HERE_1 # Required, get from moralis.io\"\n"
        "MORALIS_API_KEY_2=\"YOUR_MORALIS_API_KEY_HERE_2 # Optional, additional key\"\n"
        "MORALIS_API_KEY_3=\"YOUR_MORALIS_API_KEY_HERE_3 # Optional, additional key\"\n"
        "MORALIS_API_KEY_4=\"YOUR_MORALIS_API_KEY_HERE_4 # Optional, additional key\"\n"
        "MORALIS_API_KEY_5=\"YOUR_MORALIS_API_KEY_HERE_5 # Optional, additional key\"\n"
        "EXCHANGE_NAME=\"pumpfun # e.g., pumpfun, raydium_v4, etc.\"\n"
        "SNIPE_GRADUATED_DELTA_MINUTES=\"60 # Max age of token since graduation (minutes)\"\n"
        "WHALE_TRAP_WINDOW_MINUTES=\"1,5 # Comma-separated window(s) in minutes for whale trap analysis\"\n"
        "PRELIM_LIQUIDITY_THRESHOLD=\"5000 # Minimum liquidity in USD for preliminary filter\"\n"
        "PRELIM_MIN_PRICE_USD=\"0.00001 # Minimum token price in USD for preliminary filter\"\n"
        "PRELIM_MAX_PRICE_USD=\"0.0004 # Maximum token price in USD for preliminary filter\"\n"
        "PRELIM_AGE_DELTA_MINUTES=\"120 # Max age of token for preliminary filter (minutes from creation)\"\n"
        "WHALE_PRICE_UP_PCT=\"0.0 # Price increase percentage for whale detection (e.g., 0.2 for 20%)\"\n"
        "WHALE_LIQUIDITY_UP_PCT=\"0.0 # Liquidity increase percentage for whale detection\"\n"
        "WHALE_VOLUME_DOWN_PCT=\"0.0 # Volume decrease percentage for whale detection\"\n"
        "SNIPE_LIQUIDITY_MIN_PCT_1M=\"0.1 # Min liquidity % increase over 1m for SNIPE\"\n"
        "SNIPE_LIQUIDITY_MULTIPLIER_1M=\"1.5 # Liquidity multiplier for 1m SNIPE (e.g. 1.5x current)\"\n"
        "SNIPE_LIQUIDITY_UP_PCT=\"0.30 # General liquidity up % for SNIPE category (used as default for 5m if not set)\"\n"
        "SNIPE_LIQUIDITY_MIN_PCT_5M=\"0.30 # Min liquidity % increase over 5m for SNIPE (defaults to SNIPE_LIQUIDITY_UP_PCT)\"\n"
        "SNIPE_LIQUIDITY_MULTIPLIER_5M=\"5 # Liquidity multiplier for 5m SNIPE (e.g. 5x current)\"\n"
        "GHOST_VOLUME_MIN_PCT_1M=\"0.5 # Min volume % of liquidity for GHOST (1m)\"\n"
        "GHOST_VOLUME_MIN_PCT_5M=\"0.5 # Min volume % of liquidity for GHOST (5m)\"\n"
        "GHOST_PRICE_REL_MULTIPLIER=\"2.0 # Price multiplier relative to SNIPE for GHOST (e.g. 2x SNIPE price)\"\n"
        "SLAVE_WATCHDOG_INTERVAL_SECONDS=\"300 # How often the watchdog checks the slave script (Monitoring.py)\"\n"
        "SLAVE_SCRIPT_NAME=\"Monitoring.py # Name of the slave script to monitor (not used by current watchdog)\"\n"
    )
    env_file_path = os.path.join(script_dir_path, sniperx_config_env_name)
    if not os.path.exists(env_file_path):
        try:
            with open(env_file_path, 'w', encoding='utf-8') as f_env:
                f_env.write(sniperx_config_env_content)
            logging.info(f"Created template file: {sniperx_config_env_name}")
        except Exception as e:
            logging.error(f"Failed to create template file {sniperx_config_env_name}: {e}")

    for filename, header_list_or_empty in files_to_initialize.items():
        file_path = os.path.join(script_dir_path, filename)
        if not os.path.exists(file_path): # Only create if it wasn't created/reset above
            try:
                with open(file_path, 'w', newline='', encoding='utf-8') as f_generic:
                    if header_list_or_empty:
                        writer = csv.writer(f_generic)
                        writer.writerow(header_list_or_empty)
                        logging.info(f"Created CSV file with headers: {filename}")
                    else:
                        logging.info(f"Created empty file: {filename}")
            except Exception as e:
                logging.error(f"Failed to create template file {filename}: {e}")
    # --- END: Comprehensive File Initialization ---
    logging.info("File initialisation and template check complete.")

def start_slave_watchdog(script_dir_path):
    slave_script_path = os.path.join(script_dir_path, 'run_testchrone_on_csv_change.py')
    pid_file = os.path.join(script_dir_path, 'testchrone_watchdog.pid')
    if not os.path.exists(slave_script_path):
        logging.error(f"Watchdog script '{slave_script_path}' not found.")
        return None

    # Terminate previous watchdog process if PID file exists
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as pf:
                old_pid = int(pf.read().strip())
            logging.info(f"Found previous watchdog PID {old_pid}. Attempting termination...")
            os.kill(old_pid, signal.SIGTERM)
            start_time = time.time()
            while time.time() - start_time < 5:
                try:
                    os.kill(old_pid, 0)
                    time.sleep(0.5)
                except OSError:
                    break
            else:
                os.kill(old_pid, signal.SIGKILL)
                logging.info(f"Force killed watchdog PID {old_pid} after timeout.")
        except Exception as e:
            logging.info(f"Unable to terminate previous watchdog PID from file: {e}")
        finally:
            try:
                os.remove(pid_file)
            except FileNotFoundError:
                pass

    try:
        process = subprocess.Popen([sys.executable, slave_script_path], cwd=script_dir_path)
        with open(pid_file, 'w') as pf:
            pf.write(str(process.pid))
        logging.info(f"Watchdog script '{slave_script_path}' started with PID {process.pid}.")
        return process
    except Exception as e:
        logging.error(f"Failed to start watchdog: {e}")
        return None

def main_token_processing_loop(script_dir_path):
    tokens = get_graduated_tokens()
    prelim_filtered_tokens = filter_preliminary(tokens)
    window_results_aggregator = {}
    if prelim_filtered_tokens:
        max_workers_for_windows = DEFAULT_MAX_WORKERS # This will be 1
        
        with ThreadPoolExecutor(max_workers=max_workers_for_windows, thread_name_prefix="WindowProc") as executor:
            future_to_window = {executor.submit(process_window, wd_min, prelim_filtered_tokens, script_dir_path): wd_min for wd_min in WINDOW_MINS}
            for future in as_completed(future_to_window):
                wd_min = future_to_window[future]
                try: window_results_aggregator[wd_min] = future.result()
                except Exception as exc: logging.error(f'[ERROR] Window {wd_min}m exc: {exc}')
    else: logging.info("[INFO] No tokens passed preliminary filters for this cycle.")
    return window_results_aggregator

if __name__ == "__main__":
    print("--- SniperX V2 Starting ---")
    SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
    monitoring_process = None
    watchdog_process = None
    
    initialize_all_files_once(SCRIPT_DIRECTORY) 

    monitoring_script_path = os.path.join(SCRIPT_DIRECTORY, "Monitoring.py")
    if os.path.exists(monitoring_script_path):
        try:
            monitoring_process = subprocess.Popen([sys.executable, monitoring_script_path], cwd=SCRIPT_DIRECTORY)
            logging.info(f"Successfully started Monitoring.py (PID: {monitoring_process.pid}).")
        except Exception as e:
            logging.error(f"Failed to start Monitoring.py: {e}")
    else:
        logging.warning(f"Monitoring.py not found at {monitoring_script_path}. It will not be started.")
    
    watchdog_process = start_slave_watchdog(SCRIPT_DIRECTORY)
    if not WINDOW_MINS: 
        logging.error("No WHALE_TRAP_WINDOW_MINUTES defined. Exiting.")
        if watchdog_process: watchdog_process.terminate()
        if monitoring_process: monitoring_process.terminate()
        sys.exit(1)
        
    monitoring_lock_file_path = os.path.join(SCRIPT_DIRECTORY, "monitoring_active.lock")
    check_interval_seconds = 3  # Short pause between lock checks
    try:
        while True:
            if os.path.exists(monitoring_lock_file_path):
                logging.info(
                    f"Monitoring.py is active (lock file found: {monitoring_lock_file_path}). SniperX V2 pausing for {check_interval_seconds} seconds..."
                )
                time.sleep(check_interval_seconds)
                continue

            logging.info(f"\n--- Starting new SniperX processing cycle at {datetime.datetime.now()} ---")
            
            aggregated_results = {}
            try:
                aggregated_results = main_token_processing_loop(SCRIPT_DIRECTORY)
                if aggregated_results: logging.info(f"Cycle complete. Results for windows: {list(aggregated_results.keys())}")
                else: logging.info("Cycle complete. No results from window processing.")
            except Exception as e_main_loop:
                logging.error(f"Unhandled exception in main processing loop: {e_main_loop}", exc_info=True)
            
            current_time_utc = datetime.datetime.now(datetime.timezone.utc)
            if current_time_utc.minute == 0 and aggregated_results: 
                hr_ts = current_time_utc.strftime('%Y%m%d_%H00')
                hr_fn = os.path.join(SCRIPT_DIRECTORY, f"sniperx_hourly_report_{hr_ts}.csv") 
                logging.info(f"Writing hourly report to {hr_fn}")
                try:
                    with open(hr_fn, 'w', newline='', encoding='utf-8') as hr_f: 
                        w = csv.writer(hr_f)
                        w.writerow(['Window_Minutes','Category','Token_Address','Token_Name','DexScreener_URL'])
                        for wm, cats_data in aggregated_results.items():
                            for cat_name, tk_list in cats_data.items():
                                for tk_item in tk_list:
                                    addr = tk_item.get('tokenAddress','N/A')
                                    name = sanitize_name(tk_item.get('name'),tk_item.get('symbol'))
                                    url = f"https://dexscreener.com/{DEXSCREENER_CHAIN_ID}/{addr}"
                                    w.writerow([wm,cat_name,addr,name,url])
                except Exception as e_rep: logging.error(f"Hourly report error: {e_rep}")
            
            logging.info(f"Rechecking monitoring lock in {check_interval_seconds}s...")
            time.sleep(check_interval_seconds)
            
    except KeyboardInterrupt: 
        logging.info("\nKeyboardInterrupt. Shutting down SniperX V2...")
    finally:
        if monitoring_process and monitoring_process.poll() is None:
            logging.info("Terminating Monitoring.py process...")
            monitoring_process.terminate()
            try:
                monitoring_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                monitoring_process.kill()
                logging.info("Monitoring.py process killed after timeout due to no response.")
            except Exception as e_mon_term:
                logging.error(f"Error during Monitoring.py termination: {e_mon_term}")

        if watchdog_process and watchdog_process.poll() is None:
            logging.info("Terminating watchdog process...")
            watchdog_process.terminate()
            pid_file = os.path.join(SCRIPT_DIRECTORY, 'testchrone_watchdog.pid')
            try:
                watchdog_process.wait(timeout=5)
                logging.info(f"Watchdog process PID {watchdog_process.pid} terminated.")
            except subprocess.TimeoutExpired:
                watchdog_process.kill()
                logging.info(f"Watchdog process PID {watchdog_process.pid} killed after timeout.")
            finally:
                if os.path.exists(pid_file):
                    os.remove(pid_file)
        logging.info("--- SniperX V2 Finished ---")