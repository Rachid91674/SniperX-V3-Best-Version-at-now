#!/usr/bin/env python3

import os
import logging
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
import threading
from dotenv import load_dotenv
import subprocess
import signal
from typing import Optional, Dict, Any
from logger_util import setup_logger

# Initialize logger with a special name to prevent recursion
logger = logging.getLogger('SniperXMain')
logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicates
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Create file handler which logs even debug messages
log_file = os.path.join(log_dir, f'sniperx_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# Create console handler with a higher log level
console_handler = logging.StreamHandler(sys.__stdout__)
console_handler.setLevel(logging.INFO)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Store original stdout/stderr
_original_stdout = sys.__stdout__
_original_stderr = sys.__stderr__

class StreamToLogger:
    _lock = threading.Lock()
    _in_write = False
    
    def __init__(self, logger, log_level=logging.INFO, original_stream=None):
        self.logger = logger
        self.log_level = log_level
        self.original_stream = original_stream
        self._buffer = []

    def write(self, buf):
        if not buf.strip():
            return
            
        # Prevent recursive logging
        if StreamToLogger._in_write:
            return
            
        try:
            StreamToLogger._in_write = True
            with StreamToLogger._lock:
                # Log the message
                self.logger.log(self.log_level, buf.strip())
                # Also write to original stream if needed
                if self.original_stream:
                    self.original_stream.write(buf)
        except Exception as e:
            # If logging fails, write to stderr directly
            if self.original_stream:
                self.original_stream.write(f"[Logging Error] {str(e)}\n")
        finally:
            StreamToLogger._in_write = False

    def flush(self):
        if self.original_stream:
            self.original_stream.flush()

# Only redirect stdout/stderr if they haven't been redirected already
if not isinstance(sys.stdout, StreamToLogger):
    sys.stdout = StreamToLogger(logger, logging.INFO, _original_stdout)
if not isinstance(sys.stderr, StreamToLogger):
    sys.stderr = StreamToLogger(logger, logging.ERROR, _original_stderr)

load_dotenv()

def format_with_emojis(message: str, level: str = "INFO") -> str:
    """Add emojis to log messages based on content and log level."""
    # Emoji mapping for log levels
    level_emojis = {
        "INFO": "ℹ️",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "CRITICAL": "🔥",
        "DEBUG": "🐛"
    }
    
    # Common patterns to emoji
    emoji_mapping = {
        # Levels
        "INFO": f"{level_emojis['INFO']} INFO",
        "WARNING": f"{level_emojis['WARNING']} WARNING",
        "ERROR": f"{level_emojis['ERROR']} ERROR",
        "CRITICAL": f"{level_emojis['CRITICAL']} CRITICAL",
        "DEBUG": f"{level_emojis['DEBUG']} DEBUG",
        # Common patterns
        "fetching": "🔍 Fetching",
        "fetched": "✅ Fetched",
        "processing": "⚙️ Processing",
        "processed": "✅ Processed",
        "token": "🪙 Token",
        "tokens": "🪙 Tokens",
        "price": "💰 Price",
        "liquidity": "💧 Liquidity",
        "volume": "📊 Volume",
        "age": "⏳ Age",
        "passed": "✅ Passed",
        "filtered": "🚫 Filtered",
        "starting": "🚀 Starting",
        "completed": "🏁 Completed",
        "error": "❌ Error",
        "warning": "⚠️ Warning",
        "whale": "🐋 Whale",
        "snipe": "🎯 Snipe",
        "ghost": "👻 Ghost",
        "cycle": "🔄 Cycle"
    }
    
    # Replace level first
    for level_name in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        message = message.replace(f"{level_name}", f"{level_emojis.get(level_name, level_name)} {level_name}")
    
    # Replace other patterns
    for pattern, emoji_text in emoji_mapping.items():
        if pattern in message.lower():
            message = message.replace(pattern.title(), emoji_text)
            message = message.replace(pattern.upper(), emoji_text.upper())
            
    # Special case for cycle start/end
    if "starting new cycle" in message.lower():
        message = f"🔄 {message}"
    elif "cycle complete" in message.lower():
        message = f"✅ {message}"
    
    return message

# Custom formatter that adds emojis
class EmojiLogFormatter(logging.Formatter):
    def format(self, record):
        original = super().format(record)
        return format_with_emojis(original, record.levelname)

def get_env_float(env_name, default):
    """Safely get and convert an environment variable to float, handling quotes and comments."""
    value = os.getenv(env_name, str(default)).split('#')[0].strip('\'" ')
    try:
        return float(value) if value else default
    except (ValueError, TypeError):
        return default

# --- Global Configurations & Constants ---
TEST_MODE = len(sys.argv) > 1 and sys.argv[1] == "--test"
MORALIS_API_KEYS = [k for k in [os.getenv(f"MORALIS_API_KEY_{i}", "").split('#')[0].strip() for i in range(1,6)] if k]
if not MORALIS_API_KEYS:
    logger.error("[ERROR] No Moralis API keys configured in .env. Exiting.")
    sys.exit(1)

EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "pumpfun")
FETCH_LIMIT = 100
MORALIS_API_URL = "https://deep-index.moralis.io/api/v2.2/tokens/trending?chain=solana"
DEFAULT_MAX_WORKERS = 1  # <<<< SET TO 1 AS PER USER REQUEST >>>>
DEXSCREENER_CHAIN_ID = "solana"

# Parse window minutes
raw_wt = os.getenv("WHALE_TRAP_WINDOW_MINUTES", "1,5").split('#')[0].strip()
WINDOW_MINS = []
for part in raw_wt.split(','):
    try:
        val = int(part.strip())
        if val > 0: 
            WINDOW_MINS.append(val)
    except (ValueError, TypeError):
        pass
if not WINDOW_MINS:
    WINDOW_MINS = [1, 5]

# Load configuration values using the get_env_float helper
SNIPE_GRADUATED_DELTA_MINUTES_FLOAT = get_env_float("SNIPE_GRADUATED_DELTA_MINUTES", 60.0)
PRELIM_LIQUIDITY_THRESHOLD = get_env_float('PRELIM_LIQUIDITY_THRESHOLD', 20000.0)  # $20,000 minimum liquidity
PRELIM_MIN_PRICE_USD = get_env_float('PRELIM_MIN_PRICE_USD', 0.0001)  # $0.0001 minimum price (reduced from $0.00065)
PRELIM_MAX_PRICE_USD = get_env_float('PRELIM_MAX_PRICE_USD', 0.005)  # widened funnel
PRELIM_AGE_DELTA_MINUTES = get_env_float('PRELIM_AGE_DELTA_MINUTES', 360.0)  # widened funnel

# Whale trap configuration
WHALE_PRICE_UP_PCT = get_env_float("WHALE_PRICE_UP_PCT", 0.0)
WHALE_LIQUIDITY_UP_PCT = get_env_float("WHALE_LIQUIDITY_UP_PCT", 0.0)
WHALE_VOLUME_DOWN_PCT = get_env_float("WHALE_VOLUME_DOWN_PCT", 0.0)

# Snipe configuration
SNIPE_LIQUIDITY_UP_PCT_CONFIG = get_env_float("SNIPE_LIQUIDITY_UP_PCT", 0.30)
SNIPE_LIQUIDITY_MIN_PCT_1M = get_env_float("SNIPE_LIQUIDITY_MIN_PCT_1M", 0.1)
SNIPE_LIQUIDITY_MULTIPLIER_1M = get_env_float("SNIPE_LIQUIDITY_MULTIPLIER_1M", 1.5)
SNIPE_LIQUIDITY_MIN_PCT_5M = get_env_float("SNIPE_LIQUIDITY_MIN_PCT_5M", SNIPE_LIQUIDITY_UP_PCT_CONFIG)
SNIPE_LIQUIDITY_MULTIPLIER_5M = get_env_float("SNIPE_LIQUIDITY_MULTIPLIER_5M", 5.0)

# Ghost buyer configuration
GHOST_VOLUME_MIN_PCT_1M = get_env_float("GHOST_VOLUME_MIN_PCT_1M", 0.5)
GHOST_VOLUME_MIN_PCT_5M = get_env_float("GHOST_VOLUME_MIN_PCT_5M", 0.5)
GHOST_PRICE_REL_MULTIPLIER = get_env_float("GHOST_PRICE_REL_MULTIPLIER", 2.0)

def get_trending_tokens():
    logger.info("Fetching trending tokens from Moralis...")
    url = "https://deep-index.moralis.io/api/v2.2/tokens/trending?chain=solana"

    for idx, key in enumerate(MORALIS_API_KEYS, start=1):
        logger.info(f"Trying Moralis API key {idx}/{len(MORALIS_API_KEYS)}")
        headers = {
            "Accept": "application/json",
            "X-API-Key": key,
        }
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            tokens = resp.json()
            if isinstance(tokens, list):
                print(f"[INFO] Retrieved {len(tokens)} trending tokens.")
                return tokens
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
    tokens_checked = 0
    filtered_by_liquidity = 0
    filtered_by_price = 0
    filtered_by_age = 0
    
    for token in tokens:
        tokens_checked += 1
        if not isinstance(token, dict):
            logging.debug(f"Token skipped: Not a dictionary")
            continue
            
        token_address = token.get('tokenAddress', 'unknown')
        token_name = token.get('name', 'unnamed')
        
        # Liquidity check
        raw_liq_val = token.get("liquidityUsd", 0)
        try:
            liquidity = float(raw_liq_val if raw_liq_val is not None else 0)
        except (ValueError, TypeError) as e:
            logging.warning(f"Token {token_name} ({token_address}): Invalid liquidity value '{raw_liq_val}': {e}")
            liquidity = 0.0
            
        if liquidity < PRELIM_LIQUIDITY_THRESHOLD:
            logging.debug(f"Token {token_name} ({token_address}): Filtered by liquidity (${liquidity:,.2f} < ${PRELIM_LIQUIDITY_THRESHOLD:,.2f})")
            filtered_by_liquidity += 1
            continue
            
        # Price check
        try:
            price_usd = float(token.get("usdPrice", 0))
        except (ValueError, TypeError) as e:
            logging.warning(f"Token {token_name} ({token_address}): Invalid price value: {e}")
            price_usd = 0.0
            
        if not (PRELIM_MIN_PRICE_USD <= price_usd <= PRELIM_MAX_PRICE_USD):
            logging.debug(f"Token {token_name} ({token_address}): Filtered by price (${price_usd:.8f} not in [${PRELIM_MIN_PRICE_USD:.8f}, ${PRELIM_MAX_PRICE_USD:.8f}])")
            filtered_by_price += 1
            continue
            
        # Age check
        token_created_at = token.get("createdAt")
        minutes_diff = float("inf")

        if token_created_at is not None:
            try:
                created_at_str = str(token_created_at)
                created_dt = None

                # Handle timestamp values (seconds or milliseconds)
                if created_at_str.replace(".", "", 1).isdigit():
                    try:
                        ts = float(created_at_str)
                        if ts > 1e12:
                            ts /= 1000
                        created_dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                        logging.debug(
                            f"Token {token_name} ({token_address}): Parsed timestamp {ts}"
                        )
                    except (ValueError, TypeError):
                        created_dt = None

                if created_dt is None:
                    created_dt = dateparser.parse(created_at_str)
                    if created_dt is None:
                        raise ValueError(f"Failed to parse date string: {created_at_str}")

                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=datetime.timezone.utc)

                age_seconds = (now - created_dt).total_seconds()
                minutes_diff = age_seconds / 60
                logging.debug(
                    f"Token {token_name} ({token_address}): Age = {minutes_diff:.1f} minutes"
                )
            except Exception as e:
                logging.warning(
                    f"Token {token_name} ({token_address}): Error calculating age: {e}"
                )
                minutes_diff = float("inf")
                
        if minutes_diff > PRELIM_AGE_DELTA_MINUTES:
            logging.debug(f"Token {token_name} ({token_address}): Filtered by age ({minutes_diff:.1f} > {PRELIM_AGE_DELTA_MINUTES:.1f} minutes)")
            filtered_by_age += 1
            continue
            
        # If we get here, the token passed all filters
        filtered.append(token)
        logging.info(f"Token {token_name} ({token_address}) passed preliminary filters: "
                   f"Liquidity=${liquidity:,.2f}, Price=${price_usd:.8f}, Age={minutes_diff:.1f}min")
    
    # Log summary
    logging.info(f"Preliminary filters summary - Checked: {tokens_checked}, "
                f"Passed: {len(filtered)}, "
                f"Filtered - Liquidity: {filtered_by_liquidity}, "
                f"Price: {filtered_by_price}, "
                f"Age: {filtered_by_age}")
    
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
    
    # Calculate percentage changes
    price_change_pct = (price2 - price1) / price1 if price1 else float('inf') if price2 > 0 else 0
    volume_change_pct = (volume2 - volume1) / volume1 if volume1 else float('inf') if volume2 > 0 else 0
    liq_change_pct = (liquidity2 - liquidity1) / liquidity1 if liquidity1 else float('inf') if liquidity2 > 0 else 0
    
    # Log the metrics for this token
    logging.debug(f"Token {token_address} - Price: ${price1:.8f}→${price2:.8f} ({price_change_pct:+.2%}), "
                f"Liq: ${liquidity1:,.2f}→${liquidity2:,.2f} ({liq_change_pct:+.2%}), "
                f"Vol: ${volume1:,.2f}→${volume2:,.2f} ({volume_change_pct:+.2%})")
    
    # Check for whale trap pattern
    if price_change_pct > WHALE_PRICE_UP_PCT and liq_change_pct > WHALE_LIQUIDITY_UP_PCT:
        if volume_change_pct < WHALE_VOLUME_DOWN_PCT:
            logging.warning(f"Whale trap detected for {token_address}: "
                          f"Price↑ {price_change_pct:+.2%} > {WHALE_PRICE_UP_PCT:.0%}, "
                          f"Liq↑ {liq_change_pct:+.2%} > {WHALE_LIQUIDITY_UP_PCT:.0%}, "
                          f"Vol↓ {volume_change_pct:+.2%} < {WHALE_VOLUME_DOWN_PCT:.0%}")
            return False
        else:
            logging.info(f"Token {token_address} passed whale trap check: "
                       f"Price↑ {price_change_pct:+.2%}, "
                       f"Liq↑ {liq_change_pct:+.2%}, "
                       f"Vol Δ {volume_change_pct:+.2%}")
            return True
    
    # Log why token didn't pass whale trap check
    if price_change_pct <= WHALE_PRICE_UP_PCT:
        logging.debug(f"Token {token_address}: Price change {price_change_pct:+.2%} ≤ threshold {WHALE_PRICE_UP_PCT:.0%}")
    if liq_change_pct <= WHALE_LIQUIDITY_UP_PCT:
        logging.debug(f"Token {token_address}: Liquidity change {liq_change_pct:+.2%} ≤ threshold {WHALE_LIQUIDITY_UP_PCT:.0%}")
    
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

    logging.info(f"Processing {len(passed_whale_trap)} tokens that passed whale trap check...")
    
    for token_info in passed_whale_trap:
        token_address = token_info.get('tokenAddress', 'unknown')
        token_name = token_info.get('name', 'unnamed')
        
        # Age check - handle different createdAt formats
        created_at = token_info.get("createdAt")
        if created_at is not None:
            try:
                created_dt = None
                
                # Convert to string first to handle all cases
                if not isinstance(created_at, str):
                    created_at_str = str(created_at)
                else:
                    created_at_str = created_at
                
                # Try parsing as a timestamp first
                try:
                    # Check if it's a numeric string (timestamp)
                    if created_at_str.replace('.', '', 1).isdigit():
                        timestamp = float(created_at_str)
                        # If it's in milliseconds, convert to seconds
                        if timestamp > 1e12:
                            timestamp = timestamp / 1000
                        created_dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
                        logging.debug(f"Token {token_name} ({token_address}): Parsed timestamp {timestamp} to datetime {created_dt}")
                except (ValueError, TypeError) as e:
                    logging.debug(f"Token {token_name} ({token_address}): Not a timestamp, trying date string parsing")
                
                # If not a timestamp, try parsing as a date string
                if created_dt is None:
                    try:
                        created_dt = dateparser.parse(created_at_str)
                        if created_dt is None:
                            raise ValueError(f"Failed to parse date string: {created_at_str}")
                        logging.debug(f"Token {token_name} ({token_address}): Parsed date string: {created_at_str} as {created_dt}")
                    except Exception as e:
                        logging.warning(f"Token {token_name} ({token_address}): Failed to parse date string '{created_at_str}': {e}")
                        # Skip this token as we can't determine its age
                        continue
                
                # Ensure we have a timezone-aware datetime
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=datetime.timezone.utc)
                
                # Calculate age in minutes
                age_minutes = (datetime.datetime.now(datetime.timezone.utc) - created_dt).total_seconds() / 60
                
                # Log the token's age for debugging
                logging.debug(f"Token {token_name} ({token_address}): Age: {age_minutes:.1f} minutes")
                
                # Filter by age threshold
                if age_minutes > SNIPE_GRADUATED_DELTA_MINUTES_FLOAT:
                    logging.debug(f"Token {token_name} ({token_address}): Filtered by age ({age_minutes:.1f} > {SNIPE_GRADUATED_DELTA_MINUTES_FLOAT:.1f} minutes)")
                    continue
                    
            except Exception as e:
                logging.warning(f"Error calculating age for token {token_name} ({token_address}): {e}")
                # Skip this token as we can't determine its age
                continue
        
        # Get price, liquidity, and volume data
        p1, l1, v1 = first_snaps.get(token_address, (0, 0, 0))
        p2, l2, v2 = second_snaps.get(token_address, (0, 0, 0))
        
        # Calculate changes
        liq_chg = (l2-l1)/l1 if l1 else float('inf') if l2 else 0
        vol_chg = (v2-v1)/v1 if v1 else float('inf') if v2 else 0
        prc_chg = (p2-p1)/p1 if p1 else float('inf') if p2 else 0
        
        # Log metrics
        logging.debug(f"Token {token_name} ({token_address}) metrics - "
                    f"Price: ${p1:.8f}→${p2:.8f} ({prc_chg:+.2%}), "
                    f"Liq: ${l1:,.2f}→${l2:,.2f} ({liq_chg:+.2%}), "
                    f"Vol: ${v1:,.2f}→${v2:,.2f} ({vol_chg:+.2%})")
        
        # Check for Snipe conditions
        snipe_conditions_met = [
            vol_chg > 0.01,
            prc_chg > 0.01,
            liq_chg >= 0.015,
        ]
        
        if all(snipe_conditions_met):
            logging.info(f"Token {token_name} ({token_address}) added to SNIPE candidates: "
                       f"Vol↑ {vol_chg:+.2%}, Price↑ {prc_chg:+.2%}, Liq↑ {liq_chg:+.2%}")
            snipe_candidates.append(token_info)
        else:
            logging.debug(f"Token {token_name} ({token_address}) did not meet all SNIPE conditions: "
                         f"Vol↑ {vol_chg:+.2%} > 1%: {vol_chg > 0.01}, "
                         f"Price↑ {prc_chg:+.2%} > 1%: {prc_chg > 0.01}, "
                         f"Liq↑ {liq_chg:+.2%} ≥ 1.5%: {liq_chg >= 0.015}")
        
        # Check for Ghost Buyer conditions
        ghost_vol_min = 0.30  # 30% minimum volume increase for 1m (overriding GHOST_VOLUME_MIN_PCT_1M)
        ghost_price_multiplier = 3.0  # 3x volume to price ratio (overriding GHOST_PRICE_REL_MULTIPLIER)
        ghost_conditions_met = [
            vol_chg > ghost_vol_min,  # Volume change > 30%
            abs(prc_chg) < (vol_chg * ghost_price_multiplier)  # Price change < (Volume change × 3)
        ]
        
        if all(ghost_conditions_met):
            logging.info(f"Token {token_name} ({token_address}) added to GHOST candidates: "
                       f"Vol↑ {vol_chg:+.2%} > {ghost_vol_min:.0%}, "
                       f"|Price| {abs(prc_chg):.2%} < {vol_chg * ghost_price_multiplier:.2%} (Vol × {ghost_price_multiplier})")
            ghost_buyer_candidates.append(token_info)
        else:
            logging.debug(f"Token {token_name} ({token_address}) did not meet GHOST conditions: "
                         f"Vol↑ {vol_chg:+.2%} > {ghost_vol_min:.0%}: {vol_chg > ghost_vol_min}, "
                         f"|Price| {abs(prc_chg):.2%} < {vol_chg * GHOST_PRICE_REL_MULTIPLIER:.2%}: {abs(prc_chg) < (vol_chg * GHOST_PRICE_REL_MULTIPLIER)}")
            
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

def is_process_running(pid):
    """Check if a process is running by its PID."""
    if pid is None:
        return False
        
    try:
        if os.name == 'nt':
            # On Windows, use tasklist to check if process exists
            result = subprocess.run(
                ['tasklist', '/FI', f'PID eq {pid}'],
                capture_output=True, text=True, timeout=5
            )
            return str(pid) in result.stdout
        else:
            # On Unix-like systems, use kill -0
            try:
                os.kill(pid, 0)
                return True
            except (ProcessLookupError, PermissionError):
                return False
    except Exception as e:
        logging.warning(f"Error checking if process {pid} is running: {e}")
        return False

def terminate_process(pid, process_name, timeout=5):
    """Terminate a process by PID with proper error handling."""
    if not is_process_running(pid):
        logging.info(f"{process_name} (PID: {pid}) is not running.")
        return True
        
    logging.info(f"Attempting to stop {process_name} (PID: {pid})...")
    
    def try_terminate():
        try:
            if os.name == 'nt':
                # On Windows, try taskkill first
                try:
                    result = subprocess.run(
                        ['taskkill', '/F', '/T', '/PID', str(pid)],
                        timeout=5, capture_output=True, text=True
                    )
                    if result.returncode == 0:
                        logging.info(f"{process_name} terminated successfully using taskkill.")
                        return True
                    else:
                        logging.warning(f"taskkill failed with return code {result.returncode}: {result.stderr}")
                except (subprocess.SubprocessError, FileNotFoundError) as e:
                    logging.warning(f"taskkill command failed: {e}")
                
                # Fall back to terminate() if taskkill fails
                try:
                    os.kill(pid, signal.CTRL_BREAK_EVENT)
                    return True
                except (OSError, AttributeError) as e:
                    logging.warning(f"CTRL_BREAK_EVENT failed: {e}")
                    return False
            else:
                # On Unix-like systems
                try:
                    os.kill(pid, signal.SIGTERM)
                    return True
                except ProcessLookupError:
                    logging.info(f"Process {pid} not found.")
                    return True
                except PermissionError:
                    logging.error(f"Permission denied when trying to terminate process {pid}")
                    return False
        except Exception as e:
            logging.error(f"Unexpected error in termination attempt: {e}")
            return False

    # Try graceful termination first
    if try_terminate():
        # Wait for process to terminate
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not is_process_running(pid):
                logging.info(f"{process_name} (PID: {pid}) terminated gracefully.")
                return True
            time.sleep(0.5)
    
    # Forceful termination if graceful failed
    logging.warning(f"{process_name} (PID: {pid}) did not terminate, attempting force kill...")
    if os.name == 'nt':
        try:
            # Try one more time with taskkill /F
            result = subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(pid)],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                logging.info(f"{process_name} (PID: {pid}) force terminated successfully.")
                return True
            else:
                logging.warning(f"Force termination failed: {result.stderr}")
        except Exception as e:
            logging.error(f"Error during force termination: {e}")
    else:
        try:
            os.kill(pid, signal.SIGKILL)
            time.sleep(1)  # Give it a moment to terminate
            if not is_process_running(pid):
                logging.info(f"{process_name} (PID: {pid}) force terminated with SIGKILL.")
                return True
        except Exception as e:
            logging.error(f"Failed to force kill process {pid}: {e}")
    
    # Final check if process is still running
    if is_process_running(pid):
        logging.error(f"Failed to terminate {process_name} (PID: {pid}). Manual intervention may be required.")
        return False
    else:
        logging.info(f"{process_name} (PID: {pid}) has been terminated.")
        return True

def start_slave_watchdog(script_dir_path):
    slave_script_path = os.path.join(script_dir_path, 'run_testchrone_on_csv_change.py')
    pid_file = os.path.join(script_dir_path, 'testchrone_watchdog.pid')
    if not os.path.exists(slave_script_path):
        logging.error(f"Watchdog script '{slave_script_path}' not found.")
        return None

    # Terminate previous watchdog process if PID file exists
    old_pid = None
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as pf:
                old_pid = int(pf.read().strip())
                logging.info(f"Found previous watchdog PID {old_pid} in {pid_file}.")
        except Exception as e:
            logging.warning(f"Error reading PID file {pid_file}: {e}")
        
        # Try to terminate the old process
        if old_pid is not None:
            terminate_process(old_pid, "Previous Watchdog Process")
        
        # Clean up the PID file
        try:
            os.remove(pid_file)
        except FileNotFoundError:
            pass
        except Exception as e:
            logging.warning(f"Error removing PID file {pid_file}: {e}")

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
    tokens = get_trending_tokens()
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