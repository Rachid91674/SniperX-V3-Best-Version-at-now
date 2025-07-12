#!/usr/bin/env python3
"""
Bubblemaps Extractor - Multi-Threaded (Multiple Windows)
"""
import sys
import datetime
import pytest
pytest.importorskip("selenium")
import csv
import os
import re
import logging
import time
import subprocess
from pathlib import Path
import concurrent.futures
import threading
import random
import hashlib
from collections import OrderedDict

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException,
    NoSuchWindowException
)

# --- Configuration ---
CSV_FILE = 'sniperx_results_1m.csv'
OPENED_TOKENS_FILE = 'opened_tokens.txt'
EXTRACTED_DATA_DIR = 'bubblemaps_token_data'
CLUSTER_SUMMARY_FILE = 'cluster_summaries.csv'

# Exact CSS class string that Bubblemaps uses for non-clustered addresses.
INDIVIDUAL_ADDRESS_MUIBOX_CLASS = 'css-141d73e'
# --- START: NEW CODE ---
# Special key for a synthetic cluster created from the top individual holder
SYNTHETIC_CLUSTER_KEY = "Top_Individual_Holder_as_Cluster" 
# --- END: NEW CODE ---

CHECK_INTERVAL = 15
CHROME_DRIVER_PATH = None
# Path to Chrome binary used if no other path is provided
DEFAULT_CHROME_BINARY_PATH = (
    r'C:\Users\Rachid Aitali\AppData\Roaming\Microsoft\Windows\Start '
    r'Menu\Programs\Chromium\chrome.exe'
)
MAX_WORKERS = 1
MAX_BUBBLEMAPS_RETRIES = 3

PROCESSED_TOKENS_LOCK = threading.Lock()
CLUSTER_SUMMARY_LOCK = threading.Lock()

# Initialize logger with a special name to prevent recursion
logger = logging.getLogger('BubblemapsExtractor')
logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicates
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Create file handler which logs even debug messages
log_file = os.path.join(log_dir, f'bubblemaps_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# Create console handler with a higher log level
console_handler = logging.StreamHandler(sys.__stdout__)
console_handler.setLevel(logging.INFO)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s')
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

def detect_chrome_binary_path(provided_path: str | None) -> str | None:
    paths_to_check = []
    if provided_path and os.path.exists(provided_path) and os.path.isfile(provided_path):
        paths_to_check.append(provided_path)
    paths_to_check.extend([
        DEFAULT_CHROME_BINARY_PATH,
        r'C:\Program Files\Google\Chrome\Application\chrome.exe',
        r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
        '/usr/bin/google-chrome-stable',
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    ])
    for path_str in paths_to_check:
        path_obj = Path(path_str)
        if path_obj.exists() and path_obj.is_file():
            logger.info(f"Chrome binary found at: {path_str}")
            return str(path_obj)
    logger.warning("Could not automatically detect Chrome binary path from common locations.")
    return None

def load_processed_tokens_threadsafe(filepath: str) -> set:
    with PROCESSED_TOKENS_LOCK:
        if not os.path.exists(filepath): return set()
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f if line.strip())
        except Exception as e:
            logger.error(f"Error loading processed tokens from {filepath}: {e}")
            return set()

def save_processed_token_threadsafe(filepath: str, token_address: str):
    try:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(token_address + '\n')
    except Exception as e:
        logger.error(f"Error saving processed token {token_address} to {filepath}: {e}")

def get_new_tokens_from_csv_threadsafe(csv_filepath: str, current_processed_tokens: set) -> list:
    new_tokens = []
    if not os.path.exists(csv_filepath):
        logger.warning(f"Monitored CSV file {csv_filepath} not found.")
        return new_tokens
    
    try:
        with open(csv_filepath, mode='r', newline='', encoding='utf-8') as f:
            # Read all lines to get the latest content
            lines = f.readlines()
            if not lines:
                return new_tokens

            # Skip header if present
            start_idx = 1 if lines[0].strip().lower().startswith('address') else 0
            processed_lines = set()

            # Process lines in reverse order to get the most recent entries first
            for line in reversed(lines[start_idx:]):
                line = line.strip()
                if not line:  # Skip empty lines
                    continue

                # Extract token address - handle both CSV and plain text formats
                if ',' in line:
                    token_address = line.split(',')[0].strip()
                else:
                    token_address = line.strip()

                # Skip if we've already processed this token in this batch
                if token_address in processed_lines:
                    continue
                    
                processed_lines.add(token_address)

                if token_address and token_address not in current_processed_tokens:
                    new_tokens.append(token_address)
                    logger.info(f"Found new token to process: {token_address}")
                    
                    # Limit the number of new tokens to process in one batch
                    if len(new_tokens) >= 10:  # Process max 10 new tokens at a time
                        break

    except Exception as e:
        logger.error(f"Error reading CSV file {csv_filepath}: {e}", exc_info=True)
    
    logger.info(f"Found {len(new_tokens)} new tokens to process")
    return new_tokens

def initialize_driver(chrome_binary_path: str, driver_path: str | None = None) -> webdriver.Chrome | None:
    chrome_options = Options()
    if not chrome_binary_path:
        logger.error("Chrome binary path is not configured."); return None
    chrome_options.binary_location = chrome_binary_path
    chrome_options.add_argument("--start-maximized")
    try:
        service_args = {}
        if driver_path and os.path.exists(driver_path):
            service_args['executable_path'] = driver_path
        service = Service(**service_args)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logger.debug(f"[{threading.get_ident()}] WebDriver initialized.")
        return driver
    except WebDriverException as e: logger.error(f"WebDriverException on init: {e}"); return None
    except Exception as e: logger.error(f"Unexpected error on WebDriver init: {e}"); return None

def take_screenshot(driver, name="screenshot"):
    """Take a screenshot for debugging"""
    try:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        os.makedirs("debug_screenshots", exist_ok=True)
        filename = os.path.join("debug_screenshots", f"{name}_{timestamp}.png")
        driver.save_screenshot(filename)
        logger.info(f"Screenshot saved as {filename}")
        return filename
    except Exception as e:
        logger.error(f"Failed to take screenshot: {e}")
        return None

def is_element_visible(driver, by, value, timeout=10):
    """Check if element is visible and in viewport"""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((by, value))
        )
        return element.is_displayed() and element.is_enabled()
    except Exception as e:
        logger.debug(f"Element not visible: {value} - {str(e)}")
        return False

def switch_to_correct_frame(driver):
    """Switch to the correct iframe if needed"""
    try:
        # Try to find and switch to the main content iframe
        iframe = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "iframe"))
        )
        driver.switch_to.frame(iframe)
        logger.info("Switched to iframe")
        return True
    except Exception as e:
        logger.debug(f"No iframe found or error switching: {e}")
        return False

def click_element_with_fallback(driver, element, timeout: int = 10, max_attempts: int = 3, log_prefix: str = ""):
    """
    Enhanced element click with multiple fallback strategies and better error handling.
    """
    thread_id = f"Thread-{threading.get_ident()}"
    full_log_prefix = f"[{thread_id}] {log_prefix}" if log_prefix else f"[{thread_id}]"
    
    for attempt in range(1, max_attempts + 1):
        try:
            # Ensure element is in viewport
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
            time.sleep(0.5)  # Small delay for scroll to complete
            
            # Wait for element to be clickable
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(element)
            )
            
            # Try standard click first
            element.click()
            logger.debug(f"{full_log_prefix} Click successful on attempt {attempt}")
            return True
            
        except StaleElementReferenceException:
            logger.warning(f"{full_log_prefix} Element is stale, attempting to relocate")
            take_screenshot(driver, f"stale_element_attempt_{attempt}")
            time.sleep(1)
            continue
            
        except Exception as click_exc:
            logger.warning(f"{full_log_prefix} Standard click failed on attempt {attempt}: {str(click_exc)[:200]}")
            
            # Try JavaScript click as fallback
            try:
                driver.execute_script("arguments[0].click();", element)
                logger.debug(f"{full_log_prefix} JS click succeeded on attempt {attempt}")
                return True
            except Exception as js_exc:
                logger.warning(f"{full_log_prefix} JS click failed on attempt {attempt}: {str(js_exc)[:200]}")
                
                # Try one more approach - move to element and click
                try:
                    from selenium.webdriver.common.action_chains import ActionChains
                    ActionChains(driver).move_to_element(element).click().perform()
                    logger.debug(f"{full_log_prefix} ActionChains click succeeded on attempt {attempt}")
                    return True
                except Exception as ac_exc:
                    logger.warning(f"{full_log_prefix} ActionChains click failed: {str(ac_exc)[:200]}")
        
        if attempt < max_attempts:
            logger.info(f"{full_log_prefix} Retrying click in 1 second... (attempt {attempt + 1}/{max_attempts})")
            time.sleep(1)
    
    take_screenshot(driver, "click_failed_final")
    logger.error(f"{full_log_prefix} Failed to click element after {max_attempts} attempts")
    return False

def ensure_address_list_panel_open(driver):
    """Ensure the Address List panel is expanded."""
    try:
        header_btn = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//p[contains(text(),'Address List')]/ancestor::button"))
        )
        collapse_div = header_btn.find_element(By.XPATH, "following-sibling::div")
        collapsed = (
            'MuiCollapse-hidden' in collapse_div.get_attribute('class') or
            collapse_div.size.get('height', 0) == 0
        )
        if collapsed:
            logger.info(f"[{threading.get_ident()}] Address List panel is collapsed, clicking to expand.")
            header_btn.click()
            time.sleep(1) # Wait for animation
    except Exception as e:
        logger.debug(f"[{threading.get_ident()}] ensure_address_list_panel_open error: {e}")

def extract_initial_address_list_data(driver) -> list:
    TARGET_MAX_RANK_EXTRACTION = 10
    logger.debug(f"[{threading.get_ident()}] Attempting direct extraction (ranks 1-{TARGET_MAX_RANK_EXTRACTION}).")
    address_data_map = {}
    try:
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, "//p[contains(text(),'Address List')]"))
        )
        ensure_address_list_panel_open(driver)
        scroller = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@data-testid='virtuoso-scroller']"))
        )
        driver.execute_script("arguments[0].scrollTop = 0", scroller)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@data-testid='virtuoso-item-list']/div[@data-item-index='0']")))
        time.sleep(2)

        items_in_view = driver.find_elements(By.XPATH, "//div[@data-testid='virtuoso-item-list']/div[@data-item-index]")
        if not items_in_view: logger.warning(f"[{threading.get_ident()}] No items in list after scroll to top."); return []

        for item_container in items_in_view:
            if len(address_data_map) >= TARGET_MAX_RANK_EXTRACTION:
                logger.debug(f"[{threading.get_ident()}] All {TARGET_MAX_RANK_EXTRACTION} target ranks collected."); break
            try:
                btn = item_container.find_element(By.XPATH, ".//div[contains(@class, 'MuiListItemButton-root')]")

                rank_el = btn.find_element(By.XPATH, ".//span[starts-with(normalize-space(), '#')]")
                addr_el = btn.find_element(By.XPATH, ".//p[@aria-label]")
                perc_el = btn.find_element(By.XPATH, ".//span[contains(text(), '%')]")
                
                mui_box = None
                try: 
                    mui_box = btn.find_element(By.XPATH, "./div[contains(@class, 'MuiBox-root') and not(contains(@class, 'MuiCircularProgress-root'))][1]")
                except NoSuchElementException:
                    mui_box = addr_el.find_element(By.XPATH, "./preceding-sibling::div[contains(@class, 'MuiBox-root') and not(contains(@class, 'MuiCircularProgress-root'))][1]")
                
                mui_class = mui_box.get_attribute("class") if mui_box else "MuiBox-Not-Found"
                rank_txt = rank_el.text.strip().replace("#","")
                addr_txt = addr_el.text.strip()
                
                if not rank_txt.isdigit(): continue
                rank_int = int(rank_txt)

                if rank_int not in address_data_map and len(address_data_map) < TARGET_MAX_RANK_EXTRACTION:
                    perc_txt = perc_el.text.strip().replace("%","")
                    address_data_map[rank_int] = {'Rank':rank_txt,'Address':addr_txt,'Individual_Percentage':perc_txt,'MuiBox_Class_String':mui_class,'Cluster_Supply_Percentage':'N/A'}

            except (StaleElementReferenceException, NoSuchElementException) as e: 
                logger.debug(f"[{threading.get_ident()}] Stale/missing sub-element in item list: {e}"); continue
            except Exception as e: 
                logger.error(f"[{threading.get_ident()}] Error extracting from item: {e}")

        final_data = sorted(address_data_map.values(), key=lambda x: int(x['Rank']))
        logger.info(f"[{threading.get_ident()}] Extracted {len(final_data)} initial holder ranks.")
        return final_data
    except Exception as e: 
        logger.error(f"[{threading.get_ident()}] Err in extract_initial_address_list_data: {e}", exc_info=True)
        return []

def click_clusters_and_extract_supply_data(driver, initial_data_list: list) -> tuple[list, dict]:
    thread_id_str = f"Thread-{threading.get_ident()}"
    logger.info(f"[{thread_id_str}] Starting cluster click processing for {len(initial_data_list)} items.")
    
    take_screenshot(driver, "cluster_processing_start")

    if not isinstance(initial_data_list, list):
        logger.error(f"[{thread_id_str}] initial_data_list is not a list. Type: {type(initial_data_list)}. Aborting.")
        return [], {}

    switch_to_correct_frame(driver)
    
    try:
        scroller = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//div[@data-testid='virtuoso-scroller']"))
        )
    except TimeoutException:
        logger.error(f"[{thread_id_str}] Main scroller for address list not found. Cannot process clusters.")
        take_screenshot(driver, "scroller_not_found")
        return initial_data_list, {}

    processed_cluster_data = {}
    augmented_initial_data = initial_data_list[:]
    
    ensure_address_list_panel_open(driver)
    time.sleep(1)

    for item_index, item_data in enumerate(augmented_initial_data):
        if not isinstance(item_data, dict) or not all(k in item_data for k in ['Rank', 'Address', 'MuiBox_Class_String']):
            logger.warning(f"[{thread_id_str}] Skipping item at index {item_index} due to missing keys or incorrect type: {item_data}")
            continue

        rank_to_find_str = item_data['Rank']
        address_to_find = item_data['Address']
        current_muibox_class_string = item_data.get('MuiBox_Class_String', "MuiBox-Not-Found")
        
        logger.info(f"[{thread_id_str}] Processing Rank #{rank_to_find_str} - {address_to_find}")

        normalized_class = " ".join(current_muibox_class_string.split())
        individual_class_pattern = f"MuiBox-root {INDIVIDUAL_ADDRESS_MUIBOX_CLASS}"
        is_individual_wallet_visual = normalized_class == individual_class_pattern

        if is_individual_wallet_visual:
            item_data['Cluster_Supply_Percentage'] = '0'
            continue

        normalized_muibox_key = " ".join(sorted(current_muibox_class_string.split()))
        
        if normalized_muibox_key in processed_cluster_data:
            item_data['Cluster_Supply_Percentage'] = processed_cluster_data[normalized_muibox_key]
            continue

        logger.info(f"[{thread_id_str}] Rank #{rank_to_find_str} ({address_to_find}) is a NEW visual cluster. Finding & Clicking...")
        
        if not click_cluster_item(driver, scroller, rank_to_find_str, address_to_find, thread_id_str):
            logger.warning(f"[{thread_id_str}] Failed to find/click cluster for Rank #{rank_to_find_str}")
            item_data['Cluster_Supply_Percentage'] = 'Error:ClickFailed'
            continue
            
        extracted_supply_value = extract_cluster_supply(driver, thread_id_str, rank_to_find_str, normalized_muibox_key)
        
        item_data['Cluster_Supply_Percentage'] = extracted_supply_value
        processed_cluster_data[normalized_muibox_key] = extracted_supply_value
        
        take_screenshot(driver, f"cluster_{rank_to_find_str}_processed")
        time.sleep(1)
    
    logger.info(f"[{thread_id_str}] Completed processing {len(augmented_initial_data)} items. "
                 f"Processed {len(processed_cluster_data)} unique clusters.")
    
    return augmented_initial_data, processed_cluster_data

def click_cluster_item(driver, scroller, rank_to_find_str, address_to_find, thread_id_str, max_scroll_attempts=15):
    """Helper function to find and click a cluster item in the virtualized list."""
    driver.execute_script("arguments[0].scrollTop = 0", scroller)
    time.sleep(2)  # Wait for list to reset to the top

    for scroll_attempt in range(max_scroll_attempts):
        logger.debug(f"[{thread_id_str}] Scroll attempt {scroll_attempt + 1}/{max_scroll_attempts} to find Rank #{rank_to_find_str}")
        try:
            # Re-find elements on each attempt as the DOM changes in a virtualized list
            list_item_buttons = driver.find_elements(
                By.XPATH, 
                "//div[@data-testid='virtuoso-item-list']//div[contains(@class, 'MuiListItemButton-root')]"
            )
            
            if not list_item_buttons:
                logger.warning(f"[{thread_id_str}] No list item buttons found on attempt {scroll_attempt + 1}")

            for button in list_item_buttons:
                try:
                    rank_el = button.find_element(By.XPATH, ".//span[starts-with(normalize-space(), '#')]")
                    addr_el = button.find_element(By.XPATH, ".//p[@aria-label]")
                    
                    rank_text = rank_el.text.strip().replace('#', '')
                    addr_text = addr_el.text.strip()
                    
                    if rank_text == rank_to_find_str and addr_text == address_to_find:
                        logger.info(f"[{thread_id_str}] Found target Rank #{rank_to_find_str}. Attempting click.")
                        if click_element_with_fallback(
                            driver, button, timeout=10, max_attempts=3, log_prefix=f"Cluster Item #{rank_to_find_str}"
                        ):
                            logger.info(f"[{thread_id_str}] Successfully clicked on Rank #{rank_to_find_str}")
                            time.sleep(1.5)  # Wait for cluster details panel to appear
                            return True
                        else:
                            logger.error(f"[{thread_id_str}] Click FAILED for Rank #{rank_to_find_str} even with fallback.")
                            return False
                            
                except (NoSuchElementException, StaleElementReferenceException):
                    # This is expected in a virtual list, means this button is not our target or is stale. Continue to next.
                    continue
            
            # If we finished the loop and didn't find it, scroll down for the next attempt
            if scroll_attempt < max_scroll_attempts - 1:
                logger.debug(f"[{thread_id_str}] Rank #{rank_to_find_str} not in current view, scrolling down.")
                driver.execute_script("arguments[0].scrollTop += arguments[0].clientHeight * 0.85;", scroller)
                time.sleep(2)  # Wait for new items to render
                
        except Exception as e:
            logger.error(f"[{thread_id_str}] Unhandled error in click_cluster_item (attempt {scroll_attempt+1}): {e}", exc_info=True)
            take_screenshot(driver, f"error_click_cluster_{rank_to_find_str}_attempt_{scroll_attempt+1}")
            time.sleep(1)
    
    logger.error(f"[{thread_id_str}] FAILED to find and click cluster for Rank #{rank_to_find_str} after {max_scroll_attempts} attempts.")
    return False

def extract_cluster_supply(driver, thread_id_str, rank_to_find_str, normalized_muibox_key):
    """Helper function to extract the cluster supply percentage after clicking"""
    try:
        supply_el = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (By.XPATH, "//p[starts-with(normalize-space(),'Cluster Supply:')][1]")
            )
        )
        
        supply_text = supply_el.text.strip()
        match = re.search(r"Cluster Supply:\s*([\d\.]+)\s*%", supply_text)
        
        if match:
            extracted_value = match.group(1)
            logger.info(f"[{thread_id_str}] Extracted Cluster Supply for Rank {rank_to_find_str}: {extracted_value}%")
            return extracted_value
        else:
            logger.warning(f"[{thread_id_str}] Could not parse cluster supply from text: '{supply_text}' for Rank {rank_to_find_str}")
            return 'Error:Format'
            
    except TimeoutException:
        logger.warning(f"[{thread_id_str}] Timeout waiting for cluster supply element for Rank {rank_to_find_str}.")
        return 'N/A:TimeoutOnSupply'
        
    except Exception as e:
        error_type = type(e).__name__
        logger.warning(f"[{thread_id_str}] Error extracting cluster supply for Rank {rank_to_find_str}: {e}")
        take_screenshot(driver, f"error_extract_supply_{rank_to_find_str}_{error_type}")
        return f'N/A:ErrorOnSupply ({error_type})'

def save_address_data_txt(token_address, augmented_address_data, directory):
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, f"bubblemaps_data_{token_address}.txt")
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("Rank\tAddress\tIndividual_Percentage\tMuiBox_Class\tCluster_Supply_Percentage\n")
            for entry in augmented_address_data:
                f.write(f"{entry.get('Rank','N/A')}\t{entry.get('Address','N/A')}\t{entry.get('Individual_Percentage','N/A')}\t{entry.get('MuiBox_Class_String','N/A')}\t{entry.get('Cluster_Supply_Percentage','N/A')}\n")
    except IOError as e: logger.error(f"IOError saving to {filepath}: {e}")

def cleanup_token_data(token_address: str):
    """Remove the saved bubblemaps text file for a token if it exists."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(script_dir, EXTRACTED_DATA_DIR)
    filepath = os.path.join(target_dir, f"bubblemaps_data_{token_address}.txt")
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Deleted token data file {filepath}")
        else:
            logger.debug(f"Token data file not found for cleanup: {filepath}")
    except Exception as e:
        logger.error(f"Error deleting token data file {filepath}: {e}")

def save_cluster_summary_data(token_address: str, processed_cluster_data: dict):
    # This function must be called with CLUSTER_SUMMARY_LOCK acquired
    if not processed_cluster_data:
        num_distinct_clusters = 0
        global_cluster_percentage_sum_str = "0.00"
        individual_cluster_percentages_str = "N/A"
        token_status_eval = "CLEAN"
        status_color_eval = "GREEN"
    else:
        num_distinct_clusters = len(processed_cluster_data)
        global_sum_float = 0.0
        valid_percentages_list = []

        for cluster_key, perc_str_val in processed_cluster_data.items():
            try:
                if isinstance(perc_str_val, str) and not any(err in perc_str_val for err in ["N/A", "Error"]):
                    current_perc_float = float(perc_str_val)
                    global_sum_float += current_perc_float
                    # --- START: MODIFIED CODE ---
                    # Distinguish between real and synthetic clusters in the output
                    if cluster_key == SYNTHETIC_CLUSTER_KEY:
                        cluster_type_label = "TopIndividual"
                    else:
                        cluster_type_label = "VisualCluster"
                    valid_percentages_list.append(f"{cluster_type_label}:{current_perc_float:.2f}%")
                    # --- END: MODIFIED CODE ---
            except (ValueError, TypeError):
                logger.warning(f"[{threading.get_ident()}] Could not convert cluster supply '{perc_str_val}' for {token_address}, key '{cluster_key}'.")

        global_cluster_percentage_sum_str = f"{global_sum_float:.2f}"
        individual_cluster_percentages_str = "; ".join(valid_percentages_list) if valid_percentages_list else "None Valid"

        if global_sum_float == 0 and not valid_percentages_list:
             token_status_eval = "NO_VALID_CLUSTER_DATA"
             status_color_eval = "GREY"
        elif global_sum_float < 5.0:
            token_status_eval = "OPPORTUNITY"
            status_color_eval = "ORANGE"
        else:
            token_status_eval = "RISKY"
            status_color_eval = "RED"

    script_dir = os.path.dirname(os.path.abspath(__file__))
    summary_filepath = os.path.join(script_dir, CLUSTER_SUMMARY_FILE)

    os.makedirs(os.path.dirname(summary_filepath), exist_ok=True)
    file_needs_header = not (os.path.exists(summary_filepath) and os.path.getsize(summary_filepath) > 0)
    
    try:
        with open(summary_filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if file_needs_header:
                writer.writerow(['Token_Address','Num_Unique_Clusters','Global_Cluster_Percentage','Individual_Cluster_Percentages','Status','Status_Color'])
            writer.writerow([token_address,num_distinct_clusters,global_cluster_percentage_sum_str,individual_cluster_percentages_str,token_status_eval,status_color_eval])
            f.flush()
            os.fsync(f.fileno())
        logger.info(f"Saved cluster summary for {token_address}: Clusters={num_distinct_clusters}, GlobalSupply={global_cluster_percentage_sum_str}%, Status={token_status_eval}")
    except IOError as e:
        logger.error(f"Error saving cluster summary for {token_address}: {e}")

# The rest of the file remains the same...

def process_single_token_threaded(token_address_with_config: tuple):
    token_address, chrome_binary_path_config, chrome_driver_path_override_config = token_address_with_config
    thread_id_str = f"Thread-{threading.get_ident()}"
    thread_driver = None
    logger.info(f"[{thread_id_str}] --- Starting processing for token: {token_address} ---")

    try: # Outer try for driver initialization and final cleanup
        thread_driver = initialize_driver(chrome_binary_path_config, chrome_driver_path_override_config)
        if not thread_driver:
            return token_address, False

        bubblemaps_url = f"https://v2.bubblemaps.io/map?address={token_address}&chain=solana&limit=100"

        for attempt in range(MAX_BUBBLEMAPS_RETRIES):
            logger.info(f"[{thread_id_str}] Attempt {attempt + 1}/{MAX_BUBBLEMAPS_RETRIES} for token: {token_address}")
            attempt_successful = False
            try: # Inner try for a single attempt's logic
                if attempt > 0:
                    logger.info(f"[{thread_id_str}] Reloading page for attempt {attempt + 1}.")
                    thread_driver.refresh()
                else:
                    thread_driver.get(bubblemaps_url)

                WebDriverWait(thread_driver, 75).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                logger.info(f"[{thread_id_str}] Page readyState complete for {token_address} (Attempt {attempt + 1}).")

                try:
                    WebDriverWait(thread_driver, 45).until(
                        EC.visibility_of_element_located((By.XPATH, "//p[contains(text(),'Address List')]"))
                    )
                    ensure_address_list_panel_open(thread_driver)
                    logger.info(f"[{thread_id_str}] 'Address List' is visible for {token_address}.")
                    time.sleep(random.uniform(1.5, 2.5))
                except TimeoutException:
                    logger.error(f"[{thread_id_str}] Critical: 'Address List' not visible on attempt {attempt + 1}.")
                    if attempt < MAX_BUBBLEMAPS_RETRIES - 1: continue
                    else: return token_address, False

                # Data Freshness Check
                data_is_fresh = False
                try:
                    timestamp_el = WebDriverWait(thread_driver, 15).until(EC.presence_of_element_located((By.XPATH, "//p[contains(text(), 'a few seconds ago') or contains(text(), 'Live')]")))
                    logger.info(f"[{thread_id_str}] Data is fresh: '{timestamp_el.text.strip()}'.")
                    data_is_fresh = True
                except TimeoutException:
                    logger.warning(f"[{thread_id_str}] Data not immediately fresh. Attempting in-page refresh.")
                    try:
                        refresh_icon = WebDriverWait(thread_driver, 15).until(EC.element_to_be_clickable((By.XPATH, "//*[@data-testid='RefreshIcon']")))
                        click_element_with_fallback(thread_driver, refresh_icon, log_prefix=f"[{thread_id_str}] Refresh")
                        WebDriverWait(thread_driver, 60).until(EC.presence_of_element_located((By.XPATH, "//p[contains(text(), 'a few seconds ago') or contains(text(), 'Live')]")))
                        data_is_fresh = True
                        logger.info(f"[{thread_id_str}] Data fresh after in-page refresh.")
                    except Exception as e_refresh:
                        logger.error(f"[{thread_id_str}] In-page refresh failed or data still not fresh: {e_refresh}")

                if not data_is_fresh:
                    logger.error(f"[{thread_id_str}] CRITICAL: Data not fresh after all checks on attempt {attempt + 1}.")
                    if attempt < MAX_BUBBLEMAPS_RETRIES - 1: continue
                    else: return token_address, False

                # "Magic Nodes" Interaction
                try:
                    magic_nodes_button = WebDriverWait(thread_driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Magic Nodes')]")))
                    click_element_with_fallback(thread_driver, magic_nodes_button, timeout=5, max_attempts=2, log_prefix="[BUBBLEMAPS] Magic Nodes")
                    time.sleep(2)
                    logger.info(f"[BUBBLEMAPS] Clicked Magic Nodes for {token_address}")
                except Exception as e:
                    logger.warning(f"[BUBBLEMAPS] Magic Nodes button not found or not clickable: {e}")

                # Initial Data Extraction
                initial_data = extract_initial_address_list_data(thread_driver)
                if not initial_data:
                    logger.error(f"[{thread_id_str}] Failed to extract initial address list (Attempt {attempt + 1}).")
                    if attempt < MAX_BUBBLEMAPS_RETRIES - 1: continue
                    else: return token_address, False

                # Cluster Data Extraction
                logger.info(f"[{thread_id_str}] Attempting to click clusters and extract supply data.")
                aug_data, clusters_info = click_clusters_and_extract_supply_data(thread_driver, initial_data)
                
                # --- START: NEW LOGIC ---
                # Fallback: If no visual clusters were found, use the top individual holder as a synthetic cluster.
                if not clusters_info and aug_data:
                    logger.info(f"[{thread_id_str}] No visual clusters found. Searching for top individual holder to use as a proxy.")
                    
                    CONTRACT_IDENTIFIERS = ['pump', 'raydium', 'creator', 'squads']
                    top_individual_holder = None
                    
                    for holder_data in aug_data:
                        address_name = holder_data.get('Address', '').lower()
                        # Find the first holder that is NOT a known contract/LP address
                        if not any(sub in address_name for sub in CONTRACT_IDENTIFIERS):
                            top_individual_holder = holder_data
                            break # Found the first valid individual holder
                            
                    if top_individual_holder:
                        rank = top_individual_holder.get('Rank')
                        percentage = top_individual_holder.get('Individual_Percentage')
                        
                        logger.info(f"[{thread_id_str}] Treating Rank #{rank} (Individual Supply: {percentage}%) as the primary risk cluster.")
                        
                        # Create the synthetic cluster entry using its individual percentage
                        if percentage and percentage not in ('N/A', '0'):
                            clusters_info[SYNTHETIC_CLUSTER_KEY] = percentage
                        else:
                            logger.warning(f"[{thread_id_str}] Top individual holder (Rank #{rank}) had an invalid percentage: {percentage}")
                    else:
                        logger.warning(f"[{thread_id_str}] No visual clusters were found, and could not identify a top individual holder to use as a proxy.")
                # --- END: NEW LOGIC ---

                if not aug_data and initial_data:
                    logger.warning(f"[{thread_id_str}] No augmented data from cluster processing, but initial data existed.")
                else:
                    logger.info(f"[{thread_id_str}] Cluster processing completed.")

                # Save data and mark as successful
                save_address_data_txt(token_address, aug_data, EXTRACTED_DATA_DIR)
                if isinstance(clusters_info, dict):
                    with CLUSTER_SUMMARY_LOCK:
                        save_cluster_summary_data(token_address, clusters_info)
                else:
                    logger.warning(f"[{thread_id_str}] clusters_info was not a dict, type: {type(clusters_info)}. Skipping summary.")

                logger.info(f"[{thread_id_str}] Successfully processed and saved data for {token_address} on attempt {attempt + 1}.")
                attempt_successful = True
                break

            except NoSuchWindowException as e_critical:
                logger.error(f"[{thread_id_str}] CRITICAL DRIVER ERROR for {token_address}: {e_critical}. Window closed.")
                return token_address, False
            except Exception as e_attempt:
                logger.error(f"[{thread_id_str}] Error during attempt {attempt + 1} for token {token_address}: {e_attempt}", exc_info=True)

            if not attempt_successful and attempt < MAX_BUBBLEMAPS_RETRIES - 1:
                logger.info(f"[{thread_id_str}] Attempt {attempt + 1} failed for {token_address}. Retrying after delay...")
                time.sleep(random.uniform(5, 8))

        if attempt_successful:
            return token_address, True
        else:
            logger.error(f"[{thread_id_str}] All {MAX_BUBBLEMAPS_RETRIES} attempts failed for token {token_address}.")
            return token_address, False
    except Exception as e:
        logger.error(f"[{thread_id_str}] General error in process_single_token_threaded for {token_address}: {e}", exc_info=True)
        return token_address, False
    finally:
        if thread_driver:
            try:
                thread_driver.quit()
                logger.info(f"[{thread_id_str}] WebDriver quit for {token_address}.")
            except Exception as e_quit:
                logger.error(f"[{thread_id_str}] Error quitting WebDriver for {token_address}: {e_quit}")
    return token_address, False

def run_risk_detector_once():
    logger.info("Attempting to run risk_detector.py...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    risk_detector_script = os.path.join(script_dir, "risk_detector.py")
    if not os.path.exists(risk_detector_script):
        logger.error(f"risk_detector.py not found at {risk_detector_script}.")
        return False
    try:
        cluster_summary_path = os.path.join(script_dir, CLUSTER_SUMMARY_FILE)
        if not os.path.exists(cluster_summary_path):
            logger.error(f"Cluster summary file not found at {cluster_summary_path}")
            return False

        proc = subprocess.run(
            [sys.executable, risk_detector_script],
            check=True, cwd=script_dir, capture_output=True, text=True
        )
        logger.info("risk_detector.py completed successfully.")
        if proc.stdout: logger.info(f"Risk Detector STDOUT:\n{proc.stdout.strip()}")
        if proc.stderr: logger.warning(f"Risk Detector STDERR:\n{proc.stderr.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Risk detector failed with exit code {e.returncode}")
        if e.stdout: logger.error(f"STDOUT: {e.stdout}")
        if e.stderr: logger.error(f"STDERR: {e.stderr}")
    except Exception as e:
        logger.error(f"Error running risk_detector.py: {e}", exc_info=True)
    return False

def clean_duplicate_entries(csv_path: str) -> None:
    try:
        if not os.path.exists(csv_path): return
        with open(csv_path, 'r', newline='', encoding='utf-8') as f: lines = f.readlines()
        if not lines: return
            
        header = lines[0] if lines[0].strip().lower().startswith('address') else None
        seen = set()
        unique_lines = []
        
        for line in lines[1:] if header else lines:
            line = line.strip()
            if not line: continue
            token = line.split(',')[0].strip()
            if token and token not in seen:
                seen.add(token)
                unique_lines.append(line + '\n')
        
        if len(unique_lines) < (len(lines) - (1 if header else 0)):
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                if header: f.write(header)
                f.writelines(unique_lines)
            logger.info(f"Removed duplicates from {csv_path}")
    except Exception as e:
        logger.error(f"Error cleaning duplicate entries from {csv_path}: {e}")

def get_file_checksum(file_path: str) -> str:
    if not os.path.exists(file_path): return ""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""): hash_md5.update(chunk)
    return hash_md5.hexdigest()

def monitor_and_process_tokens(csv_fpath: str, chrome_bin_path: str, chrome_drv_path: str | None):
    clean_duplicate_entries(csv_fpath)
    processed_set = load_processed_tokens_threadsafe(OPENED_TOKENS_FILE)
    in_flight = set()
    newly_processed_count = 0
    last_checksum = get_file_checksum(csv_fpath)
    processed_since_last_run: set[str] = set()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="BubbleWorker") as executor:
            active_futures = {}
            while True:
                for fkey in [f for f in active_futures if f.done()]:
                    token_addr = active_futures.pop(fkey)
                    in_flight.discard(token_addr)
                    try:
                        _, success = fkey.result()
                        if success:
                            with PROCESSED_TOKENS_LOCK:
                                save_processed_token_threadsafe(OPENED_TOKENS_FILE, token_addr)
                                processed_set.add(token_addr)
                            logger.info(f"Bubblemaps SUCCESS for {token_addr}.")
                            newly_processed_count += 1
                            processed_since_last_run.add(token_addr)
                        else: logger.error(f"Bubblemaps FAILED for {token_addr}.")
                    except Exception as exc: logger.error(f"Token {token_addr} thread exception: {exc}")

                if newly_processed_count > 0 and not active_futures:
                    logger.info(f"{newly_processed_count} tokens updated. Triggering risk_detector.py.")
                    if run_risk_detector_once():
                        for addr in processed_since_last_run: cleanup_token_data(addr)
                        processed_since_last_run.clear()
                    newly_processed_count = 0

                current_checksum = get_file_checksum(csv_fpath)
                if current_checksum != last_checksum:
                    logger.info("CSV file changed, checking for new tokens.")
                    last_checksum = current_checksum
                    clean_duplicate_entries(csv_fpath)

                if len(active_futures) < MAX_WORKERS:
                    with PROCESSED_TOKENS_LOCK:
                        new_tokens = get_new_tokens_from_csv_threadsafe(csv_fpath, processed_set.union(in_flight))
                    
                    if new_tokens:
                        to_submit = new_tokens[:MAX_WORKERS - len(active_futures)]
                        logger.info(f"Submitting {len(to_submit)} new tokens: {', '.join(to_submit)}")
                        for token_s in to_submit:
                            in_flight.add(token_s)
                            fut = executor.submit(process_single_token_threaded, (token_s, chrome_bin_path, chrome_drv_path))
                            active_futures[fut] = token_s
                    elif not active_futures:
                        logger.info(f"No new tokens, no active tasks. Waiting {CHECK_INTERVAL}s...")
                        time.sleep(CHECK_INTERVAL)
                    else:
                        time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.critical(f"Main loop error: {e}", exc_info=True)
    finally:
        logger.info("Monitor loop finished.")

if __name__ == '__main__':
    logger.info("--- Starting Bubblemaps Extractor (Multi-Threaded) ---")
    cli_chrome = sys.argv[1] if len(sys.argv) > 1 else None
    actual_chrome = detect_chrome_binary_path(cli_chrome)
    if not actual_chrome: logger.error("Exiting: Chrome binary undetermined."); sys.exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_monitor_fpath = os.path.join(script_dir, CSV_FILE)
    data_dir_fpath = os.path.join(script_dir, EXTRACTED_DATA_DIR)
    os.makedirs(data_dir_fpath, exist_ok=True)

    monitor_and_process_tokens(csv_monitor_fpath, actual_chrome, CHROME_DRIVER_PATH)
    logging.info("--- Bubblemaps Extractor (Multi-Threaded) finished. ---")