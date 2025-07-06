#!/usr/bin/env python3
"""
Bubblemaps Extractor - Multi-Threaded (Multiple Windows)
"""
import sys
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
RANK1_AS_CLUSTER_KEY = "Rank1_Treated_As_Cluster" # Special key for Rank #1 if it's individual but treated as cluster

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

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(module)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler("bubblemaps_extractor_threaded.log", mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
setup_logging()

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
            logging.info(f"Chrome binary found at: {path_str}")
            return str(path_obj)
    logging.warning("Could not automatically detect Chrome binary path from common locations.")
    return None

def load_processed_tokens_threadsafe(filepath: str) -> set:
    with PROCESSED_TOKENS_LOCK:
        if not os.path.exists(filepath): return set()
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f if line.strip())
        except Exception as e:
            logging.error(f"Error loading processed tokens from {filepath}: {e}")
            return set()

def save_processed_token_threadsafe(filepath: str, token_address: str):
    try:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(token_address + '\n')
    except Exception as e:
        logging.error(f"Error saving processed token {token_address} to {filepath}: {e}")

def get_new_tokens_from_csv_threadsafe(csv_filepath: str, current_processed_tokens: set) -> list:
    new_tokens = []
    if not os.path.exists(csv_filepath):
        logging.warning(f"Monitored CSV file {csv_filepath} not found.")
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
                    logging.info(f"Found new token to process: {token_address}")
                    
                    # Limit the number of new tokens to process in one batch
                    if len(new_tokens) >= 10:  # Process max 10 new tokens at a time
                        break

    except Exception as e:
        logging.error(f"Error reading CSV file {csv_filepath}: {e}", exc_info=True)
    
    logging.info(f"Found {len(new_tokens)} new tokens to process")
    return new_tokens

def initialize_driver(chrome_binary_path: str, driver_path: str | None = None) -> webdriver.Chrome | None:
    chrome_options = Options()
    if not chrome_binary_path:
        logging.error("Chrome binary path is not configured."); return None
    chrome_options.binary_location = chrome_binary_path
    chrome_options.add_argument("--start-maximized")
    try:
        service_args = {}
        if driver_path and os.path.exists(driver_path):
            service_args['executable_path'] = driver_path
        service = Service(**service_args)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logging.debug(f"[{threading.get_ident()}] WebDriver initialized.")
        return driver
    except WebDriverException as e: logging.error(f"WebDriverException on init: {e}"); return None
    except Exception as e: logging.error(f"Unexpected error on WebDriver init: {e}"); return None

def click_element_with_fallback(driver, element, timeout: int = 10, max_attempts: int = 2, log_prefix: str = "") -> bool:
    """Attempt to click an element, falling back to JS if needed."""
    for attempt in range(1, max_attempts + 1):
        try:
            WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element))
            element.click()
            return True
        except Exception as click_exc:
            logging.debug(f"{log_prefix} standard click failed on attempt {attempt}: {click_exc}")
            try:
                driver.execute_script("arguments[0].click();", element)
                logging.debug(f"{log_prefix} JS click succeeded on attempt {attempt}")
                return True
            except Exception as js_exc:
                logging.debug(f"{log_prefix} JS click failed on attempt {attempt}: {js_exc}")
        time.sleep(0.5)
    logging.error(f"{log_prefix} Failed to click element after {max_attempts} attempts.")
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
            header_btn.click()
            time.sleep(1)
    except Exception as e:
        logging.debug(f"[{threading.get_ident()}] ensure_address_list_panel_open error: {e}")

def extract_initial_address_list_data(driver) -> list:
    TARGET_MAX_RANK_EXTRACTION = 10
    logging.debug(f"[{threading.get_ident()}] Attempting direct extraction (ranks 1-{TARGET_MAX_RANK_EXTRACTION}).")
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
        if not items_in_view: logging.warning(f"[{threading.get_ident()}] No items in list after scroll to top."); return []

        for item_container in items_in_view:
            if sum(1 for r_key in address_data_map if 1 <= r_key <= TARGET_MAX_RANK_EXTRACTION) >= TARGET_MAX_RANK_EXTRACTION and \
               all(r_chk in address_data_map for r_chk in range(1, TARGET_MAX_RANK_EXTRACTION + 1)):
                logging.debug(f"[{threading.get_ident()}] All {TARGET_MAX_RANK_EXTRACTION} target ranks collected."); break
            try:
                btn = item_container.find_element(By.XPATH, ".//div[contains(@class, 'MuiListItemButton-root')]")

                # The Bubblemaps site frequently changes its CSS classes. Instead
                # of relying on those volatile class names, locate elements using
                # stable patterns in the DOM structure. The rank always starts
                # with a '#' character, the address has an 'aria-label' attribute
                # and the percentage contains a '%' sign.
                rank_el = btn.find_element(By.XPATH, ".//span[starts-with(normalize-space(), '#')]")
                addr_el = btn.find_element(By.XPATH, ".//p[@aria-label]")
                perc_el = btn.find_element(By.XPATH, ".//span[contains(text(), '%')]")
                mui_box = None
                try: mui_box = btn.find_element(By.XPATH, "./div[contains(@class, 'MuiBox-root') and not(contains(@class, 'MuiCircularProgress-root'))][1]")
                except NoSuchElementException:
                    mui_box = addr_el.find_element(By.XPATH, "./preceding-sibling::div[contains(@class, 'MuiBox-root') and not(contains(@class, 'MuiCircularProgress-root'))][1]")
                mui_class = mui_box.get_attribute("class") if mui_box else "MuiBox-Not-Found"
                rank_txt, addr_txt = rank_el.text.strip().replace("#",""), addr_el.text.strip()
                if not rank_txt.isdigit(): continue
                rank_int = int(rank_txt)
                if 1 <= rank_int <= TARGET_MAX_RANK_EXTRACTION and rank_int not in address_data_map:
                    perc_txt = perc_el.text.strip().replace("%","")
                    address_data_map[rank_int] = {'Rank':rank_txt,'Address':addr_txt,'Individual_Percentage':perc_txt,'MuiBox_Class_String':mui_class,'Cluster_Supply_Percentage':'N/A'}
                elif rank_int > TARGET_MAX_RANK_EXTRACTION and address_data_map: break
            except (StaleElementReferenceException, NoSuchElementException): logging.debug(f"[{threading.get_ident()}] Stale/missing sub-element in item."); continue
            except Exception as e: logging.error(f"[{threading.get_ident()}] Error extracting from item: {e}")
        final_data = [address_data_map[r] for r in sorted(address_data_map.keys()) if 1<=r<=TARGET_MAX_RANK_EXTRACTION]
        logging.debug(f"[{threading.get_ident()}] Extracted {len(final_data)} for ranks 1-{TARGET_MAX_RANK_EXTRACTION}.")
        return final_data
    except Exception as e: logging.error(f"[{threading.get_ident()}] Err in extract_initial_address_list_data: {e}", exc_info=True); return []

# --- MODIFIED FUNCTION ---
def click_clusters_and_extract_supply_data(driver, initial_data_list: list) -> tuple[list, dict]:
    thread_id_str = f"Thread-{threading.get_ident()}"
    logging.debug(f"[{thread_id_str}] Starting cluster click processing for {len(initial_data_list)} items.")

    # Ensure initial_data_list is a list
    if not isinstance(initial_data_list, list):
        logging.error(f"[{thread_id_str}] initial_data_list is not a list. Received type: {type(initial_data_list)}. Aborting cluster processing.")
        return [], {}

    try:
        scroller = WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.XPATH, "//div[@data-testid='virtuoso-scroller']")))
    except TimeoutException:
        logging.error(f"[{thread_id_str}] Main scroller for address list not found. Cannot process clusters.")
        return initial_data_list, {} # Return original data and empty cluster info

    processed_cluster_data = {} # Stores {cluster_muibox_key: supply_percentage_str}
    augmented_initial_data = initial_data_list[:] # Create a copy to modify

    # This function is called AFTER "Magic Nodes" button is clicked.
    # We assume initial_data_list now contains the "Magic Nodes" or linked wallets
    # as they appear in the side panel list.

    for item_index, item_data in enumerate(augmented_initial_data):
        if not isinstance(item_data, dict) or not all(k in item_data for k in ['Rank', 'Address', 'MuiBox_Class_String', 'Individual_Percentage']):
            logging.warning(f"[{thread_id_str}] Skipping item at index {item_index} due to missing keys or incorrect type: {item_data}")
            continue

        rank_to_find_str = item_data['Rank']
        address_to_find = item_data['Address']
        current_muibox_class_string = item_data.get('MuiBox_Class_String', "MuiBox-Not-Found")
        # cluster_id_for_log can be address or rank, using address for more specific logging
        cluster_id_for_log = address_to_find 

        is_rank_one = (rank_to_find_str == "1")
        normalized_class = " ".join(current_muibox_class_string.split())
        individual_class_check = f"MuiBox-root {INDIVIDUAL_ADDRESS_MUIBOX_CLASS}"
        is_visually_individual_in_list = normalized_class == individual_class_check

        # Handle Rank #1: if visually individual, its own percentage is its cluster data.
        if is_rank_one and is_visually_individual_in_list:
            logging.info(f"[{thread_id_str}] Magic Nodes: Rank #1 ({address_to_find}) is visually individual. Treating its own holdings as its cluster data.")
            individual_perc_str = item_data.get('Individual_Percentage', '0')
            item_data['Cluster_Supply_Percentage'] = individual_perc_str
            processed_cluster_data[RANK1_AS_CLUSTER_KEY] = individual_perc_str
            # Log Rank 1 special handling as a "found" cluster
            try:
                perc_float_for_log = float(individual_perc_str)
                logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log} (Rank 1 Individual), found {perc_float_for_log:.2f}%")
            except ValueError:
                 logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log} (Rank 1 Individual), found {individual_perc_str}%")
            continue

        # For other items (or Rank 1 not visually individual) in Magic Nodes view:
        # Use MuiBox class string to group items that might belong to the same underlying cluster visually.
        # If Magic Nodes mode makes all MuiBox strings unique, this still works fine (each becomes its own key).
        # If "MuiBox-Not-Found" or an empty string, use a default key or the address itself to ensure uniqueness if needed.
        if not current_muibox_class_string or current_muibox_class_string == "MuiBox-Not-Found":
            # Fallback key if MuiBox class is missing/invalid in Magic Nodes context for an item
            # Using address ensures we try to process it if it's not Rank 1 individual.
            normalized_muibox_key = f"MagicNodeAddress_{address_to_find}"
        else:
            normalized_muibox_key = " ".join(sorted(current_muibox_class_string.split()))

        if normalized_muibox_key in processed_cluster_data:
            item_data['Cluster_Supply_Percentage'] = processed_cluster_data[normalized_muibox_key]
            logging.debug(f"[{thread_id_str}] Magic Node item Rank #{rank_to_find_str} ({address_to_find}) belongs to already processed visual group '{normalized_muibox_key}'. Skipping click.")
            continue

        logging.info(f"[{thread_id_str}] Magic Node item: Rank #{rank_to_find_str} ({address_to_find}), MuiBoxKey: '{normalized_muibox_key}'. Attempting click to find cluster supply.")
        
        target_button_element = None
        found_item_for_processing = False
        for scroll_attempt in range(7): # Max 7 scroll attempts
            list_item_buttons_xpath = "//div[@data-testid='virtuoso-item-list']//div[contains(@class, 'MuiListItemButton-root')]"
            visible_buttons = driver.find_elements(By.XPATH, list_item_buttons_xpath)
            for button_element_candidate in visible_buttons:
                try:
                    rank_text_candidate = button_element_candidate.find_element(By.XPATH, ".//span[starts-with(normalize-space(), '#')]").text.strip().replace('#', '')
                    addr_text_candidate = button_element_candidate.find_element(By.XPATH, ".//p[@aria-label]").text.strip()
                    if rank_text_candidate == rank_to_find_str and addr_text_candidate == address_to_find:
                        target_button_element = button_element_candidate
                        found_item_for_processing = True
                        break
                except (NoSuchElementException, StaleElementReferenceException): continue
            if found_item_for_processing: break
            if scroll_attempt < 6:
                driver.execute_script("arguments[0].scrollTop += arguments[0].clientHeight * 0.75;", scroller)
                time.sleep(1.5)

        if target_button_element:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center', behavior: 'smooth'});", target_button_element)
                time.sleep(0.7)

                clicked_successfully = click_element_with_fallback(driver, target_button_element, timeout=10, max_attempts=3, log_prefix=f"[{thread_id_str}] Magic Node Item")
                
                extracted_supply_value = 'N/A' # Default
                
                if clicked_successfully:
                    logging.info(f"[{thread_id_str}] Successfully clicked Magic Node list item: Rank #{rank_to_find_str} ({address_to_find}). Waiting 1s.")
                    time.sleep(1) # Explicit 1-second wait after click, before extraction

                    try:
                        # XPath for the "Cluster Supply: X.XX%" text. This is critical.
                        # It might change in Magic Nodes mode.
                        supply_el_xpath = "//p[starts-with(normalize-space(),'Cluster Supply:')][1]"
                        supply_el = WebDriverWait(driver,15).until(EC.visibility_of_element_located((By.XPATH,supply_el_xpath)))
                        
                        raw_supply_text = supply_el.text.strip()
                        match = re.search(r"Cluster Supply:\s*([\d\.]+)\s*%", raw_supply_text)
                        if match:
                            extracted_supply_value = match.group(1)
                            try:
                                perc_float_for_log = float(extracted_supply_value)
                                logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found {perc_float_for_log:.2f}%")
                            except ValueError:
                                logging.warning(f"[{thread_id_str}] Could not convert supply '{extracted_supply_value}' to float for logging for {cluster_id_for_log}.")
                                logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found {extracted_supply_value}%") # Log with string
                        else:
                            extracted_supply_value = 'Error:Format'
                            logging.warning(f"[{thread_id_str}] Could not parse cluster supply from text: '{raw_supply_text}' for Rank {rank_to_find_str} ({address_to_find})")
                            logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found Format Error (Text: {raw_supply_text})")


                    except TimeoutException:
                        extracted_supply_value = 'N/A:TimeoutOnSupply'
                        logging.warning(f"[{thread_id_str}] Timeout waiting for cluster supply element for '{normalized_muibox_key}' (Rank {rank_to_find_str}, Addr {address_to_find}).")
                        logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found N/A (Timeout)")
                    except Exception as e_supply_extract:
                        extracted_supply_value = f'N/A:ErrorOnSupply ({type(e_supply_extract).__name__})'
                        logging.warning(f"[{thread_id_str}] Error extracting cluster supply for '{normalized_muibox_key}' (Rank {rank_to_find_str}, Addr {address_to_find}): {e_supply_extract}")
                        logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found N/A (Extraction Error)")
                else: # click_element_with_fallback failed
                    logging.warning(f"[{thread_id_str}] Failed to click Magic Node list item: Rank #{rank_to_find_str} ({address_to_find}).")
                    extracted_supply_value = 'Error:ClickFailedOnNode'
                    # Log failure to click as per requirements
                    logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found N/A (Click Failed)")


                item_data['Cluster_Supply_Percentage'] = extracted_supply_value
                processed_cluster_data[normalized_muibox_key] = extracted_supply_value
                time.sleep(0.5) # Small pause for UI to settle, original code had time.sleep(1) here

            except StaleElementReferenceException:
                logging.warning(f"[{thread_id_str}] StaleElementReferenceException processing Magic Node Rank #{rank_to_find_str}. Item might have changed.")
                item_data['Cluster_Supply_Percentage'] = 'Error:StaleElementOnClick'
                processed_cluster_data[normalized_muibox_key] = 'Error:StaleElementOnClick'
                logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found N/A (Stale Element)")
            except TimeoutException:
                logging.warning(f"[{thread_id_str}] TimeoutException processing Magic Node Rank #{rank_to_find_str}. Element not clickable or disappeared.")
                item_data['Cluster_Supply_Percentage'] = 'Error:TimeoutOnClick'
                processed_cluster_data[normalized_muibox_key] = 'Error:TimeoutOnClick'
                logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found N/A (Timeout On Click)")
            except Exception as e_click_general:
                logging.error(f"[{thread_id_str}] General error processing Magic Node Rank #{rank_to_find_str} ({address_to_find}): {e_click_general}", exc_info=True)
                error_type_name = type(e_click_general).__name__
                item_data['Cluster_Supply_Percentage'] = f'Error:ProcessingFailed ({error_type_name})'
                processed_cluster_data[normalized_muibox_key] = f'Error:ProcessingFailed ({error_type_name})'
                logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found N/A (General Error: {error_type_name})")
        else: # target_button_element not found
            logging.warning(f"[{thread_id_str}] Could not find Magic Node item on page for Rank #{rank_to_find_str} ({address_to_find}) after scroll attempts.")
            item_data['Cluster_Supply_Percentage'] = 'Error:ItemNotFoundOnPage'
            # Do not add to processed_cluster_data if item itself not found, as MuiBox key might be unreliable
            # Log this outcome as per requirements
            logging.info(f"[CLUSTER] Clicked linked wallet: {cluster_id_for_log}, found N/A (Item Not Found In List)")

    return augmented_initial_data, processed_cluster_data

# --- END MODIFIED FUNCTION ---

def collect_rank_cluster_percentages(augmented_address_data: list) -> dict:
    """Create a mapping of Rank -> cluster percentage from the augmented data."""
    rank_map: OrderedDict[str, str] = OrderedDict()
    for entry in augmented_address_data:
        rank = entry.get("Rank")
        if not rank:
            continue
        percent_val = entry.get("Cluster_Supply_Percentage", "N/A")
        rank_key = f"Rank #{rank}"
        logging.info(f"Cluster Rank {rank_key} → {percent_val}%")
        rank_map[rank_key] = percent_val
    return rank_map

def save_address_data_txt(token_address, augmented_address_data, directory):
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, f"bubblemaps_data_{token_address}.txt")
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("Rank\tAddress\tIndividual_Percentage\tMuiBox_Class\tCluster_Supply_Percentage\n")
            for entry in augmented_address_data:
                f.write(f"{entry.get('Rank','N/A')}\t{entry.get('Address','N/A')}\t{entry.get('Individual_Percentage','N/A')}\t{entry.get('MuiBox_Class_String','N/A')}\t{entry.get('Cluster_Supply_Percentage','N/A')}\n")
    except IOError as e: logging.error(f"IOError saving to {filepath}: {e}")

def cleanup_token_data(token_address: str):
    """Remove the saved bubblemaps text file for a token if it exists."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(script_dir, EXTRACTED_DATA_DIR)
    filepath = os.path.join(target_dir, f"bubblemaps_data_{token_address}.txt")
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.info(f"Deleted token data file {filepath}")
        else:
            logging.debug(f"Token data file not found for cleanup: {filepath}")
    except Exception as e:
        logging.error(f"Error deleting token data file {filepath}: {e}")

# --- MODIFIED FUNCTION ---
def save_cluster_summary_data(token_address: str, processed_cluster_data: dict): # Renamed arg
    # This function must be called with CLUSTER_SUMMARY_LOCK acquired

    # processed_cluster_data now contains either {normalized_mui_box_key: supply_perc} OR {RANK1_AS_CLUSTER_KEY: individual_perc}

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
                if isinstance(perc_str_val, str):
                    perc_clean = perc_str_val.replace('%', '')
                else:
                    perc_clean = str(perc_str_val)
                if not any(err_indicator in perc_clean for err_indicator in ["N/A", "Error", "Pending"]):
                    current_perc_float = float(perc_clean)
                    global_sum_float += current_perc_float
                    if cluster_key == RANK1_AS_CLUSTER_KEY or cluster_key.startswith("Rank #1"):
                        valid_percentages_list.append(f"Rank#1:{current_perc_float:.2f}%")
                    elif str(cluster_key).startswith("Rank #"):
                        valid_percentages_list.append(f"{cluster_key}:{current_perc_float:.2f}%")
                    else:
                        valid_percentages_list.append(f"VisualCluster:{current_perc_float:.2f}%")
            except ValueError:
                logging.warning(f"[{threading.get_ident()}] ValueError converting cluster supply '{perc_str_val}' for {token_address}, key '{cluster_key}'.")

        global_cluster_percentage_sum_str = f"{global_sum_float:.2f}"
        individual_cluster_percentages_str = "; ".join(valid_percentages_list) if valid_percentages_list else "None Valid"

        if global_sum_float == 0 and not valid_percentages_list:
             token_status_eval = "NO_VALID_CLUSTER_DATA"
             status_color_eval = "GREY"
        elif global_sum_float < 5.0: # Threshold for OPPORTUNITY
            token_status_eval = "OPPORTUNITY"
            status_color_eval = "ORANGE"
        else: # >= 5.0 is RISKY
            token_status_eval = "RISKY"
            status_color_eval = "RED"

    script_dir = os.path.dirname(os.path.abspath(__file__))
    summary_filepath = os.path.join(script_dir, CLUSTER_SUMMARY_FILE)

    # Ensure directory exists
    os.makedirs(os.path.dirname(summary_filepath), exist_ok=True)

    file_needs_header = not (os.path.exists(summary_filepath) and os.path.getsize(summary_filepath) > 0)
    try:
        with open(summary_filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if file_needs_header:
                writer.writerow(['Token_Address','Num_Unique_Clusters','Global_Cluster_Percentage','Individual_Cluster_Percentages','Status','Status_Color'])
                logging.info(f"Created new cluster summary file with headers at {summary_filepath}")
            writer.writerow([token_address,num_distinct_clusters,global_cluster_percentage_sum_str,individual_cluster_percentages_str,token_status_eval,status_color_eval])
            f.flush()  # Force write to disk
            os.fsync(f.fileno())  # Ensure it's written to disk
        logging.info(f"Saved cluster summary for {token_address}: Clusters={num_distinct_clusters}, GlobalSupply={global_cluster_percentage_sum_str}%, Status={token_status_eval}")
    except IOError as e:
        logging.error(f"Error saving cluster summary for {token_address}: {e}")
        logging.error(f"File path: {summary_filepath}")
        logging.error(f"Directory exists: {os.path.exists(os.path.dirname(summary_filepath))}")
        logging.error(f"Directory writable: {os.access(os.path.dirname(summary_filepath), os.W_OK)}")

def process_single_token_threaded(token_address_with_config: tuple):
    token_address, chrome_binary_path_config, chrome_driver_path_override_config = token_address_with_config
    thread_id_str = f"Thread-{threading.get_ident()}"
    thread_driver = None
    logging.info(f"[{thread_id_str}] --- Starting processing for token: {token_address} ---")

    try: # Outer try for driver initialization and final cleanup
        thread_driver = initialize_driver(chrome_binary_path_config, chrome_driver_path_override_config)
        if not thread_driver:
            return token_address, False

        bubblemaps_url = f"https://v2.bubblemaps.io/map?address={token_address}&chain=solana&limit=100"

        for attempt in range(MAX_BUBBLEMAPS_RETRIES):
            logging.info(f"[{thread_id_str}] Attempt {attempt + 1}/{MAX_BUBBLEMAPS_RETRIES} for token: {token_address}")
            attempt_successful = False
            try: # Inner try for a single attempt's logic
                # Initial Navigation and Page Load
                if attempt > 0: # Reload page on retries
                    logging.info(f"[{thread_id_str}] Reloading page for attempt {attempt + 1}.")
                    thread_driver.refresh()
                else: # First attempt, just load
                    thread_driver.get(bubblemaps_url)

                WebDriverWait(thread_driver, 75).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                logging.info(f"[{thread_id_str}] Page readyState complete for {token_address} (Attempt {attempt + 1}).")

                # Wait for critical content 'Address List' before proceeding
                try:
                    WebDriverWait(thread_driver, 45).until(
                        EC.visibility_of_element_located((By.XPATH, "//p[contains(text(),'Address List')]"))
                    )
                    ensure_address_list_panel_open(thread_driver)
                    logging.info(f"[{thread_id_str}] Initial 'Address List' is visible for {token_address} (Attempt {attempt + 1}).")
                    time.sleep(random.uniform(1.5, 2.5))
                except TimeoutException:
                    logging.error(f"[{thread_id_str}] Critical: 'Address List' not visible on attempt {attempt + 1}.")
                    if attempt < MAX_BUBBLEMAPS_RETRIES - 1: continue
                    else: return token_address, False # All retries failed for this critical step

                # 1. Data Freshness Check
                data_is_fresh = False
                try:
                    timestamp_el = WebDriverWait(thread_driver, 15).until(EC.presence_of_element_located((By.XPATH, "//p[contains(@class, 'MuiTypography-root') and contains(@class, 'css-fm451k')]")))
                    ts_text = timestamp_el.text.strip().lower()
                    logging.info(f"[{thread_id_str}] Timestamp: '{ts_text}' (Attempt {attempt + 1}).")
                    if "a few seconds ago" in ts_text or "live" in ts_text:
                        data_is_fresh = True
                        logging.info(f"[{thread_id_str}] Data is fresh (Attempt {attempt + 1}).")
                except TimeoutException:
                    logging.warning(f"[{thread_id_str}] Timestamp element not found (Attempt {attempt + 1}). Assuming refresh needed.")
                except Exception as e_ts:
                    logging.warning(f"[{thread_id_str}] Error checking timestamp (Attempt {attempt + 1}): {e_ts}. Assuming refresh needed.")

                if not data_is_fresh:
                    logging.info(f"[{thread_id_str}] Data not fresh. Attempting in-page refresh (Attempt {attempt + 1}).")
                    refresh_icon_found_and_clicked = False
                    try:
                        # Try to find and click the refresh icon
                        refresh_icon = WebDriverWait(thread_driver, 15).until(
                            EC.element_to_be_clickable((By.XPATH, "//*[@data-testid='RefreshIcon']"))
                        )
                        thread_driver.execute_script("arguments[0].scrollIntoView(true);", refresh_icon)
                        time.sleep(0.5)
                        click_element_with_fallback(thread_driver, refresh_icon, timeout=5, max_attempts=3, log_prefix=f"[{thread_id_str}] Refresh")
                        logging.info(f"[{thread_id_str}] Clicked in-page refresh (Attempt {attempt + 1}).")
                        refresh_icon_found_and_clicked = True
                    except TimeoutException:
                        logging.warning(f"[{thread_id_str}] Refresh icon (data-testid='RefreshIcon') not found or not clickable for in-page refresh (Attempt {attempt + 1}). Continuing process as per request.")
                        # data_is_fresh remains False, will proceed to full browser refresh if needed.
                    except Exception as e_icon_click:
                        logging.warning(f"[{thread_id_str}] Error finding/clicking refresh icon during in-page refresh (Attempt {attempt + 1}): {e_icon_click}. Continuing.")
                        # data_is_fresh remains False.

                    if refresh_icon_found_and_clicked:
                        try:
                            # If icon was clicked, now wait for list and re-check freshness
                            WebDriverWait(thread_driver, 60).until(
                                EC.visibility_of_element_located((By.XPATH, "//p[contains(text(),'Address List')]"))
                            )
                            ensure_address_list_panel_open(thread_driver)
                            time.sleep(random.uniform(4, 6)) # Wait for list to reload
                            timestamp_el = WebDriverWait(thread_driver, 15).until(
                                EC.presence_of_element_located((By.XPATH, "//p[contains(@class, 'MuiTypography-root') and contains(@class, 'css-fm451k')]"))
                            )
                            ts_text = timestamp_el.text.strip().lower()
                            if "a few seconds ago" in ts_text or "live" in ts_text:
                                data_is_fresh = True
                                logging.info(f"[{thread_id_str}] Data fresh after successful in-page refresh (Attempt {attempt + 1}).")
                            else:
                                logging.warning(f"[{thread_id_str}] Data still not fresh after in-page refresh completed. Timestamp: '{ts_text}' (Attempt {attempt + 1}).")
                        except Exception as e_after_click:
                            logging.warning(f"[{thread_id_str}] Error after clicking in-page refresh (e.g., waiting for list or re-checking freshness) (Attempt {attempt + 1}): {e_after_click}.")
                            # data_is_fresh remains as it was before this inner try (likely False).
                    # If refresh_icon_found_and_clicked is False, data_is_fresh is also False (or its previous state),
                    # and the code will naturally proceed to the full browser refresh check if needed.

                if not data_is_fresh:
                    logging.info(f"[{thread_id_str}] Data still not fresh. Attempting full browser refresh (Attempt {attempt + 1}).")
                    try:
                        thread_driver.refresh()
                        WebDriverWait(thread_driver, 75).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                        WebDriverWait(thread_driver, 60).until(
                            EC.visibility_of_element_located((By.XPATH, "//p[contains(text(),'Address List')]"))
                        )
                        ensure_address_list_panel_open(thread_driver)
                        time.sleep(random.uniform(4,6))
                        # Re-check freshness again
                        timestamp_el = WebDriverWait(thread_driver, 15).until(EC.presence_of_element_located((By.XPATH, "//p[contains(@class, 'MuiTypography-root') and contains(@class, 'css-fm451k')]")))
                        ts_text = timestamp_el.text.strip().lower()
                        if "a few seconds ago" in ts_text or "live" in ts_text:
                            data_is_fresh = True
                            logging.info(f"[{thread_id_str}] Data fresh after full browser refresh (Attempt {attempt + 1}).")
                        else:
                            logging.error(f"[{thread_id_str}] Data NOT fresh after all refresh attempts (Attempt {attempt + 1}). Timestamp: '{ts_text}'.")
                    except Exception as e_full_refresh:
                        logging.error(f"[{thread_id_str}] Full browser refresh failed or data still not fresh (Attempt {attempt + 1}): {e_full_refresh}.")

                if not data_is_fresh:
                    logging.error(f"[{thread_id_str}] CRITICAL: Data not fresh after all checks and refreshes on attempt {attempt + 1}.")
                    if attempt < MAX_BUBBLEMAPS_RETRIES - 1: continue
                    else: return token_address, False


                # 2. "Magic Nodes" Interaction (replaces "Show Contract/Exchanges")
                try:
                    magic_nodes_button = WebDriverWait(thread_driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Magic Nodes')]"))
                    )
                    click_element_with_fallback(thread_driver, magic_nodes_button, timeout=5, max_attempts=2, log_prefix="[BUBBLEMAPS] Magic Nodes")
                    time.sleep(2)
                    logging.info(f"[BUBBLEMAPS] Clicked Magic Nodes for {token_address}")
                except Exception as e:
                    logging.warning(f"[BUBBLEMAPS] Magic Nodes button not found or not clickable: {e}")

                # 3. Initial Data Extraction & Rank 01 Check
                initial_data = extract_initial_address_list_data(thread_driver)
                if not initial_data:
                    logging.error(f"[{thread_id_str}] Failed to extract initial address list (Attempt {attempt + 1}).")
                    if attempt < MAX_BUBBLEMAPS_RETRIES - 1: continue
                    else: return token_address, False

                rank_1_found = any(item.get('Rank') == '1' for item in initial_data)
                if not rank_1_found:
                    logging.error(f"[{thread_id_str}] CRITICAL: Rank #1 data not found in initial list (Attempt {attempt + 1}).")
                    if attempt < MAX_BUBBLEMAPS_RETRIES - 1: continue
                    else: return token_address, False
                logging.info(f"[{thread_id_str}] Rank #1 found in initial data (Attempt {attempt + 1}).")

                # 4. Cluster Data Extraction (Presence of clusters is optional, but function should not error)
                logging.info(f"[{thread_id_str}] Attempting to click clusters and extract supply data (Attempt {attempt + 1}).")
                aug_data, _ = click_clusters_and_extract_supply_data(thread_driver, initial_data)
                # aug_data now contains cluster supply values for each rank
                if not aug_data and initial_data: # If we had initial data but got no augmented data, it's a bit suspicious but not a hard fail for retry unless an exception occurred.
                    logging.warning(f"[{thread_id_str}] No augmented data from click_clusters_and_extract_supply_data, but initial data existed (Attempt {attempt + 1}).")
                elif not initial_data and not aug_data:
                    logging.info(f"[{thread_id_str}] No initial or augmented data, likely an empty/new token (Attempt {attempt + 1}).")
                else:
                    logging.info(f"[{thread_id_str}] click_clusters_and_extract_supply_data completed (Attempt {attempt + 1}).")

                # If all checks passed and critical data extracted:
                save_address_data_txt(token_address, aug_data, EXTRACTED_DATA_DIR)
                rank_cluster_map = collect_rank_cluster_percentages(aug_data)
                with CLUSTER_SUMMARY_LOCK:
                    save_cluster_summary_data(token_address, rank_cluster_map)

                logging.info(f"[{thread_id_str}] Successfully processed and saved data for {token_address} on attempt {attempt + 1}.")
                attempt_successful = True
                break # Exit the retry loop as this attempt was successful

            except NoSuchWindowException as e_critical_driver_attempt:
                logging.error(f"[{thread_id_str}] CRITICAL DRIVER ERROR during attempt {attempt + 1} for {token_address}: {e_critical_driver_attempt}. Window closed or driver crashed.")
                return token_address, False # Critical, cannot recover this thread's driver
            except Exception as e_attempt:
                logging.error(f"[{thread_id_str}] Error during attempt {attempt + 1} for token {token_address}: {e_attempt}", exc_info=True)
                # This catch-all will handle unexpected errors during an attempt.
                # If it's not the last attempt, the loop will naturally continue after a delay (handled below).

            if attempt_successful:
                return token_address, True # Successful attempt, exit function

            # If this attempt was not successful and it's not the last attempt
            if not attempt_successful and attempt < MAX_BUBBLEMAPS_RETRIES - 1:
                logging.info(f"[{thread_id_str}] Attempt {attempt + 1} failed for {token_address}. Retrying after delay...")
                time.sleep(random.uniform(5, 8)) # General delay before next attempt

        # After the for loop, check the outcome
        if attempt_successful: # This flag is from the scope of the attempt that successfully broke the loop
            # The success log ("Successfully processed and saved data...") would have already been printed within the successful attempt.
            return token_address, True
        else:
            # This 'else' is reached if the loop completed all iterations without 'attempt_successful' becoming True.
            # Critical driver errors (NoSuchWindowException) would have returned False earlier and exited the function.
            logging.error(f"[{thread_id_str}] All {MAX_BUBBLEMAPS_RETRIES} attempts failed for token {token_address} after exhausting retries.")
            return token_address, False
    except NoSuchWindowException:
        logging.error(f"[{thread_id_str}] Browser window closed unexpectedly for token: {token_address}")
        # No driver to quit here as it's already gone.
        thread_driver = None # Ensure it's None so finally block doesn't try to quit again
        return token_address, False
    except WebDriverException as e_wd:
        logging.error(f"[{thread_id_str}] WebDriverException for {token_address}: {e_wd}")
        return token_address, False
    except Exception as e:
        logging.error(f"[{thread_id_str}] General error in process_single_token_threaded for {token_address}: {e}", exc_info=True)
        return token_address, False
    finally:
        if thread_driver:
            try:
                thread_driver.quit()
                logging.info(f"[{thread_id_str}] WebDriver quit for {token_address}.")
            except Exception as e_quit:
                logging.error(f"[{thread_id_str}] Error quitting WebDriver for {token_address}: {e_quit}")
    return token_address, False # Should be unreachable if try block returns, but as a safeguard
def run_risk_detector_once():
    logging.info("Attempting to run risk_detector.py...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    risk_detector_script = os.path.join(script_dir, "risk_detector.py")
    if not os.path.exists(risk_detector_script):
        logging.error(f"risk_detector.py not found at {risk_detector_script}.")
        return False
    try:
        # Check if input files exist
        cluster_summary_path = os.path.join(script_dir, CLUSTER_SUMMARY_FILE)
        if not os.path.exists(cluster_summary_path):
            logging.error(f"Cluster summary file not found at {cluster_summary_path}")
            return False

        # Run risk detector with full path to Python executable
        proc = subprocess.run(
            [sys.executable, risk_detector_script],
            check=True,
            cwd=script_dir,
            capture_output=True,
            text=True
        )
        logging.info("risk_detector.py completed successfully.")
        if proc.stdout: logging.info(f"Risk Detector STDOUT:\n{proc.stdout.strip()}")
        if proc.stderr: logging.warning(f"Risk Detector STDERR:\n{proc.stderr.strip()}")

        # Verify output file was created
        output_file = os.path.join(script_dir, "filtered_tokens_with_all_risks.csv")
        if os.path.exists(output_file):
            logging.info(f"Risk detector output file created at {output_file}")
        else:
            logging.error(f"Risk detector output file not created at {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Risk detector failed with exit code {e.returncode}")
        if e.stdout:
            logging.error(f"STDOUT: {e.stdout}")
        if e.stderr:
            logging.error(f"STDERR: {e.stderr}")
    except Exception as e:
        logging.error(f"Error running risk_detector.py: {e}", exc_info=True)
    return False

def clean_duplicate_entries(csv_path: str) -> None:
    """Remove duplicate token entries from the CSV file."""
    try:
        if not os.path.exists(csv_path):
            return
            
        # Read all lines and remove duplicates while preserving order
        with open(csv_path, 'r', newline='', encoding='utf-8') as f:
            lines = f.readlines()
            
        if not lines:
            return
            
        header = lines[0] if lines[0].strip().lower().startswith('address') else None
        seen = set()
        unique_lines = []
        
        for line in lines[1:] if header else lines:
            line = line.strip()
            if not line:
                continue
                
            # Extract token address (first column)
            token = line.split(',')[0].strip()
            if token and token not in seen:
                seen.add(token)
                unique_lines.append(line + '\n')
        
        # Only write back if we found duplicates
        if len(unique_lines) < (len(lines) - (1 if header else 0)):
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                if header:
                    f.write(header)
                f.writelines(unique_lines)
            logging.info(f"Removed {len(lines) - len(unique_lines) - (1 if header else 0)} duplicate entries from {csv_path}")
            
    except Exception as e:
        logging.error(f"Error cleaning duplicate entries from {csv_path}: {e}")

def get_file_checksum(file_path: str) -> str:
    """Calculate MD5 checksum of a file to detect changes."""
    if not os.path.exists(file_path):
        return ""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def monitor_and_process_tokens(csv_fpath: str, chrome_bin_path: str, chrome_drv_path: str | None):
    # Clean up any duplicate entries in the CSV file first
    clean_duplicate_entries(csv_fpath)
    
    processed_set = load_processed_tokens_threadsafe(OPENED_TOKENS_FILE)
    in_flight = set()
    newly_processed_count = 0
    last_checksum = get_file_checksum(csv_fpath)
    last_check_time = time.time()
    processed_since_last_run: set[str] = set()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="BubbleWorker") as executor:
            active_futures = {}
            while True:
                processed_this_cycle = False
                done_fkeys = [f for f in active_futures if f.done()]
                for fkey in done_fkeys:
                    token_addr = active_futures.pop(fkey)
                    in_flight.discard(token_addr)
                    try:
                        _, success = fkey.result()
                        if success:
                            with PROCESSED_TOKENS_LOCK:
                                save_processed_token_threadsafe(OPENED_TOKENS_FILE, token_addr)
                                processed_set.add(token_addr)
                            logging.info(f"Bubblemaps SUCCESS for {token_addr}.")
                            newly_processed_count += 1
                            processed_since_last_run.add(token_addr)
                            processed_this_cycle = True
                        else: logging.error(f"Bubblemaps FAILED for {token_addr}.")
                    except Exception as exc: logging.error(f"Token {token_addr} thread exception: {exc}")

                if newly_processed_count > 0 and not active_futures:
                    logging.info(f"{newly_processed_count} tokens updated. Triggering risk_detector.py.")
                    detector_success = run_risk_detector_once()
                    if detector_success:
                        for addr in processed_since_last_run:
                            cleanup_token_data(addr)
                        processed_since_last_run.clear()
                    newly_processed_count = 0

                # Check if file has been modified using checksum for better reliability
                current_time = time.time()
                if current_time - last_check_time >= 5:  # Check every 5 seconds
                    current_checksum = get_file_checksum(csv_fpath)
                    if current_checksum != last_checksum:
                        logging.info("CSV file content changed, checking for new tokens")
                        last_checksum = current_checksum
                        # Clean up duplicates whenever we detect a change
                        clean_duplicate_entries(csv_fpath)
                    last_check_time = current_time

                if len(active_futures) < MAX_WORKERS:
                    with PROCESSED_TOKENS_LOCK:
                        to_avoid = processed_set.union(in_flight)
                        new_tokens = get_new_tokens_from_csv_threadsafe(csv_fpath, to_avoid)
                        to_submit = []
                        for token in new_tokens:
                            if len(active_futures) + len(to_submit) < MAX_WORKERS:
                                to_submit.append(token)
                                in_flight.add(token)  # Add to in_flight before submitting
                            else:
                                break

                    if to_submit:
                        logging.info(f"Submitting {len(to_submit)} new tokens: {', '.join(to_submit)}")
                        for token_s in to_submit:
                            fut = executor.submit(process_single_token_threaded, (token_s, chrome_bin_path, chrome_drv_path))
                            active_futures[fut] = token_s
                    elif not active_futures and not processed_this_cycle and newly_processed_count == 0:
                        logging.info(f"No new tokens, no active tasks. Waiting {CHECK_INTERVAL}s...")
                        time.sleep(CHECK_INTERVAL)
                    else:
                        time.sleep(5)
                else:
                    logging.info(f"Pool full ({MAX_WORKERS}). Waiting...")
                    time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt. Shutting down...")
    except Exception as e:
        logging.critical(f"Main loop error: {e}", exc_info=True)
    finally:
        logging.info("Monitor loop finished.")

if __name__ == '__main__':
    logging.info("--- Starting Bubblemaps Extractor (Multi-Threaded) ---")
    cli_chrome = sys.argv[1] if len(sys.argv) > 1 else None
    actual_chrome = detect_chrome_binary_path(cli_chrome)
    if not actual_chrome: logging.error("Exiting: Chrome binary undetermined."); sys.exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_monitor_fpath = os.path.join(script_dir, CSV_FILE)
    data_dir_fpath = os.path.join(script_dir, EXTRACTED_DATA_DIR)
    os.makedirs(data_dir_fpath, exist_ok=True)

    if not os.path.exists(os.path.dirname(csv_monitor_fpath)): logging.warning(f"Dir for CSV ('{os.path.dirname(csv_monitor_fpath)}') missing.")
    elif not os.path.exists(csv_monitor_fpath): logging.warning(f"CSV ('{csv_monitor_fpath}') missing.")

    monitor_and_process_tokens(csv_monitor_fpath, actual_chrome, CHROME_DRIVER_PATH)
    logging.info("--- Bubblemaps Extractor (Multi-Threaded) finished. ---")
