import csv
import math
import requests
import time
import os
import sys
import logging
import re
from collections import OrderedDict
import pandas as pd

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# --- Configuration ---
HIGH_CLUSTER_THRESHOLD_PERCENT = 5.0
DUMP_RISK_THRESHOLD_LP_VS_CLUSTER = 15.0
PRICE_IMPACT_THRESHOLD_CLUSTER_SELL = 35.69
TOTAL_SUPPLY = 1_000_000_000
DEXSCREENER_API_ENDPOINT_TEMPLATE = "https://api.dexscreener.com/v1/dex/tokens/{token_address}"
REQUESTS_TIMEOUT = 15

# --- File Paths (assuming in the same directory as the script) ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_TOKENS_CSV = os.path.join(SCRIPT_DIR, "sniperx_results_1m.csv")
CLUSTER_SUMMARY_CSV = os.path.join(SCRIPT_DIR, "cluster_summaries.csv")
OUTPUT_RISK_ANALYSIS_CSV = os.path.join(SCRIPT_DIR, "token_risk_analysis.csv")

# --- Helper Functions ---
def get_primary_pool_data_from_dexscreener(token_address, chain_id="solana"):
    search_url = f"https://api.dexscreener.com/latest/dex/search?q={token_address}"
    logging.info(f"[DEXSCREENER] Querying for pairs: {search_url}")
    try:
        response = requests.get(search_url, headers={"Accept": "*/*"}, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, dict) or "pairs" not in data or not isinstance(data["pairs"], list):
            logging.warning(f"[DEXSCREENER] Unrecognized DexScreener response structure for {token_address}. Response: {str(data)[:200]}")
            return None, None, None, None
        
        pair_list = data["pairs"]
        if not pair_list:
            logging.info(f"[DEXSCREENER] No pairs found for {token_address}.")
            return None, None, None, None

        best_pair = None
        highest_liquidity_usd = -1.0
        common_quote_symbols = ["SOL", "USDC", "USDT", "JUP", "RAY", "BONK", "WIF"] 

        for current_pair_data in pair_list:
            if not isinstance(current_pair_data, dict):
                continue
            if current_pair_data.get("chainId", "").lower() != chain_id.lower():
                continue
            
            base_token_addr = current_pair_data.get("baseToken", {}).get("address", "").lower()
            quote_token_addr = current_pair_data.get("quoteToken", {}).get("address", "").lower()
            token_addr_lower = token_address.lower()

            if token_addr_lower != base_token_addr and token_addr_lower != quote_token_addr:
                continue 

            current_liquidity_usd = 0.0
            liquidity_info = current_pair_data.get("liquidity")
            if liquidity_info and isinstance(liquidity_info, dict) and liquidity_info.get("usd") is not None:
                try:
                    current_liquidity_usd = float(liquidity_info["usd"])
                except (ValueError, TypeError):
                    current_liquidity_usd = 0.0
            
            other_token_symbol = ""
            if token_addr_lower == base_token_addr:
                other_token_symbol = current_pair_data.get("quoteToken", {}).get("symbol", "")
            elif token_addr_lower == quote_token_addr:
                 other_token_symbol = current_pair_data.get("baseToken", {}).get("symbol", "")
            
            is_current_preferred_quote = any(q_sym.lower() == (other_token_symbol or "").lower() for q_sym in common_quote_symbols)

            is_best_pair_preferred_quote = False
            if best_pair:
                bp_base_token_addr = best_pair.get("baseToken", {}).get("address", "").lower()
                bp_other_symbol = ""
                if token_addr_lower == bp_base_token_addr:
                    bp_other_symbol = best_pair.get("quoteToken", {}).get("symbol", "")
                else:
                    bp_other_symbol = best_pair.get("baseToken", {}).get("symbol", "")
                is_best_pair_preferred_quote = any(q_sym.lower() == (bp_other_symbol or "").lower() for q_sym in common_quote_symbols)

            if best_pair is None or \
               (is_current_preferred_quote and not is_best_pair_preferred_quote) or \
               (is_current_preferred_quote == is_best_pair_preferred_quote and current_liquidity_usd > highest_liquidity_usd) or \
               (not is_current_preferred_quote and not is_best_pair_preferred_quote and current_liquidity_usd > highest_liquidity_usd): 
                best_pair = current_pair_data
                highest_liquidity_usd = current_liquidity_usd
        
        if not best_pair:
            logging.info(f"[DEXSCREENER] No suitable primary pair found for {token_address} on {chain_id} after filtering.")
            return None, None, None, None

        pair_address_dex = best_pair.get("pairAddress")
        liquidity_usd_dex = highest_liquidity_usd
        price_usd_str = best_pair.get("priceUsd", "0")
        try:
            price_usd_dex = float(price_usd_str)
        except (ValueError, TypeError):
            logging.warning(f"[DEXSCREENER] Could not parse priceUsd '{price_usd_str}' for {token_address}. Defaulting to 0.")
            price_usd_dex = 0.0
        
        token_name_dex = ""
        if best_pair.get("baseToken", {}).get("address", "").lower() == token_address.lower():
            token_name_dex = best_pair.get("baseToken", {}).get("name", "")
        elif best_pair.get("quoteToken", {}).get("address", "").lower() == token_address.lower():
             token_name_dex = best_pair.get("quoteToken", {}).get("name", "") 

        logging.info(f"[DEXSCREENER] Primary pair for {token_address}: {pair_address_dex}, Liq_USD: {liquidity_usd_dex:.2f}, Price_USD: {price_usd_dex:.8f}")
        return liquidity_usd_dex, price_usd_dex, pair_address_dex, token_name_dex

    except requests.exceptions.RequestException as e:
        logging.error(f"[DEXSCREENER] API request failed for {token_address}: {e}")
        return None, None, None, None
    except ValueError as e: 
        logging.error(f"[DEXSCREENER] Failed to decode JSON response for {token_address}: {e}")
        return None, None, None, None

def calculate_lp_percent(liquidity_usd_in_pool, token_price_usd):
    if token_price_usd is None or token_price_usd == 0 or liquidity_usd_in_pool is None:
        return 0.0
    project_token_value_in_lp = liquidity_usd_in_pool / 2.0
    project_tokens_in_lp = project_token_value_in_lp / token_price_usd
    lp_percentage_of_total_supply = (project_tokens_in_lp / TOTAL_SUPPLY) * 100
    return lp_percentage_of_total_supply

def calculate_dump_risk_lp_vs_cluster(cluster_percent_supply, lp_percent_supply):
    if lp_percent_supply is None or lp_percent_supply == 0:
        return float('inf') if cluster_percent_supply > 0 else 0.0
    if cluster_percent_supply is None: return 0.0
    return cluster_percent_supply / lp_percent_supply

def calculate_price_impact_cluster_sell(pool_project_token_amount, cluster_sell_token_amount):
    if cluster_sell_token_amount is None or cluster_sell_token_amount <= 0:
        return 0.0
    if pool_project_token_amount is None or pool_project_token_amount <= 0:
        return 100.0
    
    price_ratio_after_sell = (pool_project_token_amount / (pool_project_token_amount + cluster_sell_token_amount))
    price_impact_percent = (1 - price_ratio_after_sell) * 100
    return price_impact_percent


def load_csv_data(path, key_column='Token_Address'):
    if not os.path.exists(path):
        logging.warning(f"CSV file not found at {path}")
        return {}
    data_map = OrderedDict()
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row.get(key_column)
                if key:
                    data_map[key] = row
            logging.info(f"Successfully loaded {len(data_map)} entries from {path}")
    except Exception as e:
        logging.error(f"Error loading data from {path}: {e}", exc_info=True)
    return data_map

def write_csv_data(path, headers, data_rows, mode='w'):
    try:
        with open(path, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if mode == 'w' or not os.path.getsize(path) > 0:
                writer.writeheader()
            writer.writerows(data_rows)
        return True
    except Exception as e:
        logging.error(f"Error writing to {path}: {e}")
        return False


def run_full_risk_analysis():
    logging.info("--- Starting Full Token Risk Analysis ---")

    input_tokens_map = load_csv_data(INPUT_TOKENS_CSV, key_column='Address')
    if not input_tokens_map:
        logging.info("No new tokens found in input file. Exiting.")
        return

    cluster_data_map = load_csv_data(CLUSTER_SUMMARY_CSV, key_column='Token_Address')
    
    results_to_write = []
    processed_token_addresses = set()

    # Define the full set of headers for the output CSV
    base_headers = list(next(iter(input_tokens_map.values())).keys()) if input_tokens_map else []
    risk_headers = [
        "Global_Cluster_Percentage", "DexScreener_Pair_Address", "DexScreener_Liquidity_USD", 
        "DexScreener_Token_Price_USD", "DexScreener_Token_Name", "LP_Percent_Supply", 
        "Cluster_Token_Amount_Est", "Pool_Project_Token_Amount_Est", "Dump_Risk_LP_vs_Cluster_Ratio", 
        "Price_Impact_Cluster_Sell_Percent", "Overall_Risk_Status", "Risk_Warning_Details"
    ]
    output_headers = base_headers + [h for h in risk_headers if h not in base_headers]

    for token_address, token_row in input_tokens_map.items():
        processed_token_addresses.add(token_address)
        logging.info(f"Processing token: {token_address} ({token_row.get('Name', 'N/A')})")
        
        output_row = token_row.copy()
        output_row.update({h: "N/A" for h in risk_headers})

        cluster_info = cluster_data_map.get(token_address)
        if not cluster_info:
            logging.warning(f"No cluster summary found for {token_address}. Cannot perform risk analysis.")
            output_row["Overall_Risk_Status"] = "Data Missing"
            output_row["Risk_Warning_Details"] = "Bubblemaps data not available."
            results_to_write.append(output_row)
            continue
        
        try:
            cluster_percent_supply_val = float(cluster_info.get("Global_Cluster_Percentage", "0.0"))
            output_row["Global_Cluster_Percentage"] = f"{cluster_percent_supply_val:.2f}"
        except (ValueError, TypeError):
            logging.warning(f"Could not parse Global_Cluster_Percentage for {token_address}. Skipping.")
            output_row["Overall_Risk_Status"] = "Data Error"
            output_row["Risk_Warning_Details"] = "Invalid cluster percentage format."
            results_to_write.append(output_row)
            continue

        # Parse top holder percentage from the Individual_Cluster_Percentages field
        top_holder_pct = None
        indiv_percent_str = cluster_info.get("Individual_Cluster_Percentages", "")
        match = re.search(r"TopIndividual:\s*([0-9]*\.?[0-9]+)%", indiv_percent_str)
        if match:
            try:
                top_holder_pct = float(match.group(1))
            except ValueError:
                top_holder_pct = None

        time.sleep(0.2)
        liquidity_usd, price_usd, pair_addr, token_name_dex = get_primary_pool_data_from_dexscreener(token_address)
        
        if liquidity_usd is None or price_usd is None:
            logging.warning(f"No valid DexScreener data for {token_address}. Cannot perform risk analysis.")
            output_row["Overall_Risk_Status"] = "Data Missing"
            output_row["Risk_Warning_Details"] = "DexScreener data not available."
            results_to_write.append(output_row)
            continue

        # Early risk filters based on liquidity and top holder percentage
        if liquidity_usd < 15000:
            logging.info(f"Filtering {token_address} due to low liquidity: ${liquidity_usd:.2f}")
            output_row["DexScreener_Pair_Address"] = pair_addr
            output_row["DexScreener_Liquidity_USD"] = f"{liquidity_usd:.2f}"
            output_row["DexScreener_Token_Price_USD"] = f"{price_usd:.8f}"
            output_row["DexScreener_Token_Name"] = token_name_dex or token_row.get('Name', 'N/A')
            output_row["Overall_Risk_Status"] = "High Risk"
            output_row["Risk_Warning_Details"] = "Low Liquidity"
            results_to_write.append(output_row)
            continue

        if top_holder_pct is not None and top_holder_pct > 10.0:
            logging.info(f"Filtering {token_address} due to whale trap risk: top holder {top_holder_pct:.2f}%")
            output_row["DexScreener_Pair_Address"] = pair_addr
            output_row["DexScreener_Liquidity_USD"] = f"{liquidity_usd:.2f}"
            output_row["DexScreener_Token_Price_USD"] = f"{price_usd:.8f}"
            output_row["DexScreener_Token_Name"] = token_name_dex or token_row.get('Name', 'N/A')
            output_row["Overall_Risk_Status"] = "High Risk"
            output_row["Risk_Warning_Details"] = "Whale Trap"
            results_to_write.append(output_row)
            continue

        output_row["DexScreener_Pair_Address"] = pair_addr
        output_row["DexScreener_Liquidity_USD"] = f"{liquidity_usd:.2f}"
        output_row["DexScreener_Token_Price_USD"] = f"{price_usd:.8f}"
        output_row["DexScreener_Token_Name"] = token_name_dex or token_row.get('Name', 'N/A')

        lp_percent = calculate_lp_percent(liquidity_usd, price_usd)
        output_row["LP_Percent_Supply"] = f"{lp_percent:.4f}"

        cluster_token_amount = (cluster_percent_supply_val / 100.0) * TOTAL_SUPPLY
        output_row["Cluster_Token_Amount_Est"] = f"{cluster_token_amount:.2f}"
        
        pool_project_token_amount = (liquidity_usd / 2.0) / price_usd if price_usd > 0 else 0
        output_row["Pool_Project_Token_Amount_Est"] = f"{pool_project_token_amount:.2f}"
        
        dump_risk_ratio = calculate_dump_risk_lp_vs_cluster(cluster_percent_supply_val, lp_percent)
        output_row["Dump_Risk_LP_vs_Cluster_Ratio"] = f"{dump_risk_ratio:.2f}"

        price_impact_pct = calculate_price_impact_cluster_sell(pool_project_token_amount, cluster_token_amount)
        output_row["Price_Impact_Cluster_Sell_Percent"] = f"{price_impact_pct:.2f}"

        risk_warnings = []
        if cluster_percent_supply_val >= HIGH_CLUSTER_THRESHOLD_PERCENT:
            risk_warnings.append(f"HighCluster({cluster_percent_supply_val:.1f}% >= {HIGH_CLUSTER_THRESHOLD_PERCENT:.1f}%)")
        if dump_risk_ratio * 100 >= DUMP_RISK_THRESHOLD_LP_VS_CLUSTER: # Corrected calculation
             risk_warnings.append(f"HighDumpRisk({dump_risk_ratio:.1f}x >= {DUMP_RISK_THRESHOLD_LP_VS_CLUSTER/100:.1f}x)")
        if price_impact_pct >= PRICE_IMPACT_THRESHOLD_CLUSTER_SELL:
            risk_warnings.append(f"HighPriceImpact({price_impact_pct:.1f}% >= {PRICE_IMPACT_THRESHOLD_CLUSTER_SELL:.1f}%)")

        if not risk_warnings:
            output_row["Overall_Risk_Status"] = "Low Risk"
            output_row["Risk_Warning_Details"] = "All risk metrics passed."
        else:
            output_row["Overall_Risk_Status"] = "High Risk"
            output_row["Risk_Warning_Details"] = "; ".join(risk_warnings)
        
        results_to_write.append(output_row)

    if results_to_write:
        # Append results to the main analysis file
        existing_risk_analysis = load_csv_data(OUTPUT_RISK_ANALYSIS_CSV, key_column='Address')
        for row in results_to_write:
            existing_risk_analysis[row['Address']] = row
        
        write_csv_data(OUTPUT_RISK_ANALYSIS_CSV, output_headers, existing_risk_analysis.values(), mode='w')
        logging.info(f"Wrote/Updated {len(results_to_write)} token(s) in {OUTPUT_RISK_ANALYSIS_CSV}")

    if processed_token_addresses:
        # Clean up input files by removing only the processed tokens
        remaining_input_tokens = {k: v for k, v in input_tokens_map.items() if k not in processed_token_addresses}
        remaining_cluster_data = {k: v for k, v in cluster_data_map.items() if k not in processed_token_addresses}

        if remaining_input_tokens:
            write_csv_data(
                INPUT_TOKENS_CSV,
                list(next(iter(remaining_input_tokens.values())).keys()),
                remaining_input_tokens.values(),
                'w'
            )
        elif os.path.exists(INPUT_TOKENS_CSV):
            df_in = pd.read_csv(INPUT_TOKENS_CSV)
            df_in = df_in[~df_in['Address'].isin(processed_token_addresses)]
            df_in.to_csv(INPUT_TOKENS_CSV, index=False)

        if remaining_cluster_data:
            write_csv_data(
                CLUSTER_SUMMARY_CSV,
                list(next(iter(remaining_cluster_data.values())).keys()),
                remaining_cluster_data.values(),
                'w'
            )
        elif os.path.exists(CLUSTER_SUMMARY_CSV):
            df_cs = pd.read_csv(CLUSTER_SUMMARY_CSV)
            df_cs = df_cs[~df_cs['Token_Address'].isin(processed_token_addresses)]
            df_cs.to_csv(CLUSTER_SUMMARY_CSV, index=False)
            
        logging.info(f"Cleaned up {len(processed_token_addresses)} processed token(s) from source files.")

    logging.info("--- Full Token Risk Analysis Finished ---")

if __name__ == "__main__":
    try:
        run_full_risk_analysis()
    except Exception as e:
        logging.critical(f"Critical error in risk_detector main execution: {e}", exc_info=True)