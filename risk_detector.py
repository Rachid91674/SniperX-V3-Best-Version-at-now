import csv
import math
import requests
import time
import os
import sys 
import logging

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
FILTERED_TOKENS_WITH_ALL_RISKS_CSV = os.path.join(
    SCRIPT_DIR, "filtered_tokens_with_all_risks.csv"
)

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
    return (cluster_percent_supply / lp_percent_supply) * 100 

def calculate_price_impact_cluster_sell(pool_project_token_amount, cluster_sell_token_amount):
    if cluster_sell_token_amount is None or cluster_sell_token_amount == 0:
        return 0.0  # No sell pressure, no impact

    # At this point, cluster_sell_token_amount is a positive number
    if pool_project_token_amount is None: # pool_project_token_amount could be None
        return 0.0 # Consistent with test (None, 100) expecting 0.0

    if pool_project_token_amount == 0: # Pool is empty, but there's sell pressure
        return 100.0
    
    # Normal calculation: pool_project_token_amount > 0 and cluster_sell_token_amount > 0
    # Denominator (pool_project_token_amount + cluster_sell_token_amount) cannot be zero here.
    price_ratio_after_sell = (pool_project_token_amount / (pool_project_token_amount + cluster_sell_token_amount)) ** 2
    price_impact_percent = (1 - price_ratio_after_sell) * 100
    return price_impact_percent

def load_cluster_summaries(path):
    if not os.path.exists(path):
        logging.error(f"Cluster summary file not found at {path}")
        return {}
    summaries = {}
    try:
        with open(path, 'r', encoding='utf-8-sig') as f: 
            reader = csv.DictReader(f)
            for row in reader:
                if 'Token_Address' in row:
                    summaries[row['Token_Address']] = row
                else:
                    logging.warning(f"Skipping row in cluster summary due to missing 'Token_Address': {row}")
            logging.info(f"Successfully loaded {len(summaries)} entries from {path}")
    except Exception as e:
        logging.error(f"Error loading cluster summaries from {path}: {e}", exc_info=True)
    return summaries

def run_full_risk_analysis():
    logging.info("--- Starting Full Token Risk Analysis ---")

    if not os.path.exists(INPUT_TOKENS_CSV):
        logging.error(f"Input tokens file not found: {INPUT_TOKENS_CSV}. Aborting.")
        return
    
    input_tokens = []
    input_headers = []
    try:
        with open(INPUT_TOKENS_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            input_headers = reader.fieldnames if reader.fieldnames else []
            for row in reader:
                input_tokens.append(row)
        logging.info(f"Loaded {len(input_tokens)} tokens from {INPUT_TOKENS_CSV}")
    except Exception as e:
        logging.error(f"Error loading input tokens from {INPUT_TOKENS_CSV}: {e}", exc_info=True)
        return

    if not input_tokens:
        logging.info("No tokens found in input file. Exiting.")
        return

    cluster_data_map = load_cluster_summaries(CLUSTER_SUMMARY_CSV)
    if not cluster_data_map:
        logging.warning("Cluster summary data is empty or failed to load. Proceeding without cluster-specific risk factors.")

    results_to_write = []    
    output_headers = input_headers + [
        "Global_Cluster_Percentage", "Highest_Risk_Reason_Cluster",
        "DexScreener_Pair_Address", "DexScreener_Liquidity_USD", "DexScreener_Token_Price_USD", "DexScreener_Token_Name",
        "LP_Percent_Supply", "Cluster_Token_Amount_Est", "Pool_Project_Token_Amount_Est",
        "Dump_Risk_LP_vs_Cluster_Ratio", "Price_Impact_Cluster_Sell_Percent",
        "Overall_Risk_Status", "Risk_Warning_Details"
    ]
    output_headers = sorted(list(set(output_headers)), key=output_headers.index) 

    for token_row in input_tokens:
        output_row = {header: token_row.get(header, "") for header in input_headers} 
        token_address = token_row.get("Address")

        if not token_address:
            logging.warning(f"Skipping row due to missing 'Address': {token_row}")
            for risk_header in output_headers[len(input_headers):]: output_row[risk_header] = "N/A"
            results_to_write.append(output_row)
            continue
        
        logging.info(f"Processing token: {token_address} ({token_row.get('Name', 'N/A')})")

        output_row.update({
            "Global_Cluster_Percentage": "N/A", "Highest_Risk_Reason_Cluster": "N/A",
            "DexScreener_Pair_Address": "N/A", "DexScreener_Liquidity_USD": "N/A", 
            "DexScreener_Token_Price_USD": "N/A", "DexScreener_Token_Name": "N/A",
            "LP_Percent_Supply": "N/A", "Cluster_Token_Amount_Est": "N/A", 
            "Pool_Project_Token_Amount_Est": "N/A",
            "Dump_Risk_LP_vs_Cluster_Ratio": "N/A", "Price_Impact_Cluster_Sell_Percent": "N/A",
            "Overall_Risk_Status": "Data N/A", "Risk_Warning_Details": ""
        })

        # Treat tokens without cluster data as having a high rank individual cluster
        cluster_info = cluster_data_map.get(token_address)
        if cluster_info:
            output_row["Global_Cluster_Percentage"] = cluster_info.get("Global_Cluster_Percentage", "0.0")
            output_row["Highest_Risk_Reason_Cluster"] = cluster_info.get("Highest_Risk_Reason", "N/A")
            try:
                cluster_percent_supply_val = float(output_row["Global_Cluster_Percentage"])
            except ValueError:
                logging.warning(f"Could not parse Global_Cluster_Percentage '{output_row['Global_Cluster_Percentage']}' for {token_address}. Defaulting to 0.0 for calculations.")
                cluster_percent_supply_val = 0.0
        else:
            # When no cluster data is found, treat it as a high rank individual cluster (1% of supply)
            logging.info(f"No cluster summary found for {token_address}. Treating as high rank individual cluster (1% supply).")
            output_row["Global_Cluster_Percentage"] = "1.00"
            output_row["Highest_Risk_Reason_Cluster"] = "High rank individual (no cluster data)"
            cluster_percent_supply_val = 1.0  # 1% of supply as a conservative estimate

        time.sleep(0.2) 
        liquidity_usd, price_usd, pair_addr, token_name_dex = get_primary_pool_data_from_dexscreener(token_address)

        risk_warnings = []
        if liquidity_usd is not None and price_usd is not None and pair_addr is not None:
            output_row["DexScreener_Pair_Address"] = pair_addr
            output_row["DexScreener_Liquidity_USD"] = f"{liquidity_usd:.2f}"
            output_row["DexScreener_Token_Price_USD"] = f"{price_usd:.8f}"
            output_row["DexScreener_Token_Name"] = token_name_dex if token_name_dex else token_row.get('Name', 'N/A') 

            lp_percent = calculate_lp_percent(liquidity_usd, price_usd)
            output_row["LP_Percent_Supply"] = f"{lp_percent:.4f}"

            cluster_token_amount = (cluster_percent_supply_val / 100.0) * TOTAL_SUPPLY
            output_row["Cluster_Token_Amount_Est"] = f"{cluster_token_amount:.2f}"

            pool_project_token_amount = 0
            if price_usd > 0:
                pool_project_token_amount = (liquidity_usd / 2.0) / price_usd
            output_row["Pool_Project_Token_Amount_Est"] = f"{pool_project_token_amount:.2f}"

            dump_risk_ratio = calculate_dump_risk_lp_vs_cluster(cluster_percent_supply_val, lp_percent)
            output_row["Dump_Risk_LP_vs_Cluster_Ratio"] = f"{dump_risk_ratio:.2f}"

            price_impact_pct = calculate_price_impact_cluster_sell(pool_project_token_amount, cluster_token_amount)
            output_row["Price_Impact_Cluster_Sell_Percent"] = f"{price_impact_pct:.2f}"

            if cluster_percent_supply_val >= HIGH_CLUSTER_THRESHOLD_PERCENT:
                risk_warnings.append(f"HighCluster({cluster_percent_supply_val:.1f}% >= {HIGH_CLUSTER_THRESHOLD_PERCENT:.1f}%)")
            if dump_risk_ratio >= DUMP_RISK_THRESHOLD_LP_VS_CLUSTER:
                risk_warnings.append(f"HighDumpRisk({dump_risk_ratio:.1f}x >= {DUMP_RISK_THRESHOLD_LP_VS_CLUSTER:.1f}x)")
            if price_impact_pct >= PRICE_IMPACT_THRESHOLD_CLUSTER_SELL:
                risk_warnings.append(f"HighPriceImpact({price_impact_pct:.1f}% >= {PRICE_IMPACT_THRESHOLD_CLUSTER_SELL:.1f}%)")
            
            if not risk_warnings and lp_percent > 0: 
                output_row["Overall_Risk_Status"] = "Low Risk"
            elif risk_warnings:
                output_row["Overall_Risk_Status"] = "High Risk"
            else:
                output_row["Overall_Risk_Status"] = "Medium Risk / Check Data" 
        else:
            risk_warnings.append("DexScreenerDataN/A")

        if risk_warnings:
            output_row["Risk_Warning_Details"] = "; ".join(risk_warnings)
        else:
            # Show the conditions that made it low risk
            conditions = []
            if cluster_percent_supply_val < HIGH_CLUSTER_THRESHOLD_PERCENT:
                conditions.append(f"Cluster% < {HIGH_CLUSTER_THRESHOLD_PERCENT}%")
            if 'Dump_Risk_LP_vs_Cluster_Ratio' in output_row and output_row['Dump_Risk_LP_vs_Cluster_Ratio'] != 'N/A':
                if float(output_row['Dump_Risk_LP_vs_Cluster_Ratio']) < DUMP_RISK_THRESHOLD_LP_VS_CLUSTER:
                    conditions.append(f"DumpRisk < {DUMP_RISK_THRESHOLD_LP_VS_CLUSTER}x")
            if 'Price_Impact_Cluster_Sell_Percent' in output_row and output_row['Price_Impact_Cluster_Sell_Percent'] != 'N/A':
                if float(output_row['Price_Impact_Cluster_Sell_Percent']) < PRICE_IMPACT_THRESHOLD_CLUSTER_SELL:
                    conditions.append(f"PriceImpact < {PRICE_IMPACT_THRESHOLD_CLUSTER_SELL}%")
            output_row["Risk_Warning_Details"] = "Low Risk: " + "; ".join(conditions) if conditions else "No conditions met"
        results_to_write.append(output_row)

    if results_to_write:
        try:
            # Write to token_risk_analysis.csv
            with open(OUTPUT_RISK_ANALYSIS_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=output_headers, quoting=csv.QUOTE_NONNUMERIC)
                writer.writeheader()
                for row in results_to_write:
                    try:
                        writer.writerow(row)
                    except Exception as row_error:
                        logging.error(f"Error writing row to {OUTPUT_RISK_ANALYSIS_CSV}: {row_error}\nRow data: {row}")
                        continue

            # Write to filtered_tokens_with_all_risks.csv
            with open(FILTERED_TOKENS_WITH_ALL_RISKS_CSV, 'w', newline='', encoding='utf-8') as f_filtered:
                filtered_writer = csv.DictWriter(f_filtered, fieldnames=output_headers, quoting=csv.QUOTE_NONNUMERIC)
                filtered_writer.writeheader()
                for row in results_to_write:
                    try:
                        filtered_writer.writerow(row)
                    except Exception as row_error:
                        logging.error(f"Error writing row to {FILTERED_TOKENS_WITH_ALL_RISKS_CSV}: {row_error}\nRow data: {row}")
                        continue

            logging.info(f"Successfully wrote {len(results_to_write)} processed token entries to {OUTPUT_RISK_ANALYSIS_CSV} and {FILTERED_TOKENS_WITH_ALL_RISKS_CSV}")
            
            # Clean up input files
            try:
                # Remove processed tokens from sniperx_results_1m.csv
                if os.path.exists(INPUT_TOKENS_CSV):
                    os.remove(INPUT_TOKENS_CSV)
                    logging.info(f"Removed processed tokens from {INPUT_TOKENS_CSV}")
                
                # Clean up cluster_summaries.csv
                if os.path.exists(CLUSTER_SUMMARY_CSV):
                    os.remove(CLUSTER_SUMMARY_CSV)
                    logging.info(f"Cleaned up {CLUSTER_SUMMARY_CSV}")
                
            except Exception as cleanup_error:
                logging.error(f"Error cleaning up input files: {cleanup_error}")
            
            # Verify the output files were written correctly
            try:
                with open(OUTPUT_RISK_ANALYSIS_CSV, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    logging.info(f"{OUTPUT_RISK_ANALYSIS_CSV} contains {len(lines)} lines (including header)")
                
                with open(FILTERED_TOKENS_WITH_ALL_RISKS_CSV, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    logging.info(f"{FILTERED_TOKENS_WITH_ALL_RISKS_CSV} contains {len(lines)} lines (including header)")
            except Exception as verify_error:
                logging.error(f"Error verifying output files: {verify_error}")
                
        except Exception as e:
            logging.error(f"Error in file operations: {e}", exc_info=True)
    else:
        logging.info("No token data processed to write.")
        
    logging.info("--- Full Token Risk Analysis Finished ---")

if __name__ == "__main__":
    try:
        run_full_risk_analysis()
    except Exception as e:
        logging.error(f"Critical error in risk_detector main execution: {e}", exc_info=True)