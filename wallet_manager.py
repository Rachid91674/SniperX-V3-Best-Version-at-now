#!/usr/bin/env python3

import asyncio
import json
import os
import time
from decimal import Decimal # For precise arithmetic with token amounts
from datetime import datetime

import httpx # For fetching SOL price, can be replaced with requests
from dotenv import load_dotenv
from solders.keypair import Keypair # For loading your wallet
from solders.pubkey import Pubkey
from solders.rpc.config import RpcAccountInfoConfig # For account subscription
from solders.rpc.filter import Memcmp # If needed for more specific subscriptions
from solana.rpc.async_api import AsyncClient as SolanaAsyncClient
from solana.rpc.commitment import Confirmed # Or Finalized, Processed
from websockets.legacy.client import connect as websocket_connect # Using legacy for broader compatibility for now

load_dotenv(dotenv_path='.env') # Ensure your env.txt is loaded

# --- Configuration ---
RPC_URL = os.getenv("QUICKNODE_RPC") # Or HELIUS_RPC, ANKR_RPC etc.
WALLET_KEYPAIR_PATH = os.getenv("WALLET_KEYPAIR_PATH")
SOL_MINT_ADDRESS = "So11111111111111111111111111111111111111112" # For SPL token balance if needed
LAMPORTS_PER_SOL = 1_000_000_000

# Placeholder for SOL price - replace with a reliable price feed API
async def get_sol_price_usd(client: httpx.AsyncClient) -> Decimal:
    try:
        # Example using CoinGecko - ensure you respect their rate limits
        # response = await client.get("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd")
        # response.raise_for_status()
        # price_data = response.json()
        # return Decimal(str(price_data["solana"]["usd"]))

        # Using DexScreener for SOL price (e.g., SOL/USDC pair) - more robust if available
        # Find a very liquid SOL/USDC or SOL/USDT pair on DexScreener
        # Example: JUP/SOL pair (replace with a stable SOL/USD(C/T) pair)
        # dex_url = "https://api.dexscreener.com/latest/dex/pairs/solana/ criticizing" # Example SOL/USDC raydium
        dex_url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112" # Price for SOL itself
        response = await client.get(dex_url)
        response.raise_for_status()
        data = response.json()
        if data.get("pairs") and data["pairs"][0].get("priceUsd"):
            return Decimal(str(data["pairs"][0]["priceUsd"]))
        print("[WALLET_MANAGER] Could not fetch SOL price from DexScreener. Defaulting to 0.")
        return Decimal("0") # Fallback
    except Exception as e:
        print(f"[WALLET_MANAGER] Error fetching SOL price: {e}. Defaulting to 0.")
        return Decimal("0")

class CustomAsyncClient:
    def __init__(self, endpoint: str, commitment=Confirmed):
        self.endpoint = endpoint
        self.commitment = commitment
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def get_balance(self, pubkey) -> dict:
        try:
            response = await self.client.post(
                self.endpoint,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getBalance",
                    "params": [str(pubkey), {"commitment": str(self.commitment)}]
                }
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[WALLET_MANAGER] Error in get_balance: {e}")
            raise
    
    async def close(self):
        await self.client.aclose()

class WalletManager:
    def __init__(self, rpc_url: str, keypair_path: str):
        if not rpc_url:
            raise ValueError("RPC_URL is not set in environment variables.")
        if not keypair_path or not os.path.exists(keypair_path):
            raise ValueError(f"Wallet keypair path '{keypair_path}' is invalid or file does not exist.")

        self.rpc_url = rpc_url
        self.ws_url = rpc_url.replace("http", "ws") # Convert HTTP RPC to WS RPC
        self.keypair = Keypair.from_json(open(keypair_path).read())
        self.public_key = self.keypair.pubkey()
        self.current_sol_balance_lamports: int = 0  # Initialize the attribute
        
        # Debug logging
        print(f"[WALLET_MANAGER] RPC URL: {self.rpc_url}")
        print(f"[WALLET_MANAGER] Environment variables:")
        for key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
            if key in os.environ:
                print(f"[WALLET_MANAGER] {key}: {os.environ[key]}")
        
        # Initialize custom client
        self.async_client = CustomAsyncClient(endpoint=self.rpc_url, commitment=Confirmed)
        self.httpx_client = httpx.AsyncClient(timeout=10) # For price fetching

        self.current_sol_price_usd: Decimal = Decimal("0")
        self.on_balance_change_callback = None

        print(f"[WALLET_MANAGER] Initialized for wallet: {self.public_key}")
        print(f"[WALLET_MANAGER] Using RPC: {self.rpc_url}")
        print(f"[WALLET_MANAGER] Using WebSocket: {self.ws_url}")


    async def close_sessions(self):
        await self.async_client.close()
        await self.httpx_client.aclose()
        print("[WALLET_MANAGER] Closed RPC and HTTPX sessions.")

    async def get_balance(self) -> tuple[Decimal, Decimal]:
        """Fetches SOL balance and its USD equivalent."""
        try:
            balance_response = await self.async_client.get_balance(self.public_key)
            self.current_sol_balance_lamports = balance_response['result']['value']
            sol_balance = Decimal(self.current_sol_balance_lamports) / Decimal(LAMPORTS_PER_SOL)
            
            self.current_sol_price_usd = await get_sol_price_usd(self.httpx_client)
            usd_balance = sol_balance * self.current_sol_price_usd
            
            print(f"[WALLET_MANAGER] Current Balance: {sol_balance:.6f} SOL (${usd_balance:.2f} USD @ ${self.current_sol_price_usd:.2f}/SOL)")
            return sol_balance, usd_balance
        except Exception as e:
            print(f"[WALLET_MANAGER] Error getting balance: {e}")
            return Decimal("0"), Decimal("0")

    def set_on_balance_change_callback(self, callback_func):
        """Set a callback function to be invoked when balance changes."""
        self.on_balance_change_callback = callback_func

    async def _handle_account_notification(self, message_str: str):
        try:
            message = json.loads(message_str)
            # print(f"[WALLET_MANAGER] WS Raw Message: {message}") # For debugging
            if message.get("method") == "accountNotification":
                result = message.get("params", {}).get("result", {})
                if result: # and result.get("context", {}).get("slot") is not None: # Check if it's a valid update
                    # The full account data is in result.get("value", {}).get("data")
                    # For SOL balance, the lamports are directly available in the new account info
                    new_lamports_value = result.get("value", {}).get("lamports")
                    if new_lamports_value is not None and new_lamports_value != self.current_sol_balance_lamports:
                        self.current_sol_balance_lamports = new_lamports_value
                        sol_balance = Decimal(self.current_sol_balance_lamports) / Decimal(LAMPORTS_PER_SOL)
                        
                        # Optionally re-fetch SOL price, or use the last known one for quicker update
                        # For now, using last known price for simplicity on balance change notification
                        usd_balance = sol_balance * self.current_sol_price_usd 
                        
                        print(f"[WALLET_MANAGER] WebSocket Balance Update: {sol_balance:.6f} SOL (${usd_balance:.2f} USD)")
                        if self.on_balance_change_callback:
                            try:
                                # If callback is async, await it; otherwise, call directly
                                if asyncio.iscoroutinefunction(self.on_balance_change_callback):
                                    await self.on_balance_change_callback(sol_balance, usd_balance, self.current_sol_price_usd)
                                else:
                                    self.on_balance_change_callback(sol_balance, usd_balance, self.current_sol_price_usd)
                            except Exception as cb_e:
                                print(f"[WALLET_MANAGER] Error in on_balance_change_callback: {cb_e}")
        except json.JSONDecodeError:
            print(f"[WALLET_MANAGER] WS Error: Could not decode JSON: {message_str}")
        except Exception as e:
            print(f"[WALLET_MANAGER] WS Error processing message: {e}")


    async def subscribe_to_balance_changes(self):
        """Subscribes to account changes for the wallet's public key."""
        print(f"[WALLET_MANAGER] Subscribing to balance changes for {self.public_key} via WebSocket: {self.ws_url}...")
        
        # Initial balance fetch
        await self.get_balance()

        # AccountSubscribe request payload
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                str(self.public_key),
                {"encoding": "jsonParsed", "commitment": str(Confirmed)}
            ],
        }
        
        while True: # Loop to attempt reconnection if connection drops
            try:
                async with websocket_connect(self.ws_url) as websocket:
                    await websocket.send(json.dumps(request))
                    # Handle the first response which is the subscription ID
                    first_resp = await websocket.recv()
                    print(f"[WALLET_MANAGER] Subscription Response: {first_resp}")

                    # Listen for notifications
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=60.0) # Add timeout to check connection
                            await self._handle_account_notification(str(message))
                        except asyncio.TimeoutError:
                            # print("[WALLET_MANAGER] WebSocket keep-alive check (no message in 60s).")
                            # You might want to send a ping if supported by RPC, or just continue listening
                            pass # Or send a ping if your RPC supports it: await websocket.ping()
                        except Exception as e_inner:
                            print(f"[WALLET_MANAGER] Inner WebSocket loop error: {e_inner}")
                            break # Break inner to reconnect
            except ConnectionRefusedError:
                print(f"[WALLET_MANAGER] WebSocket connection refused. Retrying in 10 seconds...")
            except Exception as e:
                print(f"[WALLET_MANAGER] WebSocket connection error: {e}. Retrying in 10 seconds...")
            
            await asyncio.sleep(10) # Wait before retrying connection


# --- Example Usage (can be run directly) ---
async def my_balance_update_handler(balance: Decimal, usd_value: Decimal):
    """Handle balance updates and write to file for Telegram bot."""
    try:
        balance_data = {
            "sol": str(balance),
            "usd": str(usd_value),
            "timestamp": time.time()
        }
        
        # Write to file immediately
        with open("wallet_balance.json", "w") as f:
            json.dump(balance_data, f)
            f.flush()  # Force write to disk
            os.fsync(f.fileno())  # Ensure it's written to disk
        
        print(f"[WALLET_MANAGER] Balance updated: {balance} SOL (${usd_value})")
    except Exception as e:
        print(f"[WALLET_MANAGER] Error writing balance to file: {e}")

async def main():
    if not RPC_URL:
        print("[ERROR] RPC_URL not found in env.txt. Please set it (e.g., QUICKNODE_RPC).")
        return
    if not WALLET_KEYPAIR_PATH:
        print("[ERROR] WALLET_KEYPAIR_PATH not found in env.txt.")
        return

    manager = WalletManager(rpc_url=RPC_URL, keypair_path=WALLET_KEYPAIR_PATH)
    manager.set_on_balance_change_callback(my_balance_update_handler)

    try:
        # Initial balance check and write
        sol_balance, usd_balance = await manager.get_balance()
        await my_balance_update_handler(sol_balance, usd_balance)
        
        # Start the WebSocket subscription
        await manager.subscribe_to_balance_changes()
    except KeyboardInterrupt:
        print("[WALLET_MANAGER] Keyboard interrupt received. Closing sessions...")
    except Exception as e:
        print(f"[WALLET_MANAGER] Error in main: {e}")
    finally:
        await manager.close_sessions()

if __name__ == "__main__":
    print("[WALLET_MANAGER] Starting Wallet Manager standalone for testing...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[WALLET_MANAGER] Program terminated by user.")