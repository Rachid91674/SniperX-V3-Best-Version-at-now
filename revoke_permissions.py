#!/usr/bin/env python3
"""Utility to revoke delegates and close SPL token accounts."""

# pip install solana==0.32.1 spl-token-utils python-dotenv solders

import argparse
import asyncio
import base64
from base58 import b58decode
import json
import os
import sys
from typing import List

from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import Transaction
from solana.rpc.api import Client
from solana.rpc.types import TokenAccountOpts, TxOpts
from spl.token._layouts import ACCOUNT_LAYOUT
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import (
    AuthorityType,
    CloseAccountParams,
    RevokeParams,
    SetAuthorityParams,
    close_account,
    revoke,
    set_authority,
)


def load_keypair() -> Keypair:
    """Load a Keypair from env var or the default Solana CLI location."""
    secret = os.getenv("PHANTOM_SECRET_KEY")
    if secret:
        try:
            secret = secret.strip()
            if secret.startswith("["):
                secret_bytes = bytes(json.loads(secret))
            else:
                secret_bytes = b58decode(secret)
            return Keypair.from_bytes(secret_bytes)
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to decode PHANTOM_SECRET_KEY: {exc}", file=sys.stderr)
            sys.exit(1)
    default_path = os.path.expanduser("~/.config/solana/id.json")
    if os.path.exists(default_path):
        try:
            with open(default_path, "r", encoding="utf8") as fh:
                secret_bytes = bytes(json.load(fh))
            return Keypair.from_bytes(secret_bytes)
        except Exception as exc:  # noqa: BLE001
            print(f"Failed to load keypair from {default_path}: {exc}", file=sys.stderr)
            sys.exit(1)
    print(
        "Secret key not found. Set PHANTOM_SECRET_KEY or ensure ~/.config/solana/id.json exists.",
        file=sys.stderr,
    )
    sys.exit(1)


def decode_account(data: str) -> dict:
    """Decode base64 account data using ACCOUNT_LAYOUT."""
    decoded = ACCOUNT_LAYOUT.parse(base64.b64decode(data))
    return {
        "mint": Pubkey(decoded.mint),
        "owner": Pubkey(decoded.owner),
        "amount": decoded.amount,
        "delegate": None if decoded.delegate_option == 0 else Pubkey(decoded.delegate),
        "delegated_amount": decoded.delegated_amount,
        "close_authority": None
        if decoded.close_authority_option == 0
        else Pubkey(decoded.close_authority),
    }


async def send_transaction(client: Client, tx: Transaction) -> str:
    """Send transaction and confirm within 30 seconds."""
    resp = await asyncio.to_thread(
        client.send_transaction,
        tx,
        opts=TxOpts(skip_preflight=False, skip_confirmation=False),
    )
    sig = str(resp.value)
    return sig


async def main() -> None:
    """Entry point for the script."""
    load_dotenv()
    parser = argparse.ArgumentParser(description="Revoke delegates and close token accounts")
    parser.add_argument(
        "--endpoint",
        default="https://api.mainnet-beta.solana.com",
        help="RPC endpoint to use",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print actions without sending transactions",
    )
    parser.add_argument(
        "--max-per-tx",
        type=int,
        default=10,
        help="Max instructions per transaction",
    )
    args = parser.parse_args()

    keypair = load_keypair()
    owner = keypair.pubkey()
    client = Client(args.endpoint)

    # Fetch all token accounts owned by our keypair
    opts = TokenAccountOpts(program_id=str(TOKEN_PROGRAM_ID))
    resp = client.get_token_accounts_by_owner(owner, opts)
    accounts = resp.value

    revoke_count = 0
    close_count = 0
    recovered_lamports = 0

    instr_queue: List = []

    for acc in accounts:
        acc_pubkey = acc.pubkey
        info = decode_account(acc.account.data.decode("utf-8"))
        lamports = acc.account.lamports

        # Check delegate
        if info["delegate"] and info["delegate"] != owner:
            instr_queue.append(
                revoke(
                    RevokeParams(
                        program_id=TOKEN_PROGRAM_ID,
                        account=acc_pubkey,
                        owner=owner,
                        signers=[],
                    )
                )
            revoke_count += 1

        # Close authority not self
        if info["close_authority"] and info["close_authority"] != owner:
            instr_queue.append(
                set_authority(
                    SetAuthorityParams(
                        program_id=TOKEN_PROGRAM_ID,
                        account=acc_pubkey,
                        authority=AuthorityType.CLOSE_ACCOUNT,
                        current_authority=owner,
                        signers=[],
                        new_authority=owner,
                    )
                )
            # Not counting separately

        # Close account if empty
        if info["amount"] == 0:
            instr_queue.append(
                close_account(
                    CloseAccountParams(
                        program_id=TOKEN_PROGRAM_ID,
                        account=acc_pubkey,
                        dest=owner,
                        owner=owner,
                        signers=[],
                    )
                )
            close_count += 1
            recovered_lamports += lamports

    if args.dry_run:
        print(f"Would send {len(instr_queue)} instructions")
        print(f"Revoke: {revoke_count}, Close: {close_count}, Recovered lamports: {recovered_lamports}")
        return

    # Batch instructions into transactions
    batch_size = args.max_per_tx
    batches = [instr_queue[i : i + batch_size] for i in range(0, len(instr_queue), batch_size)]

    for batch in batches:
        bh = client.get_latest_blockhash().value
        tx = Transaction.new_signed_with_payer(batch, owner, [keypair], bh.blockhash)
        sig = await send_transaction(client, tx)
        print(f"Tx: {sig} https://solscan.io/tx/{sig}")

    print(f"Revoked {revoke_count} delegates")
    print(f"Closed {close_count} accounts")
    print(f"Recovered {recovered_lamports / 1_000_000_000:.9f} SOL")


if __name__ == "__main__":
    asyncio.run(main())
