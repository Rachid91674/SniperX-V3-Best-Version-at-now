import asyncio
import time
from pathlib import Path
import datetime
import csv
import importlib.util
import types
import sys

spec = importlib.util.spec_from_file_location("sniper", Path(__file__).resolve().parent / "SniperX V2.py")
sniper = importlib.util.module_from_spec(spec)
dp_mod = types.ModuleType('dateparser')
dp_mod.parse = lambda s: datetime.datetime.fromisoformat(s)
sys.modules.setdefault('dateparser', dp_mod)
spec.loader.exec_module(sniper)


def test_process_window_async(monkeypatch, tmp_path):
    call = {'count': 0}
    async def mock_fetch(session, addr):
        await asyncio.sleep(0.2)
        call['count'] += 1
        if call['count'] <= 5:
            return [{'priceUsd': '0.1', 'liquidity': {'usd': 1000}, 'volume': {'m5': 10}}]
        else:
            return [{'priceUsd': '0.15', 'liquidity': {'usd': 1500}, 'volume': {'m5': 20}}]
    monkeypatch.setattr(sniper, 'fetch_token_data', mock_fetch)
    monkeypatch.setattr(sniper.time, 'sleep', lambda s: None)

    tokens = []
    now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)
    for i in range(5):
        tokens.append({'tokenAddress': f'T{i}', 'name': f'Token{i}', 'symbol': f'T{i}', 'graduatedAt': now.isoformat()})

    start = time.perf_counter()
    sniper.process_window(1, tokens, str(tmp_path))
    elapsed = time.perf_counter() - start
    assert elapsed < 0.6

    csv_file = tmp_path / "sniperx_results_1m.csv"
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
    assert header == ['Address','Name','Price USD','Liquidity(1m)','Volume(1m)','1m Change','Open Chart','Snipe','Ghost Buyer']
