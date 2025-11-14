#!/usr/bin/env python3
"""
Export historical Aave delegation edges to newline-delimited JSON.

The script scans Transfer + DelegateChanged events for the governance tokens
(AAVE, stkAAVE, aAAVE) to reconstruct per-delegator balances and delegation
edges. Results include delegator, delegatee, amount (wei), block number,
transaction hash, and timestamp.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

import aiohttp

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
DELEGATE_CHANGED_TOPIC = "0xdea7fd6a26067b4c0ebf6a41fb19ce78143b6c320bb8721293bf8a1b5f6ae4c8"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

DEFAULT_BLOCK_RANGE = int(os.getenv("GOVERNANCE_LOG_BLOCK_RANGE", "25000"))
MIN_BLOCK_RANGE = 10
MAX_PENDING_BLOCK_REQUESTS = 3


def hex_to_int(value: str) -> int:
  return int(value, 16)


def format_timestamp(ts: int) -> str:
  return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


@dataclass(frozen=True)
class TokenConfig:
  symbol: str
  address: str
  start_block: int


def get_token_configs(selection: List[str]) -> List[TokenConfig]:
  tokens = {
    "AAVE": TokenConfig("AAVE", "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", 11_300_000),
    "STKAAVE": TokenConfig("STKAAVE", "0x4da27a545c0c5B758a6BA100e3a049001de870f5", 11_400_000),
    "AAAVE": TokenConfig("AAAVE", "0xA700b4eB416Be35b2911fd5Dee80678ff64fF6C9", 14_500_000),
  }
  if not selection:
    return list(tokens.values())
  result: List[TokenConfig] = []
  for key in selection:
    upper = key.upper()
    if upper not in tokens:
      raise SystemExit(f"Unknown token '{key}'. Options: {', '.join(tokens)}")
    result.append(tokens[upper])
  return result


class RpcClient:
  def __init__(self, rpc_url: str, session: aiohttp.ClientSession):
    self.rpc_url = rpc_url
    self.session = session
    self._request_id = 0
    self._block_cache: Dict[int, int] = {}
    self._semaphore = asyncio.Semaphore(MAX_PENDING_BLOCK_REQUESTS)

  async def call(self, method: str, params: List[Any]) -> Any:
    self._request_id += 1
    payload = {
      "jsonrpc": "2.0",
      "id": self._request_id,
      "method": method,
      "params": params,
    }
    async with self.session.post(self.rpc_url, json=payload, timeout=120) as resp:
      data = await resp.json()
      if "error" in data:
        raise RuntimeError(f"RPC error {data['error']}")
      return data["result"]

  async def latest_block(self) -> int:
    result = await self.call("eth_blockNumber", [])
    return hex_to_int(result)

  async def get_logs(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    return await self.call("eth_getLogs", [params])

  async def block_timestamp(self, block_number: int) -> int:
    if block_number in self._block_cache:
      return self._block_cache[block_number]
    async with self._semaphore:
      result = await self.call("eth_getBlockByNumber", [hex(block_number), False])
    timestamp = hex_to_int(result["timestamp"])
    self._block_cache[block_number] = timestamp
    return timestamp


def should_reduce_span(error: Exception) -> bool:
  message = str(error).lower()
  return "block range" in message or "too many" in message or "-32600" in message


def address_from_topic(topic: str) -> str:
  return "0x" + topic[-40:]


def handle_transfer_log(balances: Dict[str, int], log: Dict[str, Any]) -> None:
  from_addr = address_from_topic(log["topics"][1]).lower()
  to_addr = address_from_topic(log["topics"][2]).lower()
  value = hex_to_int(log["data"])
  if from_addr != ZERO_ADDRESS:
    balances[from_addr] = balances.get(from_addr, 0) - value
  if to_addr != ZERO_ADDRESS:
    balances[to_addr] = balances.get(to_addr, 0) + value


def format_record(
  token: TokenConfig,
  delegator: str,
  delegatee: str,
  delegation_type: str,
  amount: int,
  block_number: int,
  log: Dict[str, Any],
  timestamp: int,
) -> Dict[str, Any]:
  return {
    "tokenSymbol": token.symbol,
    "tokenAddress": token.address,
    "delegator": delegator,
    "delegatee": delegatee,
    "delegationType": delegation_type,
    "amountWei": str(amount),
    "amountTokens": str(Decimal(amount) / Decimal(10 ** 18)),
    "blockNumber": block_number,
    "txHash": log["transactionHash"],
    "logIndex": hex_to_int(log["logIndex"]),
    "timestamp": timestamp,
    "timestampIso": format_timestamp(timestamp),
  }


async def handle_delegate_log(
  rpc: RpcClient,
  balances: Dict[str, int],
  token: TokenConfig,
  block_number: int,
  log: Dict[str, Any],
) -> Dict[str, Any] | None:
  topics = log["topics"]
  if len(topics) < 3:
    return None
  delegator = address_from_topic(topics[1]).lower()
  delegatee = address_from_topic(topics[2]).lower()
  delegation_type = "PROPOSITION" if hex_to_int(log["data"]) == 1 else "VOTING"
  amount = balances.get(delegator, 0)
  timestamp = await rpc.block_timestamp(block_number)
  return format_record(token, delegator, delegatee, delegation_type, amount, block_number, log, timestamp)


async def export_token(
  rpc: RpcClient,
  token: TokenConfig,
  out_dir: Path,
  end_block: int,
  chunk_size: int,
) -> None:
  balances: Dict[str, int] = {}
  out_dir.mkdir(parents=True, exist_ok=True)
  out_path = out_dir / f"{token.symbol.lower()}_delegations.ndjson"
  block_range = max(chunk_size, MIN_BLOCK_RANGE)
  print(f"[{token.symbol}] Writing results to {out_path}")

  with out_path.open("w", encoding="utf-8") as handle:
    from_block = token.start_block
    while from_block <= end_block:
      to_block = min(from_block + block_range - 1, end_block)
      params = {"address": token.address, "fromBlock": hex(from_block), "toBlock": hex(to_block)}
      try:
        transfer_logs = await rpc.get_logs({**params, "topics": [TRANSFER_TOPIC]})
        delegate_logs = await rpc.get_logs({**params, "topics": [DELEGATE_CHANGED_TOPIC]})
      except Exception as exc:  # noqa: BLE001
        if block_range > MIN_BLOCK_RANGE and should_reduce_span(exc):
          previous = block_range
          block_range = max(MIN_BLOCK_RANGE, math.floor(block_range / 2))
          print(
            f"[{token.symbol}] Range {from_block}-{to_block} rejected. "
            f"Reducing span from {previous} to {block_range}."
          )
          continue
        raise

      events: List[Tuple[int, int, str, Dict[str, Any]]] = []
      for log in transfer_logs:
        events.append((hex_to_int(log["blockNumber"]), hex_to_int(log["logIndex"]), "transfer", log))
      for log in delegate_logs:
        events.append((hex_to_int(log["blockNumber"]), hex_to_int(log["logIndex"]), "delegate", log))
      events.sort(key=lambda entry: (entry[0], entry[1]))

      for block_number, _idx, kind, log in events:
        if kind == "transfer":
          handle_transfer_log(balances, log)
        else:
          record = await handle_delegate_log(rpc, balances, token, block_number, log)
          if record:
            handle.write(json.dumps(record) + "\n")
      from_block = to_block + 1

  print(f"[{token.symbol}] Completed up to block {end_block}.")


async def main() -> None:
  parser = argparse.ArgumentParser(description="Export Aave delegation edges")
  parser.add_argument("--rpc-url", required=True, help="HTTPS RPC endpoint to query")
  parser.add_argument("--out-dir", default="delegation_export", help="Directory for NDJSON output")
  parser.add_argument("--tokens", default="", help="Comma-separated token symbols (AAVE,STKAAVE,AAAVE)")
  parser.add_argument("--end-block", type=int, default=None, help="Optional ending block")
  parser.add_argument("--chunk-size", type=int, default=DEFAULT_BLOCK_RANGE, help="Block span per request")
  args = parser.parse_args()

  tokens = get_token_configs([tok for tok in args.tokens.split(",") if tok])
  out_dir = Path(args.out_dir)

  async with aiohttp.ClientSession() as session:
    rpc = RpcClient(args.rpc_url, session)
    end_block = args.end_block or await rpc.latest_block()
    tasks = [export_token(rpc, token, out_dir, end_block, args.chunk_size) for token in tokens]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
  asyncio.run(main())
