#!/usr/bin/env python3
import argparse
import json
import logging
import math
import os
import time
import threading
from datetime import datetime, timedelta

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from dotenv import load_dotenv

# Importing from your modules
from clients import Environment
from KalshiClient import ExchangeClient  # Provides get_orderbook, get_trades, etc.

# -------------------------------
# Basic Logging Setup
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Global dictionary to track seen trade IDs per ticker
seen_trade_ids = {}


# -------------------------------
# Global Rate Limited Client with Lock
# -------------------------------
class RateLimitedExchangeClient(ExchangeClient):
    # Class-level lock for rate limiting across all requests
    _rate_lock = threading.Lock()

    def __init__(self, host, key_id, private_key, environment=Environment.PROD):
        # Call the parent constructor so that all API methods are available.
        super().__init__(host, key_id, private_key)
        self.last_api_call = datetime.now()

    def rate_limit(self) -> None:
        # Use a global lock to ensure that only one thread updates last_api_call at a time.
        with RateLimitedExchangeClient._rate_lock:
            THRESHOLD_IN_MILLISECONDS = 100
            now = datetime.now()
            if now - self.last_api_call < timedelta(milliseconds=THRESHOLD_IN_MILLISECONDS):
                sleep_time = THRESHOLD_IN_MILLISECONDS / 1000.0
                time.sleep(sleep_time)
            self.last_api_call = datetime.now()


# -------------------------------
# Scoring Function: retrieve_all_markets
# -------------------------------
def retrieve_all_markets(api_client: RateLimitedExchangeClient) -> list:
    """
    Retrieves all active markets via REST.
    Computes a score for each market using market-level data.
    Score = (effective_volume / (volatility_est + ε)) * exp(-((spread - target_spread)^2)/(2 * σ^2))
    effective_volume = volume_24h if >0, else liquidity.
    volatility_est = max(spread/10, 1.0)
    target_spread = 10, σ = 5, MIN_SPREAD = 3, ε = 1e-5.
    Returns the union of the top 50 markets and any markets where positions are held.
    """
    try:
        markets = []
        cursor = None
        while True:
            response = api_client.get_markets(cursor=cursor, status="open")
            markets.extend(response.get("markets", []))
            cursor = response.get("cursor")
            if not cursor:
                break

        active_markets = [m for m in markets if m.get("status") == "active"]
        print("retrieved", len(active_markets))
        return active_markets

    except Exception as e:
        logging.error(f"Error retrieving markets: {e}")
        return []


# -------------------------------
# Single Polling Function for Market Data
# -------------------------------
def poll_market_data(api_client: RateLimitedExchangeClient, ticker: str, data_dir: str):
    """
    Fetch orderbook and trade data for a given ticker once.
    Append each API response (with timestamp) to separate JSONL files.
    For trades, filter out duplicates based on trade_id.
    """
    timestamp = datetime.now().isoformat()
    orderbook_file = os.path.join(data_dir, f"{ticker}_orderbook.jsonl")
    trades_file = os.path.join(data_dir, f"{ticker}_trades.jsonl")
    logging.info(f"Polling data for ticker {ticker} at {timestamp}")

    # Get orderbook data
    try:
        orderbook_data = api_client.get_orderbook(ticker)["orderbook"]
        market_data = api_client.get_market(ticker)["market"]
        volume_data = {"volume_24h": market_data.get("volume_24h", 0), "timestamp": timestamp}
        yes_orders = orderbook_data.get("yes", [])
        no_orders = orderbook_data.get("no", [])
        best_yes_bid = max(order[0] for order in yes_orders)
        best_no_bid = max(order[0] for order in no_orders)
        best_yes_ask = 100 - best_no_bid
        spread = best_yes_ask - best_yes_bid
        spread_data = {"spread": spread, "timestamp": timestamp}
        liquidity_data = {"liquidity": market_data.get("liquidity", 0), "timestamp": timestamp}

        with open(orderbook_file, "a") as f:
            json_line = json.dumps({"timestamp": timestamp, "data": orderbook_data})
            f.write(json_line + "\n")
        with open(os.path.join(data_dir, f"{ticker}_volume.jsonl"), "a") as f:
            json_line = json.dumps({"timestamp": timestamp, "data": volume_data})
            f.write(json_line + "\n")
        with open(os.path.join(data_dir, f"{ticker}_spread.jsonl"), "a") as f:
            json_line = json.dumps({"timestamp": timestamp, "data": spread_data})
            f.write(json_line + "\n")
        with open(os.path.join(data_dir, f"{ticker}_liquidity.jsonl"), "a") as f:
            json_line = json.dumps({"timestamp": timestamp, "data": liquidity_data})
            f.write(json_line + "\n")

    except Exception as e:
        logging.error(f"Error fetching orderbook for {ticker}: {e}")

    # Get trades data and deduplicate
    try:
        trade_data = api_client.get_trades(ticker)
        trades = trade_data.get("trades", [])
        # Initialize seen set for ticker if not already present.
        if ticker not in seen_trade_ids:
            seen_trade_ids[ticker] = set()
        new_trades = []
        for trade in trades:
            trade_id = trade.get("trade_id")
            if trade_id and trade_id not in seen_trade_ids[ticker]:
                seen_trade_ids[ticker].add(trade_id)
                new_trades.append(trade)
        # Only write new trades if there are any.
        if new_trades:
            trade_data["trades"] = new_trades
            with open(trades_file, "a") as f:
                json_line = json.dumps({"timestamp": timestamp, "data": trade_data})
                f.write(json_line + "\n")
        else:
            logging.info(f"No new trades for ticker {ticker} at {timestamp}")
    except Exception as e:
        logging.error(f"Error fetching trades for {ticker}: {e}")


# -------------------------------
# Main Runner
# -------------------------------
def main():
    parser = argparse.ArgumentParser(description="Real-Time Data Archiving Service for Kalshi Markets")
    parser.add_argument("--duration", type=int, default=3600000, help="Duration to run in seconds (default: 3600)")
    parser.add_argument("--polling_interval", type=float, default=5.0, help="Polling interval in seconds (default: 5.0)")
    parser.add_argument("--data_dir", type=str, default="data/market_orderbook_and_trades", help="Directory to store archived data")
    parser.add_argument("--environment", type=str, choices=["demo", "prod"], default="prod", help="API environment (default: demo)")
    args = parser.parse_args()

    load_dotenv()

    KALSHI_URL = os.getenv("KALSHI_URL")
    KALSHI_API_KEY = os.getenv("KALSHI_API_KEY")
    KALSHI_CERT_FILE_PATH = os.getenv("KALSHI_CERT_FILE_PATH", "crt.pem")

    # Create the data directory if it doesn't exist.
    os.makedirs(args.data_dir, exist_ok=True)

    # Load RSA private key from file.
    try:
        with open(KALSHI_CERT_FILE_PATH, "rb") as key_file:
            private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())
    except Exception as e:
        logging.error(f"Failed to load private key: {e}")
        return

    # Initialize the RateLimitedExchangeClient for both scoring and polling.
    env = Environment.PROD
    api_client = RateLimitedExchangeClient(KALSHI_URL, KALSHI_API_KEY, private_key, environment=env)

    # Retrieve and score all active markets using the provided function.
    top_markets = retrieve_all_markets(api_client)
    filtered_markets = [market for market in top_markets if "ticker" in market]

    # Filter out markets that are closing soon (less than 1 hour remaining).
    MIN_TIME_TO_CLOSE = timedelta(days=7)

    tickers = [market["ticker"] for market in filtered_markets if "ticker" in market]
    logging.info(f"Found {len(tickers)} markets that remain open for more than {MIN_TIME_TO_CLOSE}.")

    # Set the end time for archiving.
    end_time = datetime.now() + timedelta(seconds=args.duration)

    # Sequentially poll each market in a round-robin loop until the duration expires.
    while datetime.now() < end_time:
        for ticker in tickers:
            poll_market_data(api_client, ticker, args.data_dir)
        # Sleep for the polling interval after each complete cycle.
        time.sleep(args.polling_interval)

    logging.info("Data archiving completed.")


if __name__ == "__main__":
    main()
