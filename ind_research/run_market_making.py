#!/usr/bin/env python3
"""
Used to be ind_research_3.py
Comprehensive Market-Making Bot for Kalshi Markets with Dynamic Market Selection

This script integrates:
  1. Robust REST API integration with retry logic.
  2. A WebSocket client (using KalshiHttpClient for WS) that supports dynamic subscriptions.
  3. A MarketDataHandler that first uses the WS cache and falls back to REST.
  4. An enhanced RiskManager with dynamic risk limits and caching.
  5. A StrategyEngine implementing an enhanced Avellaneda–Stoikov pricing model.
  6. An OrderManager for order placement and cancellation.
  7. A MarketMaker that executes trading cycles.
  8. A MarketOpportunityBot that:
       - Retrieves all markets via REST.
       - Computes a custom score (based on volume and spread) using market object data
         so that subsequent API calls to compute spread/midpoint are not needed.
       - Maintains a top-10 list of promising markets, plus any market where a position is held.
  9. A simulation/backtesting routine.

Notes:
  - For accurate 24h volume, the REST API’s get_trades endpoint is used.
  - The WebSocket connection is authenticated using Kalshi’s signing method.
  - This script requires external libraries: requests, websockets, asyncio, cryptography, dotenv.
"""

import os
import time
import math
import random
import json
import logging
import threading
import asyncio
import websockets  # pip install websockets
from datetime import datetime, timedelta
from enum import Enum
from dotenv import load_dotenv
import base64
from typing import Any, Dict, Optional

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
import requests

# ----------------------------------------------------------------------
# Import official Kalshi client classes:
# ExchangeClient is for REST API and KalshiHttpClient is for WebSocket.
# ----------------------------------------------------------------------
from clients import KalshiHttpClient, Environment
from KalshiClient import ExchangeClient

# Load environment variables.
load_dotenv()

# ----------------------------------------------------------------------
# Global Logging Configuration
# ----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ----------------------------------------------------------------------
# API Integration Module: Retry Helper
# ----------------------------------------------------------------------
def api_call_with_retry(api_func, max_retries=3, initial_delay=1, *args, **kwargs):
    """
    Wraps an API call with exponential backoff retry logic.
    Logs each attempt with function name, args, kwargs, and attempt number.
    """
    for attempt in range(max_retries):
        try:
            result = api_func(*args, **kwargs)
            return result
        except Exception as e:
            logging.error(f"Attempt {attempt+1} for {api_func.__name__} failed with error: {e}")
            time.sleep(initial_delay * (2**attempt))
    raise Exception(f"API call to {api_func.__name__} failed after {max_retries} attempts.")


# ----------------------------------------------------------------------
# Configuration Module: RiskManagerConfig
# ----------------------------------------------------------------------
class RiskManagerConfig:
    """
    Centralizes configuration parameters for risk management.
    Loaded from environment variables with sensible defaults.
    """

    def __init__(self):
        self.high_vol_threshold = float(os.getenv("HIGH_VOL_THRESHOLD", "5.0"))
        self.vol_reduction_factor = float(os.getenv("VOL_REDUCTION_FACTOR", "0.5"))
        self.vol_cache_duration = int(os.getenv("VOL_CACHE_DURATION", "60"))  # seconds


# ----------------------------------------------------------------------
# WebSocket Integration Module: KalshiWSClient (Dynamic Subscriptions)
# ----------------------------------------------------------------------
class KalshiWSClient(KalshiHttpClient):
    """
    Extends the official KalshiHttpClient for WebSocket usage.
    Supports dynamic subscriptions to specific market tickers.
    """

    def __init__(self, key_id: str, private_key: rsa.RSAPrivateKey, environment: Environment = Environment.DEMO):
        super().__init__(key_id, private_key, environment)
        self.ws = None
        self.url_suffix = "/trade-api/ws/v2"
        self.message_id = 1
        self.subscribed_tickers = set()  # For ticker subscriptions.
        self.subscribed_orderbook_tickers = set()  # For orderbook_delta subscriptions.
        self.orderbook_cache = {}  # Cache for orderbook data.
        self.ticker_cache = {}  # Cache for ticker data.
        self.lock = threading.Lock()  # For thread-safe cache access.

    async def connect(self):
        """Connects to the WS endpoint using authentication."""
        host = self.WS_BASE_URL + self.url_suffix
        auth_headers = self.request_headers("GET", self.url_suffix)
        while True:
            try:
                self.ws = await websockets.connect(host, additional_headers=auth_headers)
                logging.info("WebSocket connection established.")
                break
            except Exception as e:
                logging.error(f"WebSocket connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    async def subscribe_to_ticker(self, ticker: str):
        """Subscribes to the ticker channel for a specific market ticker."""
        if ticker in self.subscribed_tickers:
            logging.info(f"Already subscribed to ticker {ticker}")
            return
        command = {"id": self.message_id, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_tickers": [ticker]}}
        try:
            await self.ws.send(json.dumps(command))
            logging.info(f"Sent ticker subscription for {ticker}: {command}")
            self.message_id += 1
            self.subscribed_tickers.add(ticker)
        except Exception as e:
            logging.error(f"Error subscribing to ticker {ticker}: {e}")

    async def subscribe_to_orderbook_delta(self, ticker: str):
        """Subscribes to the orderbook_delta channel for a specific market ticker."""
        if ticker in self.subscribed_orderbook_tickers:
            logging.info(f"Already subscribed to orderbook_delta for {ticker}")
            return
        command = {"id": self.message_id, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": [ticker]}}
        try:
            await self.ws.send(json.dumps(command))
            logging.info(f"Sent orderbook_delta subscription for {ticker}: {command}")
            self.message_id += 1
            self.subscribed_orderbook_tickers.add(ticker)
        except Exception as e:
            logging.error(f"Error subscribing to orderbook_delta for {ticker}: {e}")

    async def handler(self):
        """Handles incoming WS messages and updates caches."""
        try:
            async for message in self.ws:
                data = json.loads(message)
                msg_type = data.get("type")
                if msg_type in ["orderbook_snapshot", "orderbook_delta"]:
                    market_ticker = data.get("msg", {}).get("market_ticker")
                    if market_ticker:
                        with self.lock:
                            self.orderbook_cache[market_ticker] = data.get("msg")
                        logging.debug(f"Updated orderbook cache for {market_ticker}")
                elif msg_type == "ticker":
                    market_ticker = data.get("msg", {}).get("market_ticker")
                    if market_ticker:
                        with self.lock:
                            self.ticker_cache[market_ticker] = data.get("msg")
                        logging.debug(f"Updated ticker cache for {market_ticker}")
                else:
                    logging.debug(f"Received WS message: {data}")
        except Exception as e:
            logging.error(f"WebSocket message handling error: {e}")

    async def run(self):
        await self.connect()
        # We do not subscribe globally; subscriptions occur dynamically.
        await self.handler()

    def start(self):
        """Starts the WS client in a separate thread."""

        def run_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.run())

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()


with open("crt.pem", "rb") as key_file:
    private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())
# with open("crt_demo.pem", "rb") as key_file:
#     private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())
# Create a global WS client instance.
ws_client = KalshiWSClient(
    key_id=os.getenv("KALSHI_API_KEY"),
    private_key=private_key,
    environment=Environment.PROD,  # or Environment.PROD as needed
)
# Start WS connection.
ws_client.start()


# ----------------------------------------------------------------------
# MarketDataHandler Module (Using WS and REST for Volume)
# ----------------------------------------------------------------------
class MarketDataHandler:
    """
    Retrieves and computes market data.
    First attempts to use WS cache; falls back to REST.
    Uses REST get_trades for accurate 24h volume.
    Also uses market-level data (from get_markets) to compute mid-price and spread without extra API calls.
    """

    def __init__(self, client):
        self.client = client
        self.ws_client = ws_client

    def get_orderbook(self, market_ticker: str) -> dict:
        try:
            response = api_call_with_retry(self.client.get_orderbook, max_retries=3, initial_delay=1, ticker=market_ticker)
            return response.get("orderbook", {})
        except Exception as e:
            logging.error(f"Error fetching orderbook via REST for {market_ticker}: {e}")
            return {}

    def calculate_mid_price(self, market_ticker: str) -> float:
        """
        Calculates the mid-price as the average of the best YES bid and best YES ask.

        For a YES market:
          - Best YES bid: max price in the "yes" orders.
          - Best YES ask: computed as 100 - (max price in "no" orders).

        :param market_ticker: The market identifier.
        :return: Mid-price, or None if data is insufficient.
        """
        orderbook = self.get_orderbook(market_ticker)
        yes_orders = orderbook.get("yes", [])
        no_orders = orderbook.get("no", [])
        if not yes_orders or not no_orders:
            logging.warning(f"Missing order data for {market_ticker}")
            return None
        best_yes_bid = max(order[0] for order in yes_orders)
        best_no_bid = max(order[0] for order in no_orders)
        best_yes_ask = 100 - best_no_bid
        return (best_yes_bid + best_yes_ask) / 2.0

    def calculate_spread(self, market_ticker: str) -> float:
        """
        Calculates the bid-ask spread for the market.

        :param market_ticker: The market identifier.
        :return: Spread, or None if data is insufficient.
        """
        orderbook = self.get_orderbook(market_ticker)
        yes_orders = orderbook.get("yes", [])
        no_orders = orderbook.get("no", [])
        if not yes_orders or not no_orders:
            logging.warning(f"Missing order data for {market_ticker}")
            return None
        best_yes_bid = max(order[0] for order in yes_orders)
        best_no_bid = max(order[0] for order in no_orders)
        best_yes_ask = 100 - best_no_bid
        return best_yes_ask - best_yes_bid

    def calculate_volume_24h(self, market_ticker: str) -> int:
        now = time.time()
        start_ts = int(now - 24 * 60 * 60)
        total_volume = 0
        cursor = None
        try:
            while True:
                response = api_call_with_retry(self.client.get_trades, max_retries=3, initial_delay=1, ticker=market_ticker, min_ts=start_ts, max_ts=int(now), cursor=cursor)
                trades = response.get("trades", [])
                total_volume += sum(trade.get("count", 0) for trade in trades)
                cursor = response.get("cursor")
                if not cursor:
                    break
            return total_volume
        except Exception as e:
            logging.error(f"Error fetching 24h trades for {market_ticker}: {e}")
            return 0

    def get_recent_trades(self, market_ticker: str, minutes: int = 5) -> list:
        now = time.time()
        start_ts = int(now - minutes * 60)
        trades = []
        cursor = None
        try:
            while True:
                response = api_call_with_retry(self.client.get_trades, max_retries=3, initial_delay=1, ticker=market_ticker, min_ts=start_ts, cursor=cursor)
                trades.extend(response.get("trades", []))
                cursor = response.get("cursor")
                if not cursor:
                    break
            return trades
        except Exception as e:
            logging.error(f"Error fetching trades for {market_ticker}: {e}")
            return []

    def calculate_volatility(self, market_ticker: str, minutes: int = 5) -> float:
        trades = self.get_recent_trades(market_ticker, minutes)
        if not trades:
            return 0
        prices = [trade.get("yes_price", 0) for trade in trades if "yes_price" in trade]
        if not prices:
            return 0
        avg_price = sum(prices) / len(prices)
        variance = sum((p - avg_price) ** 2 for p in prices) / len(prices)
        return math.sqrt(variance)


# ----------------------------------------------------------------------
# RiskManager Module (Enhanced)
# ----------------------------------------------------------------------
class RiskManager:
    """
    Manages risk by enforcing dynamic position limits and adjusting order sizes.
    """

    def __init__(self, client, data_handler=None, base_max_position: int = 100, config: RiskManagerConfig = None):
        self.client = client
        self.base_max_position = base_max_position
        self.data_handler = data_handler if data_handler is not None else MarketDataHandler(client)
        self.volatility_cache = {}
        self.cache_lock = threading.Lock()
        self.config = config if config is not None else RiskManagerConfig()

    def get_current_position(self, market_ticker: str) -> int:
        """
        Retrieves the current net position for a given market using pagination.
        Now includes both filled positions and resting orders (if available).
        """
        positions = []
        cursor = None
        try:
            while True:
                response = self.client.get_positions(ticker=market_ticker, cursor=cursor)
                positions.extend(response.get("market_positions", []))
                cursor = response.get("cursor")
                if not cursor:
                    break
            # Sum filled positions.
            filled = sum(pos.get("position", 0) for pos in positions if pos.get("ticker") == market_ticker)
            # Sum resting orders; many APIs provide a "resting_orders_count" field.
            resting = sum(pos.get("resting_orders_count", 0) for pos in positions if pos.get("ticker") == market_ticker)
            net_position = filled + resting
            logging.info(f"[{market_ticker}] Filled: {filled}, Resting: {resting}, Total Effective Position: {net_position}")
            return net_position
        except Exception as e:
            logging.error(f"Error fetching positions for {market_ticker}: {e}")
            return 0

    def get_cached_volatility(self, market_ticker: str) -> float:
        current_time = time.time()
        with self.cache_lock:
            if market_ticker in self.volatility_cache:
                cached_vol, timestamp = self.volatility_cache[market_ticker]
                logging.debug(f"[{market_ticker}] Cached volatility timestamp: {timestamp}, current time: {current_time}")
                if (current_time - timestamp) < self.config.vol_cache_duration:
                    logging.info(f"[{market_ticker}] Using cached volatility: {cached_vol:.4f}")
                    return cached_vol
            volatility = self.data_handler.calculate_volatility(market_ticker)
            self.volatility_cache[market_ticker] = (volatility, current_time)
            logging.info(f"[{market_ticker}] New volatility calculated: {volatility:.4f} at time: {current_time}")
            return volatility

    def get_dynamic_max_position(self, market_ticker: str) -> int:
        volatility = self.get_cached_volatility(market_ticker)
        if volatility > self.config.high_vol_threshold:
            dynamic_max = int(self.base_max_position * self.config.vol_reduction_factor)
        else:
            dynamic_max = self.base_max_position
        logging.info(f"[{market_ticker}] Dynamic max position: {dynamic_max} (Base: {self.base_max_position}) based on volatility: {volatility:.4f}")
        return dynamic_max

    def adjust_order_size(self, size: int, market_ticker: str) -> int:
        current_position = self.get_current_position(market_ticker)
        dynamic_max = self.get_dynamic_max_position(market_ticker)
        if abs(current_position) >= dynamic_max:
            logging.warning(f"[{market_ticker}] ALERT: Current position ({current_position}) meets/exceeds dynamic max ({dynamic_max}). No further orders allowed.")
            return 0
        if abs(current_position) + size > dynamic_max:
            size = max(0, dynamic_max - abs(current_position))
            logging.info(f"[{market_ticker}] Adjusted order size to {size} to remain within dynamic max ({dynamic_max}).")
        return size

    def should_trade(self, market_ticker: str) -> bool:
        current_position = self.get_current_position(market_ticker)
        dynamic_max = self.get_dynamic_max_position(market_ticker)
        if abs(current_position) >= dynamic_max:
            logging.info(f"[{market_ticker}] Trading halted: current position ({current_position}) meets/exceeds dynamic max ({dynamic_max}).")
            return False
        return True

    def order_safeguards(self, order_details: dict) -> bool:
        return True


# ----------------------------------------------------------------------
# StrategyEngine Module
# ----------------------------------------------------------------------
class StrategyEngine:
    """
    Computes optimal bid and ask prices using a refined Avellaneda–Stoikov framework.

    New configurable parameters:
      - gamma: Inventory risk aversion parameter.
      - session_duration: Total trading session length (T). For an infinite-horizon model, set this to 1.
      - kappa: Order book liquidity parameter.

    The reservation price is computed as:
        r = s - q * gamma * sigma^2 * (T-t)
    and the optimal total spread is computed as:
        delta = gamma * sigma^2 * (T-t) + (2/gamma)*ln(1 + gamma/kappa)
    Then orders are placed symmetrically around r:
        bid = r - delta/2
        ask = r + delta/2
    """

    def __init__(self, risk_manager, gamma=0.1, session_duration=1.0, kappa=1.5):
        self.risk_manager = risk_manager
        self.gamma = gamma
        self.session_duration = session_duration  # T (and for an infinite horizon, you can set T = 1)
        self.kappa = kappa

    def compute_time_remaining(self):
        """
        Computes (T-t). For an infinite-horizon model we can simply return 1.
        In a more advanced implementation, this could be based on the current time and a target closing time.
        """
        # For our purposes, we assume an infinite horizon:
        return 1.0

    def determine_reservation_price(self, s, q, sigma, time_remaining):
        """
        Calculates the reservation price.
        s: current market mid-price
        q: current inventory
        sigma: market volatility
        time_remaining: (T-t)
        """
        return s - q * self.gamma * (sigma**2) * time_remaining

    def determine_optimal_spread(self, sigma, time_remaining):
        """
        Calculates the optimal total spread.
        sigma: market volatility
        time_remaining: (T-t)
        """
        # This follows the idea of the Avellaneda-Stoikov spread formula.
        return self.gamma * (sigma**2) * time_remaining + (2 / self.gamma) * math.log(1 + self.gamma / self.kappa)

    def determine_order_prices(self, market_ticker: str, mid_price: float, volatility: float, spread: float) -> tuple:
        """
        Computes the optimal bid and ask prices.
        """
        time_remaining = self.compute_time_remaining()
        q = self.risk_manager.get_current_position(market_ticker)
        reservation_price = self.determine_reservation_price(mid_price, q, volatility, time_remaining)
        optimal_spread = self.determine_optimal_spread(volatility, time_remaining)

        bid_price = reservation_price - optimal_spread / 2
        ask_price = reservation_price + optimal_spread / 2

        logging.info(f"[{market_ticker}] Mid-price: {mid_price:.2f}, Volatility: {volatility:.4f}, q: {q}, T-t: {time_remaining}")
        logging.info(f"[{market_ticker}] Reservation price: {reservation_price:.2f}, Optimal spread: {optimal_spread:.2f}")
        logging.info(f"[{market_ticker}] Final bid: {bid_price:.2f}, Final ask: {ask_price:.2f}")

        return math.floor(bid_price), math.ceil(ask_price)

    def determine_order_size(self, market_ticker: str, volatility: float) -> int:
        base_size = 10
        # If volatility is low, you might want to place larger orders.
        size = max(1, int(base_size / volatility)) if volatility > 0 else base_size
        approved_size = self.risk_manager.adjust_order_size(size, market_ticker)
        logging.info(f"[{market_ticker}] Determined order size: {approved_size}")
        return approved_size


# ----------------------------------------------------------------------
# OrderManager Module
# ----------------------------------------------------------------------


class OrderManager:
    """
    Manages order placement, cancellation, and tracking via the API.
    """

    def __init__(self, client):
        self.client = client
        self.active_orders = {}
        self.ticker_to_order_id = {}

    def place_order(self, market_ticker: str, side: str, price: float, size: int):
        client_order_id = f"{market_ticker}-{side}-{int(time.time() % 1000)}-{random.randint(100, 999)}".replace(".", "")
        order_type = "limit"
        try:
            if side == "bid":
                order = self.client.create_order(ticker=market_ticker, client_order_id=client_order_id, action="buy", side="yes", count=size, type=order_type, yes_price=price, post_only=True)
            elif side == "ask":
                order = self.client.create_order(ticker=market_ticker, client_order_id=client_order_id, action="sell", side="yes", count=size, type=order_type, no_price=100 - price, post_only=True)
            else:
                logging.error("Invalid order side specified.")
                return None
            if order:
                order_id = order["order"]["order_id"]
                # Add a timestamp to track when the order was placed.
                order["timestamp"] = time.time()
                self.active_orders[order_id] = order
                if self.ticker_to_order_id.get(market_ticker) is None:
                    self.ticker_to_order_id[market_ticker] = {"bid": [], "ask": []}

                if side == "bid":
                    self.ticker_to_order_id[market_ticker]["bid"].append(order_id)
                elif side == "ask":
                    self.ticker_to_order_id[market_ticker]["ask"].append(order_id)
                logging.info(f"[{market_ticker}] Placed {side} order {client_order_id} at price {price} for size {size}")
            return order
        except Exception as e:
            logging.error(f"Error placing order on {market_ticker}: {e}")
            return None

    def cancel_order(self, order_id: str):
        try:
            result = self.client.cancel_order(order_id)
            if result:
                logging.info(f"Canceled order {order_id}")
                self.active_orders.pop(order_id, None)
            return result
        except Exception as e:
            logging.error(f"Error canceling order {order_id}: {e}")
            return None

    def cancel_all_orders(self):
        for order_id in list(self.active_orders.keys()):
            self.cancel_order(order_id)
        self.active_orders = {}


# ----------------------------------------------------------------------
# MarketMaker Module (Updated to use full market object)
# ----------------------------------------------------------------------
# --- MarketMaker Module (modified process_cycle) ---


class MarketMaker:
    """
    Executes a market-making cycle for a single market.
    Now stores the full market object for calculations.
    """

    def __init__(self, client, market: dict):
        self.market = market  # Store full market dictionary.
        self.ticker = market.get("ticker")
        self.data_handler = MarketDataHandler(client)
        self.risk_manager = RiskManager(client)
        self.strategy_engine = StrategyEngine(self.risk_manager)
        self.order_manager = OrderManager(client)

    async def process_cycle(self):
        try:
            if not self.risk_manager.should_trade(self.ticker):
                self.order_manager.cancel_all_orders()
                return
            current_time = time.time()
            order_age_threshold = 45  # seconds
            for order_id, order in list(self.order_manager.active_orders.items()):
                order_action = order["order"].get("action", "").lower()
                order_timestamp = order.get("timestamp", current_time)
                age = current_time - order_timestamp

                # If an order is older than the threshold, cancel it.
                if age > order_age_threshold:
                    logging.info(f"[{self.ticker}] Cancelling order {order_id} due to age {age:.2f} seconds.")
                    self.order_manager.cancel_order(order_id)

            # Calculate current optimal prices using market data.
            mid_price = self.data_handler.calculate_mid_price(self.ticker)
            spread = self.data_handler.calculate_spread(self.ticker)
            volatility = self.data_handler.calculate_volatility(self.ticker)
            if mid_price is None or spread is None:
                logging.warning(f"[{self.ticker}] Insufficient data; skipping cycle.")
                return

            logging.info(f"[{self.ticker}] mid_price={mid_price:.2f}, spread={spread:.2f}, volatility={volatility:.4f}")
            bid_price, ask_price = self.strategy_engine.determine_order_prices(self.ticker, mid_price, volatility, spread)
            bid_size = self.strategy_engine.determine_order_size(self.ticker, volatility)
            # ask_size = self.strategy_engine.determine_order_size(self.ticker, volatility) DO THIS USING INVENTORY
            ask_size = bid_size

            if bid_size > 0 and bid_price > 0 and ask_size > 0 and ask_price < 100:
                if self.order_manager.ticker_to_order_id.get(self.ticker) is not None:
                    for side in ["bid", "ask"]:
                        for order_id in self.order_manager.ticker_to_order_id[self.ticker][side]:
                            result = self.order_manager.cancel_order(order_id)
                            if result:
                                self.order_manager.ticker_to_order_id[self.ticker][side].remove(order_id)
                self.order_manager.place_order(self.ticker, "bid", bid_price, bid_size)
                self.order_manager.place_order(self.ticker, "ask", ask_price, ask_size)

        except Exception as e:
            logging.error(f"Error processing cycle for {self.ticker}: {e}")


# ----------------------------------------------------------------------
# MarketOpportunityBot Module
# ----------------------------------------------------------------------
class MarketOpportunityBot:
    """
    Scans active markets, ranks them based on a custom score, and triggers market-making cycles.
    Maintains a top 10 list (plus markets with existing positions) for continuous monitoring.
    """

    MIN_SPREAD = 3.0
    MAX_VOLATILITY = 5.0
    MIN_VOLUME_24H = 1000

    def __init__(self, client):
        self.client = client
        self.market_makers = {}
        self.data_handler = MarketDataHandler(client)
        self.full_market_list = []
        self.top_markets = []  # Top 10 markets to monitor continuously.

    def retrieve_all_markets(self) -> list:
        """
        Retrieves all active markets via REST.
        Computes a score for each market using market-level data.
        Score = (effective_volume / (volatility_est + ε)) * exp(-((spread - target_spread)^2)/(2 * σ^2))
        effective_volume = volume_24h if >0, else liquidity.
        volatility_est = max(spread/10, 1.0)
        target_spread = 10, σ = 5, MIN_SPREAD = 3, ε = 1e-5.
        Returns the union of the top 10 markets and any markets where positions are held.
        """
        try:
            markets = []
            cursor = None
            while True:
                response = self.client.get_markets(cursor=cursor, status="open")
                markets.extend(response.get("markets", []))
                cursor = response.get("cursor")
                if not cursor:
                    break
            active_markets = [m for m in markets if m.get("status") == "active"]

            # Define scoring parameters.
            target_spread = 10.0
            sigma = target_spread / 2.0  # i.e., 5.0
            MIN_SPREAD = 3.0
            epsilon = 1e-5

            for market in active_markets:
                yes_bid = market.get("yes_bid", 0)
                yes_ask = market.get("yes_ask", 0)
                spread = yes_ask - yes_bid
                volume = market.get("volume_24h", 0)
                if spread < MIN_SPREAD or volume == 0:
                    market["score"] = 0.0
                else:
                    liquidity = market.get("liquidity", 0)
                    effective_volume = volume if volume > 0 else liquidity
                    volatility_est = max(spread / 10.0, 1.0)
                    market["score"] = (effective_volume / (volatility_est + epsilon)) * math.exp(-((spread - target_spread) ** 2) / (2 * (sigma**2)))
            active_markets.sort(key=lambda m: m["score"], reverse=True)
            top_10 = active_markets[:10]

            # Get markets with positions using a single API call.
            pos_response = self.data_handler.client.get_positions(count_filter="position", settlement_status="unsettled")
            positions = pos_response.get("market_positions", [])
            tickers_with_positions = {p["ticker"] for p in positions if p.get("position", 0) != 0}
            markets_with_positions = [m for m in active_markets if m.get("ticker") in tickers_with_positions]

            union_markets = {m["ticker"]: m for m in (top_10 + markets_with_positions)}
            self.top_markets = list(union_markets.values())
            print("Top markets", self.top_markets)
            with open("top_markets.json", "w") as f:
                json.dump(self.top_markets, f, indent=2)
            return self.top_markets
        except Exception as e:
            logging.error(f"Error retrieving markets: {e}")
            return []

    async def evaluate_market(self, market: dict) -> bool:
        """
        Asynchronously evaluates if a market meets our criteria based on market-level data.
        Uses the data already provided by get_markets.
        """
        ticker = market.get("ticker")
        volume = market.get("volume_24h", 0)
        if volume < self.MIN_VOLUME_24H:
            return False

        spread = market.get("yes_ask", 0) - market.get("yes_bid", 0)
        if spread < self.MIN_SPREAD:
            return False

        logging.info(f"Market {ticker} qualifies: spread={spread:.2f}, volume_24h={volume}")
        await ws_client.subscribe_to_ticker(ticker)
        await ws_client.subscribe_to_orderbook_delta(ticker)
        return True

    async def run(self):
        logging.info("Starting Market Opportunity Bot.")
        start_time = time.time()
        self.top_markets = self.retrieve_all_markets()
        while True:
            current_time = time.time()
            if current_time - start_time > 60:
                start_time = current_time
                self.top_markets = self.retrieve_all_markets()
            logging.info(f"Monitoring top {len(self.top_markets)} markets.")
            tickers = [m.get("ticker") for m in self.top_markets]
            print(tickers)
            for market in self.top_markets:
                ticker = market.get("ticker")
                if not ticker:
                    continue
                if await self.evaluate_market(market):
                    if ticker not in self.market_makers:
                        self.market_makers[ticker] = MarketMaker(self.client, market)
                    await self.market_makers[ticker].process_cycle()


# ----------------------------------------------------------------------
# Simulation/Backtesting Routine
# ----------------------------------------------------------------------
def simulate_backtest():
    logging.info("Starting simulation/backtesting routine.")
    simulated_mid_price = 50.0
    simulated_volatility = 3.0
    simulated_spread = 5.0
    net_inventory = 0
    num_cycles = 10
    gamma = 0.5
    T = 1.0
    k = 1.5

    for cycle in range(num_cycles):
        net_inventory += random.choice([-1, 0, 1])
        delta_bid = (gamma * (simulated_volatility**2) * T) / 2 + (1 / gamma) * math.log(1 + gamma / k)
        delta_ask = (gamma * (simulated_volatility**2) * T) / 2 - (1 / gamma) * math.log(1 + gamma / k)
        inventory_adjustment = gamma * (simulated_volatility**2) * T * net_inventory
        bid_price = simulated_mid_price - delta_bid - inventory_adjustment
        ask_price = simulated_mid_price + delta_ask - inventory_adjustment
        logging.info(f"Cycle {cycle+1}:")
        logging.info(f"  Net Inventory: {net_inventory}")
        logging.info(f"  δ_bid: {delta_bid:.4f}, δ_ask: {delta_ask:.4f}")
        logging.info(f"  Inventory adjustment: {inventory_adjustment:.4f}")
        logging.info(f"  Final bid: {bid_price:.2f}, Final ask: {ask_price:.2f}")
        logging.info("--------------------------------------------------")
        time.sleep(1)


# ----------------------------------------------------------------------
# Main Function
# ----------------------------------------------------------------------
def main():
    load_dotenv()
    KALSHI_URL = os.getenv("KALSHI_URL")
    KALSHI_API_KEY = os.getenv("KALSHI_API_KEY")
    KALSHI_CERT_FILE_PATH = os.getenv("KALSHI_CERT_FILE_PATH", "crt.pem")
    # KALSHI_CERT_FILE_PATH = os.getenv("KALSHI_CERT_FILE_PATH", "crt_demo.pem")

    with open(KALSHI_CERT_FILE_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())

    # Use ExchangeClient for REST API calls.
    client = ExchangeClient(KALSHI_URL, KALSHI_API_KEY, private_key)

    # Create MarketOpportunityBot instance.
    bot = MarketOpportunityBot(client)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Stopping Market Opportunity Bot...")

    # Uncomment the following line to run simulation/backtesting.
    # simulate_backtest()


if __name__ == "__main__":
    main()
