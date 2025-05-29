#!/usr/bin/env python3
"""
THIS IS A WORK IN PROGRESS, DO NOT RUN THIS SCRIPT YET.

Comprehensive Market-Making Bot for Kalshi Markets with Asynchronous Market Updates

This script integrates:
  1. Robust REST API integration with retry logic.
  2. A WebSocket client (using KalshiHttpClient for WS) that supports dynamic subscriptions.
  3. A MarketDataHandler that first uses the WS cache and falls back to REST.
  4. An enhanced RiskManager with dynamic risk limits and caching.
  5. A StrategyEngine implementing an enhanced Avellaneda–Stoikov pricing model.
  6. An OrderManager for order placement and cancellation.
  7. A MarketMaker that executes trading cycles (asynchronously).
  8. A MarketOpportunityBot that scans markets and dynamically updates the top markets
     while continuously running trading cycles.
  9. A simulation/backtesting routine.

Notes:
  - For accurate 24h volume, the REST API’s get_trades endpoint is used.
  - The WS connection is authenticated using Kalshi’s signing method.
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
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature
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
    """

    def __init__(self):
        self.high_vol_threshold = float(os.getenv("HIGH_VOL_THRESHOLD", "5.0"))
        self.vol_reduction_factor = float(os.getenv("VOL_REDUCTION_FACTOR", "0.5"))
        self.vol_cache_duration = int(os.getenv("VOL_CACHE_DURATION", "60"))  # seconds


# ----------------------------------------------------------------------
# WebSocket Integration Module: KalshiWSClient (Dynamic Subscription)
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
        self.subscribed_tickers = set()  # For ticker channel subscriptions.
        self.subscribed_orderbook_tickers = set()  # For orderbook_delta subscriptions.
        self.orderbook_cache = {}  # Cache for orderbook data keyed by market ticker.
        self.ticker_cache = {}  # Cache for ticker data keyed by market ticker.
        self.lock = threading.Lock()  # For thread-safe access to caches.

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
        # We no longer subscribe globally; subscriptions occur dynamically.
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
    Also uses market-level data from the market object to compute spread and mid-price.
    """

    def __init__(self, client):
        self.client = client
        self.ws_client = ws_client

    def get_orderbook(self, market_ticker: str) -> dict:
        if self.ws_client.ws is not None:
            with self.ws_client.lock:
                orderbook = self.ws_client.orderbook_cache.get(market_ticker)
            if orderbook:
                logging.info(f"Using WS orderbook data for {market_ticker}.")
                return orderbook
        logging.info(f"No WS orderbook data for {market_ticker}; falling back to REST API.")
        try:
            response = api_call_with_retry(self.client.get_orderbook, max_retries=3, initial_delay=1, ticker=market_ticker)
            return response.get("orderbook", {})
        except Exception as e:
            logging.error(f"Error fetching orderbook via REST for {market_ticker}: {e}")
            return {}

    def calculate_mid_price(self, market: dict) -> Optional[float]:
        """
        Computes mid-price directly from the market object.
        """
        yes_bid = market.get("yes_bid")
        yes_ask = market.get("yes_ask")
        if yes_bid is None or yes_ask is None:
            logging.warning(f"Missing bid/ask data in market object for {market.get('ticker')}")
            return None
        return (yes_bid + yes_ask) / 2.0

    def calculate_spread(self, market: dict) -> Optional[float]:
        """
        Computes spread directly from the market object.
        """
        yes_bid = market.get("yes_bid")
        yes_ask = market.get("yes_ask")
        if yes_bid is None or yes_ask is None:
            logging.warning(f"Missing bid/ask data in market object for {market.get('ticker')}")
            return None
        return yes_ask - yes_bid

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
        positions = []
        cursor = None
        try:
            while True:
                response = self.client.get_positions(ticker=market_ticker, cursor=cursor)
                positions.extend(response.get("market_positions", []))
                cursor = response.get("cursor")
                if not cursor:
                    break
            net_position = sum(pos.get("position", 0) for pos in positions if pos.get("ticker") == market_ticker)
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
    Computes optimal bid and ask prices using an enhanced Avellaneda–Stoikov framework.
    """

    def __init__(self, risk_manager):
        self.risk_manager = risk_manager

    def determine_order_prices(self, market_ticker: str, mid_price: float, volatility: float, spread: float) -> tuple:
        gamma = 0.5  # Risk aversion.
        T = 1.0  # Time horizon.
        k = 1.5  # Order flow intensity.

        delta_bid = (gamma * (volatility**2) * T) / 2 + (1 / gamma) * math.log(1 + gamma / k)
        delta_ask = (gamma * (volatility**2) * T) / 2 - (1 / gamma) * math.log(1 + gamma / k)

        net_inventory = self.risk_manager.get_current_position(market_ticker)
        inventory_adjustment = gamma * (volatility**2) * T * net_inventory

        bid_price = mid_price - delta_bid - inventory_adjustment
        ask_price = mid_price + delta_ask - inventory_adjustment

        logging.info(f"[{market_ticker}] Mid-price: {mid_price:.2f}, Volatility: {volatility:.4f}")
        logging.info(f"[{market_ticker}] δ_bid: {delta_bid:.4f}, δ_ask: {delta_ask:.4f}")
        logging.info(f"[{market_ticker}] Net inventory: {net_inventory}, Inventory adjustment: {inventory_adjustment:.4f}")
        logging.info(f"[{market_ticker}] Final bid: {bid_price:.2f}, Final ask: {ask_price:.2f}")

        return math.floor(bid_price), math.ceil(ask_price)

    def determine_order_size(self, market_ticker: str, volatility: float) -> int:
        base_size = 10
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

    def place_order(self, market_ticker: str, side: str, price: float, size: int):
        client_order_id = f"{market_ticker}-{side}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
        order_type = "limit"
        try:
            if side == "bid":
                order = self.client.create_order(ticker=market_ticker, client_order_id=client_order_id, action="buy", side="yes", count=size, type=order_type, yes_price=price)
            elif side == "ask":
                order = self.client.create_order(ticker=market_ticker, client_order_id=client_order_id, action="sell", side="yes", count=size, type=order_type, no_price=price)
            else:
                logging.error("Invalid order side specified.")
                return None
            if order:
                order_id = order["order"]["order_id"]
                self.active_orders[order_id] = order
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
# MarketMaker Module
# ----------------------------------------------------------------------
class MarketMaker:
    """
    Executes a market-making cycle for a single market.
    """

    def __init__(self, client, market_ticker: str):
        self.market_ticker = market_ticker
        self.data_handler = MarketDataHandler(client)
        self.risk_manager = RiskManager(client)
        self.strategy_engine = StrategyEngine(self.risk_manager)
        self.order_manager = OrderManager(client)

    async def process_cycle(self):
        try:
            if not self.risk_manager.should_trade(self.market_ticker):
                self.order_manager.cancel_all_orders()
                return

            # For speed, we use market-level data (spread and mid-price) from the market object.
            # In a full implementation, these values might be directly retrieved from the get_markets REST response.
            # Here, we fall back to our MarketDataHandler if needed.
            # (For demo purposes, we'll call calculate_mid_price and calculate_spread on the market ticker.)
            mid_price = self.data_handler.calculate_mid_price(self.market_ticker)
            spread = self.data_handler.calculate_spread(self.market_ticker)
            volatility = self.data_handler.calculate_volatility(self.market_ticker)
            if mid_price is None or spread is None:
                logging.warning(f"[{self.market_ticker}] Insufficient data; skipping cycle.")
                return

            logging.info(f"[{self.market_ticker}] mid_price={mid_price:.2f}, spread={spread:.2f}, volatility={volatility:.4f}")
            bid_price, ask_price = self.strategy_engine.determine_order_prices(self.market_ticker, mid_price, volatility, spread)
            bid_size = self.strategy_engine.determine_order_size(self.market_ticker, volatility)
            ask_size = self.strategy_engine.determine_order_size(self.market_ticker, volatility)

            if bid_size > 0:
                self.order_manager.place_order(self.market_ticker, "bid", bid_price, bid_size)
            if ask_size > 0:
                self.order_manager.place_order(self.market_ticker, "ask", ask_price, ask_size)

            await asyncio.sleep(5)
            self.order_manager.cancel_all_orders()

        except Exception as e:
            logging.error(f"Error processing cycle for {self.market_ticker}: {e}")


# ----------------------------------------------------------------------
# MarketOpportunityBot Module (Updated Top 10 Logic)
# ----------------------------------------------------------------------
class MarketOpportunityBot:
    """
    Scans active markets, ranks them based on a custom score, and triggers market-making cycles.
    Maintains a top 10 list (plus markets with existing positions) for continuous monitoring.
    """

    MIN_SPREAD = 3.0
    MAX_VOLATILITY = 5.0
    MIN_VOLUME_24H = 10000

    def __init__(self, client):
        self.client = client
        self.market_makers = {}
        self.data_handler = MarketDataHandler(client)
        self.full_market_list = []
        self.top_markets = []  # Top 10 markets to monitor continuously.

    def retrieve_all_markets(self) -> list:
        """
        Retrieves all active markets via REST.
        Then computes a score for each market using market-level data from get_markets.
        The score is defined as:
            score = (volume_24h / (volatility_est + ε)) * exp(-((spread - target_spread)^2) / (2 * sigma^2))
        where:
            - volume_24h is the 24h trading volume.
            - volatility_est is estimated as spread/10 (an arbitrary proxy).
            - target_spread is the desired optimal spread (set to 5 cents here).
            - sigma is target_spread/2.
        Returns the union of the top 10 markets by score and any market where positions exist.
        """
        markets = []
        cursor = None
        try:
            while True:
                response = self.client.get_markets(cursor=cursor, status="open")
                markets.extend(response.get("markets", []))
                cursor = response.get("cursor")
                if not cursor:
                    break
            # Filter active markets.
            active_markets = [m for m in markets if m.get("status") == "active"]

            # Define parameters for scoring.
            target_spread = 5.0  # desired optimal spread (in cents)
            sigma = target_spread / 2.0  # standard deviation for Gaussian penalty
            epsilon = 1e-5  # small constant to avoid division by zero
            top_score = 0

            for market in active_markets:
                # Use market-level data provided by get_markets.
                yes_bid = market.get("yes_bid", 0)
                yes_ask = market.get("yes_ask", 0)
                spread = yes_ask - yes_bid
                volume = market.get("volume_24h", 0)
                # Estimate volatility as a simple proxy; if spread is zero, use a default value.
                volatility_est = spread / 10.0 if spread > 0 else 1.0
                # Compute score: higher volume and a spread near the target are preferred.
                market["score"] = (volume / (volatility_est + epsilon)) * math.exp(-((spread - target_spread) ** 2) / (2 * (sigma**2)))
                top_score = max(top_score, market["score"])
            print(f"Top score: {top_score}")
            # Sort markets by score (highest first).
            active_markets.sort(key=lambda m: m["score"], reverse=True)
            top_10 = active_markets[:10]

            # Also include markets where positions exist (via REST get_positions).
            markets_with_positions = []
            for market in active_markets:
                ticker = market.get("ticker")
                if ticker:
                    pos_response = self.data_handler.client.get_positions(ticker=ticker)
                    if any(p.get("position", 0) != 0 for p in pos_response.get("market_positions", [])):
                        markets_with_positions.append(market)

            # Create the union of the top 10 and markets with positions.
            union_markets = {m["ticker"]: m for m in (top_10 + markets_with_positions)}
            self.top_markets = list(union_markets.values())
            with open("top_markets.json", "w") as f:
                json.dump(self.top_markets, f, indent=2)
            return self.top_markets
        except Exception as e:
            logging.error(f"Error retrieving markets: {e}")
            return []

    async def run_trading_cycles(self):
        """
        Asynchronously runs trading cycles for each market in the top list.
        """
        while True:
            for market in self.top_markets:
                ticker = market.get("ticker")
                if not ticker:
                    continue
                if self.evaluate_market(market):
                    if ticker not in self.market_makers:
                        self.market_makers[ticker] = MarketMaker(self.client, ticker)
                    await self.market_makers[ticker].process_cycle()
            await asyncio.sleep(1)

    async def update_markets(self):
        """
        Updates the top markets list by scanning the full market list every 30 seconds.
        """
        while True:
            self.top_markets = self.retrieve_all_markets()
            logging.info(f"Updated top markets: {[m.get('ticker') for m in self.top_markets]}")
            await asyncio.sleep(30)

    def evaluate_market(self, market: dict) -> bool:
        """
        Evaluates if a market qualifies based on market-level data.
        """
        ticker = market.get("ticker")
        volume = market.get("volume_24h", 0)
        if volume < self.MIN_VOLUME_24H:
            return False

        spread = market.get("yes_ask", 0) - market.get("yes_bid", 0)
        if spread < self.MIN_SPREAD:
            return False

        logging.info(f"Market {ticker} qualifies: spread={spread:.2f}, volume_24h={volume}")
        # Dynamically subscribe to WS channels for this ticker.
        asyncio.run(ws_client.subscribe_to_ticker(ticker))
        asyncio.run(ws_client.subscribe_to_orderbook_delta(ticker))
        return True

    async def run(self):
        """
        Runs two asynchronous tasks concurrently:
         - Updating the top markets list.
         - Running trading cycles on the top markets.
        """
        await asyncio.gather(self.update_markets(), self.run_trading_cycles())


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

    with open(KALSHI_CERT_FILE_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None, backend=default_backend())

    # IMPORTANT: Use ExchangeClient for REST API.
    client = ExchangeClient(KALSHI_URL, KALSHI_API_KEY, private_key)

    # Create MarketOpportunityBot instance.
    bot = MarketOpportunityBot(client)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Stopping Market Opportunity Bot...")

    # Uncomment to run simulation/backtesting.
    # simulate_backtest()


if __name__ == "__main__":
    main()
