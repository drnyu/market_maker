import os
import json
import math
import statistics
import heapq
import logging
import argparse
from collections import deque, defaultdict
from datetime import datetime, timezone

# ------------------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")


# ------------------------------------------------------------------------------
# Timestamp Conversion Function
# ------------------------------------------------------------------------------
def iso_to_epoch(ts_str):
    """Converts an ISO 8601 timestamp string to a Unix epoch timestamp (float).

    Handles timestamps with or without 'Z' (UTC indicator) and ensures
    naive timestamps are treated as UTC.

    Args:
        ts_str (str): The ISO 8601 formatted timestamp string.

    Returns:
        float: The corresponding Unix epoch timestamp.
    """
    try:
        # First, try to parse the string
        dt = datetime.fromisoformat(ts_str)
    except ValueError:
        # If it contains Z, replace with +00:00
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    # If dt is naive, attach UTC timezone
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


# ------------------------------------------------------------------------------
# Utility: Load JSONL file with error handling
# ------------------------------------------------------------------------------
def load_jsonl(filepath):
    """Loads data from a JSON Lines (JSONL) file.

    Each line in the file is expected to be a valid JSON object.

    Args:
        filepath (str): The path to the JSONL file.

    Returns:
        list: A list of dictionaries, where each dictionary is loaded from a line
              in the file. Returns an empty list if the file cannot be read or is empty.
    """
    try:
        with open(filepath, "r") as f:
            data = [json.loads(line) for line in f]
        if not data:
            logging.warning(f"No data loaded from file: {filepath}")
        return data
    except Exception as e:
        logging.error(f"Error loading file {filepath}: {e}")
        return []


# ------------------------------------------------------------------------------
# Simulation core components (from previous versions)
# ------------------------------------------------------------------------------
class StrategyEngine:
    """
    Implements the Avellaneda-Stoikov market making strategy logic.

    Calculates reservation price, optimal spread, and order prices based on
    market conditions (mid-price, volatility) and the agent's inventory.
    Also determines appropriate order sizes based on volatility.

    Attributes:
        gamma (float): Inventory risk aversion parameter.
        kappa (float): Order book liquidity parameter.
        base_order_size (int): The base size for orders, adjusted by volatility.
    """
    
    def __init__(self, gamma=0.1, kappa=1.5, base_order_size=10):
        self.gamma = gamma
        self.kappa = kappa
        self.base_order_size = base_order_size

    def reservation_price(self, mid, q, sigma, T=1.0):
        """Calculates the reservation price (indifference price). Value dependent on 
        gamma. In scenario where reservation price calculation cannot be completed, the 
        mid price is returned.

            Args:
                mid (float): The current mid-price of the market.
                q (int): The current inventory (number of contracts held).
                sigma (float): The estimated market volatility.
                T (float): The time horizon (fraction of the trading day remaining,
                           which defaults to 1).

            Returns:
                float: The calculated reservation price. Returns the mid-price if
                       an error occurs during calculation.
        """
        return mid - q * self.gamma * (sigma**2) * T

    def optimal_spread(self, sigma, T=1.0):
        """Calculates the optimal spread around the reservation price. Dependent on both kappa
        and gamma.

            Args:
                sigma (float): The estimated market volatility.
                T (float): The time horizon.

            Returns:
                float: The calculated optimal total spread. Returns a simplified
                       spread calculation if an error occurs (e.g., log domain error).
        """
        return self.gamma * (sigma**2) * T + (2 / self.gamma) * math.log(1 + self.gamma / self.kappa)

    def order_prices(self, mid, q, sigma, T=1.0):
        """Calculates the bid and ask prices based on the strategy.

        Args:
            mid (float): The current mid-price.
            q (int): The current inventory.
            sigma (float): The estimated market volatility.
            T (float): The time horizon.

        Returns:
            tuple: A tuple containing:
                - bid (float): The calculated bid price.
                - ask (float): The calculated ask price.
                - spread (float): The calculated optimal spread.
        """
        r = self.reservation_price(mid, q, sigma, T)
        spread = self.optimal_spread(sigma, T)
        bid = r - spread / 2
        ask = r + spread / 2
        return bid, ask, spread

    def determine_order_size(self, sigma):
        """Determines the order size, inversely proportional to volatility. Fewer orders placed
        as volatility increases.

        Returns:
            int: The calculated order size (minimum of 1).
        """

        if sigma > 0:
            return max(1, int(self.base_order_size / sigma))
        return self.base_order_size


class SimulationEvent:
    """Represents a single event in the simulation timeline.

    Events are comparable based on their timestamp, with a tie-breaker
    rule to process order book updates before trades at the same timestamp.

    Attributes:
        timestamp (float): The epoch timestamp of the event.
        event_type (str): The type of event ("orderbook" or "trade").
        data (dict): The raw data associated with the event.
    """

    def __init__(self, timestamp, event_type, data):
        self.timestamp = timestamp  # Numeric timestamp (epoch seconds)
        self.event_type = event_type  # "orderbook" or "trade"
        self.data = data

    def __lt__(self, other):
        """Compares two events based on timestamp for ordering in the priority queue.

        Order book events are prioritized over trade events if timestamps are equal.
        """

        # Tie-breaker: if timestamps are identical, prioritize orderbook events.
        if self.timestamp == other.timestamp:
            priority = {"orderbook": 0, "trade": 1}
            return priority[self.event_type] < priority[other.event_type]
        return self.timestamp < other.timestamp


class EventDrivenBacktester:
    """
    Performs an event-driven backtest of the market making strategy.

    Processes a stream of order book and trade events chronologically,
    updates the strategy state, places simulated orders, and tracks
    performance (cash and inventory).

    Attributes:
        events (list): A min-heap (priority queue) of SimulationEvent objects.
        strategy_engine (StrategyEngine): The strategy logic implementation.
        current_orderbook (dict): The most recent order book data received.
        current_inventory (int): The current number of contracts held.
        cash (float): The current cash balance.
        active_orders (list): A list of currently active bid/ask orders.
        recent_trades (deque): A deque holding recent trade data for volatility estimation.
        volatility_window (int): The time window (in seconds) for volatility calculation.
    """

    def __init__(self, orderbook_events, trade_events, strategy_engine):
        self.events = []
        # Process orderbook events.
        for ev in orderbook_events:
            try:
                ts_raw = ev.get("timestamp")
                if ts_raw is None:
                    logging.warning("Orderbook event missing 'timestamp'; skipping.")
                    continue
                ts = iso_to_epoch(ts_raw)
                heapq.heappush(self.events, SimulationEvent(ts, "orderbook", ev))
            except Exception as e:
                logging.error(f"Error processing orderbook event: {e}")
        # Process trade events, unrolling batched trades.
        for tr in trade_events:
            try:
                trades = tr.get("data", {}).get("trades", [])
                # note that batched trades are individually added to priority queue
                if not trades:
                    logging.warning("Trade event contains no trades; skipping.")
                    continue
                for trade in trades:
                    created_time = trade.get("created_time")
                    if created_time is None:
                        logging.warning("Trade missing 'created_time'; skipping.")
                        continue
                    ts = iso_to_epoch(created_time)
                    heapq.heappush(self.events, SimulationEvent(ts, "trade", trade))
            except Exception as e:
                logging.error(f"Error processing trade event: {e}")
        
        # At this point priority queue is populated by both order book and trade events
        self.current_orderbook = None
        self.strategy_engine = strategy_engine
        self.current_inventory = 0
        self.cash = 0.0
        self.active_orders = []
        self.recent_trades = deque()
        self.volatility_window = 60

    def update_recent_trades(self, current_time, trade_price):
        """Updates the deque of recent trades for volatility calculation.

        Removes trades older than the volatility window.

        Args:
            current_time (float): The current simulation time (epoch seconds).
            trade_price (float): The price of the trade to add.
        """

        while self.recent_trades and (current_time - self.recent_trades[0][0] > self.volatility_window):
            self.recent_trades.popleft()
        self.recent_trades.append((current_time, trade_price))

    def estimate_volatility(self):
        """Estimates market volatility based on recent trade prices.

        Calculates the standard deviation of trade prices within the volatility window
        represented by the recent_trades stored for the back tester.

        Returns:
            float: The estimated volatility (standard deviation). Returns 1.0 if
                   there are fewer than 2 trades or if calculation fails.
        """

        prices = [price for ts, price in self.recent_trades]
        if len(prices) >= 2:
            try:
                return statistics.stdev(prices)
            except Exception as e:
                logging.error(f"Error calculating volatility: {e}")
                return 1.0
        return 1.0

    def cancel_active_orders(self, current_time):
        """Cancels all currently active orders.

        In a real system, this would involve sending cancellation requests to the exchange.
        In this simulation, it simply clears the list of active orders.

        Args:
            current_time (float): The current simulation time.
        """
        self.active_orders = []

    def place_orders(self, current_time):
        """Places new bid and ask orders based on the current strategy state.

        Args:
            current_time (float): The current simulation time.

        Uses the StrategyEngine to determine prices and sizes.
        """

        try:
            if not self.current_orderbook:
                return
            orderbook = self.current_orderbook.get("data", {}).get("orderbook", {})
            yes_levels = orderbook.get("yes", [])
            no_levels = orderbook.get("no", [])
            if not yes_levels or not no_levels:
                return
            best_yes_bid = max(level[0] for level in yes_levels)
            best_no_bid = max(level[0] for level in no_levels)
            best_yes_ask = 100 - best_no_bid
            mid_price = (best_yes_bid + best_yes_ask) / 2.0
            sigma = self.estimate_volatility()
            bid, ask, spread = self.strategy_engine.order_prices(mid_price, self.current_inventory, sigma)
            size = self.strategy_engine.determine_order_size(sigma)

            # These are the bid/ask orders that the strategy would require.
            # In backtesting, only when matching trades exist
            # can such orders be included in the simulation.

            self.active_orders = []

            # order size stored separately from remaining amount of order to fill

            self.active_orders.append({
                "side": "bid", 
                "price": bid, 
                "size": size, 
                "remaining": size, 
                "timestamp": current_time
            })
            self.active_orders.append({
                "side": "ask", 
                "price": ask, 
                "size": size, 
                "remaining": size, 
                "timestamp": current_time
            })
        except Exception as e:
            logging.error(f"Error placing orders at time {current_time}: {e}")

    def process_trade(self, current_time, trade_event):
        """Processes an incoming trade event against active orders.

        Checks if the trade price matches any active bid or ask orders. If a match
        occurs, it simulates a fill, updates inventory and cash accordingly.

        Args:
            current_time (float): The timestamp of the trade event.
            trade_event (dict): The trade data.
        """

        try:
            trade_price = trade_event.get("yes_price")
            trade_volume = trade_event.get("count", 1)
            if trade_price is None:
                return
            # update recent trades both adds the trade event and removes
            # trades that are outside of the volatility window
            self.update_recent_trades(current_time, trade_price)
            for order in self.active_orders:
                if order.get("remaining", order["size"]) <= 0:
                    continue
                if order["side"] == "bid" and trade_price <= order["price"]:
                    fill_qty = min(order["remaining"], trade_volume)
                    order["remaining"] -= fill_qty
                    self.current_inventory += fill_qty
                    self.cash -= fill_qty * trade_price
                elif order["side"] == "ask" and trade_price >= order["price"]:
                    fill_qty = min(order["remaining"], trade_volume)
                    order["remaining"] -= fill_qty
                    self.current_inventory -= fill_qty
                    self.cash += fill_qty * trade_price
        except Exception as e:
            logging.error(f"Error processing trade at time {current_time}: {e}")

    def simulate(self):
        while self.events:
            event = heapq.heappop(self.events)
            current_time = event.timestamp
            if event.event_type == "orderbook":
                self.cancel_active_orders(current_time)
                self.current_orderbook = event.data
                self.place_orders(current_time)
            elif event.event_type == "trade":
                self.process_trade(current_time, event.data)
        return {"cash": self.cash, "inventory": self.current_inventory}


# ------------------------------------------------------------------------------
# Functions to Run Simulation for a Market and Across Markets
# ------------------------------------------------------------------------------
def run_simulation_for_market(orderbook_path, trade_path, gamma_values, kappa_values):
    """Loads the specified files and runs the grid search simulation for one market.
    Returns a list of results for each (gamma,kappa) combination."""
    orderbook_events = load_jsonl(orderbook_path)
    trade_events = load_jsonl(trade_path)
    market_results = []
    for gamma in gamma_values:
        for kappa in kappa_values:
            strategy = StrategyEngine(gamma=gamma, kappa=kappa, base_order_size=10)
            backtester = EventDrivenBacktester(orderbook_events, trade_events, strategy)
            result = backtester.simulate()
            result.update({"gamma": gamma, "kappa": kappa})
            market_results.append(result)
    return market_results


def aggregate_overall_results(per_market_results):
    """Combine results from multiple markets into overall averages.
    per_market_results is a dict mapping market name to the list of its simulation results."""
    overall = defaultdict(lambda: {"total_cash": 0, "total_inventory": 0, "count": 0, "gamma": None, "kappa": None})
    for market, results in per_market_results.items():
        for res in results:
            key = f"{res['gamma']}_{res['kappa']}"
            overall[key]["total_cash"] += res["cash"]
            overall[key]["total_inventory"] += res["inventory"]
            overall[key]["count"] += 1
            overall[key]["gamma"] = res["gamma"]
            overall[key]["kappa"] = res["kappa"]
    # Compute averages
    for key, val in overall.items():
        val["avg_cash"] = val["total_cash"] / val["count"]
        val["avg_inventory"] = val["total_inventory"] / val["count"]
    return dict(overall)


# ------------------------------------------------------------------------------
# Main Execution: Iterate over each market file pair in the "data" folder.
# ------------------------------------------------------------------------------
def main():
    data_folder = "data"  # Adjust if needed.
    files = os.listdir(data_folder)
    markets = {}  # Map: market_name -> {"orderbook": path, "trades": path}
    for f in files:
        if f.endswith("_orderbook.jsonl"):
            market_name = f[: -len("_orderbook.jsonl")]
            markets.setdefault(market_name, {})["orderbook"] = os.path.join(data_folder, f)
        elif f.endswith("_trades.jsonl"):
            market_name = f[: -len("_trades.jsonl")]
            markets.setdefault(market_name, {})["trades"] = os.path.join(data_folder, f)
    if not markets:
        logging.error("No markets found in the data folder.")
        return

    # gamm_values from .1 to 1 by .05 increments
    gamma_values = [round(0.05 * i, 2) for i in range(1, 21)]  # 0.05 to 1.0
    # kappa_values from .5 to 5 by .1 increments
    kappa_values = [round(0.1 * i, 2) for i in range(5, 51)]  # 0.5 to 5.0

    per_market_results = {}
    for market, paths in markets.items():
        ob_path = paths.get("orderbook")
        tr_path = paths.get("trades")
        if not ob_path or not tr_path:
            logging.warning(f"Market {market} is missing one of the files; skipping.")
            continue
        logging.info(f"Running simulation for market: {market}")
        results = run_simulation_for_market(ob_path, tr_path, gamma_values, kappa_values)
        per_market_results[market] = results

    overall_results = aggregate_overall_results(per_market_results)

    # Save per-market results and overall results.
    try:
        with open("market_results.json", "w") as f:
            json.dump(per_market_results, f, indent=4)
        logging.info("Per-market simulation results saved to market_results.json")
    except Exception as e:
        logging.error(f"Error saving market results: {e}")
    try:
        with open("overall_results.json", "w") as f:
            json.dump(overall_results, f, indent=4)
        logging.info("Overall simulation results saved to overall_results.json")
    except Exception as e:
        logging.error(f"Error saving overall results: {e}")


if __name__ == "__main__":
    main()
