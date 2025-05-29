import unittest
import logging
import math
import statistics
import heapq
from collections import deque
from datetime import datetime, timezone

# ------------------------------------------------------------------------------
# Utility: Helper function for ISO to epoch conversion.
# ------------------------------------------------------------------------------
def iso_to_epoch(ts_str):
    try:
        # Try to parse normally.
        dt = datetime.fromisoformat(ts_str)
    except ValueError as e:
        # If there is a ValueError, try replacing 'Z' with '+00:00'
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    # If dt is naive (no tzinfo), assume UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()

# ------------------------------------------------------------------------------
# Simulation Code (Definitions)
# ------------------------------------------------------------------------------
class StrategyEngine:
    def __init__(self, gamma=0.1, kappa=1.5, base_order_size=10):
        self.gamma = gamma
        self.kappa = kappa
        self.base_order_size = base_order_size

    def reservation_price(self, mid, q, sigma, T=1.0):
        try:
            return mid - q * self.gamma * (sigma ** 2) * T
        except Exception as e:
            logging.error(f"Error calculating reservation price: {e}")
            return mid

    def optimal_spread(self, sigma, T=1.0):
        try:
            return self.gamma * (sigma ** 2) * T + (2 / self.gamma) * math.log(1 + self.gamma / self.kappa)
        except Exception as e:
            logging.error(f"Error calculating optimal spread: {e}")
            return self.gamma * (sigma ** 2) * T

    def order_prices(self, mid, q, sigma, T=1.0):
        r = self.reservation_price(mid, q, sigma, T)
        spread = self.optimal_spread(sigma, T)
        bid = r - spread / 2
        ask = r + spread / 2
        return bid, ask, spread

    def determine_order_size(self, sigma):
        if sigma > 0:
            return max(1, int(self.base_order_size / sigma))
        return self.base_order_size

class SimulationEvent:
    def __init__(self, timestamp, event_type, data):
        self.timestamp = timestamp  # Numeric timestamp (epoch seconds)
        self.event_type = event_type  # "orderbook" or "trade"
        self.data = data

    def __lt__(self, other):
        if self.timestamp == other.timestamp:
            order_priority = {"orderbook": 0, "trade": 1}
            return order_priority[self.event_type] < order_priority[other.event_type]
        return self.timestamp < other.timestamp

class EventDrivenBacktester:
    def __init__(self, orderbook_events, trade_events, strategy_engine):
        self.events = []
        # Process orderbook events.
        for ev in orderbook_events:
            try:
                ts_raw = ev.get("timestamp")
                if ts_raw is None:
                    logging.warning("Orderbook event missing 'timestamp'; skipping.")
                    continue
                try:
                    ts = float(ts_raw)
                except ValueError:
                    ts = iso_to_epoch(ts_raw)
                heapq.heappush(self.events, SimulationEvent(ts, "orderbook", ev))
            except Exception as e:
                logging.error(f"Error processing orderbook event: {e}")
        # Process trade events (unrolling batched trades)
        for tr in trade_events:
            try:
                trades = tr.get("data", {}).get("trades", [])
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
        self.current_orderbook = None
        self.strategy_engine = strategy_engine
        self.current_inventory = 0
        self.cash = 0.0
        self.active_orders = []
        self.recent_trades = deque()
        self.volatility_window = 60

    def update_recent_trades(self, current_time, trade_price):
        while self.recent_trades and (current_time - self.recent_trades[0][0] > self.volatility_window):
            self.recent_trades.popleft()
        self.recent_trades.append((current_time, trade_price))

    def estimate_volatility(self):
        prices = [price for ts, price in self.recent_trades]
        if len(prices) >= 2:
            try:
                return statistics.stdev(prices)
            except Exception as e:
                logging.error(f"Error calculating volatility: {e}")
                return 1.0
        return 1.0

    def cancel_active_orders(self, current_time):
        self.active_orders = []

    def place_orders(self, current_time):
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
            self.active_orders = []
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
        try:
            trade_price = trade_event.get("yes_price")
            trade_volume = trade_event.get("count", 1)
            if trade_price is None:
                return
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
# Unit Tests
# ------------------------------------------------------------------------------
class TestSimulationComponents(unittest.TestCase):

    def test_orderbook_parsing_and_order_placement(self):
        fake_orderbook = {
            "timestamp": "2025-04-12T04:09:47.033780",
            "data": {
                "orderbook": {
                    "yes": [[40, 100], [38, 50]],
                    "no":  [[60, 100], [62, 50]]
                }
            }
        }
        # Expected: best yes bid = 40, best no bid = 62, best ask = 100 - 62 = 38, mid = (40+38)/2 = 39.
        strategy = StrategyEngine(gamma=0.1, kappa=1.5, base_order_size=10)
        backtester = EventDrivenBacktester(orderbook_events=[fake_orderbook], trade_events=[], strategy_engine=strategy)
        ts = iso_to_epoch(fake_orderbook["timestamp"])
        event = SimulationEvent(ts, "orderbook", fake_orderbook)
        backtester.current_orderbook = event.data
        backtester.place_orders(ts)
        self.assertEqual(len(backtester.active_orders), 2)
        bid_order = [o for o in backtester.active_orders if o["side"]=="bid"][0]
        ask_order = [o for o in backtester.active_orders if o["side"]=="ask"][0]
        expected_bid = 39 - strategy.optimal_spread(1.0)/2
        expected_ask = 39 + strategy.optimal_spread(1.0)/2
        self.assertAlmostEqual(bid_order["price"], expected_bid, places=2)
        self.assertAlmostEqual(ask_order["price"], expected_ask, places=2)
        self.assertEqual(bid_order["size"], 10)

    def test_trade_parsing_and_timestamp_conversion(self):
        fake_trade_event = {
            "timestamp": "dummy",
            "data": {
                "trades": [
                    {"trade_id": "t1", "ticker": "TEST", "count": 5, "created_time": "1970-01-01T00:00:10Z", "yes_price": 39, "no_price": 61, "taker_side": "yes"},
                    {"trade_id": "t2", "ticker": "TEST", "count": 3, "created_time": "1970-01-01T00:00:12Z", "yes_price": 38, "no_price": 62, "taker_side": "yes"}
                ],
                "cursor": "dummy_cursor"
            }
        }
        strategy = StrategyEngine(gamma=0.1, kappa=1.5, base_order_size=10)
        backtester = EventDrivenBacktester(orderbook_events=[], trade_events=[fake_trade_event], strategy_engine=strategy)
        trade_events_in_heap = [e for e in backtester.events if e.event_type == "trade"]
        self.assertEqual(len(trade_events_in_heap), 2)
        timestamps = sorted([e.timestamp for e in trade_events_in_heap])
        self.assertAlmostEqual(timestamps[0], 10, places=0)
        self.assertAlmostEqual(timestamps[1], 12, places=0)

    def test_volatility_estimation(self):
        strategy = StrategyEngine(gamma=0.1, kappa=1.5, base_order_size=10)
        backtester = EventDrivenBacktester(orderbook_events=[], trade_events=[], strategy_engine=strategy)
        backtester.recent_trades = deque([
            (0, 40),
            (10, 42),
            (20, 41),
            (30, 43)
        ])
        expected_vol = statistics.stdev([40, 42, 41, 43])
        estimated_vol = backtester.estimate_volatility()
        self.assertAlmostEqual(estimated_vol, expected_vol, places=2)

    def test_trade_fill_logic_bid(self):
        fake_orderbook = {
            "timestamp": "2025-04-12T04:09:47.033780",
            "data": {
                "orderbook": {
                    "yes": [[40, 100]],
                    "no":  [[60, 100]]
                }
            }
        }
        strategy = StrategyEngine(gamma=0.1, kappa=1.5, base_order_size=10)
        backtester = EventDrivenBacktester(orderbook_events=[fake_orderbook], trade_events=[], strategy_engine=strategy)
        ts = iso_to_epoch(fake_orderbook["timestamp"])
        backtester.current_orderbook = fake_orderbook
        backtester.place_orders(ts)
        bid_order = [o for o in backtester.active_orders if o["side"]=="bid"][0]
        expected_bid = 40 - strategy.optimal_spread(1.0)/2  # For mid=40 when volatility=1
        trade_event = {"yes_price": expected_bid - 0.05, "count": 5}
        backtester.process_trade(ts + 1, trade_event)
        self.assertEqual(bid_order["remaining"], 10 - 5)
        self.assertEqual(backtester.current_inventory, 5)
        self.assertAlmostEqual(backtester.cash, -5 * (expected_bid - 0.05), places=2)

    def test_grid_search_results(self):
        # Set up a fake orderbook that yields mid=50.
        fake_orderbook = {
            "timestamp": "2025-04-12T04:09:47.033780",
            "data": {
                "orderbook": {
                    "yes": [[40, 100]],
                    "no":  [[40, 100]]
                }
            }
        }
        # This orderbook gives: best yes bid = 40, best no bid = 40, so best ask = 60, mid = (40+60)/2 = 50.
        # With gamma=0.1, the bid price should be approximately: 50 - (optimal_spread/2),
        # where optimal_spread ≈ 0.1 + 20*ln(1+0.1/1.5) ≈ 1.3908, so bid ≈ 50 - 0.6954 = 49.3046.
        # With gamma=0.5, the bid price should be ≈ 50 - (optimal_spread/2),
        # where optimal_spread ≈ 0.5 + 4*ln(1+0.5/1.5) ≈ 1.6507, so bid ≈ 50 - 0.82535 = 49.17465.
        # We create a trade event with created_time "2025-04-12T04:09:50Z" and yes_price = 49.30.
        # For gamma=0.1, 49.30 <= 49.3046 (fill should occur) and inventory should increase.
        # For gamma=0.5, 49.30 > 49.17465 (no fill), so inventory should remain 0.
        fake_trade_event = {
            "timestamp": "dummy",
            "data": {
                "trades": [
                    {
                        "trade_id": "test_trade",
                        "ticker": "TEST",
                        "count": 10,
                        "created_time": "2025-04-12T04:09:50Z",
                        "yes_price": 49.30,
                        "no_price": 50.70,
                        "taker_side": "yes"
                    }
                ],
                "cursor": "dummy_cursor"
            }
        }
        orderbook_events = [fake_orderbook]
        trade_events = [fake_trade_event]
        results = []
        for gamma in [0.1, 0.5]:
            backtester = EventDrivenBacktester(orderbook_events, trade_events, StrategyEngine(gamma=gamma, kappa=1.5, base_order_size=10))
            res = backtester.simulate()
            res.update({"gamma": gamma, "kappa": 1.5})
            results.append(res)
        result_gamma_0_1 = [r for r in results if r["gamma"] == 0.1][0]
        result_gamma_0_5 = [r for r in results if r["gamma"] == 0.5][0]
        # For gamma=0.1, we expect a fill so inventory should be > 0.
        self.assertGreater(result_gamma_0_1["inventory"], 0, "Gamma 0.1 should have a fill (nonzero inventory)")
        # For gamma=0.5, no fill should occur so inventory should equal 0.
        self.assertEqual(result_gamma_0_5["inventory"], 0, "Gamma 0.5 should have no fill (zero inventory)")

# ------------------------------------------------------------------------------
# Run Unit Tests
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main()
