import json
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")


def load_json(filename):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading {filename}: {e}")
        return None


def find_best_overall(overall_results):
    """
    overall_results is a dictionary where each key is "gamma_kappa"
    and its value is a dictionary containing, among other things,
    'avg_cash' (the average cash over markets) plus additional info.

    We return the key and value for the best overall result based on avg_cash.
    """
    best_key = None
    best_value = None
    for key, value in overall_results.items():
        # Assuming we want the maximum average cash
        if best_value is None or value.get("avg_cash", float("-inf")) > best_value.get("avg_cash", float("-inf")):
            best_key = key
            best_value = value
    return best_key, best_value


def find_best_by_market(market_results):
    """
    market_results is a dict mapping market names to a list of simulation result dictionaries.
    Each result dictionary has keys: "cash", "inventory", "gamma", and "kappa".

    For each market, choose the result with the highest cash value.
    If there's a tie (same cash), break ties by choosing the one with the smallest absolute inventory.
    Returns a dictionary mapping market names to their best result.
    """
    best_results = {}
    for market, results in market_results.items():
        best = None
        for res in results:
            cash = res.get("cash", float("-inf"))
            inv = res.get("inventory", 0)
            if best is None:
                best = res
            else:
                best_cash = best.get("cash", float("-inf"))
                best_inv = best.get("inventory", 0)
                # Primary criterion: higher cash wins
                if cash > best_cash:
                    best = res
                # If cash is tied, choose the one with smallest absolute inventory
                elif cash == best_cash and abs(inv) < abs(best_inv):
                    best = res
        best_results[market] = best
    return best_results


def save_json(data, filename):
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
        logging.info(f"Saved results to {filename}")
    except Exception as e:
        logging.error(f"Error saving to {filename}: {e}")


def main():
    print("Finding best results...")
    overall_results = load_json("overall_results.json")
    if overall_results is None:
        logging.error("Could not load overall_results.json")
        return

    market_results = load_json("market_results.json")
    if market_results is None:
        logging.error("Could not load market_results.json")
        return

    # Find best overall combination
    best_key, best_overall = find_best_overall(overall_results)
    logging.info(f"Best overall combination: {best_key} with avg_cash={best_overall.get('avg_cash')}")
    save_json({best_key: best_overall}, "best_overall.json")

    # Find best combination per market
    best_by_market = find_best_by_market(market_results)
    for market, res in best_by_market.items():
        logging.info(f"Market: {market}, Best gamma: {res.get('gamma')}, Best kappa: {res.get('kappa')}, Cash: {res.get('cash')}, Inventory: {res.get('inventory')}")
    save_json(best_by_market, "best_by_market.json")


if __name__ == "__main__":
    main()
