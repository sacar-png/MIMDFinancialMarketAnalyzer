# analyzers.py
from collections import defaultdict
import math
from typing import Any, DefaultDict, Dict, Generator, List
from models import WorkResult

class VolumeAnalyzer:
    def __init__(self) -> None:
        self.total_volume_usd: DefaultDict[str, float] = defaultdict(float)
        self.trade_counts: DefaultDict[str, int] = defaultdict(int)

    def process_stream(self, trade_stream: Generator[Dict[str, Any], None, None]) -> int:
        count = 0
        for trade in trade_stream:
            symbol = trade["symbol"]
            usd_volume = trade["price"] * trade["volume"]
            self.total_volume_usd[symbol] += usd_volume
            self.trade_counts[symbol] += 1
            count += 1
        return count

    def get_result(self) -> Dict[str, Any]:
        return {
            "total_usd_volume": dict(self.total_volume_usd),
            "trade_counts": dict(self.trade_counts)
        }

class VolatilityAnalyzer:
    def __init__(self) -> None:
        self.prices: DefaultDict[str, List[float]] = defaultdict(list)
        self.min_prices: Dict[str, float] = {}
        self.max_prices: Dict[str, float] = {}

    def process_stream(self, trade_stream: Generator[Dict[str, Any], None, None]) -> int:
        count = 0
        for trade in trade_stream:
            symbol, price = trade["symbol"], trade["price"]
            self.prices[symbol].append(price)
            self.min_prices[symbol] = min(self.min_prices.get(symbol, float('inf')), price)
            self.max_prices[symbol] = max(self.max_prices.get(symbol, float('-inf')), price)
            count += 1
        return count

    def get_result(self) -> Dict[str, Any]:
        volatility_data = {}
        for symbol, prices in self.prices.items():
            if len(prices) > 1:
                mean = sum(prices) / len(prices)
                variance = sum((p - mean) ** 2 for p in prices) / len(prices)
                std_dev = math.sqrt(variance)
                volatility_pct = (std_dev / mean) * 100 if mean > 0 else 0
            else:
                std_dev, volatility_pct = 0.0, 0.0

            volatility_data[symbol] = {
                "std_deviation": round(std_dev, 2),
                "volatility_pct": round(volatility_pct, 4),
                "min_price": self.min_prices.get(symbol, 0),
                "max_price": self.max_prices.get(symbol, 0),
                "price_range": round(self.max_prices.get(symbol, 0) - self.min_prices.get(symbol, 0), 2)
            }
        return {"volatility_by_symbol": volatility_data}

class MovingAverageAnalyzer:
    def __init__(self, window_sizes: List[int] = None) -> None:
        self.window_sizes = window_sizes or [5, 10, 20]
        self.price_history: DefaultDict[str, List[float]] = defaultdict(list)
        self.latest_prices: Dict[str, float] = {}

    def process_stream(self, trade_stream: Generator[Dict[str, Any], None, None]) -> int:
        count = 0
        for trade in trade_stream:
            symbol, price = trade["symbol"], trade["price"]
            self.price_history[symbol].append(price)
            self.latest_prices[symbol] = price
            count += 1
        return count

    def get_result(self) -> Dict[str, Any]:
        ma_data = {}
        for symbol, prices in self.price_history.items():
            ma_data[symbol] = {
                "latest_price": self.latest_prices.get(symbol, 0),
                "total_samples": len(prices)
            }
            for window in self.window_sizes:
                if len(prices) >= window:
                    sma = sum(prices[-window:]) / window
                    ma_data[symbol][f"sma_{window}"] = round(sma, 2)
                    
                    alpha = 2 / (window + 1)
                    ema = prices[0]
                    for price in prices[1:]:
                        ema = alpha * price + (1 - alpha) * ema
                    ma_data[symbol][f"ema_{window}"] = round(ema, 2)
                else:
                    ma_data[symbol][f"sma_{window}"] = None
                    ma_data[symbol][f"ema_{window}"] = None
        return {"ma_by_symbol": ma_data}

class SequentialAnalyzer:
    def __init__(self) -> None:
        self.total_volume_usd: DefaultDict[str, float] = defaultdict(float)
        self.trade_counts: DefaultDict[str, int] = defaultdict(int)

    def process_stream(self, trade_stream: Generator[Dict[str, Any], None, None]) -> None:
        for trade in trade_stream:
            symbol, price, volume = trade["symbol"], trade["price"], trade["volume"]
            self.total_volume_usd[symbol] += price * volume
            self.trade_counts[symbol] += 1

    def get_report(self) -> Dict[str, Any]:
        return {
            "total_usd_volume": dict(sorted(self.total_volume_usd.items())),
            "trade_counts": dict(sorted(self.trade_counts.items()))
        }

def aggregate_volume_results(results: List[WorkResult]) -> Dict[str, Any]:
    total_volume: DefaultDict[str, float] = defaultdict(float)
    total_counts: DefaultDict[str, int] = defaultdict(int)
    total_records = sum(r.records_processed for r in results)

    for result in results:
        for symbol, volume in result.data.get("total_usd_volume", {}).items():
            total_volume[symbol] += volume
        for symbol, count in result.data.get("trade_counts", {}).items():
            total_counts[symbol] += count

    return {
        "total_usd_volume": dict(sorted(total_volume.items())),
        "trade_counts": dict(sorted(total_counts.items())),
        "total_records_processed": total_records
    }

def aggregate_volatility_results(results: List[WorkResult]) -> Dict[str, Any]:
    global_min: Dict[str, float] = {}
    global_max: Dict[str, float] = {}
    total_records = sum(r.records_processed for r in results)

    for result in results:
        for symbol, metrics in result.data.get("volatility_by_symbol", {}).items():
            global_min[symbol] = min(global_min.get(symbol, float('inf')), metrics["min_price"])
            global_max[symbol] = max(global_max.get(symbol, float('-inf')), metrics["max_price"])

    volatility_summary = {}
    for symbol in global_min.keys():
        volatility_summary[symbol] = {
            "min_price": global_min[symbol],
            "max_price": global_max[symbol],
            "price_range": round(global_max[symbol] - global_min[symbol], 2)
        }

    return {
        "volatility_by_symbol": volatility_summary,
        "total_records_processed": total_records
    }

def aggregate_moving_average_results(results: List[WorkResult]) -> Dict[str, Any]:
    ma_by_symbol: Dict[str, Dict[str, Any]] = {}
    for result in results:
        for symbol, metrics in result.data.get("ma_by_symbol", {}).items():
            if symbol not in ma_by_symbol or metrics.get("total_samples", 0) > ma_by_symbol[symbol].get("total_samples", 0):
                ma_by_symbol[symbol] = metrics

    return {
        "ma_by_symbol": ma_by_symbol,
        "total_records_processed": sum(r.records_processed for r in results)
    }