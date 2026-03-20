import argparse
import json
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, Optional


class MarketDataGenerator:
    """Generates synthetic financial market data simulating a continuous order book feed."""

    INITIAL_PRICES = {
        "BTC": 65000.0,
        "ETH": 3500.0,
        "SOL": 140.0,
        "ADA": 1.15,
        "DOT": 18.5,
    }

    @classmethod
    def generate(cls, file_path: str, num_lines: int) -> None:
        """
        Generates a .jsonl file with randomized trades.
        Prices fluctuate using a simple random walk model to simulate market volatility.
        """
        print(f"Generating {num_lines} synthetic trades to '{file_path}'...")
        
        current_prices = cls.INITIAL_PRICES.copy()
        symbols = list(current_prices.keys())
        current_time = datetime.now()

        with open(file_path, "w", encoding="utf-8") as file:
            for _ in range(num_lines):
                # Pick a random symbol for the next trade
                symbol = random.choice(symbols)
                
                # Simulate a random price fluctuation (-0.5% to +0.5%)
                volatility = random.uniform(-0.005, 0.005)
                current_prices[symbol] *= (1 + volatility)
                
                # Format price to 2 decimal places for realism
                price = round(current_prices[symbol], 2)
                
                # Generate a random traded volume (e.g., 0.1 to 10.0 units)
                volume = round(random.uniform(0.1, 10.0), 4)
                
                # Advance time slightly for the next trade (1 to 500 milliseconds)
                current_time += timedelta(milliseconds=random.randint(1, 500))

                trade_record = {
                    "timestamp": current_time.isoformat(),
                    "symbol": symbol,
                    "price": price,
                    "volume": volume,
                }
                
                # Write as JSON Lines
                file.write(json.dumps(trade_record) + "\n")
                
        print("Data generation completed successfully.")


class MarketDataParser:
    """Handles the ingestion of market data files."""

    @staticmethod
    def read_trades(file_path: str) -> Generator[Dict[str, Any], None, None]:
        """
        Yields parsed JSON objects one by one.
        Using a generator ensures the application is not bounded by RAM limits.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The data file '{file_path}' does not exist.")

        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                # Strip whitespace and parse JSON
                yield json.loads(line.strip())


class SequentialAnalyzer:
    """Processes a stream of trades to calculate aggregate market metrics."""

    def __init__(self) -> None:
        # Defaultdict avoids KeyError when encountering a symbol for the first time
        self.total_volume_usd: DefaultDict[str, float] = defaultdict(float)
        self.trade_counts: DefaultDict[str, int] = defaultdict(int)

    def process_stream(self, trade_stream: Generator[Dict[str, Any], None, None]) -> None:
        """Consumes a generator of trades and updates internal state."""
        for trade in trade_stream:
            symbol = trade["symbol"]
            price = trade["price"]
            volume = trade["volume"]
            
            # Calculate total USD volume for this specific trade
            usd_volume = price * volume
            
            self.total_volume_usd[symbol] += usd_volume
            self.trade_counts[symbol] += 1

    def get_report(self) -> Dict[str, Any]:
        """Returns the final aggregated metrics formatted for display."""
        # Sort results alphabetically by symbol for consistent output
        sorted_volumes = dict(sorted(self.total_volume_usd.items()))
        sorted_counts = dict(sorted(self.trade_counts.items()))
        
        return {
            "total_usd_volume": sorted_volumes,
            "trade_counts": sorted_counts
        }


def print_report(report: Dict[str, Any]) -> None:
    """Formats and prints the analysis results to the console."""
    print("\n--- MARKET ANALYSIS REPORT ---")
    print("Total Trading Volume (USD):")
    for symbol, volume in report["total_usd_volume"].items():
        # Format as currency with commas
        print(f"  - {symbol}: ${volume:,.2f}")
        
    print("\nTrade Execution Counts:")
    for symbol, count in report["trade_counts"].items():
        print(f"  - {symbol}: {count:,} trades")
    print("------------------------------\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mini Algorithmic Trading Engine - Step 1 (Sequential Baseline)"
    )
    parser.add_argument(
        "--generate", 
        type=int, 
        metavar="NUM_LINES",
        help="Generate a mock .jsonl data file with N trades"
    )
    parser.add_argument(
        "--file", 
        type=str, 
        default="market_data.jsonl", 
        help="Path to the market data file"
    )
    parser.add_argument(
        "--analyze", 
        action="store_true", 
        help="Run the sequential volume analysis on the data file"
    )

    args = parser.parse_args()

    # Execution routing based on CLI arguments
    if args.generate is not None:
        if args.generate <= 0:
            print("Error: The number of lines to generate must be greater than 0.")
            return
        MarketDataGenerator.generate(args.file, args.generate)

    if args.analyze:
        print(f"Initializing sequential analysis on '{args.file}'...")
        try:
            analyzer = SequentialAnalyzer()
            trade_generator = MarketDataParser.read_trades(args.file)
            
            # Pass the generator directly into the analyzer
            analyzer.process_stream(trade_generator)
            
            report = analyzer.get_report()
            print_report(report)
            
        except FileNotFoundError as e:
            print(f"Error: {e}")
        except json.JSONDecodeError as e:
            print(f"Error: Malformed JSON detected in the data file. Details: {e}")

if __name__ == "__main__":
    main()