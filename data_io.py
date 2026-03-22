# data_io.py
import json
import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Tuple

class MarketDataGenerator:
    INITIAL_PRICES = {
        "BTC": 65000.0, "ETH": 3500.0, "SOL": 140.0, "ADA": 1.15, "DOT": 18.5,
    }

    @classmethod
    def generate(cls, file_path: str, num_lines: int) -> None:
        print(f"Generating {num_lines} synthetic trades to '{file_path}'...")
        current_prices = cls.INITIAL_PRICES.copy()
        symbols = list(current_prices.keys())
        current_time = datetime.now()

        with open(file_path, "w", encoding="utf-8") as file:
            for _ in range(num_lines):
                symbol = random.choice(symbols)
                volatility = random.uniform(-0.005, 0.005)
                current_prices[symbol] *= (1 + volatility)
                
                trade_record = {
                    "timestamp": current_time.isoformat(),
                    "symbol": symbol,
                    "price": round(current_prices[symbol], 2),
                    "volume": round(random.uniform(0.1, 10.0), 4),
                }
                current_time += timedelta(milliseconds=random.randint(1, 500))
                file.write(json.dumps(trade_record) + "\n")
        print("Data generation completed successfully.")

class MarketDataParser:
    @staticmethod
    def read_trades(file_path: str) -> Generator[Dict[str, Any], None, None]:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The data file '{file_path}' does not exist.")
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                yield json.loads(line.strip())

class DataPartitioner:
    @staticmethod
    def get_file_chunks(file_path: str, num_chunks: int) -> List[Tuple[int, int]]:
        file_size = os.path.getsize(file_path)
        chunk_size = file_size // num_chunks
        chunks = []
        for i in range(num_chunks):
            start_byte = i * chunk_size
            end_byte = start_byte + chunk_size if i < num_chunks - 1 else file_size
            chunks.append((start_byte, end_byte))
        return chunks

    @staticmethod
    def read_chunk_lines(file_path: str, start_byte: int, end_byte: int) -> Generator[Dict[str, Any], None, None]:
        with open(file_path, "r", encoding="utf-8") as file:
            file.seek(start_byte)
            if start_byte > 0:
                file.readline() # Skip to next complete line
            
            current_pos = file.tell()
            while current_pos < end_byte:
                line = file.readline()
                if not line: break
                current_pos = file.tell()
                yield json.loads(line.strip())