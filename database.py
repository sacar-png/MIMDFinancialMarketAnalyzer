# database.py
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

class DatabaseManager:
    """Manages SQLite database for persisting all financial analysis outputs."""

    def __init__(self, db_path: str = "financial_market.db") -> None:
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.run_id: Optional[str] = None
        self._init_database()

    def _init_database(self) -> None:
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS analysis_runs (
                run_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                file_path TEXT NOT NULL,
                mode TEXT NOT NULL,
                num_workers INTEGER,
                total_records INTEGER,
                symbols TEXT
            );

            CREATE TABLE IF NOT EXISTS volume_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                total_usd_volume REAL NOT NULL,
                trade_count INTEGER NOT NULL,
                FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS volatility_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                min_price REAL NOT NULL,
                max_price REAL NOT NULL,
                price_range REAL NOT NULL,
                std_deviation REAL,
                volatility_pct REAL,
                FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS moving_average_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                latest_price REAL NOT NULL,
                total_samples INTEGER NOT NULL,
                sma_5 REAL,
                ema_5 REAL,
                sma_10 REAL,
                ema_10 REAL,
                sma_20 REAL,
                ema_20 REAL,
                FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                volume REAL NOT NULL,
                usd_volume REAL NOT NULL,
                FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_volume_run ON volume_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_volatility_run ON volatility_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_ma_run ON moving_average_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_trades_run ON trades(run_id);
            CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
        """)
        self.conn.commit()

    def start_run(self, file_path: str, mode: str, num_workers: Optional[int] = None) -> str:
        self.run_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now().isoformat()
        self.conn.execute(
            """INSERT INTO analysis_runs (run_id, timestamp, file_path, mode, num_workers)
               VALUES (?, ?, ?, ?, ?)""",
            (self.run_id, timestamp, file_path, mode, num_workers)
        )
        self.conn.commit()
        return self.run_id

    def end_run(self, total_records: int, symbols: List[str]) -> None:
        if not self.run_id: return
        symbols_str = ",".join(sorted(symbols))
        self.conn.execute(
            "UPDATE analysis_runs SET total_records = ?, symbols = ? WHERE run_id = ?",
            (total_records, symbols_str, self.run_id)
        )
        self.conn.commit()

    def save_volume_results(self, results: Dict[str, Any]) -> None:
        if not self.run_id: return
        for symbol, volume in results.get("total_usd_volume", {}).items():
            count = results.get("trade_counts", {}).get(symbol, 0)
            self.conn.execute(
                """INSERT INTO volume_results (run_id, symbol, total_usd_volume, trade_count)
                   VALUES (?, ?, ?, ?)""",
                (self.run_id, symbol, volume, count)
            )
        self.conn.commit()

    def save_volatility_results(self, results: Dict[str, Any]) -> None:
        if not self.run_id: return
        for symbol, metrics in results.get("volatility_by_symbol", {}).items():
            self.conn.execute(
                """INSERT INTO volatility_results 
                   (run_id, symbol, min_price, max_price, price_range, std_deviation, volatility_pct)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (self.run_id, symbol, metrics["min_price"], metrics["max_price"],
                 metrics["price_range"], metrics.get("std_deviation"),
                 metrics.get("volatility_pct"))
            )
        self.conn.commit()

    def save_moving_average_results(self, results: Dict[str, Any]) -> None:
        if not self.run_id: return
        for symbol, metrics in results.get("ma_by_symbol", {}).items():
            self.conn.execute(
                """INSERT INTO moving_average_results 
                   (run_id, symbol, latest_price, total_samples, sma_5, ema_5, sma_10, ema_10, sma_20, ema_20)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (self.run_id, symbol, metrics.get("latest_price", 0),
                 metrics.get("total_samples", 0),
                 metrics.get("sma_5"), metrics.get("ema_5"),
                 metrics.get("sma_10"), metrics.get("ema_10"),
                 metrics.get("sma_20"), metrics.get("ema_20"))
            )
        self.conn.commit()

    def save_trades(self, trades: List[Dict[str, Any]]) -> None:
        if not self.run_id: return
        batch = [
            (self.run_id, t["timestamp"], t["symbol"], t["price"],
             t["volume"], t["price"] * t["volume"])
            for t in trades
        ]
        self.conn.executemany(
            """INSERT INTO trades (run_id, timestamp, symbol, price, volume, usd_volume)
               VALUES (?, ?, ?, ?, ?, ?)""", batch
        )
        self.conn.commit()

    def query_volume_by_symbol(self, symbol: str) -> List[Tuple]:
        return self.conn.execute(
            "SELECT run_id, total_usd_volume, trade_count FROM volume_results WHERE symbol = ?",
            (symbol,)
        ).fetchall()

    def list_analysis_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """SELECT run_id, timestamp, file_path, mode, num_workers, 
                      total_records, symbols FROM analysis_runs 
               ORDER BY timestamp DESC LIMIT ?""", (limit,)
        ).fetchall()
        
        history = []
        for row in rows:
            history.append({
                "run_id": row[0], "timestamp": row[1], "file_path": row[2],
                "mode": row[3], "num_workers": row[4], "total_records": row[5],
                "symbols": row[6].split(",") if row[6] else []
            })
        return history

    def close(self) -> None:
        if self.conn:
            self.conn.close()

# Global database instances
_db_manager: Optional[DatabaseManager] = None

def get_db() -> Optional[DatabaseManager]:
    return _db_manager

def init_db(db_path: str = "financial_market.db") -> DatabaseManager:
    global _db_manager
    _db_manager = DatabaseManager(db_path)
    return _db_manager