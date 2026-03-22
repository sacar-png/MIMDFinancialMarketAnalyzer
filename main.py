# main.py
import argparse
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Type
from multiprocessing import Process, Queue, cpu_count

# Dışa aktarılan modüllerimiz
from models import AnalyzerType, WorkUnit, WorkResult
from database import init_db, DatabaseManager
from data_io import MarketDataGenerator, MarketDataParser, DataPartitioner
from analyzers import (
    VolumeAnalyzer, VolatilityAnalyzer, MovingAverageAnalyzer, 
    SequentialAnalyzer, aggregate_volume_results, 
    aggregate_volatility_results, aggregate_moving_average_results
)


def mimd_worker_process(work_queue: Queue, result_queue: Queue) -> None:
    analyzer_map: Dict[AnalyzerType, Type] = {
        AnalyzerType.VOLUME: VolumeAnalyzer,
        AnalyzerType.VOLATILITY: VolatilityAnalyzer,
        AnalyzerType.MOVING_AVERAGE: MovingAverageAnalyzer,
    }

    while True:
        work_unit: Optional[WorkUnit] = work_queue.get()
        if work_unit is None: break  # Poison pill

        analyzer = analyzer_map[work_unit.analyzer_type]()
        trade_stream = DataPartitioner.read_chunk_lines(
            work_unit.file_path, work_unit.start_byte, work_unit.end_byte
        )
        
        records_processed = analyzer.process_stream(trade_stream)
        
        result_queue.put(WorkResult(
            worker_id=work_unit.worker_id,
            analyzer_type=work_unit.analyzer_type,
            data=analyzer.get_result(),
            records_processed=records_processed
        ))

class MIMDCoordinator:
    def __init__(self, num_workers: Optional[int] = None) -> None:
        self.num_workers = num_workers or cpu_count()
        self.work_queue: Queue = Queue()
        self.result_queue: Queue = Queue()
        self.workers: List[Process] = []

    def start_workers(self) -> None:
        for _ in range(self.num_workers):
            worker = Process(target=mimd_worker_process, args=(self.work_queue, self.result_queue))
            worker.start()
            self.workers.append(worker)

    def stop_workers(self) -> None:
        for _ in self.workers:
            self.work_queue.put(None)
        for worker in self.workers:
            worker.join()
        self.workers.clear()

    def distribute_work(self, file_path: str, analyzer_types: List[AnalyzerType]) -> List[WorkUnit]:
        chunks = DataPartitioner.get_file_chunks(file_path, self.num_workers)
        work_units = []
        for analyzer_type in analyzer_types:
            for i, (start_byte, end_byte) in enumerate(chunks):
                work_unit = WorkUnit(i, file_path, start_byte, end_byte, analyzer_type)
                work_units.append(work_unit)
                self.work_queue.put(work_unit)
        return work_units

    def collect_results(self, expected_count: int) -> List[WorkResult]:
        return [self.result_queue.get() for _ in range(expected_count)]

    def run_parallel_analysis(self, file_path: str, analyzer_types: List[AnalyzerType]) -> Dict[AnalyzerType, List[WorkResult]]:
        self.start_workers()
        try:
            work_units = self.distribute_work(file_path, analyzer_types)
            results = self.collect_results(len(work_units))
        finally:
            self.stop_workers()

        grouped_results = defaultdict(list)
        for result in results:
            grouped_results[result.analyzer_type].append(result)
        return dict(grouped_results)

def print_sequential_report(report: Dict[str, Any]) -> None:
    print("\n--- SEQUENTIAL MARKET ANALYSIS REPORT ---")
    for symbol, volume in report["total_usd_volume"].items():
        print(f"  - {symbol}: ${volume:,.2f}")
    for symbol, count in report["trade_counts"].items():
        print(f"  - {symbol}: {count:,} trades")
    print("-----------------------------------------\n")

def print_mimd_report(volume_report, volatility_report, ma_report, actual_workers) -> None:
    print("\n" + "=" * 60)
    print("    MIMD PARALLEL MARKET ANALYSIS REPORT")
    print("=" * 60)

    print("\n[INSTRUCTION TYPE: VOLUME ANALYSIS]")
    print("-" * 40)
    if volume_report:
        for symbol, volume in volume_report.get("total_usd_volume", {}).items():
            print(f"  - {symbol}: ${volume:,.2f}")
        for symbol, count in volume_report.get("trade_counts", {}).items():
            print(f"  - {symbol}: {count:,} trades")
        print(f"\nRecords Processed: {volume_report.get('total_records_processed', 0):,}")

    if volatility_report:
        print("\n[INSTRUCTION TYPE: VOLATILITY ANALYSIS]")
        print("-" * 40)
        for symbol, metrics in volatility_report.get("volatility_by_symbol", {}).items():
            print(f"  {symbol}: Min ${metrics['min_price']:,.2f} | Max ${metrics['max_price']:,.2f}")

    if ma_report:
        print("\n[INSTRUCTION TYPE: MOVING AVERAGE ANALYSIS]")
        print("-" * 40)
        for symbol, metrics in ma_report.get("ma_by_symbol", {}).items():
            print(f"  {symbol}: Latest Price ${metrics.get('latest_price', 0):,.2f}")

    print("\n" + "=" * 60)
    print(f"Workers Used: {actual_workers or cpu_count()}")
    print("=" * 60 + "\n")

def main() -> None:
    parser = argparse.ArgumentParser(description="MIMD Financial Market Analyzer")
    parser.add_argument("--generate", type=int, help="Generate mock .jsonl data file")
    parser.add_argument("--file", type=str, default="market_data.jsonl", help="Data file path")
    parser.add_argument("--analyze", action="store_true", help="Run sequential analysis")
    parser.add_argument("--mimd", action="store_true", help="Run MIMD parallel analysis")
    parser.add_argument("--workers", type=int, help="Number of workers")
    parser.add_argument("--instructions", type=str, default="volume,volatility,ma", help="Analyzers to run")
    parser.add_argument("--db", type=str, default="financial_market.db", help="SQLite DB path")
    parser.add_argument("--no-db", action="store_true", help="Disable database persistence")
    
    args = parser.parse_args()

    db: Optional[DatabaseManager] = None
    if not args.no_db:
        db = init_db(args.db)

    if args.generate:
        MarketDataGenerator.generate(args.file, args.generate)

    if args.analyze:
        print("Running Sequential...")
        analyzer = SequentialAnalyzer()
        analyzer.process_stream(MarketDataParser.read_trades(args.file))
        print_sequential_report(analyzer.get_report())

    if args.mimd:
        print("Running MIMD Parallel...")
        instruction_map = {
            "volume": AnalyzerType.VOLUME, "volatility": AnalyzerType.VOLATILITY, "ma": AnalyzerType.MOVING_AVERAGE
        }
        requested = [i.strip().lower() for i in args.instructions.split(",")]
        analyzer_types = [instruction_map[i] for i in requested if i in instruction_map]

        if db: db.start_run(args.file, "mimd", args.workers or cpu_count())
        
        coordinator = MIMDCoordinator(num_workers=args.workers)
        grouped_results = coordinator.run_parallel_analysis(args.file, analyzer_types)

        vol = aggregate_volume_results(grouped_results.get(AnalyzerType.VOLUME, []))
        vola = aggregate_volatility_results(grouped_results.get(AnalyzerType.VOLATILITY, []))
        ma = aggregate_moving_average_results(grouped_results.get(AnalyzerType.MOVING_AVERAGE, []))

        if db:
            db.save_volume_results(vol)
            db.save_volatility_results(vola)
            db.save_moving_average_results(ma)
            symbols = list(vol.get("total_usd_volume", {}).keys())
            db.end_run(vol.get("total_records_processed", 0), symbols)

        print_mimd_report(vol, vola, ma, args.workers)

    if db: db.close()

if __name__ == "__main__":
    main()