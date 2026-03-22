# models.py
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict

class AnalyzerType(Enum):
    """Enumeration of available analyzer types (Different Instructions)."""
    VOLUME = auto()
    VOLATILITY = auto()
    MOVING_AVERAGE = auto()

@dataclass
class WorkUnit:
    """Represents a unit of work to be processed by a worker."""
    worker_id: int
    file_path: str
    start_byte: int
    end_byte: int
    analyzer_type: AnalyzerType

@dataclass
class WorkResult:
    """Represents the result from a completed work unit."""
    worker_id: int
    analyzer_type: AnalyzerType
    data: Dict[str, Any]
    records_processed: int