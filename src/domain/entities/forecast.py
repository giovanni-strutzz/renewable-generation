from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from src.domain.enums.sources import Sources


@dataclass
class EnergyForecast:
    """
    Represents a generation forecast
    """
    id: Optional[str]
    park_id: str
    source: Sources
    estimated_generation_mwh: float
    forecast_date: datetime