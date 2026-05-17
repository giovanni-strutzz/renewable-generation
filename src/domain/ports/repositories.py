from abc import ABC, abstractmethod
from src.domain.entities.forecast import EnergyForecast


class ForecastRepository(ABC):
    @abstractmethod
    async def save(self, forecast: EnergyForecast) -> EnergyForecast:
        pass

    @abstractmethod
    async def get_by_park(self, park_id: str):
        pass