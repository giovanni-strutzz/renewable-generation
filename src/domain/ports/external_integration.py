from abc import ABC, abstractmethod
from typing import Dict, Any


class WeatherForecastProvider(ABC):
    """
    Port for the weather forecast provider API
    """

    @abstractmethod
    async def fetch_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Returns weather forecast for given latitude and longitude
        """
        pass