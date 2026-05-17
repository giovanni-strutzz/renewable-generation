import httpx
import logging
from typing import Dict, Any
from src.domain.ports.external_integration import WeatherForecastProvider

LOGGER = logging.getLogger("energy-api.open_meteo")

class OpenMeteoClient(WeatherForecastProvider):
    def __init__(self, base_url: str = "https://api.open-meteo.com/v1/forecast"):
        self.base_url = base_url
        self.timeout = httpx.Timeout(10.0, connect=5.0)

    async def fetch_forecast(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Queries Open-Meteo API to retrieve wind (100m) and solar radiation (GHI)
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "wind_speed_100m,shortwave_radiation",
            "timezone": "America/Sao_Paulo",
            "past_days": 0,
            "forecast_days": 7
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()

                data = response.json()
                LOGGER.info(f"Forecast extract successfully for LAT: {lat}, LON: {lon}")

                return data
            except httpx.HTTPStatusError as err:
                LOGGER.error(f"Error occured while querying Open-Meteo API: {err.response.status_code}")

                raise RuntimeError(f"External API error: {err}")
            except httpx.RequestError as exc:
                LOGGER.error(f"Error occured while querying Open-Meteo API: {exc}")

                raise RuntimeError(f"External API Unavailable: {exc}")
