from src.domain.ports.external_integration import WeatherForecastProvider


class ExtractWeatherDataService:
    def __init__(self, weather_provider: WeatherForecastProvider):
        self.weather_provider = weather_provider

    async def execute(self, park_id: str, lat: float, lon: float) -> dict:
        """
        Orchestrates the extraction of raw data from a park.
        This service will be called by our future Lambda Function (consuming Kafka/SQS).
        """

        raw_data = await self.weather_provider.fetch_forecast(lat, lon)
        enriched_data = {
            "park_id": park_id,
            "coordinates": {"latitude": lat, "longitude": lon},
            "source": "open-meteo",
            "raw_payload": raw_data,
        }

        return enriched_data