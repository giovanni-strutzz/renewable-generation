from motor.motor_asyncio import AsyncIOMotorClient
from src.domain.ports.repositories import ForecastRepository
from src.domain.entities.forecast import EnergyForecast


class MongoRepository(ForecastRepository):
    def __init__(self, connection_string: str):
        self.client = AsyncIOMotorClient(connection_string)
        self.db = self.client.energy_db
        self.collection = self.db.forecasts

    async def save(self, forecast: EnergyForecast) -> EnergyForecast:
        doc = forecast.__dict__.copy()

        if not doc['id']:
            del doc['_id']

        result = await self.collection.insert_one(doc)
        forecast.id = str(result.inserted_id)

        return forecast

    async def get_by_park(self, park_id: str):
        cursor = self.collection.find({'park_id': park_id})

        return [EnergyForecast(**doc) async for doc in cursor]