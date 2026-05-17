import os

from src.infrastructure.repository.mongo_repository import MongoRepository
from src.domain.ports.repositories import ForecastRepository


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
mongo_repo = MongoRepository(connection_string=MONGO_URI)

def get_forecast_repository() -> ForecastRepository:
    return mongo_repo