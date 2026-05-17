import os

from src.application.use_cases.request_forecast import RequestForecastUseCase
from src.infrastructure.messaging.kafka_publisher import KafkaEventPublisher
from src.infrastructure.repository.mongo_repository import MongoRepository
from src.domain.ports.repositories import ForecastRepository


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "energy.forecast.requests")

mongo_repo = MongoRepository(connection_string=MONGO_URI)
kafka_publisher = KafkaEventPublisher(bootstrap_servers=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC)

def get_forecast_repository() -> ForecastRepository:
    return mongo_repo

def get_request_forecast_use_case() -> RequestForecastUseCase:
    return RequestForecastUseCase(publisher=kafka_publisher)