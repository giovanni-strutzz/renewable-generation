import json
import logging
from aiokafka import AIOKafkaProducer
from src.domain.ports.messaging import EventPublisher

LOGGER = logging.getLogger("energy-api.kafka")

class KafkaEventPublisher(EventPublisher):
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def connect(self):
        """
        Initialize the kafka connection
        """
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        await self.producer.start()
        LOGGER.info("Kafka producer started successfully")

    async def disconnect(self):
        """
        Stop the kafka connection
        """
        if self.producer:
            await self.producer.stop()
            LOGGER.info("Kafka producer stopped successfully")

    async def publish(self, event: dict) -> None:
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")

        await self.producer.send_and_wait(self.topic, event)
        LOGGER.info(f"Published Event on topic '{self.topic}' : {event}")