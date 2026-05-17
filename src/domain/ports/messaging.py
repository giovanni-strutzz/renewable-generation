from abc import ABC, abstractmethod
from typing import Dict, Any


class EventPublisher(ABC):
    """
    Output port to publish events on Kafka broker
    """

    @abstractmethod
    async def publish(self, event: Dict[str, Any]) -> None:
        pass