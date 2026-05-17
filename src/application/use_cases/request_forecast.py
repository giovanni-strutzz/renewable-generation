from datetime import datetime
from src.domain.ports.messaging import EventPublisher


class RequestForecastUseCase:
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher

    async def execute(self, park_id: str) -> dict:
        """
        Created an event requesting a forecast update.
        This event will be consumed by Airflow DAG.
        """
        event = {
            "event_id": f"req-{datetime.utcnow().timestamp()}",
            "park_id": park_id,
            "action": "TRIGGER_FORECAST_ETL",
            "requested_at": datetime.utcnow().isoformat()
        }

        await self.publisher.publish(event=event)

        return event