from typing import List

from fastapi import APIRouter, Depends, status

from src.application.use_cases.request_forecast import RequestForecastUseCase
from src.domain.ports.repositories import ForecastRepository
from src.infrastructure.dependencies import get_forecast_repository, get_request_forecast_use_case
from src.interfaces.api.v1.schemas.forecast import ForecastResponse

router = APIRouter(prefix="/v1/energy")

@router.get("/forecast/{park_id}", response_model=List[ForecastResponse])
async def get_forecast(park_id: str, repo: ForecastRepository = Depends(get_forecast_repository)):
    return await repo.get_by_park(park_id)

@router.post("forecast/{park_id}/request-update", status_code=status.HTTP_202_ACCEPTED)
async def request_forecast_update(
        park_id: str,
        use_case: RequestForecastUseCase = Depends(get_request_forecast_use_case)
):
    """
    Shoot an async event to start ETL from a specific park
    """
    event_payload = await use_case.execute(park_id=park_id)

    return {
        "message": "Processing request successfully sent",
        "details": event_payload
    }