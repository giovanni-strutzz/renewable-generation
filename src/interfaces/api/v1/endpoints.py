from typing import List

from fastapi import APIRouter, Depends
from src.domain.ports.repositories import ForecastRepository
from src.infrastructure.dependencies import get_forecast_repository
from src.interfaces.api.v1.schemas.forecast import ForecastResponse

router = APIRouter(prefix="/v1/energy")

@router.get("/forecast/{park_id}", response_model=List[ForecastResponse])
async def get_forecast(park_id: str, repo: ForecastRepository = Depends(get_forecast_repository)):
    return await repo.get_by_park(park_id)