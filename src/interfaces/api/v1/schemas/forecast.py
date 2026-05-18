from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class ForecastResponse(BaseModel):
    """
    Represents a forecast response
    """
    id: str = Field(..., json_schema_extra={"example": "645f1e"})
    park_id: str = Field(..., json_schema_extra={"example": "Ventos-do-Piaui-01"})
    source: str = Field(..., json_schema_extra={"example": "wind"})
    estimated_generation_mwh: float = Field(..., json_schema_extra={"example": 150.5})
    forecast_date: datetime

    class Config:
        from_attributes = True