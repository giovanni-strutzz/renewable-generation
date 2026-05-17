from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import true

from src.infrastructure.dependencies import kafka_publisher
from src.interfaces.api.v1.endpoints import router
from src.interfaces.api.middlewares import LoggingMiddleware

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_publisher.connect()
    yield

    await kafka_publisher.disconnect()

def create_app() -> FastAPI:
    app = FastAPI(
        title="Renewable Forecast API",
        description="API for management and forecasting of renewable energy generation",
        version="1.0.0",
        lifespan=lifespan,
        openapi_tags=[{"name": "Forecast", "description": "Endpoint responsable for generation forecast"}],
    )

    app.add_middleware(LoggingMiddleware)
    app.include_router(router)

    @app.get("/health", tags=["Health"])
    async def health_check():
        return {"status": "ok", "service": "energy-API"}

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)