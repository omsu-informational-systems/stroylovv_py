from fastapi import APIRouter

from src.adapters.api.routers.health import router as health_router
from src.adapters.api.routers.market import router as market_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(market_router)

__all__ = ["api_router", "health_router", "market_router"]

