from fastapi import APIRouter, Depends, HTTPException, status

from src.adapters.api.dependencies import get_process_candle_use_case
from src.adapters.api.schemas.candle import (
    CandleRequestSchema,
    RecommendationResponseSchema,
)
from src.application.commands.process_candle import ProcessCandleCommand
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.domain.exceptions.base import DomainError

router = APIRouter(prefix="/market", tags=["Market"])


@router.post(
    "/candles",
    response_model=RecommendationResponseSchema,
    status_code=status.HTTP_200_OK,
    summary="Обработка биржевой свечи",
    description="Принимает дневную свечу, рассчитывает сигнал Дончяна и публикует рекомендацию.",
)
async def process_candle_endpoint(
    candle_in: CandleRequestSchema,
    use_case: ProcessCandleUseCase = Depends(get_process_candle_use_case),
) -> RecommendationResponseSchema:
    command = ProcessCandleCommand(
        ticker=candle_in.ticker,
        timestamp=candle_in.timestamp,
        open=candle_in.open,
        high=candle_in.high,
        low=candle_in.low,
        close=candle_in.close,
        volume=candle_in.volume,
        interval=candle_in.interval,
    )

    try:
        dto = await use_case.execute(command)
        return RecommendationResponseSchema(
            ticker=dto.ticker,
            timestamp=dto.timestamp,
            signal=dto.signal,
            close=dto.close,
            upper_band=dto.upper_band,
            lower_band=dto.lower_band,
            confidence=dto.confidence,
        )
    except DomainError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

