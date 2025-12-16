import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from ingestion.binance_ws import start_stream
from storage.tick_writer import tick_writer_loop
from analytics.resample_runner import resample_loop
from api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan:
    - Start background ingestion & processing tasks
    - Keep them running for app lifetime
    """
    symbols = ["btcusdt", "ethusdt"]

    ws_task = asyncio.create_task(start_stream(symbols))
    writer_task = asyncio.create_task(tick_writer_loop(flush_interval=1.0))
    resample_task = asyncio.create_task(resample_loop(interval=60))

    print("[LIFESPAN] Background tasks started")

    try:
        yield
    finally:
        # Graceful shutdown
        print("[LIFESPAN] Shutting down background tasks")
        ws_task.cancel()
        writer_task.cancel()
        resample_task.cancel()


app = FastAPI(
    title="Quant Analytics Platform",
    description="Real-time stat-arb analytics with alerts",
    version="1.0.0",
    lifespan=lifespan,
)

# Register routes
app.include_router(router)


@app.get("/")
def root():
    return {"status": "Quant Analytics API running"}
