import asyncio
from analytics.resampler import TickResampler

async def resample_loop(interval: int = 60):
    """
    Periodically resample ticks into bars.
    interval: seconds (60 = 1 minute)
    """
    resampler = TickResampler()
    symbols = ["BTCUSDT", "ETHUSDT"]
    timeframes = ["1m", "5m"]

    print("[RESAMPLER] Started resampling loop")

    while True:
        try:
            for symbol in symbols:
                for tf in timeframes:
                    resampler.resample(symbol, tf)
        except Exception as e:
            print(f"[RESAMPLER] Error: {e}")

        await asyncio.sleep(interval)
