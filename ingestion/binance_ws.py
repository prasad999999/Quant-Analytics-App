import asyncio
import json
from datetime import datetime

import websockets
from storage.hot_buffer import TickBuffer

BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"

# 🔥 Global hot buffer
tick_buffer = TickBuffer(maxlen=10_000)


def normalize_trade(msg: dict) -> dict:
    ts = msg.get("T") or msg.get("E")
    return {
        "symbol": msg["s"],
        "timestamp": datetime.fromtimestamp(ts / 1000).isoformat(),
        "price": float(msg["p"]),
        "qty": float(msg["q"]),
    }


async def stream_symbol(symbol: str):
    url = f"{BINANCE_FUTURES_WS}/{symbol.lower()}@trade"

    while True:  # 🔁 AUTO-RECONNECT LOOP
        try:
            print(f"[CONNECTING] {symbol}")

            async with websockets.connect(
                url,
                ping_interval=20,   # ❤️ heartbeat every 20s
                ping_timeout=20,
            ) as ws:
                print(f"[CONNECTED] {symbol}")

                async for message in ws:
                    data = json.loads(message)

                    if data.get("e") != "trade":
                        continue

                    tick = normalize_trade(data)
                    tick_buffer.add_tick(tick)

        except asyncio.CancelledError:
            # Graceful shutdown
            print(f"[SHUTDOWN] {symbol}")
            break

        except Exception as e:
            print(f"[WS ERROR] {symbol}: {e}")
            print(f"[RECONNECTING] {symbol} in 5 seconds...")
            await asyncio.sleep(5)  # ⏳ backoff


async def start_stream(symbols: list[str]):
    await asyncio.gather(*(stream_symbol(s) for s in symbols))
