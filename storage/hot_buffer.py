from collections import deque
from typing import Dict, List


class TickBuffer:
    """
    In-memory ring buffer for recent ticks (per symbol)
    """

    def __init__(self, maxlen: int = 10_000):
        self.buffers: Dict[str, deque] = {}
        self.maxlen = maxlen

    def add_tick(self, tick: dict):
        symbol = tick["symbol"]

        if symbol not in self.buffers:
            self.buffers[symbol] = deque(maxlen=self.maxlen)

        self.buffers[symbol].append(tick)

    def get_recent_ticks(self, symbol: str, n: int = 100) -> List[dict]:
        if symbol not in self.buffers:
            return []

        return list(self.buffers[symbol])[-n:]

    def get_last_tick(self, symbol: str) -> dict | None:
        if symbol not in self.buffers or not self.buffers[symbol]:
            return None
        return self.buffers[symbol][-1]

    def size(self, symbol: str) -> int:
        return len(self.buffers.get(symbol, []))
