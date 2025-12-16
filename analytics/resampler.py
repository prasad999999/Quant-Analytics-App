import pandas as pd
from typing import Literal
from storage.duckdb_manager import DuckDBManager

TimeFrame = Literal["1s", "1m", "5m"]


class TickResampler:
    def __init__(self):
        self.db = DuckDBManager()
        self._init_bar_tables()

    def _init_bar_tables(self):
        for tf in ["1s", "1m", "5m"]:
            self.db.con.execute(f"""
            CREATE TABLE IF NOT EXISTS bars_{tf} (
                timestamp TIMESTAMP,
                symbol VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                vwap DOUBLE
            )
            """)

    def resample(self, symbol: str, timeframe: TimeFrame):
        # 1️⃣ Load ticks
        df = self.db.con.execute(
            """
            SELECT timestamp, price, qty
            FROM ticks
            WHERE symbol = ?
            ORDER BY timestamp
            """,
            [symbol],
        ).fetchdf()

        if df.empty:
            print(f"[RESAMPLE] No ticks for {symbol}")
            return

        # 2️⃣ Datetime index
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")

        # 3️⃣ Resample rule (lowercase, future-proof)
        rule_map = {
            "1s": "1s",
            "1m": "1min",
            "5m": "5min",
        }
        rule = rule_map[timeframe]

        # 4️⃣ OHLC
        ohlc = df["price"].resample(rule).ohlc()

        # 5️⃣ Volume
        volume = df["qty"].resample(rule).sum()

        # 6️⃣ VWAP
        vwap = (
            (df["price"] * df["qty"]).resample(rule).sum()
            / df["qty"].resample(rule).sum()
        )

        # 7️⃣ Combine
        bars = pd.concat([ohlc, volume, vwap], axis=1)
        bars.columns = ["open", "high", "low", "close", "volume", "vwap"]

        # 8️⃣ Drop empty windows
        bars = bars.dropna(subset=["open"])
        if bars.empty:
            print(f"[RESAMPLE] No bars for {symbol} {timeframe}")
            return

        # 9️⃣ Finalize
        bars = bars.reset_index()
        bars["symbol"] = symbol

        # 🔥 FIX: reorder columns to match DuckDB schema
        bars = bars[
            [
                "timestamp",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
            ]
        ]

        # 🔟 Persist safely
        self.db.con.execute(
            f"DELETE FROM bars_{timeframe} WHERE symbol = ?",
            [symbol],
        )

        self.db.con.append(f"bars_{timeframe}", bars)

        print(f"[RESAMPLE] {symbol} {timeframe} bars={len(bars)}")
