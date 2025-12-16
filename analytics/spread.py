import pandas as pd
from storage.duckdb_manager import DuckDBManager
from analytics.regression import HedgeRatioOLS


class SpreadCalculator:
    def __init__(self, timeframe: str = "1m"):
        self.db = DuckDBManager()
        self.timeframe = timeframe
        self.reg = HedgeRatioOLS(timeframe=timeframe)

    def compute(self, symbol_x: str, symbol_y: str) -> pd.DataFrame:
        """
        Compute spread time series: X - beta * Y
        """
        # 1️⃣ Compute hedge ratio
        hr = self.reg.compute(symbol_x, symbol_y)
        beta = hr["beta"]

        # 2️⃣ Load aligned data
        query = f"""
        SELECT timestamp, symbol, close
        FROM bars_{self.timeframe}
        WHERE symbol IN (?, ?)
        ORDER BY timestamp
        """
        df = self.db.con.execute(query, [symbol_x, symbol_y]).fetchdf()

        pivot = df.pivot(index="timestamp", columns="symbol", values="close")
        pivot = pivot.dropna()

        # 3️⃣ Compute spread
        pivot["spread"] = pivot[symbol_x] - beta * pivot[symbol_y]

        return pivot[["spread"]]
