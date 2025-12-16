import pandas as pd


class ZScoreCalculator:
    def __init__(self, window: int = 20):
        self.window = window

    def compute(self, spread_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute rolling z-score of spread
        """
        df = spread_df.copy()

        df["mean"] = df["spread"].rolling(self.window).mean()
        df["std"] = df["spread"].rolling(self.window).std()

        df["zscore"] = (df["spread"] - df["mean"]) / df["std"]

        return df[["spread", "zscore"]]


class RollingCorrelationCalculator:
    def __init__(self, window: int = 20):
        self.window = window

    def compute(self, price_df: pd.DataFrame, symbol_x: str, symbol_y: str) -> pd.DataFrame:
        """
        Compute rolling correlation between two price series
        """
        df = price_df.copy()

        df["rolling_corr"] = (
            df[symbol_x]
            .rolling(self.window)
            .corr(df[symbol_y])
        )

        return df[["rolling_corr"]]


from storage.duckdb_manager import DuckDBManager

def load_aligned_prices(symbol_x: str, symbol_y: str, timeframe="1m"):
    db = DuckDBManager()

    query = f"""
    SELECT timestamp, symbol, close
    FROM bars_{timeframe}
    WHERE symbol IN (?, ?)
    ORDER BY timestamp
    """
    df = db.con.execute(query, [symbol_x, symbol_y]).fetchdf()

    pivot = df.pivot(index="timestamp", columns="symbol", values="close")
    pivot = pivot.dropna()

    return pivot
