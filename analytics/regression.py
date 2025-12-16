import pandas as pd
import statsmodels.api as sm
from storage.duckdb_manager import DuckDBManager


class HedgeRatioOLS:
    def __init__(self, timeframe: str = "1m"):
        self.db = DuckDBManager()
        self.timeframe = timeframe

    def load_pair_data(self, symbol_x: str, symbol_y: str) -> pd.DataFrame:
        """
        Load and align bar data for two symbols
        """
        query = f"""
        SELECT timestamp, symbol, close
        FROM bars_{self.timeframe}
        WHERE symbol IN (?, ?)
        ORDER BY timestamp
        """
        df = self.db.con.execute(query, [symbol_x, symbol_y]).fetchdf()

        if df.empty:
            raise ValueError("No bar data available for regression")

        # Pivot → columns = symbols
        pivot = df.pivot(index="timestamp", columns="symbol", values="close")

        # Keep only aligned timestamps
        pivot = pivot.dropna()

        return pivot

    def compute(self, symbol_x: str, symbol_y: str) -> dict:
        """
        Compute OLS hedge ratio: X ~ alpha + beta * Y
        """
        data = self.load_pair_data(symbol_x, symbol_y)

        y = data[symbol_x]
        x = data[symbol_y]

        # Add intercept
        x = sm.add_constant(x)

        model = sm.OLS(y, x).fit()

        alpha = model.params["const"]
        beta = model.params[symbol_y]
        r2 = model.rsquared

        return {
            "symbol_x": symbol_x,
            "symbol_y": symbol_y,
            "alpha": alpha,
            "beta": beta,
            "r2": r2,
            "n_obs": int(model.nobs),
        }
