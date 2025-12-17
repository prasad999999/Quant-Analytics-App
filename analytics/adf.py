import pandas as pd
from statsmodels.tsa.stattools import adfuller


class ADFTest:
    def run(self, spread_df: pd.DataFrame) -> dict:
        """
        Run ADF test on spread series
        """

        if "spread" not in spread_df.columns:
            return {
                "status": "error",
                "message": "Missing 'spread' column",
            }

        series = spread_df["spread"].dropna()

        if len(series) < 20:
            return {
                "status": "insufficient_data",
                "n_obs": int(len(series)),
            }

        result = adfuller(series.values)

        return {
            "status": "ok",
            "adf_statistic": float(result[0]),
            "p_value": float(result[1]),
            "lags_used": int(result[2]),
            "n_obs": int(result[3]),
            "critical_values": {
                str(k): float(v) for k, v in result[4].items()
            },
            "is_stationary": bool(result[1] < 0.05),
        }
