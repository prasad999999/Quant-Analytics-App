import pandas as pd


class AlertEngine:
    def __init__(
        self,
        z_threshold: float = 2.0,
        corr_threshold: float = 0.7,
    ):
        self.z_threshold = z_threshold
        self.corr_threshold = corr_threshold

    def evaluate(
        self,
        z_df: pd.DataFrame,
        corr_df: pd.DataFrame,
        symbol_x: str,
        symbol_y: str,
    ) -> list[dict]:
        """
        Evaluate alert conditions and return list of alert events
        """
        alerts = []

        df = z_df.join(corr_df, how="inner")

        for ts, row in df.iterrows():
            z = row.get("zscore")
            corr = row.get("rolling_corr")

            if pd.isna(z) or pd.isna(corr):
                continue

            if abs(z) >= self.z_threshold and corr >= self.corr_threshold:
                direction = (
                    "SHORT_SPREAD" if z > 0 else "LONG_SPREAD"
                )

                alerts.append(
                    {
                        "timestamp": ts,
                        "pair": f"{symbol_x}-{symbol_y}",
                        "zscore": float(z),
                        "correlation": float(corr),
                        "direction": direction,
                        "message": (
                            f"{direction}: |z|={abs(z):.2f}, "
                            f"corr={corr:.2f}"
                        ),
                    }
                )

        return alerts
