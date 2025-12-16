from fastapi import APIRouter, HTTPException
from analytics.regression import HedgeRatioOLS
from analytics.spread import SpreadCalculator
from analytics.stats import ZScoreCalculator, RollingCorrelationCalculator
from analytics.alerts import AlertEngine
from storage.duckdb_manager import DuckDBManager

router = APIRouter()
db = DuckDBManager()


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/bars")
def get_bars(symbol: str, timeframe: str = "1m", limit: int = 100):
    query = f"""
    SELECT *
    FROM bars_{timeframe}
    WHERE symbol = ?
    ORDER BY timestamp DESC
    LIMIT ?
    """
    df = db.con.execute(query, [symbol, limit]).fetchdf()
    return df.to_dict(orient="records")


@router.get("/hedge-ratio")
def hedge_ratio(
    symbol_x: str,
    symbol_y: str,
    timeframe: str = "1m",
):
    try:
        hr = HedgeRatioOLS(timeframe=timeframe)
        return hr.compute(symbol_x, symbol_y)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/spread")
def spread(
    symbol_x: str,
    symbol_y: str,
    timeframe: str = "1m",
):
    sc = SpreadCalculator(timeframe=timeframe)
    df = sc.compute(symbol_x, symbol_y)
    return df.reset_index().to_dict(orient="records")


@router.get("/zscore")
def zscore(
    symbol_x: str,
    symbol_y: str,
    timeframe: str = "1m",
    window: int = 20,
):
    sc = SpreadCalculator(timeframe=timeframe)
    spread_df = sc.compute(symbol_x, symbol_y)

    # 🔴 Guard: not enough data
    if spread_df.shape[0] < window:
        return []

    zs = ZScoreCalculator(window=window)
    z_df = zs.compute(spread_df)

    # Drop rows where zscore is NaN
    z_df = z_df.dropna(subset=["zscore"])

    if z_df.empty:
        return []

    return z_df.reset_index().to_dict(orient="records")



@router.get("/correlation")
def correlation(
    symbol_x: str,
    symbol_y: str,
    timeframe: str = "1m",
    window: int = 20,
):
    df = db.con.execute(
        f"""
        SELECT timestamp, symbol, close
        FROM bars_{timeframe}
        WHERE symbol IN (?, ?)
        ORDER BY timestamp
        """,
        [symbol_x, symbol_y],
    ).fetchdf()

    if df.empty:
        return []

    pivot = (
        df.pivot(index="timestamp", columns="symbol", values="close")
        .dropna()
    )

    # Not enough aligned data
    if pivot.shape[0] < window:
        return []

    rc = RollingCorrelationCalculator(window=window)
    corr_df = rc.compute(pivot, symbol_x, symbol_y)

    # 🔴 CRITICAL FIX: remove NaN / inf
    corr_df = corr_df.replace([float("inf"), float("-inf")], None)
    corr_df = corr_df.dropna(subset=["rolling_corr"])

    if corr_df.empty:
        return []

    return corr_df.reset_index().to_dict(orient="records")



@router.get("/alerts")
def alerts(
    symbol_x: str,
    symbol_y: str,
    timeframe: str = "1m",
    window: int = 20,
    z_threshold: float = 2.0,
    corr_threshold: float = 0.7,
):
    # Spread
    sc = SpreadCalculator(timeframe=timeframe)
    spread_df = sc.compute(symbol_x, symbol_y)

    # Z-score
    zs = ZScoreCalculator(window=window)
    z_df = zs.compute(spread_df)

    # Correlation
    df = db.con.execute(
        f"""
        SELECT timestamp, symbol, close
        FROM bars_{timeframe}
        WHERE symbol IN (?, ?)
        ORDER BY timestamp
        """,
        [symbol_x, symbol_y],
    ).fetchdf()

    price_df = (
        df.pivot(index="timestamp", columns="symbol", values="close")
        .dropna()
    )

    rc = RollingCorrelationCalculator(window=window)
    corr_df = rc.compute(price_df, symbol_x, symbol_y)

    # Alerts
    ae = AlertEngine(
        z_threshold=z_threshold,
        corr_threshold=corr_threshold,
    )

    return ae.evaluate(
        z_df,
        corr_df,
        symbol_x,
        symbol_y,
    )
