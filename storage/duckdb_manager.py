import duckdb
from pathlib import Path
from typing import Iterable

DB_PATH = Path("data")
DB_PATH.mkdir(exist_ok=True)

DB_FILE = DB_PATH / "market_data.duckdb"


class DuckDBManager:
    def __init__(self):
        self.con = duckdb.connect(DB_FILE)
        self._init_tables()

    def _init_tables(self):
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            timestamp TIMESTAMP,
            symbol VARCHAR,
            price DOUBLE,
            qty DOUBLE
        )
        """)

    def insert_ticks(self, ticks: Iterable[dict]):
        if not ticks:
            return

        self.con.executemany(
            "INSERT INTO ticks VALUES (?, ?, ?, ?)",
            [
                (
                    t["timestamp"],
                    t["symbol"],
                    t["price"],
                    t["qty"],
                )
                for t in ticks
            ]
        )

    def count_ticks(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]

    def get_recent_ticks(self, symbol: str, limit: int = 10):
        return self.con.execute(
            """
            SELECT *
            FROM ticks
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).fetchdf()
