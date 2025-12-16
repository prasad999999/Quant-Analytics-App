import asyncio
from ingestion.binance_ws import tick_buffer
from storage.duckdb_manager import DuckDBManager

db = DuckDBManager()


async def tick_writer_loop(flush_interval: float = 1.0):
    """
    Periodically flush ticks from hot buffer to DuckDB
    """
    print("[DB-WRITER] Started tick writer loop")

    last_sizes = {}

    while True:
        batch = []

        for symbol, buffer in tick_buffer.buffers.items():
            last_size = last_sizes.get(symbol, 0)
            current_size = len(buffer)

            if current_size > last_size:
                new_ticks = list(buffer)[last_size:current_size]
                batch.extend(new_ticks)
                last_sizes[symbol] = current_size

        if batch:
            db.insert_ticks(batch)
            print(f"[DB] Inserted {len(batch)} ticks | total={db.count_ticks()}")
        else:
            print("[DB-WRITER] No new ticks")

        await asyncio.sleep(flush_interval)
