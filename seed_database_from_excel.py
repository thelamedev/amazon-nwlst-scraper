import asyncio
import aiosqlite
from typing import List
from config import Config
import aiofiles


class SeedDatabaseFromExcel:
    def __init__(self, excel_path: str, db_path: str):
        self.excel_path = excel_path
        self.db_path = db_path

    async def create_tables_if_not_exists(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    layout_name TEXT DEFAULT NULL,
                    status TEXT DEFAULT 'pending',
                    retry_attempts INTEGER DEFAULT 0,
                    results TEXT
                );
                """
            )

    async def parse_excel_config(self) -> List[str]:
        """Parse your Excel."""
        import polars as pl

        df = pl.read_excel(self.excel_path)
        urls = [str(u) for u in df["Current URL"].drop_nulls() if str(u).startswith("http")]

        return urls

    async def seed_database(self):
        await self.create_tables_if_not_exists()

        urls = await self.parse_excel_config()
        async with aiosqlite.connect(self.db_path) as db:
            await db.executemany(
                "INSERT INTO urls (url, layout_name) VALUES (?, ?)",
                [
                    (
                        url,
                        "amazon",
                    )
                    for url in urls
                ],
            )
            await db.commit()

        print(f"Seeded {len(urls)} URLs")


if __name__ == "__main__":
    config = Config()
    loop = asyncio.get_event_loop()

    seeder = SeedDatabaseFromExcel(config.input_file_path, config.db_path)
    print(f"Seeding database from {config.input_file_path}")

    loop.run_until_complete(seeder.seed_database())
