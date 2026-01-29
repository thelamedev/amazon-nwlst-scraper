from time import time
from datetime import timedelta
import asyncio
from typing import Sequence
from gspread_asyncio import (
    AsyncioGspreadClient,
    AsyncioGspreadClientManager,
    AsyncioGspreadSpreadsheet,
    AsyncioGspreadWorksheet,
)
from google.oauth2.service_account import Credentials


class GoogleSheetInterface:
    def __init__(self, service_account_path: str, refresh_time: timedelta = timedelta(minutes=15)):
        self.token_refresh_time = refresh_time.total_seconds()

        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        self.creds = Credentials.from_service_account_file(service_account_path)
        self.creds = self.creds.with_scopes(scopes)

        self.manager = AsyncioGspreadClientManager(self._get_creds)
        self.client: AsyncioGspreadClient | None = None
        self.client_refresh_lock = asyncio.Lock()
        self.client_last_refresh_time = 0

    def _get_creds(self) -> Credentials:
        return self.creds

    async def get_client(self):
        async with self.client_refresh_lock:
            if self.client is None:
                self.client = await self.manager.authorize()
                self.client_last_refresh_time = time()
                return self.client

            # if the client is not expired, return it
            if time() - self.client_last_refresh_time < self.token_refresh_time:
                return self.client

            return await self.manager.authorize()

    async def get_spreadsheet_by_url(self, client: AsyncioGspreadClient, url: str):
        return await client.open_by_url(url)

    async def get_all_worksheets(self, sheet: AsyncioGspreadSpreadsheet):
        return await sheet.worksheets()

    async def get_all_worksheet_titles(self, sheet: AsyncioGspreadSpreadsheet):
        worksheets = await self.get_all_worksheets(sheet)
        return [x.title for x in worksheets]

    async def create_worksheet(self, sheet: AsyncioGspreadSpreadsheet, name: str, cols: int):
        return await sheet.add_worksheet(title=name, rows=100, cols=cols)

    async def get_worksheet_by_name(self, sheet: AsyncioGspreadSpreadsheet, name: str):
        return await sheet.worksheet(name)

    async def read_row(self, sheet: AsyncioGspreadWorksheet, row: int):
        return await sheet.row_values(row)

    async def pop_row(self, sheet: AsyncioGspreadWorksheet, row: int):
        vals = await sheet.row_values(row)
        resp = await sheet.delete_rows(row)
        print("[DELETE ROW]", resp)
        return vals

    async def add_row(self, sheet: AsyncioGspreadWorksheet, data: Sequence[str | int | float], row_index: int = 2):
        await sheet.insert_row(data, index=row_index)  # type: ignore
