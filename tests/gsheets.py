import unittest
from src.gsheet_interface import GoogleSheetInterface
from config import Config


class TestGsheets(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.client = GoogleSheetInterface("keys/zenreactor-c5bf4e9b37ab.json")
        self.settings = Config()

    async def test_gsheets_workbooks(self):
        print()

        client = await self.client.get_client()
        spreadsheet = await self.client.get_spreadsheet_by_url(client, self.settings.spreadsheet_url)
        ws_titles = await self.client.get_all_worksheet_titles(spreadsheet)

        data_sheets = {}

        for sheet_name in ws_titles:
            if sheet_name.startswith("input_"):
                layout_name = sheet_name.replace("input_", "")
                if layout_name not in data_sheets:
                    data_sheets[layout_name] = {}
                data_sheets[layout_name].update({"input_sheet": sheet_name})
            elif sheet_name.startswith("output_"):
                layout_name = sheet_name.replace("output_", "")
                if layout_name not in data_sheets:
                    data_sheets[layout_name] = {}
                data_sheets[layout_name].update({"output_sheet": sheet_name})

        # ensure that all layouts have input and output sheets
        for layout_name, sheets in data_sheets.items():
            print(layout_name)
            for k, v in sheets.items():
                print(f"-> {k}: {v}")

            if "output_sheet" not in sheets:
                ws_input = await spreadsheet.worksheet(sheets["input_sheet"])

                ws_output = await self.client.create_worksheet(spreadsheet, f"output_{layout_name}", ws_input.col_count)
                # insert a header row from the input sheet
                header_row = await ws_input.row_values(1)
                # clean the None values because we cannot write them to the sheet using this API
                await self.client.add_row(ws_output, [x or "" for x in header_row])
                sheets.update({"output_sheet": f"output_{layout_name}"})
