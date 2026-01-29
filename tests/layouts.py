import unittest

from config import Config
from src.layout_loader import LayoutLoader
from src.gsheet_interface import GoogleSheetInterface


class TestLayoutLoader(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.manager = GoogleSheetInterface("keys/zenreactor-c5bf4e9b37ab.json")
        self.settings = Config()

    async def test_load_layout_from_worksheet(self):
        loader = LayoutLoader()
        client = await self.manager.get_client()
        spreadsheet = await self.manager.get_spreadsheet_by_url(client, self.settings.spreadsheet_url)
        input_sheet = await spreadsheet.worksheet("input_amazon")

        await loader.add_from_worksheet(input_sheet)

        self.assertEqual(len(loader.layouts), 1)
        self.assertEqual(len(loader.domain_to_layout_map), 1)

        layout = loader.get_layout(layout_name="amazon")
        self.assertIsNotNone(layout)
        assert layout is not None

        print()
        print(
            {
                "layout_name": layout.name,
                "layout_domain": layout.domain,
                "xpaths": len(layout.xpaths),
                "css": len(layout.css),
            }
        )
