import json
from dataclasses import dataclass, field
from pathlib import Path
import aiofiles
import os


@dataclass
class Layout:
    name: str
    domain: str
    path: Path
    xpaths: dict[str, str] = field(default_factory=dict)
    css: dict[str, str] = field(default_factory=dict)


class LayoutLoader:
    def __init__(self):
        self.layouts: dict[str, Layout] = {}
        self.domain_to_layout_map: dict[str, Layout] = {}

    async def load_all_layouts(self, layout_path: Path):
        for layout_file in os.listdir(layout_path):
            if not layout_file.endswith(".json"):
                continue

            layout_path = layout_path / layout_file
            await self.load_layout(layout_path)
            print(f"[LAYOUT] Loaded {str(layout_path)!r}")

    async def load_layout(self, layout_path: Path):
        async with aiofiles.open(layout_path, "r") as f:
            layout_data = await f.read()
            layout = json.loads(layout_data)

        layout_name = layout.get("name", None)
        if not layout_name:
            raise ValueError("Layout name is missing")

        if not isinstance(layout_name, str):
            raise TypeError("Layout name must be a string")

        layout_domain = layout.get("domain", None)
        if not layout_domain:
            raise ValueError("Layout domain is missing")

        if not isinstance(layout_domain, str):
            raise TypeError("Layout domain must be a string")

        xpaths = layout.get("xpaths", {})
        css = layout.get("css", {})

        if layout_name in self.layouts:
            raise ValueError(f"Layout {layout_name!r} already exists")

        layout = Layout(
            name=layout_name,
            domain=layout_domain,
            path=layout_path,
            xpaths=xpaths,
            css=css,
        )

        self.layouts[layout_name] = layout
        self.domain_to_layout_map[layout_domain] = layout

        return layout

    def get_all_layouts(self):
        return self.layouts

    def get_layout(
        self,
        layout_name: str | None = None,
        domain: str | None = None,
    ) -> Layout | None:
        if layout_name is not None:
            return self.layouts.get(layout_name, None)

        if domain is not None:
            return self.domain_to_layout_map.get(domain, None)

        return None
