import json
import asyncio
import time
import random
from typing import Any, List

from playwright.async_api import Page, async_playwright, ProxySettings, Browser

from config import Config
from src.layout_loader import LayoutLoader
from src.gsheet_interface import GoogleSheetInterface
from gspread_asyncio import gspread
import src.errors as errors

type InputQueueItem = tuple[str, str] | None
type OutputQueueItem = tuple[str, dict[str, str] | None]

# INPUT_BATCH_SIZE = 10


class DynamicScraper:
    def __init__(self, config: Config, gsheet_manager: GoogleSheetInterface):
        self.config = config
        self.gsheet_manager = gsheet_manager
        self.layout_loader = LayoutLoader()
        self.success_count = 0
        self.input_count = 0

    async def detect_interstitial(self, page: Page) -> bool:
        """Detect Amazon interstitials instantly."""
        interstitial_selectors = 'button:has-text("Continue Shopping")'
        try:
            h = await page.wait_for_selector(interstitial_selectors, timeout=1000)
            return h is not None
        except Exception:
            return False

    async def handle_interstitial(self, page: Page, max_clicks=2) -> bool:
        """Click interstitial → wait for product page."""
        clicks = 0
        while clicks < max_clicks:
            try:
                # Try multiple button selectors
                button = page.locator("button", has_text="Continue Shopping").first
                if await button.is_visible():
                    await button.click()

                    await page.wait_for_load_state("domcontentloaded", timeout=3000)

                    # Check if we're now on product page
                    if await page.wait_for_selector("#productTitle", timeout=3000):
                        return True

            except Exception:
                pass
            finally:
                clicks += 1

        return False

    async def scrape_worker(
        self,
        worker_id: int,
        browser: Browser,
        input_queue: asyncio.Queue[InputQueueItem],
        output_queue: asyncio.Queue[OutputQueueItem],
    ):
        proxy_settings = None
        if self.config.proxy and self.config.proxy.enabled:
            print("[INFO] Proxy enabled")
            proxy_settings = ProxySettings(
                server=f"http://{self.config.proxy.host}:{self.config.proxy.port}",
                username=self.config.proxy.username,
                password=self.config.proxy.password,
            )

        ctx = await browser.new_context(
            user_agent=random.choice(self.config.user_agents),
            viewport={
                "width": 1920,
                "height": 1080,
            },
            color_scheme=random.choice(["dark", "light", "no-preference", "null"]),
            java_script_enabled=True,
            bypass_csp=True,
            proxy=proxy_settings,
        )

        page = await ctx.new_page()

        print(f"[INFO] Worker {worker_id} started")
        while True:
            try:
                input_item = input_queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
                continue

            if input_item is None:
                output_queue.put_nowait(("", None))
                break

            worksheet_name, url_path = input_item

            print(f"[INFO] W{worker_id:02d} | input_item: {input_item}")
            if not url_path:
                break

            # if attempt > self.config.retries:
            #     await output_queue.put((worksheet_name, ["error", attempt]))

            # print(f"[INFO] W{worker_id:02d} | attempt: {attempt} | url: {url!r}")
            t_start = time.time()

            layout = self.layout_loader.get_layout(layout_name=worksheet_name)
            if not layout:
                raise Exception(f"Layout not found for url: {url_path!r}")

            # this is to make sure we keep the same order of keys and values
            xpath_keys = list(layout.xpaths.keys())
            xpath_items = [layout.xpaths[k].replace("/@html", "") for k in xpath_keys]

            # this is to make sure we keep the same order of keys and values
            css_keys = list(layout.css.keys())
            css_items = [layout.css[k] for k in css_keys]

            js_items = []
            js_items.extend([{"type": "css", "path": path} for path in css_items])
            js_items.extend([{"type": "xpath", "path": path} for path in xpath_items])

            try:
                t1 = time.time()

                await page.goto(url_path, wait_until="commit", timeout=2000)
                await page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort("aborted"))

                if await self.detect_interstitial(page):
                    if not await self.handle_interstitial(page):
                        raise Exception("Interstitial not handled")

                await page.wait_for_selector("#productTitle", timeout=3000)

                t2 = time.time() - t1
                print(f"[INFO] W{worker_id:02d} | Page load took {t2:.2f}s", flush=True)

                data: dict[str, Any] = {"url": url_path}

                js_code = f"""
                    ({json.dumps(js_items)}).map((item) => {{
                        try {{
                            if (item.type === "css") {{
                                const el = document.querySelector(item.path);
                                if (el) {{ return el.innerText.trim(); }}
                            }} else if (item.type === "xpath") {{
                                const el = document.evaluate(item.path, document, null, XPathResult.STRING_TYPE, null);
                                if (el?.stringValue) {{ return el.stringValue.trim(); }}
                            }}

                            return `ERROR: {errors.ERR_XPATH_NOT_FOUND} (${{item.type}} | ${{item.path}})`;
                        }} catch (e) {{ 
                            return `ERROR: ${{ e.message }}`;
                        }}
                    }})
                """

                t1 = time.time()
                xpath_results = await page.evaluate(js_code)
                t2 = time.time() - t1
                print(f"[INFO] W{worker_id:02d} | XPath evaluation took {t2:.2f}s", flush=True)

                for i, col in enumerate(xpath_keys):
                    texts = xpath_results[i]
                    if isinstance(texts, list):
                        if texts:
                            data[col] = " | ".join(t.strip() for t in texts if t.strip())
                    else:
                        data[col] = str(texts)

                error_cols = [row for row in xpath_results if row.startswith("ERROR:")]
                error_rate = len(error_cols) / len(xpath_keys)

                t_elapsed = time.time() - t_start

                log_message = f"[INFO] W{worker_id:02d} | url: {url_path!r} | error rate: {error_rate:.2%} | time_elapsed: {t_elapsed:.2f}s"

                print(log_message, flush=True)

                await output_queue.put((worksheet_name, data))

            except Exception as e:
                t_elapsed = time.time() - t_start
                print(
                    f"[INFO] W{worker_id:02d} | url: {url_path!r} | time_elapsed: {t_elapsed:.2f}s | exception: {e}",
                    flush=True,
                )
                data = {"url": url_path}
                await output_queue.put((worksheet_name, data))

        await ctx.close()

    async def input_feeder(self, input_queue: asyncio.Queue[InputQueueItem]):
        client = await self.gsheet_manager.get_client()
        spreadsheet = await self.gsheet_manager.get_spreadsheet_by_url(
            client,
            self.config.spreadsheet_url,
        )
        worksheet_names = await self.gsheet_manager.get_all_worksheet_titles(spreadsheet)
        input_sheet_names = [x for x in worksheet_names if x.startswith("input_")]
        batch_offsets = {k: 3 for k in input_sheet_names}

        print(f"[INFO] Starting input feeder for {input_sheet_names}")
        while True:
            did_something = False
            for sheet_name in input_sheet_names:
                elapsed_time = time.time()

                worksheet = await spreadsheet.worksheet(sheet_name)
                batch_start = batch_offsets[sheet_name]
                batch_end = batch_start + self.config.max_concurrent

                row_range = await worksheet.get("A{0}:A{1}".format(batch_start, batch_end))
                row_range = [x for x in row_range if x]

                for row in row_range:
                    if row:
                        await input_queue.put((sheet_name, row[0]))
                        did_something = True

                elapsed_time = time.time() - elapsed_time
                batch_offsets[sheet_name] = batch_end

                print(
                    f"[INFO] input_feeder | sheet_name: {sheet_name!r} | rows added: {len(row_range)} | elapsed: {elapsed_time:.02f} seconds"
                )

            if not did_something:
                break

            await asyncio.sleep(5)

        for _ in range(self.config.max_concurrent):
            await input_queue.put(None)

    async def output_writer(self, output_queue: asyncio.Queue[OutputQueueItem]):
        print("[INFO] Starting output feeder")
        batches: dict[str, list[list[str]]] = {}
        while True:
            worksheet_name, data = await output_queue.get()
            if data is None:
                break

            output_sheet_name = worksheet_name.replace("input_", "output_")

            layout = self.layout_loader.get_layout(layout_name=worksheet_name)
            if not layout:
                print(f"[WARN] Layout not found for worksheet: {output_sheet_name!r}")
                continue

            row = [data["url"]]
            for col in layout.headers[1:]:
                row.append(data.get(col, ""))

            if output_sheet_name not in batches:
                batches[output_sheet_name] = []

            batches[output_sheet_name].append(row)

            if len(batches[output_sheet_name]) > 10:
                # write each row to the worksheet
                await self._write_batch_rows(output_sheet_name, batches[output_sheet_name])
                batches[output_sheet_name].clear()

            self.success_count += 1
            output_queue.task_done()

        for worksheet_name in batches:
            await self._write_batch_rows(worksheet_name, batches[worksheet_name])
            batches[worksheet_name].clear()

    async def _write_batch_rows(self, worksheet_name: str, rows: list[list[str]]):
        if not rows:
            return

        elapsed_t = time.time()
        # write to the worksheet
        client = await self.gsheet_manager.get_client()
        spreadsheet = await self.gsheet_manager.get_spreadsheet_by_url(
            client,
            self.config.spreadsheet_url,
        )
        worksheet = await self.gsheet_manager.get_worksheet_by_name(spreadsheet, worksheet_name)
        await worksheet.append_rows(rows)
        elapsed_t = time.time() - elapsed_t
        print(
            f"[INFO] output_writer | output_sheet_name: {worksheet_name!r} | row_count: {len(rows)} | elapsed: {elapsed_t:.02f} seconds"
        )

    async def scrape(self):
        input_queue = asyncio.Queue[InputQueueItem]()
        output_queue = asyncio.Queue[OutputQueueItem]()

        feeder_task = asyncio.create_task(self.input_feeder(input_queue))
        writer_task = asyncio.create_task(self.output_writer(output_queue))

        print("[INFO] Waiting for feeders to start and stabilize")
        await asyncio.sleep(3)

        concurrency = self.config.max_concurrent

        t0 = time.time()

        # results_df = pl.DataFrame(schema=["url", "status", *xpaths.keys()])
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.config.headless_mode,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-background-timer-throttling",
                    "--no-first-run",
                    "--disable-images",  # CRITICAL: 1s saved
                    "--disable-background-networking",
                ],
            )

            tasks: List[asyncio.Task] = [feeder_task, writer_task]

            print(f"[INFO] Starting {concurrency} scraping workers")
            for i in range(concurrency):
                t = asyncio.create_task(self.scrape_worker(i + 1, browser, input_queue, output_queue))
                tasks.append(t)

            # ✅ Gather ALL tasks
            await asyncio.gather(*tasks)

            t_total = time.time() - t0

            # Close the browser
            await browser.close()

        throughput_sec = self.success_count / t_total
        throughput_min = throughput_sec * 60
        throughput_hour = throughput_sec * 3600

        target_throughput_sec = 15_000 / 3600
        target_throughput_min = target_throughput_sec * 60
        target_throughput_hour = target_throughput_sec * 3600

        target_time_s = self.success_count / target_throughput_sec

        # print(f"✅ {t_total:.2f} seconds")
        print(f"✅ {self.success_count} URLs | {t_total:.2f} seconds")
        print(f"\t-> Target time for {self.success_count} URLs: {target_time_s:.0f} sec")
        print(f"\t-> {throughput_sec:.0f} URLs/sec | Target: {target_throughput_sec:.0f} URLs/sec")
        print(f"\t-> {throughput_min:.0f} URLs/min | Target: {target_throughput_min:.0f} URLs/min")
        print(f"\t-> {throughput_hour:.0f} URLs/hour | Target: {target_throughput_hour:.0f} URLs/hour")

    async def start(self):
        client = await self.gsheet_manager.get_client()
        sheet = await self.gsheet_manager.get_spreadsheet_by_url(
            client,
            self.config.spreadsheet_url,
        )
        sheets = await sheet.worksheets()
        for worksheet in sheets:
            # load add input layouts from all sheets
            if not worksheet.title.startswith("input_"):
                continue

            await self.layout_loader.add_from_worksheet(worksheet)
            output_sheet_name = worksheet.title.replace("input_", "output_")

            try:
                await sheet.worksheet(output_sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                print(f"[WARN] Creating worksheet {output_sheet_name!r}")
                input_header_row = await worksheet.row_values(1)
                nwsheet = await sheet.add_worksheet(title=output_sheet_name, rows=100, cols=len(input_header_row))
                await nwsheet.insert_row(input_header_row)

        print("[LAYOUT] Loaded all layouts")
        await self.scrape()


if __name__ == "__main__":
    cfg = Config()
    loop = asyncio.get_event_loop()
    gsheet_manager = GoogleSheetInterface(cfg.service_account_path)
    scraper = DynamicScraper(cfg, gsheet_manager)
    loop.run_until_complete(scraper.start())
