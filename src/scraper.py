import json
import pathlib
import asyncio
import time
import random
from typing import Any
import aiosqlite
from urllib.parse import urlparse

from playwright.async_api import Page, async_playwright, ProxySettings, Browser

from config import Config
from src.layout_loader import LayoutLoader
import src.errors as errors

type InputQueueItem = aiosqlite.Row
type OutputQueueItem = tuple[dict[str, str] | None, str, int]

INPUT_BATCH_SIZE = 50


class DynamicScraper:
    def __init__(self, config: Config):
        self.config = config
        self.layout_loader = LayoutLoader()
        self.success_count = 0
        self.input_count = 0

    async def create_tables_if_not_exists(self):
        async with aiosqlite.connect(self.config.db_path) as db:
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

            await db.commit()

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
            input_item = None
            try:
                input_item = input_queue.get_nowait()
            except asyncio.QueueEmpty:
                output_queue.put_nowait((None, "done", 0))
                break

            print(f"[INFO] W{worker_id:02d} | input_item: {input_item}")
            if input_item is None:
                output_queue.put_nowait((None, "done", 0))
                break

            url_id, url_path, layout_name, _, attempt, *others = input_item

            if not url_path:
                break
            if attempt > self.config.retries:
                await output_queue.put(({"url_id": url_id}, "error", attempt))

            # print(f"[INFO] W{worker_id:02d} | attempt: {attempt} | url: {url!r}")
            t_start = time.time()

            url_domain = urlparse(url_path).netloc
            layout = self.layout_loader.get_layout(layout_name=layout_name, domain=url_domain)
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
                # await wait_amazon_ready(page)

                t2 = time.time() - t1
                print(f"[INFO] W{worker_id:02d} | Page load took {t2:.2f}s", flush=True)

                data: dict[str, Any] = {"url_id": url_id}

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

                log_message = f"[INFO] W{worker_id:02d} | attempt: {attempt} | url: {url_path!r} | error rate: {error_rate:.2%} | time_elapsed: {t_elapsed:.2f}s"

                print(log_message, flush=True)

                # very high (>80%) error rate will be reprocessed
                if error_rate > 0.8:
                    await output_queue.put((data, "retry", attempt + 1))
                else:
                    await output_queue.put((data, "success", attempt))

            except Exception as e:
                t_elapsed = time.time() - t_start
                print(
                    f"[INFO] W{worker_id:02d} | attempt: {attempt} | url: {url_path!r} | time_elapsed: {t_elapsed:.2f}s | exception: {e}",
                    flush=True,
                )
                data = {"url_id": url_id}
                await output_queue.put((data, "retry", attempt + 1))

        await ctx.close()

    async def input_feeder(self, input_queue: asyncio.Queue[InputQueueItem]):
        async with aiosqlite.connect(self.config.db_path) as db:
            print("[INFO] Starting input feeder")
            while True:
                query = f"SELECT * FROM urls WHERE status in ('pending', 'retry') and retry_attempts < {self.config.retries} LIMIT {INPUT_BATCH_SIZE}"

                count = 0
                rows = await db.execute_fetchall(query)
                for row in rows:
                    if row is None:
                        break
                    count += 1
                    self.input_count += 1
                    await input_queue.put(row)

                if count == 0:
                    break

                await asyncio.sleep(5)

    async def output_writer(self, output_queue: asyncio.Queue[OutputQueueItem]):
        async with aiosqlite.connect(self.config.db_path) as db:
            print("[INFO] Starting output feeder")
            batch_size = 0
            while True:
                data, status, attempt = await output_queue.get()
                if data is None:
                    break

                url_id = data["url_id"]
                await db.execute(
                    """
                    UPDATE urls set status = ?, retry_attempts = ?, results = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        attempt,
                        json.dumps(data),
                        url_id,
                    ),
                )
                if batch_size > 100:
                    await db.commit()
                    batch_size = 0

                self.success_count += 1
                batch_size += 1
                output_queue.task_done()

            # Commit last batch
            await db.commit()

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

            tasks = [feeder_task, writer_task]

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

        target_throughput_sec = 50_000 / 3600
        target_throughput_min = target_throughput_sec * 60

        # print(f"✅ {t_total:.2f} seconds")
        print(f"✅ {self.success_count}/{self.input_count} URLs | {t_total:.2f} seconds")
        print(f"\tThroughput: {throughput_sec:.0f} URLs/sec | Target: {target_throughput_sec:.0f} URLs/sec")
        print(f"\tThroughput: {throughput_min:.0f} URLs/min | Target: {target_throughput_min:.0f} URLs/min")

    async def start(self):
        await self.create_tables_if_not_exists()
        await self.layout_loader.load_all_layouts(pathlib.Path(self.config.layouts_path))
        print("[LAYOUT] Loaded all layouts")
        await self.scrape()


if __name__ == "__main__":
    cfg = Config()
    loop = asyncio.get_event_loop()
    scraper = DynamicScraper(cfg)
    loop.run_until_complete(scraper.start())
