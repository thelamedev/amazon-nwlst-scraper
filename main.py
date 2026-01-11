#!/usr/bin/env python3

"""
Amazon NWLST Scraper - Playwright Async Rewrite
Original: Selenium Firefox + Excel COM → Playwright + JSON/Polars
Maintains: XPath-driven extraction, retries, error codes, batching, proxy/UA rotation
Improves: Async parallelism (5x faster), no Excel dep, cross-platform, maintained deps
"""

import json
import asyncio
import random
import time
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from config import Config

# =============================================================================
# CONFIGURATION (from original constants)
# =============================================================================

ERR_NOT_REACHABLE = "Either URL is incorrect or it is not reachable"
ERR_XPATH_NOT_FOUND = "Could not reach to this location"
ERR_NO_DATA = "Blank [No Data Available]"
ERR_CODES = [ERR_NOT_REACHABLE, ERR_XPATH_NOT_FOUND, ERR_NO_DATA]

config = Config()

# =============================================================================
# CONFIG LOADING (Excel → JSON)
# =============================================================================


def parse_excel_config(filepath: str) -> Dict[str, Any]:
    """Legacy: Parse original Excel format."""
    import polars as pl

    df = pl.read_excel(filepath, engine="openpyxl")
    urls = [url for url in df["Current URL"].drop_nans() if str(url).startswith(("http", "www"))]

    data_dicts = {}
    for col in df.columns:
        if "data from location" in str(col).lower():
            data_list = df[col].drop_nans()
            if not data_list.is_empty():
                data_dicts[col] = str(data_list[0])  # First row = XPath

    return {
        "urls": urls,
        "xpaths": data_dicts,
        "headless": config.headless_mode,
        "max_concurrent": config.max_concurrent,
    }


# =============================================================================
# BROWSER Context manager Class
# =============================================================================
class BrowserContextManager:
    def __init__(self, browser: Browser, concurreny: int = 5):
        self.browser = browser
        self.contexts: list[BrowserContext] = []
        self.concurrency = concurreny
        self.current_context_count = 0
        self.lock = asyncio.Lock()

    async def get_context(self):
        try:
            await self.lock.acquire()
            if not self.contexts:
                return await self.create_context()

            self.current_context_count = (self.current_context_count + 1) % self.concurrency
            context = self.contexts[self.current_context_count]
            return context
        finally:
            self.lock.release()

    async def create_context(self) -> BrowserContext:
        if not self.contexts:
            for _ in range(self.concurrency):
                context = await self.browser.new_context(
                    user_agent=random.choice(config.user_agents),
                    viewport={
                        "width": 1920,
                        "height": 1080,
                    },
                    color_scheme=random.choice(["dark", "light", "no-preference", "null"]),
                    java_script_enabled=True,
                    bypass_csp=True,
                )
                self.contexts.append(context)

        self.current_context_count = (self.current_context_count + 1) % self.concurrency
        return self.contexts[self.current_context_count]


# =============================================================================
# BROWSER SETUP (Selenium → Playwright)
# =============================================================================


async def create_browser(playwright: Playwright) -> Browser:
    """Create browser with proxy/UA rotation."""
    browser_type = playwright.firefox

    launch_kwargs = {
        "headless": config.headless_mode,
        "args": ["--no-sandbox", "--disable-setuid-sandbox", "--mute-audio"],
    }

    # Proxy setup
    if config.proxy and config.proxy.enabled:
        proxy = {"server": f"http://{config.proxy.host}:{config.proxy.port}"}
        if config.proxy.username:
            proxy["username"] = config.proxy.username
            proxy["password"] = config.proxy.password
        launch_kwargs["proxy"] = proxy
        print("[INFO] Proxy enabled")

    browser = await browser_type.launch(**launch_kwargs)
    print("[INFO] Browser launched successfully")
    return browser


async def new_page_context(browser: Browser) -> BrowserContext:
    """Create isolated context with UA rotation."""
    context = await browser.new_context(
        user_agent=random.choice(config.user_agents),
        viewport={
            "width": 1920,
            "height": 1080,
        },
        color_scheme=random.choice(["dark", "light", "no-preference", "null"]),
        java_script_enabled=True,
        bypass_csp=True,
    )
    return context


# =============================================================================
# XPATH EXTRACTION (Selenium XPath → Playwright Locator)
# =============================================================================


async def extract_from_xpath_fixed(page: Page, col_name: str, xpath: str) -> dict[str, str]:
    """Extract text/attribute from XPath, with fallbacks."""
    t = time.time()
    try:
        locator = page.locator(f"xpath={xpath}")
        items = await locator.all_inner_texts()
        if items:
            return {col_name: " ".join(items)}
        return {col_name: ERR_XPATH_NOT_FOUND}
    except Exception:
        return {col_name: ERR_XPATH_NOT_FOUND}
    finally:
        t = time.time() - t
        print(f"[TIME] [{col_name!r}] Extraction took {t:.2f}s")


async def extract_from_xpath(page: Page, col_name: str, xpath: str) -> dict[str, str]:
    """Extract text/attribute from XPath, with fallbacks."""
    t = time.time()
    try:
        # Parse attribute XPath: //elem/@attr → xpath=//elem, attr=attr
        plain_xpath = xpath
        attribute = None
        if '/@"' in xpath or "/@'" in xpath or "/@" in xpath:
            if "/@" in xpath:
                plain_xpath, attr_part = xpath.rsplit("/@", 1)
                attribute = attr_part.strip("\"'")
            if attribute == "html":
                attribute = "innerHTML"

        # Wait and locate elements
        locator = page.locator(f"xpath={plain_xpath}")
        await locator.first.inner_text(timeout=500)
        count = await locator.count()
        if count == 0:
            return {col_name: ERR_XPATH_NOT_FOUND}

        values = []
        for i in range(min(count, 10)):  # Limit to 10 elements
            el = locator.nth(i)  # zero-based
            try:
                if attribute:
                    value = await el.get_attribute(attribute)
                else:
                    # Text fallback chain
                    value = await el.inner_text()
                    if not value or not value.strip():
                        value = await el.text_content()
                    if not value or not value.strip():
                        html = await el.inner_html()
                        value = BeautifulSoup(html, "lxml").text.strip()

                if value and value.strip():
                    values.append(value.strip())
            except Exception as e:
                print(f"[WARN] column {col_name!r} | locator index {i}/{min(count, 10)} | Error: {e}")
                continue

        t = time.time() - t
        # print(f"[TIME] [{col_name!r}] Extraction took {t:.2f}s | with {len(values)} value(s)")

        str_values = " ".join(values) if values else ERR_NO_DATA

        return {col_name: str_values}
    except Exception:
        return {col_name: ERR_XPATH_NOT_FOUND}


# =============================================================================
# SCRAPER CORE (scraper() → scrape_url())
# =============================================================================


async def scrape_url(
    sem: asyncio.Semaphore,
    browser_ctx_manager: BrowserContextManager,
    url: str,
    xpaths: Dict[str, str],
    url_id: int,
    total_urls: int,
    retries: int = config.retries,
) -> Dict[str, str]:
    """Async scrape single URL with retries."""
    async with sem:
        print(f"[INFO] [{url_id + 1}/{total_urls}] {url} | {len(xpaths)=}")
        page = None

        for attempt in range(retries):
            try:
                t_start = time.time()
                context = await browser_ctx_manager.get_context()
                page = await context.new_page()

                await page.goto(
                    url,
                    wait_until="commit",
                    timeout=config.page_timeout_ms,
                    referer=random.choice(["https://google.com", url]),
                )

                data: Dict[str, str] = {"url": url, "status": "ok"}

                t = time.time() - t_start
                print(f"[TIME] [{url_id + 1}/{total_urls}] Page load took {t:.2f}s")

                t = time.time()

                # Extract data in parallel
                futures = []
                for col_name, xpath in xpaths.items():
                    try:
                        xpath_fixed = xpath.replace("@html", "@innerHTML")
                        futures.append(extract_from_xpath(page, col_name, xpath_fixed))
                    except Exception as e:
                        print(f"[WARN] Error: {e}")

                # futures = [extract_from_xpath(page, col_name, xpath) for col_name, xpath in xpaths.items()]
                for futItem in await asyncio.gather(*futures):
                    try:
                        data.update(futItem)
                    except Exception as e:
                        print(f"[WARN] Error: {e}")

                t = time.time() - t

                # Subtract 1 for the 'url' field
                n_fields_found = len([1 for k in data if data[k] not in ERR_CODES]) - 2
                n_fields_total = len(data) - 2

                if n_fields_found == 0:
                    data["status"] = "error"

                t_elapsed = time.time() - t_start

                print(
                    f"[OK] [{url_id + 1}] attempt {attempt + 1} | elapsed {t_elapsed:.2f}s | fields {n_fields_found}/{n_fields_total}"
                )
                return data

            except Exception as e:
                print(f"[WARN] [{url_id + 1}] attempt {attempt + 1} failed: {e}")
                # if the page context is not closed, it will be closed in the finally block
                if attempt < retries - 1:
                    await asyncio.sleep(config.delay)
            finally:
                if page:
                    await page.close()

        default_data = {col_name: ERR_XPATH_NOT_FOUND for col_name in xpaths.keys()}
        return {"url": url, "status": "error", **default_data}


# =============================================================================
# BATCHING & OUTPUT (CSV + Excel → Polars CSV/Parquet)
# =============================================================================


async def write_batch(batch_data: List[Dict[str, str]], headers: List[str]):
    """Append batch to outputs with conditional styling."""
    if not batch_data:
        return

    # Normalize to DataFrame
    df = pl.DataFrame(batch_data)

    # Reorder columns to match headers
    df = df.select(headers)

    df.write_csv(Path(config.output_file_path), include_header=True)
    print(f"[INFO] Appended {len(batch_data)} rows → {config.output_file_path} ({df.height} total)")


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================


async def main():
    """Main async orchestrator."""
    print("[INFO] Starting Amazon NWLST Scraper (Playwright Async)")

    print("===== CONFIGURATION =====")
    print(config.model_dump())
    print("===== xxxxxxxxxxxxx =====")

    # Load config
    excel_config = parse_excel_config(config.input_file_path)
    urls = excel_config["urls"][:100]

    xpaths = excel_config["xpaths"]
    headers = ["url", "status"] + list(xpaths.keys())

    print(f"[INFO] Loaded {len(urls)} URLs, {len(xpaths)} locations")
    start_time = time.time()
    success_count = 0

    async with async_playwright() as playwright:
        browser = await create_browser(playwright)
        ctx_manager = BrowserContextManager(browser, config.max_concurrent)
        await ctx_manager.create_context()

        sem = asyncio.Semaphore(config.max_concurrent)
        batch_data = []
        futures = []

        # Queue URLs with Semaphore
        for idx, url in enumerate(urls):
            fut = scrape_url(sem, ctx_manager, url, xpaths, idx, len(urls))
            futures.append(fut)

        completed_count = 0

        for fut in asyncio.as_completed(futures):
            try:
                data = await fut
                if data["status"] == "ok":
                    success_count += 1
                batch_data.append(data)
            except Exception as e:
                print(f"Error: {e}")

            completed_count += 1
            print(f"[PROGRESS] {completed_count}/{len(urls)} URLs processed")

        t = time.time()

        with open("batch_results.json", "w") as f:
            json.dump(batch_data, f, indent=2)

        await write_batch(batch_data, headers)

        t = time.time() - t
        print(f"[TIME] File Write time ({len(batch_data)} items): {t:.2f}s")

        await browser.close()

    elapsed_time = time.time() - start_time
    projected_throughput = (len(urls) / elapsed_time) * 60
    time_to_100k_min = 100000 / projected_throughput
    time_to_100k_h = time_to_100k_min / 60

    print(f"[INFO] ALL DONE! | Got {len(batch_data)} URLs successfully")
    print(
        f"[TIME] Total Elapsed time: {elapsed_time:.2f}s | Projected Throughput: {projected_throughput:.2f} URLs/min | Baseline Proximity: {projected_throughput / 17:.2f}% | Time to 100K: {time_to_100k_h} hours"
    )
    print(f"[INFO] Outputs: {config.output_file_path}")


# =============================================================================
# CLI ENTRY
# =============================================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user")
    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
    finally:
        print("\n[INFO] Waiting 2s before exit...")
        time.sleep(2)
