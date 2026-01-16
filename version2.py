#!/usr/bin/env python3
"""
Amazon NWLST Playwright Datascraper
"""

import json
import asyncio
import time
import random
import polars as pl
from typing import Dict, List
from playwright.async_api import Page, async_playwright, ProxySettings, Browser
from config import Config

# Your config
config = Config()

ERR_NOT_REACHABLE = "NOT_REACHABLE"
ERR_XPATH_NOT_FOUND = "XPATH_NOT_FOUND"
ERR_NO_DATA = "NO_DATA"

URL_LIMIT = 1000
PAGES_PER_CONTEXT = 3


def parse_excel_config(input_path: str) -> tuple[List[str], Dict[str, str]]:
    """Parse your Excel."""
    import polars as pl

    df = pl.read_excel(input_path)
    urls = [str(u) for u in df["Current URL"].drop_nulls() if str(u).startswith("http")]

    with open("xpaths.json", "r") as fp:
        xpaths = json.load(fp)

    if len(xpaths) < 34:
        diff = 34 - len(xpaths)
        keys = list(xpaths.keys())
        for i in range(diff):
            k = random.choice(keys)
            xpaths[f"{k}_{i + 35}"] = xpaths[k]

    return urls, xpaths


async def detect_interstitial(page: Page) -> bool:
    """Detect Amazon interstitials instantly."""
    interstitial_selectors = 'button:has-text("Continue Shopping")'
    try:
        h = await page.wait_for_selector(interstitial_selectors, timeout=1000)
        return h is not None
    except Exception:
        return False


async def handle_interstitial(page: Page, max_clicks=2) -> bool:
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
    worker_id: int,
    browser: Browser,
    input_queue: asyncio.Queue[tuple[str, int]],
    retry_queue: asyncio.Queue[tuple[str, int]],
    output_queue: asyncio.Queue[dict[str, str]],
    xpaths: dict[str, str],
):
    proxy_settings = None
    if config.proxy and config.proxy.enabled:
        print("[INFO] Proxy enabled")
        proxy_settings = ProxySettings(
            server=f"http://{config.proxy.host}:{config.proxy.port}",
            username=config.proxy.username,
            password=config.proxy.password,
        )

    ctx = await browser.new_context(
        user_agent=random.choice(config.user_agents),
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
        url, attempt = "", 0
        try:
            url, attempt = input_queue.get_nowait()
            if not url:
                break
            if attempt > config.retries:
                continue
        except asyncio.QueueEmpty:
            break

        # print(f"[INFO] W{worker_id:02d} | attempt: {attempt} | url: {url!r}")
        t_start = time.time()

        xpath_keys = list(xpaths.keys())
        xpath_items = [xpaths[k].replace("/@html", "") for k in xpath_keys]
        try:
            t1 = time.time()

            await page.goto(url, wait_until="commit", timeout=2000)
            await page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort("aborted"))

            if await detect_interstitial(page):
                if not await handle_interstitial(page):
                    raise Exception("Interstitial not handled")

            await page.wait_for_selector("#productTitle", timeout=3000)
            # await wait_amazon_ready(page)

            t2 = time.time() - t1
            print(f"[INFO] W{worker_id:02d} | Page load took {t2:.2f}s", flush=True)

            data = {"url": url}

            js_code = f"""
                ({json.dumps(xpath_items)}).map((xpath) => {{
                    try {{
                        const el = document.evaluate(xpath, document, null, XPathResult.STRING_TYPE, null);
                        if (el?.stringValue) {{ return el.stringValue.trim(); }}

                        return `ERROR: {ERR_XPATH_NOT_FOUND} (${{xpath}})`;
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
            error_rate = len(error_cols) / len(xpaths)

            t_elapsed = time.time() - t_start

            log_message = f"[INFO] W{worker_id:02d} | attempt: {attempt} | url: {url!r} | error rate: {error_rate:.2%} | time_elapsed: {t_elapsed:.2f}s"
            # if error_cols:
            #     log_message += f" | errors: {error_cols}"

            print(log_message, flush=True)

            # very high error rate will be reprocessed
            if error_rate > 0.9:
                # print(error_cols)
                # reprocess the url
                await retry_queue.put((url, attempt + 1))
            else:
                data["status"] = "ok"
                await output_queue.put(data)
        except Exception as e:
            t_elapsed = time.time() - t_start
            # print(f"[ERR] W{worker_id:02d}: {e}")
            print(
                f"[INFO] W{worker_id:02d} | attempt: {attempt} | url: {url!r} | time_elapsed: {t_elapsed:.2f}s | exception: {e}",
                flush=True,
            )
            data = {"url": url, "status": "error", **{col: ERR_XPATH_NOT_FOUND for col in xpaths}}
            await retry_queue.put((url, attempt + 1))
        # finally:
        #     if page:
        #         await page.close()

    await ctx.close()


async def main():
    input_queue = asyncio.Queue[tuple[str, int]]()
    output_queue = asyncio.Queue[dict[str, str]]()

    urls, xpaths = parse_excel_config("input-m1-amz-nwlst-general.xlsx")
    if len(urls) > URL_LIMIT:
        urls = urls[:URL_LIMIT]
    else:
        s = urls.copy()
        diff = URL_LIMIT - len(urls)
        while diff > 0:
            k = min(diff, 100)
            urls.extend(random.choices(s, k=k))
            diff = URL_LIMIT - len(urls)
            print(f"[INFO] Extended URLs to {len(urls)}")

    for url in urls:
        input_queue.put_nowait((url, 0))

    concurrency = min(config.max_concurrent, len(urls))

    print(f"[INFO] {len(urls)} URLs, {len(xpaths)} XPaths")

    t0 = time.time()

    # results_df = pl.DataFrame(schema=["url", "status", *xpaths.keys()])
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=config.headless_mode,
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

        # await browser.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort())

        tasks = []

        print(f"[INFO] Starting {concurrency} scraping workers")
        for i in range(concurrency):
            f = scrape_worker(i + 1, browser, input_queue, input_queue, output_queue, xpaths)
            tasks.append(f)

        # ✅ Gather ALL tasks
        await asyncio.gather(*tasks)

        t_total = time.time() - t0

        # Close the browser
        await browser.close()

        # Collect results
        data_results = []
        while not output_queue.empty():
            data = await output_queue.get()
            data_results.append(data)
            output_queue.task_done()

        results_df = pl.from_dicts(data_results)

    # Filter successes
    results_df.write_json("batch_results_v3.json")

    success = results_df.filter(pl.col("status") == "ok").count().row(0)[0]

    throughput_sec = success / t_total
    throughput_min = throughput_sec * 60

    target_throughput_sec = 100_000 / 3600
    target_throughput_min = target_throughput_sec * 60

    print(f"✅ {success}/{len(urls)} URLs | {t_total:.2f} seconds")
    print(f"\tThroughput: {throughput_sec:.0f} URLs/sec | Target: {target_throughput_sec:.0f} URLs/sec")
    print(f"\tThroughput: {throughput_min:.0f} URLs/min | Target: {target_throughput_min:.0f} URLs/min")


if __name__ == "__main__":
    asyncio.run(main())
