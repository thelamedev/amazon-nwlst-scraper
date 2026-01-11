import time
import json
import httpx
from lxml import html

# from bs4 import BeautifulSoup
import asyncio
from asyncio_throttle.throttler import Throttler
from main import parse_excel_config, config


async def scrape_http(client: httpx.AsyncClient, throttler: Throttler, url: str, xpaths: dict) -> dict:
    async with throttler:
        resp = await client.get(url, timeout=5.0)
        # soup = BeautifulSoup(resp.content, "lxml")
        tree = html.fromstring(resp.content)
        data = {"url": url}
        for col, xpath in xpaths.items():
            els = tree.xpath(xpath)  # ✅ Native XPath
            texts = [el.text_content().strip() for el in els[:3] if el.text_content()]
            if els:
                el = els[0]
                if "@html" in xpath:
                    # Extract innerHTML manually
                    html_content = html.tostring(el, encoding="unicode")
                    data[col] = str(html_content).strip()
                else:
                    data[col] = el.text_content().strip() or "NO_DATA"

            data[col] = " ".join(texts) if texts else "No Data"
        return data


async def main():
    excel_config = parse_excel_config(config.input_file_path)
    urls = excel_config["urls"][:100]

    print(f"[INFO] Loaded {len(urls)} URLs, {len(excel_config['xpaths'])} locations")
    print(urls[:5], sep="\n")

    xpaths = excel_config["xpaths"]
    xpaths = {k: v for k, v in xpaths.items() if v.startswith("(") or v.startswith("/")}
    print(json.dumps(xpaths, indent=2))
    # headers = ["url", "status"] + list(xpaths.keys())

    # Usage: 20 concurrent HTTP clients
    t_start = time.time()
    throttle = Throttler(20)
    proxy = httpx.Proxy(
        url="http://brd-customer-hl_a04ce830-zone-residential_proxy1:t1sbbu0dm7vl@brd.superproxy.io:33335"
    )
    async with httpx.AsyncClient(
        proxy=proxy,
        verify=False,
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=100),
        timeout=httpx.Timeout(8.0, read=10.0),
    ) as client:
        tasks = [scrape_http(client, throttle, url, xpaths) for url in urls]
        results = await asyncio.gather(*tasks)

    t_elapsed = time.time() - t_start

    with open("batch_results.json", "w") as f:
        json.dump(results, f, indent=2)

    projected_throughput = (len(urls) / t_elapsed) * 60
    print(f"{len(results)} Results written to batch_results.json")
    print(f"Elapsed time: {t_elapsed:.2f}s")
    print(f"Projected Throughput: {projected_throughput:.2f} URLs/min")
    print(f"Time to 100K: {100000 / projected_throughput:.2f} hours")
    print(f"Baseline Proximity (URL/min): {(projected_throughput / 6):.2f} %")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user")
    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
