# Amazon NWLST Scraper

Scrape amazon URLs with parallel processing and Interstitial bypass.

> The program is still in development. As of now, it writes the output to a json file called `batch_results_v*.json`. This will change in future to support writing to other sinks like Excel files, database records, or Google Spreadsheets.

# Configuration

Edit the `config.json` file to configure the scraper.

- `headless_mode` is the headless mode for playwright.

- `max_concurrent` is the maximum number of concurrent requests.

- `page_timeout_ms` is the page timeout in milliseconds.

- `locator_timeout` is the locator timeout in milliseconds.

- `delay` is the delay between requests in milliseconds.

- `retries` is the number of retries for failed requests.

- `user_agents` is the list of user agents for playwright.
  - This is required to lower the chances of getting blocked by Amazon.

- `input_file_path` is the path to the input file.

- `output_file_path` is the path to the output file.

- `proxy` is the proxy for playwright.
  - Use residentail proxies over VPNs or DataCenter proxies.
  - If you don't want to use a proxy, set its `enabled` to `false`.
  - If you want to use a proxy, set its `enabled` to `true` and provide the `proxy_url` and `proxy_username` and `proxy_password`.

# XPath Selectors

The program loads column names and xpath selectors from the `xpaths.json` file. If you want to add more columns, you can edit the `xpaths.json` file.

# Requirements

- Python 3.12
- uv

# Setup

Install `uv` from the official website

```sh
https://docs.astral.sh/uv/getting-started/installation/
```

Clone the repository

```sh
git clone https://github.com/thelamedev/amazon-nwlst-scraper.git
```

Sync the packages with `uv`

```sh
uv sync
```

Install required playwright browser(s)

```sh
uv run playwright install chromium
```

# Usage

```sh
uv run python version2.py
```

# License

Properieary license
