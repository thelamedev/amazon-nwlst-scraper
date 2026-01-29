FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y curl

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY pyproject.toml uv.lock ./
RUN uv sync
RUN uv run playwright install chromium

COPY . /app

CMD ["uv", "run", "python", "-m" "src.scraper"]
