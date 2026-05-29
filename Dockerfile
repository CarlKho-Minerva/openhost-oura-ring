FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN mkdir -p openhost_oura/src/openhost_oura && touch openhost_oura/src/openhost_oura/__init__.py
RUN uv sync --no-dev

COPY openhost_oura/ openhost_oura/
RUN uv sync --no-dev

EXPOSE 8080
CMD ["uv", "run", "--frozen", "--no-dev", "hypercorn", "openhost_oura.app:app", "--bind", "0.0.0.0:8080"]
