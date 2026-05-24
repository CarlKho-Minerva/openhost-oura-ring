FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY openhost_oura/ openhost_oura/
RUN uv sync --no-dev

EXPOSE 8080
CMD ["uv", "run", "--frozen", "--no-dev", "hypercorn", "openhost_oura.app:app", "--bind", "0.0.0.0:8080"]
