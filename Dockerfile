FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml .
RUN uv sync --no-install-project

COPY . .

EXPOSE 8080
CMD ["uv", "run", "hypercorn", "health_data.app:app", "--bind", "0.0.0.0:8080"]
