FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
RUN uv venv

COPY requirements.txt .
RUN uv pip install -r requirements.txt

COPY . .

EXPOSE 8080
CMD ["/app/.venv/bin/hypercorn", "health_data.app:app", "--bind", "0.0.0.0:8080"]
