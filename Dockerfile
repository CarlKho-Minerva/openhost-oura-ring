FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "litestar>=2.0" \
    "hypercorn>=0.17" \
    "httpx>=0.27" \
    "aiosqlite>=0.20" \
    "jinja2>=3.1"

COPY . .

EXPOSE 8080
CMD ["hypercorn", "app:app", "--bind", "0.0.0.0:8080"]
