FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .

ENV GRANT_DB_PATH=/data/grants.db

EXPOSE 8000

CMD ["grant-tracker", "web", "--host", "0.0.0.0", "--port", "8000"]
