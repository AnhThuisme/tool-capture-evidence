FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    EVIDENCE_WDM_DIR=/data/.wdm \
    EVIDENCE_BASE_DIR=/data

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    ca-certificates \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .
RUN mkdir -p /data

EXPOSE 8000

CMD ["sh", "-c", "uvicorn web_ui:app --host 0.0.0.0 --port ${PORT:-8000}"]
