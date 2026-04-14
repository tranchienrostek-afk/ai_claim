FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastapi uvicorn httpx pydantic openai

COPY anthropic_compat_server.py .
COPY azure_anthropic_skd.py .

EXPOSE 8009

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8009/health || exit 1

CMD ["uvicorn", "anthropic_compat_server:app", "--host", "0.0.0.0", "--port", "8009"]
