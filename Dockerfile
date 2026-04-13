FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY configs /app/configs
COPY docs /app/docs
COPY scripts /app/scripts
COPY data /app/data

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

EXPOSE 9780

CMD ["uvicorn", "src.ai_claim.main:app", "--host", "0.0.0.0", "--port", "9780"]
