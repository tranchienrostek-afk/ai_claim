FROM node:20-bookworm-slim AS claude_cli

ARG CLAUDE_CODE_VERSION=2.1.90

RUN npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}"

FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libssl-dev \
    curl \
    git \
    ripgrep \
    && rm -rf /var/lib/apt/lists/*

# Claude CLI runtime
COPY --from=claude_cli /usr/local/bin/node /usr/local/bin/node
COPY --from=claude_cli /usr/local/bin/npm /usr/local/bin/npm
COPY --from=claude_cli /usr/local/bin/npx /usr/local/bin/npx
COPY --from=claude_cli /usr/local/bin/claude /usr/local/bin/claude
COPY --from=claude_cli /usr/local/lib/node_modules /usr/local/lib/node_modules

ENV CLAUDE_CODE_DISABLE_AUTOUPDATER=1

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Runtime directories
RUN mkdir -p \
    data/uploads \
    data/pipeline_runs \
    data/extracted_text \
    data/reports \
    data/datatest \
    data/script/testcase_trace_runs \
    config/ingest_configs \
    logs

EXPOSE 9600

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:9600/health || exit 1

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "9600"]
