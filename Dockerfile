# ============================
# 1. Builder image
# ============================
FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1

# System dependencies (only what we actually need)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv (env + runner)
RUN pip install --no-cache-dir uv

# Copy just enough for dependency resolution (better Docker cache)
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Install project + deps into a local virtualenv (.venv)
# --no-dev keeps dev deps like pytest out of the runtime image
RUN uv sync --no-dev

# At this point:
#   /app/.venv         ← virtualenv with all runtime deps + console scripts
#   /app/src           ← source package
#   /app/pyproject.toml, uv.lock


# ============================
# 2. Runtime image
# ============================
FROM python:3.11-slim AS runtime

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CLIMATE_DATA_ROOT=/app/data \
    CLIMATE_LOG_ROOT=/app/logs \
    CLIMATE_ENV=docker \
    PATH="/app/.venv/bin:$PATH"

# Standard data/log directories inside the container
RUN mkdir -p /app/data /app/logs

# Bring in the virtualenv from the builder image
COPY --from=builder /app/.venv /app/.venv

# Copy the rest of the project (dbt project, dashboards, tests, etc.)
COPY . .

# Default command: shell (docker-compose overrides this per service)
CMD ["bash"]