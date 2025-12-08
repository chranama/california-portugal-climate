# ============================
# 1. Builder image
# ============================
FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1

# System dependencies (adjust if you discover you need more)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Copy project metadata and source (minimal set for dependency resolution)
COPY pyproject.toml README.md ./
COPY src ./src

# Install dependencies + project into a virtualenv (.venv by default)
# --no-dev keeps dev deps (pytest) out of the runtime image
RUN uv sync --no-dev

# At this point, we have:
#   /app/.venv         ← virtualenv with all deps and scripts
#   /app/src           ← your source package
#   /app/pyproject.toml

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
    # Ensure the venv's bin is first on PATH so your scripts work
    PATH="/app/.venv/bin:$PATH"

# Create standard data/log directories
RUN mkdir -p /app/data /app/logs

# Copy the virtualenv from the builder
COPY --from=builder /app/.venv /app/.venv

# Copy the rest of the project (dbt project, configs, etc.)
COPY . .

# Default command is just a shell; docker-compose will override `command`
CMD ["bash"]