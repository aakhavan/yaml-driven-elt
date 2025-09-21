# syntax=docker/dockerfile:1
FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential \
 && rm -rf /var/lib/apt/lists/*

# Toggle uv usage at build time: --build-arg USE_UV=false to skip
ARG USE_UV=true
ENV UV_INSTALL_DIR=/usr/local/bin

# Install uv only when enabled (no -y; script is non-interactive)
RUN if [ "$USE_UV" = "true" ]; then \
      curl -LsSf https://astral.sh/uv/install.sh | sh -s -- ; \
    else \
      echo "Skipping uv install"; \
    fi

USER airflow
ARG AIRFLOW_VERSION=2.9.1
ARG PYTHON_VERSION=3.11
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Keep pip updated for fallback path
RUN pip install --no-cache-dir --upgrade pip

# Copy requirements
COPY requirements /tmp/requirements

# Install dependencies with corrected logic
# First, install project dependencies without constraints
# Then, install Airflow providers with constraints
RUN if [ "$USE_UV" = "true" ]; then \
      uv pip install --system --no-cache -r /tmp/requirements/project.txt && \
      uv pip install --system --no-cache -r /tmp/requirements/airflow-providers.txt -c "${CONSTRAINT_URL}"; \
    else \
      pip install --no-cache-dir -r /tmp/requirements/project.txt && \
      pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" -r /tmp/requirements/airflow-providers.txt; \
    fi
