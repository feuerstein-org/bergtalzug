FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_TOOL_BIN_DIR=/usr/local/bin


WORKDIR /build

COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

# Build the framework wheel with cache mount
RUN --mount=type=cache,target=/root/.cache/uv \
    uv build --wheel --out-dir /wheels


FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

# Set uv environment variables
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN useradd --create-home --uid 1000 appuser

# Install wheel and its dependencies with cache mount
COPY --from=builder /wheels/*.whl /tmp/wheels/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system /tmp/wheels/*.whl && \
    rm -rf /tmp/wheels

# Set working directory for child containers
USER appuser
WORKDIR /app

# Verify installation
CMD ["sh", "-c", "echo 'This image contains only the bergtalzug library. Use it as a base image to build your own.'"]
