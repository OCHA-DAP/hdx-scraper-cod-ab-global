FROM python:3.14-slim

WORKDIR /srv

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

ARG TIPPECANOE_VERSION=2.79.0

RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/usr/local/bin/uv \
    --mount=type=bind,source=pyproject.toml,target=/srv/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/srv/uv.lock \
    --mount=type=bind,source=src,target=/srv/src,rw \
    --mount=type=bind,source=.git,target=/srv/.git \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        libexpat1 \
        build-essential \
        git \
        libsqlite3-dev \
        zlib1g-dev && \
    git clone --branch "$TIPPECANOE_VERSION" --depth 1 \
        https://github.com/felt/tippecanoe.git /tmp/tippecanoe && \
    make -C /tmp/tippecanoe -j"$(nproc)" && \
    make -C /tmp/tippecanoe install && \
    rm -rf /tmp/tippecanoe && \
    uv sync --frozen --no-dev --no-editable && \
    apt-get purge -y --auto-remove build-essential git libsqlite3-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

COPY src ./src

ENTRYPOINT ["python", "-m", "hdx.scraper.cod_ab_global"]
