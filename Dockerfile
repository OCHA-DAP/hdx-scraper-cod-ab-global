FROM python:3.14-slim

WORKDIR /srv

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/usr/local/bin/uv \
    --mount=type=bind,source=pyproject.toml,target=/srv/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/srv/uv.lock \
    --mount=type=bind,source=src,target=/srv/src,rw \
    --mount=type=bind,source=.git,target=/srv/.git \
    apt-get update && \
    apt-get install -y --no-install-recommends libexpat1 git && \
    uv sync --frozen --no-dev --no-editable && \
    apt-get purge -y --auto-remove git && \
    rm -rf /var/lib/apt/lists/*

COPY src ./src

ENTRYPOINT ["python", "-m", "hdx.scraper.cod_ab_global"]
