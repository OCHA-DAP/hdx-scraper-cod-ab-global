FROM public.ecr.aws/unocha/python:3.14-stable

WORKDIR /srv

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

# Alpine doesn't package tippecanoe (upstream ships source only), so it's built here.
ARG TIPPECANOE_VERSION=2.79.0

RUN --mount=type=bind,source=pyproject.toml,target=/srv/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/srv/uv.lock \
    --mount=type=bind,source=src,target=/srv/src,rw \
    --mount=type=bind,source=.git,target=/srv/.git \
    apk add --no-cache \
        gdal-driver-parquet \
        gdal-tools \
        libstdc++ \
        sqlite-libs \
        zlib && \
    apk add --no-cache --virtual .build-deps \
        build-base \
        gdal-dev \
        git \
        linux-headers \
        sqlite-dev \
        uv \
        zlib-dev && \
    git clone --branch "$TIPPECANOE_VERSION" --depth 1 \
        https://github.com/felt/tippecanoe.git /tmp/tippecanoe && \
    make -C /tmp/tippecanoe -j"$(nproc)" && \
    make -C /tmp/tippecanoe install && \
    rm -rf /tmp/tippecanoe && \
    uv sync --frozen --no-dev --no-editable && \
    apk del .build-deps

COPY src ./src

ENTRYPOINT ["python", "-m", "hdx.scraper.cod_ab_global.portolan"]
