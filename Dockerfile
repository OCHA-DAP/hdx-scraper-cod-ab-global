FROM alpine:edge

WORKDIR /srv

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN --mount=type=bind,source=requirements.txt,target=requirements.txt \
    apk add --no-cache \
        gdal-driver-parquet \
        gdal-driver-pg \
        gdal-tools \
        postgis \
        postgresql18-client \
        python3 && \
    python -m venv /opt/venv && \
    apk add --no-cache --virtual .build-deps \
        build-base \
        gdal-dev \
        python3-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del .build-deps && \
    mkdir -p /run/postgresql && \
    chown -R postgres:postgres /run/postgresql

USER postgres

RUN initdb -D /var/lib/postgresql/data && \
    pg_ctl start -D /var/lib/postgresql/data && \
    createdb app && \
    psql -d app -c "CREATE EXTENSION postgis;"

COPY docker-entrypoint.sh /usr/local/bin/
COPY src ./src

ENTRYPOINT ["docker-entrypoint.sh", "python", "-m", "hdx.scraper.cod_ab_global"]
