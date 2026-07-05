FROM python:3.14-slim

WORKDIR /src
COPY . .
RUN pip install --no-cache-dir .
RUN groupadd --system --gid 1001 aw2sqlite \
    && useradd --system --uid 1001 --gid aw2sqlite --home-dir /data --shell /usr/sbin/nologin aw2sqlite \
    && mkdir -p /data \
    && chown aw2sqlite:aw2sqlite /data

# All state (config, database, logs, metadata) lives in /data.
VOLUME /data
WORKDIR /data
USER aw2sqlite

ENTRYPOINT ["aw2sqlite"]
CMD ["serve", "--config", "/data/aw2sqlite.toml", "--host", "0.0.0.0"]
