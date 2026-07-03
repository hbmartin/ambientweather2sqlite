FROM python:3.14-slim

WORKDIR /src
COPY . .
RUN pip install --no-cache-dir .

# All state (config, database, logs, metadata) lives in /data.
VOLUME /data
WORKDIR /data

ENTRYPOINT ["aw2sqlite"]
CMD ["serve", "--config", "/data/aw2sqlite.toml", "--host", "0.0.0.0"]
