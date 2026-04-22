FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN printf '%s\n' \
    'Types: deb' \
    'URIs: http://ftp.debian.org/debian' \
    'Suites: trixie trixie-updates' \
    'Components: main' \
    'Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg' \
    > /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Retries=5 update \
    && apt-get install -y --no-install-recommends ffmpeg curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY docker/requirements.runtime.txt /tmp/requirements.runtime.txt
RUN pip install -r /tmp/requirements.runtime.txt

COPY vigilens /app/vigilens
