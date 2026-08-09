# syntax=docker/dockerfile:1.7
ARG PYTHON_IMAGE=python:3.13.7-slim-bookworm@sha256:67a1e1f215ccda113cfc024e8639049257e88f273898f595b61476d128d387e8

FROM ${PYTHON_IMAGE} AS builder
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
WORKDIR /build
COPY pyproject.toml README.md LICENSE MANIFEST.in ./
COPY constraints/ci.txt ./ci.txt
COPY src ./src
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install -c ci.txt .

FROM ${PYTHON_IMAGE} AS runtime
ARG MAL_UPDATER_UID=10001
ARG MAL_UPDATER_GID=10001
ENV PATH=/opt/venv/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/data TMPDIR=/tmp \
    MAL_UPDATER_WORKSPACE_DIR=/app MAL_UPDATER_RUNTIME_ROOT=/data MAL_UPDATER_SETTINGS_PATH=/data/config/settings.toml
RUN apt-get update \
 && apt-get install --no-install-recommends -y tini=0.19.0-1+b3 util-linux=2.38.1-5+deb12u3 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid "$MAL_UPDATER_GID" malupdater \
 && useradd --uid "$MAL_UPDATER_UID" --gid "$MAL_UPDATER_GID" --home-dir /data --shell /usr/sbin/nologin malupdater \
 && install -d -o malupdater -g malupdater -m 0700 /data /data/config /data/secrets /data/data /data/state /data/cache
COPY --from=builder /opt/venv /opt/venv
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint
WORKDIR /app
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint"]
CMD ["python", "-m", "mal_updater.container_runtime"]
