##############################################################################
# Dockerfile — trackenn backend (FastAPI + uvicorn, API only)
#
# The front is served by the nginx image (install/docker/frontend), so the
# container runs a single uvicorn process: `app.py --api-only`.
#
# Runtime contract (see docker-compose.yml):
#   /app/config.yaml  — mounted read-only from the host (API keys, level, dbname)
#   /app/db           — bind mount for the project databases (TRACKENN_DB_DIR)
#
# Targets:
#   runtime (default) — the API image used by docker compose
#   test              — runtime + dev requirements + tests, used by `just test`
##############################################################################

FROM python:3.14.8-slim-bookworm AS runtime

ARG UID=1000
ARG GID=1000

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TRACKENN_DB_DIR=/app/db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# INFO: Explicit copies, the image never gets config.yaml, databases or local scratch files
COPY app.py .
COPY core/__init__.py core/api_client.py core/bsc.py core/eth.py core/misc.py core/timestamps.py ./core/
COPY data ./data

RUN groupadd --gid "${GID}" trackenn && \
    useradd --uid "${UID}" --gid "${GID}" --no-create-home --shell /usr/sbin/nologin trackenn && \
    mkdir -p /app/db && chown trackenn:trackenn /app/db

USER trackenn

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/openapi.json', timeout=4)" || exit 1

CMD ["python", "app.py", "--api-only", "--ip", "0.0.0.0"]


FROM runtime AS test

# INFO: Tests pin the native defaults (dbname relative to cwd)
ENV TRACKENN_DB_DIR=

USER root
COPY requirements-dev.txt pytest.ini config.example.yaml ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY tests ./tests
RUN chown -R trackenn:trackenn /app
USER trackenn

CMD ["python", "-m", "pytest", "-q"]
