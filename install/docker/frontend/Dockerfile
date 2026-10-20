##############################################################################
# Dockerfile — trackenn frontend (nginx:alpine)
#
# The Angular app lives in a private repo. Two sources for its build:
#
#   FRONTEND_SOURCE=release (default)
#     Downloads the pre-built dist from a release of the PUBLIC repo
#     (published by the front repo's build-frontend.yml workflow):
#       FRONTEND_VERSION=latest  → tag frontend-latest, asset frontend-dist-latest.tar.gz
#       FRONTEND_VERSION=v1.2.3  → tag frontend-v1.2.3, asset frontend-dist-frontend-v1.2.3.tar.gz
#
#   FRONTEND_SOURCE=local
#     Builds a local checkout of the front, passed as the named build context
#     `front-src` (docker-compose.local-front.yml, `just up-local-front`).
#
# nginx serves the SPA, proxies /api/* to the backend and ships
# assets/config.json with {"apiUrl": "/api"}.
##############################################################################

ARG FRONTEND_SOURCE=release

FROM alpine:3.22 AS front-release

ARG FRONTEND_VERSION=latest
ARG GITHUB_REPO=kennbroorg/trackenn

RUN apk add --no-cache curl

RUN set -e; \
    if [ "$FRONTEND_VERSION" = "latest" ]; then \
        TAG="frontend-latest"; \
        ASSET="frontend-dist-latest.tar.gz"; \
    else \
        TAG="frontend-${FRONTEND_VERSION}"; \
        ASSET="frontend-dist-${TAG}.tar.gz"; \
    fi; \
    DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/download/${TAG}/${ASSET}"; \
    echo ">>> Downloading frontend from: ${DOWNLOAD_URL}"; \
    curl -fSL -o /tmp/frontend.tar.gz "${DOWNLOAD_URL}" || { \
        echo "ERROR: Failed to download ${DOWNLOAD_URL}"; \
        echo "       Check that release '${TAG}' exists on ${GITHUB_REPO},"; \
        echo "       or build a local front checkout with: just up-local-front"; \
        exit 1; \
    }; \
    mkdir -p /dist; \
    tar -xzf /tmp/frontend.tar.gz -C /dist; \
    test -f /dist/index.html || { echo "ERROR: ${ASSET} has no index.html at its root"; exit 1; }


FROM node:24-bookworm-slim AS front-local

WORKDIR /src
# INFO: Explicit copies, the host node_modules and dist never enter the build
COPY --from=front-src package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY --from=front-src angular.json tsconfig.json tsconfig.app.json ./
COPY --from=front-src src ./src
RUN npx ng build --output-path /dist


FROM front-${FRONTEND_SOURCE} AS front


FROM nginx:1.29-alpine

COPY --from=front /dist /usr/share/nginx/html
COPY install/docker/frontend/config.json /usr/share/nginx/html/assets/config.json
COPY install/docker/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY install/docker/frontend/security-headers.conf /etc/nginx/snippets/security-headers.conf

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1/health || exit 1
