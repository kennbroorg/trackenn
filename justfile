# trackenn task runner
# Install just: https://github.com/casey/just#installation

compose := "docker compose"
local_front := "docker compose -f docker-compose.yml -f docker-compose.local-front.yml"
gitleaks := "zricethezav/gitleaks:v8.28.0"

# List available recipes
default:
    @just --list

# Check config.yaml exists (a missing bind mount source becomes a directory) and create data-db
init:
    @test -f config.yaml || { echo "config.yaml not found: cp config.example.yaml config.yaml and set your API keys"; exit 1; }
    @mkdir -p data-db

# Build the images
build: init
    {{ compose }} build

# Start trackenn on http://127.0.0.1:4200 (front from the latest release)
up: init
    {{ compose }} up -d --build
    @echo "trackenn on http://127.0.0.1:${TRACKENN_PORT:-4200}"

# Start trackenn building the front from a local checkout (TRACKENN_FRONT_DIR, default ../trackenn-front)
up-local-front: init
    {{ local_front }} up -d --build
    @echo "trackenn on http://127.0.0.1:${TRACKENN_PORT:-4200}"

# Stop all services
down:
    {{ compose }} down

# Follow the logs of a service (backend or frontend)
logs service="backend":
    {{ compose }} logs -f {{ service }}

# Show running containers
ps:
    {{ compose }} ps

# Open a shell in the backend container
shell:
    {{ compose }} exec backend bash

# Restart a service
restart service:
    {{ compose }} restart {{ service }}

# Full rebuild without cache
rebuild: init
    {{ compose }} down
    {{ compose }} build --no-cache
    {{ compose }} up -d

# Run the backend tests in a throwaway container
test *args:
    docker build -q --target test -f install/docker/backend/Dockerfile -t trackenn-backend-test .
    docker run --rm trackenn-backend-test python -m pytest -q {{ args }}

# Copy an existing database into data-db, named after dbname in config.yaml
import file: init
    #!/usr/bin/env sh
    set -eu
    dbname=$(awk -F': *' '$1=="dbname"{print $2}' config.yaml | tr -d "\"' ")
    target="data-db/$(basename "$dbname")"
    if [ -e "$target" ]; then
        echo "$target already exists, run 'just export' first or remove it"; exit 1
    fi
    cp -- "{{ file }}" "$target"
    echo "{{ file }} -> $target"

# Copy the project database out of data-db
export dest=".":
    #!/usr/bin/env sh
    set -eu
    dbname=$(awk -F': *' '$1=="dbname"{print $2}' config.yaml | tr -d "\"' ")
    source="data-db/$(basename "$dbname")"
    target="{{ dest }}/$(basename "$dbname" .db)-$(date +%Y%m%d-%H%M%S).db"
    cp -- "$source" "$target"
    echo "$source -> $target"

# Scan branches and tags for secrets with gitleaks (refs/stash is never scanned)
scan:
    docker run --rm -v "$PWD":/repo:ro -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
        {{ gitleaks }} git /repo -c /repo/.gitleaks.toml --log-opts="-m --branches --tags" --redact --no-banner -v

# Remove containers and the locally built images (data-db is kept)
clean:
    {{ compose }} down --rmi local
