# justfile — migrated from Makefile

# Install all development dependencies
develop:
    uv sync --group develop
    uv pip install psycopg-binary

# Run ruff linter
ruff:
    uv run ruff check hasql tests

# Run mypy type checker
mypy:
    uv run mypy --install-types --non-interactive hasql tests

# Run ruff + mypy
lint: ruff mypy

# Run tests
test *ARGS:
    uv run pytest -vv \
        -n auto \
        --cov=hasql --cov-report=term-missing \
        --doctest-modules \
        --aiomisc-test-timeout=30 \
        tests {{ ARGS }}

# Start chaos test cluster
chaos-up:
    sudo docker compose -f chaos/docker-compose.yml up -d --build

# Stop chaos test cluster and remove volumes
chaos-down:
    sudo docker compose -f chaos/docker-compose.yml down -v

# Show chaos cluster status via controller API
chaos-status:
    @curl -s localhost:8080/status | python3 -m json.tool

# Reset chaos cluster (unfreeze all, restart stopped)
chaos-reset:
    @curl -s -X POST localhost:8080/reset | python3 -m json.tool

# Start chaos controller
chaos-controller:
    cd chaos && uvicorn controller:app --port 8080
