.PHONY: develop ruff mypy lint test

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
test:
	uv run pytest -vv \
		--cov=hasql --cov-report=term-missing \
		--doctest-modules \
		--aiomisc-test-timeout=30 \
		tests $(ARGS)
