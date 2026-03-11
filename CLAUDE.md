# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

**Testing:**
```bash
# Run all tests
uv run pytest -vv --cov=hasql --cov-report=term-missing --doctest-modules --aiomisc-test-timeout=30 tests

# Run specific test file
uv run pytest -vv tests/test_utils.py

# Run specific test
uv run pytest -vv tests/test_utils.py::test_parse_connection_string_basic

# Run tests with specific pattern
uv run pytest -vv tests/test_utils.py -k "connection_string"

# Run tests using tox (preferred)
uv run tox -e py310 # Python 3.10
uv run tox -e py311 # Python 3.11
```

**Linting and Type Checking:**
```bash
# Lint code
uv run ruff check hasql tests

# Type checking
uv run mypy --install-types --non-interactive hasql tests

# Using tox (preferred)
uv run tox -e lint
uv run tox -e mypy
```

**Package Installation:**
```bash
# Install development dependencies
uv sync --group develop

# Install specific driver groups
uv sync --group aiopg       # aiopg support
uv sync --group asyncpg     # asyncpg support
uv sync --group psycopg     # psycopg3 support
```

## Architecture Overview

**hasql** is a high-availability PostgreSQL connection management library that provides automatic master/replica detection and load balancing across multiple database hosts.

### Core Components

1. **BasePoolManager** (`hasql/base.py`) - Abstract base class that defines the core pooling interface and connection management logic

2. **Driver-Specific Pool Managers:**
   - `hasql.aiopg.PoolManager` - aiopg driver support
   - `hasql.asyncpg.PoolManager` - asyncpg driver support
   - `hasql.psycopg3.PoolManager` - psycopg3 driver support
   - `hasql.asyncsqlalchemy.PoolManager` - SQLAlchemy async support
   - `hasql.aiopg_sa.PoolManager` - aiopg with SQLAlchemy support

3. **Balancer Policies** (`hasql/balancer_policy/`) - Load balancing strategies:
   - `GreedyBalancerPolicy` - Chooses pool with most free connections
   - `RandomWeightedBalancerPolicy` - Weighted random selection based on response times
   - `RoundRobinBalancerPolicy` - Round-robin selection

4. **Connection String Parsing** (`hasql/utils.py`) - Handles multi-host PostgreSQL connection strings with support for:
   - Comma-separated hosts: `postgresql://db1,db2,db3/dbname`
   - Per-host ports: `postgresql://db1:1234,db2:5678/dbname`
   - Global port override: `postgresql://db1,db2:6432/dbname`
   - libpq-style connection strings

5. **Metrics and Monitoring** (`hasql/metrics.py`) - Connection and performance metrics collection

### Key Features

- **Automatic Role Detection:** Continuously monitors each host to determine if it's a master or replica
- **Health Monitoring:** Background tasks check host availability and automatically exclude unhealthy hosts
- **Load Balancing:** Multiple policies for distributing connections across healthy replicas
- **Failover Support:** Automatic fallback to master when replicas are unavailable
- **Multi-Driver Support:** Works with asyncpg, aiopg, psycopg3, and SQLAlchemy

### Connection Flow

1. Parse multi-host DSN string into individual host connections
2. Create connection pools for each host with reserved system connections
3. Background tasks continuously check each host's role (master/replica) and health
4. When acquiring connections, balancer selects appropriate pool based on read_only flag
5. Connections are automatically returned to their respective pools when released

### Testing Strategy

- Uses pytest with aiomisc test framework
- Mocks database connections for unit testing (`tests/mocks/`)
- Integration tests for each driver implementation
- Coverage reporting with pytest-cov
- Tests are organized by driver type and functionality

## Important Notes

- The codebase uses Python 3.10+ with async/await throughout
- All pool managers extend the abstract `BasePoolManager` class
- Connection strings support both single and multi-host PostgreSQL URLs
- Background health checking runs every `refresh_delay` seconds (default: 1s)
- System reserves one connection per pool for health monitoring
- The library automatically detects PostgreSQL role changes with slight delay
