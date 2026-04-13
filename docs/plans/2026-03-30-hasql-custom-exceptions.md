# Replace RuntimeError with Custom Hasql Exceptions

## Overview
- Replace all 4 production `RuntimeError` raises with domain-specific exceptions under a common `HasqlError` base class
- Re-parent existing `PoolManagerClosingError` under `HasqlError` for a consistent hierarchy
- Users can `except HasqlError` to catch all library errors, or catch specific subclasses
- Addresses TODO.md item: "add hasql errors instead of RuntimeError"

## Context (from discovery)

**Production RuntimeErrors to replace:**

| Location | Message | New Exception |
|---|---|---|
| `hasql/acquire.py:111` | "No available pool" | `NoAvailablePoolError` |
| `hasql/pool_manager.py:155` | "Pool manager is closed" | `PoolManagerClosedError` |
| `hasql/pool_manager.py:181` | "Pool manager is closed" | `PoolManagerClosedError` |
| `hasql/driver/psycopg3.py:65` | "Expected a row from SHOW transaction_read_only" | `UnexpectedDatabaseResponseError` |

**Existing custom exception:**
- `hasql/exceptions.py` — `PoolManagerClosingError` (used in `hasql/health.py:147`)

**Files to modify:**
- `hasql/exceptions.py` — define new exception hierarchy
- `hasql/acquire.py:111` — raise `NoAvailablePoolError`
- `hasql/pool_manager.py:155,181` — raise `PoolManagerClosedError`
- `hasql/driver/psycopg3.py:65` — raise `UnexpectedDatabaseResponseError`
- `hasql/base.py` — re-export new exceptions
- `tests/test_acquire.py` — update expected exception types
- `tests/test_base_pool_manager.py` — update expected exception types

**Exception hierarchy:**

```
HasqlError(Exception)
  ├── PoolManagerClosedError      — acquire() on closed/closing manager
  ├── PoolManagerClosingError     — re-parented, was Exception
  ├── NoAvailablePoolError        — balancer found no suitable pool
  └── UnexpectedDatabaseResponseError — unexpected NULL from DB query
```

## Development Approach
- **Testing approach**: TDD — write failing tests first, then implement
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- Run tests after each change
- Maintain backward compatibility

## Testing Strategy
- **Unit tests**: required for every task
- Test that each raise site throws the correct custom exception
- Test that custom exceptions are catchable via `HasqlError` base class
- Test that `PoolManagerClosingError` is still catchable as before (backward compat)
- Test that exceptions carry meaningful messages

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with + prefix
- Document issues/blockers with warning prefix
- Update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: Define exception hierarchy in hasql/exceptions.py

- [x] Write test: `HasqlError` is a subclass of `Exception`
- [x] Write test: `PoolManagerClosedError` is a subclass of `HasqlError`
- [x] Write test: `NoAvailablePoolError` is a subclass of `HasqlError`
- [x] Write test: `UnexpectedDatabaseResponseError` is a subclass of `HasqlError`
- [x] Write test: `PoolManagerClosingError` is a subclass of `HasqlError` (re-parented)
- [x] Write test: all exceptions carry custom message via `str(exc)`
- [x] Run tests — expect FAIL (exceptions don't exist yet)
- [x] Add `HasqlError` base class to `hasql/exceptions.py`
- [x] Add `PoolManagerClosedError(HasqlError)` to `hasql/exceptions.py`
- [x] Add `NoAvailablePoolError(HasqlError)` to `hasql/exceptions.py`
- [x] Add `UnexpectedDatabaseResponseError(HasqlError)` to `hasql/exceptions.py`
- [x] Re-parent `PoolManagerClosingError` to inherit from `HasqlError` instead of `Exception`
- [x] Run tests — must pass before next task
- [x] Commit: "feat(exceptions): add HasqlError base class and domain-specific exceptions"

### Task 2: Replace RuntimeError in pool_manager.py with PoolManagerClosedError

- [x] Write test: `pool_manager.acquire()` raises `PoolManagerClosedError` when closed
- [x] Write test: `pool_manager.acquire()` raises `PoolManagerClosedError` when closing
- [x] Write test: `PoolManagerClosedError` message contains "closed"
- [x] Run tests — expect FAIL (still raises RuntimeError)
- [x] Replace `RuntimeError("Pool manager is closed")` at line 155 with `PoolManagerClosedError("Pool manager is closed")`
- [x] Replace `RuntimeError("Pool manager is closed")` at line 181 with `PoolManagerClosedError("Pool manager is closed")`
- [x] Add import: `from .exceptions import PoolManagerClosedError` to `hasql/pool_manager.py`
- [x] Update any existing tests that catch `RuntimeError` for these cases to catch `PoolManagerClosedError`
- [x] Run tests — must pass before next task
- [x] Commit: "refactor(pool_manager): raise PoolManagerClosedError instead of RuntimeError"

### Task 3: Replace RuntimeError in acquire.py with NoAvailablePoolError

- [x] Write test: `PoolAcquireContext._get_pool()` raises `NoAvailablePoolError` when balancer returns None
- [x] Write test: `NoAvailablePoolError` message contains "No available pool"
- [x] Run tests — expect FAIL (still raises RuntimeError)
- [x] Replace `RuntimeError("No available pool")` at line 111 with `NoAvailablePoolError("No available pool")`
- [x] Add import: `from .exceptions import NoAvailablePoolError` to `hasql/acquire.py`
- [x] Update any existing tests that catch `RuntimeError` for this case to catch `NoAvailablePoolError`
- [x] Run tests — must pass before next task
- [x] Commit: "refactor(acquire): raise NoAvailablePoolError instead of RuntimeError"

### Task 4: Replace RuntimeError in psycopg3.py with UnexpectedDatabaseResponseError

- [x] Write test: `Psycopg3Driver.is_master()` raises `UnexpectedDatabaseResponseError` when query returns None
- [x] Write test: `UnexpectedDatabaseResponseError` message contains "SHOW transaction_read_only"
- [x] Run tests — expect FAIL (still raises RuntimeError)
- [x] Replace `RuntimeError("Expected a row from SHOW transaction_read_only")` at line 65 with `UnexpectedDatabaseResponseError("Expected a row from SHOW transaction_read_only")`
- [x] Add import: `from ..exceptions import UnexpectedDatabaseResponseError` to `hasql/driver/psycopg3.py`
- [x] Run tests — must pass before next task
- [x] Commit: "refactor(psycopg3): raise UnexpectedDatabaseResponseError instead of RuntimeError"

### Task 5: Export new exceptions from hasql/base.py and update public API

- [x] Write test: `from hasql.base import HasqlError` works
- [x] Write test: `from hasql.base import PoolManagerClosedError` works
- [x] Write test: `from hasql.base import NoAvailablePoolError` works
- [x] Write test: `from hasql.base import UnexpectedDatabaseResponseError` works
- [x] Run tests — expect FAIL (not yet exported)
- [x] Add re-exports to `hasql/base.py`: `HasqlError`, `PoolManagerClosedError`, `NoAvailablePoolError`, `UnexpectedDatabaseResponseError`
- [x] Verify `PoolManagerClosingError` is already exported (it should be)
- [x] Run tests — must pass before next task
- [x] Commit: "feat(api): export new exception classes from hasql.base"

### Task 6: Verify acceptance criteria

- [x] Verify: zero `RuntimeError` raises remain in production code (`grep -rn "raise RuntimeError" hasql/`)
- [x] Verify: all custom exceptions inherit from `HasqlError`
- [x] Verify: `except HasqlError` catches all library exceptions
- [x] Run full test suite: `uv run pytest tests/ -q -k "not pg_dsn"`
- [x] Run linter: `uv run ruff check hasql tests && uv run mypy hasql tests`
- [x] All issues must be fixed

### Task 7: Update documentation and clean up TODO

- [ ] Update `docs/migration-0.9.0-to-0.10.0.md` with new exception section
- [ ] Remove "add hasql errors instead of RuntimeError" from `TODO.md`
- [ ] Commit: "docs: document custom exception hierarchy and update migration guide"

## Technical Details

**Exception classes:**

```python
# hasql/exceptions.py

class HasqlError(Exception):
    """Base exception for all hasql errors."""


class PoolManagerClosedError(HasqlError):
    """Raised when acquire() is called on a closed or closing pool manager."""


class PoolManagerClosingError(HasqlError):
    """Raised when pool creation is exhausted during manager shutdown."""


class NoAvailablePoolError(HasqlError):
    """Raised when the balancer cannot find a suitable pool for the request."""


class UnexpectedDatabaseResponseError(HasqlError):
    """Raised when a database query returns an unexpected result."""
```

**Backward compatibility:**
- `PoolManagerClosingError` changes parent from `Exception` to `HasqlError(Exception)` — existing `except PoolManagerClosingError` still works, but `except Exception` also still catches it (HasqlError inherits from Exception)
- Test code using `RuntimeError` as mock side effects (lines 169, 189, 408 in tests) stays as-is — those test generic error handling, not hasql-specific behavior

## Post-Completion

**Manual verification:**
- Confirm that downstream projects catching `RuntimeError` from hasql are updated to catch `HasqlError` or specific subclasses
- Consider adding exception docs to README.rst in a future MR
