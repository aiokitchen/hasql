# MR Review Fixes — feature/types-and-split

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all bugs, broken docs, and inconsistencies found during deep review of the `feature/types-and-split` MR (11 commits, 64 files changed).

**Architecture:** The MR refactored hasql from inheritance to composition, extracting `PoolDriver` ABC, `PoolState`, `PoolHealthMonitor`, and enriched metrics. The code is structurally sound; fixes target broken import paths in docs/examples, a version-parsing crash, a probability edge-case, and minor inconsistencies.

**Tech Stack:** Python 3.10+, asyncio, pytest, ruff, mypy

---

## Research Summary

### Validation Results

| Check | Result |
|---|---|
| `ruff check` | PASS — 0 issues |
| `mypy` | PASS — 0 issues in 43 files |
| Unit tests (149) | PASS — all green |
| Integration tests | SKIP — no local PostgreSQL |
| Backward compat (`hasql.base` imports) | PASS — all 9 re-exports work |
| Driver imports (`hasql.aiopg` etc.) | **FAIL** — `ModuleNotFoundError` |

### Bugs Found

| # | Severity | Location | Issue |
|---|---|---|---|
| B1 | **CRITICAL** | README.rst (9 places), example/ (6 files) | Import paths `from hasql.<driver>` are dead — modules deleted. Users get `ModuleNotFoundError` |
| B2 | **HIGH** | `hasql/driver/asyncpg.py:12-15` | `_asyncpg_version()` crashes on pre-release versions (e.g. `"0.29.0rc1"`) — `int("0rc1")` raises `ValueError` |
| B3 | **LOW** | `hasql/balancer_policy/base.py:29` | `0 < rand <= weight` excludes `rand == 0.0` when `weight > 0`. Correct pattern: `rand < weight` |
| B4 | **LOW** | `hasql/driver/psycopg3.py:50` | `acquire_from_pool` passes `**kwargs` to `Psycopg3AcquireContext` which doesn't accept them — `TypeError` if any kwargs provided |

### Inconsistencies Found

| # | Location | Issue |
|---|---|---|
| I1 | `balancer_policy/greedy.py`, `random_weighted.py`, `round_robin.py` | Absolute imports `from hasql.balancer_policy.base import ...` instead of relative `from .base import ...` (all other modules use relative) |
| I2 | `round_robin.py:5` | Unnecessary absolute import `from hasql.pool_state import PoolStateProvider` — already available via `self._pool_state` from base class |
| I3 | `hasql/driver/psycopg3.py:86-87` | `terminate_pool` is a no-op (`pass`) — all other drivers actually terminate. Should log or document why |

### Docs/Plans Status

| Document | Status | Issues |
|---|---|---|
| `docs/class-scheme.md` | Accurate | None |
| `docs/types.md` | Accurate | None |
| `docs/timeouts.md` | Accurate | None |
| `docs/migration-0.9.0-to-0.10.0.md` | Accurate | Import examples correct (shows new paths) |
| `docs/metrics-dashboard-example.md` | Accurate | None |
| `plan/metrics-improvement.md` | Complete | All 7 phases implemented and verified |
| `docs/superpowers/plans/2026-03-27-lint-tox-fixes.md` | Complete | All 6 tasks implemented and verified |
| `README.rst` | **BROKEN** | 9 dead import paths |
| `example/` | **BROKEN** | 6 files with dead import paths |
| `CLAUDE.md` | Accurate | Architecture docs match code |

---

## Task 1: Fix broken import paths in README.rst

**Files:**
- Modify: `README.rst` (lines 83, 104, 128, 152, 189, 218, 375, 490, 650)

- [x] **Step 1: Fix all 9 import paths**

Replace each occurrence:

| Line | Old | New |
|---|---|---|
| 83 | `from hasql.aiopg import PoolManager` | `from hasql.driver.aiopg import PoolManager` |
| 104 | `from hasql.aiopg_sa import PoolManager` | `from hasql.driver.aiopg_sa import PoolManager` |
| 128 | `from hasql.asyncpg import PoolManager` | `from hasql.driver.asyncpg import PoolManager` |
| 152 | `from hasql.asyncsqlalchemy import PoolManager` | `from hasql.driver.asyncsqlalchemy import PoolManager` |
| 189 | `from hasql.asyncpgsa import PoolManager` | `from hasql.driver.asyncpgsa import PoolManager` |
| 218 | `from hasql.psycopg3 import PoolManager` | `from hasql.driver.psycopg3 import PoolManager` |
| 375 | `from hasql.asyncpg import PoolManager` | `from hasql.driver.asyncpg import PoolManager` |
| 490 | `from hasql.asyncpg import PoolManager` | `from hasql.driver.asyncpg import PoolManager` |
| 650 | `from hasql.aiopg import PoolManager` | `from hasql.driver.aiopg import PoolManager` |

- [x] **Step 2: Verify no old import paths remain**

Run: `grep -n "from hasql\.\(aiopg\|asyncpg\|psycopg3\|asyncsqlalchemy\|aiopg_sa\|asyncpgsa\) import" README.rst`
Expected: No output (0 matches)

- [x] **Step 3: Commit**

```bash
git add README.rst
git commit -m "fix(docs): update dead import paths in README.rst to hasql.driver.*"
```

---

## Task 2: Fix broken import paths in example files

**Files:**
- Modify: `example/simple_web_server.py:9`
- Modify: `example/otlp/aiopg.py:14`
- Modify: `example/otlp/aiopg_sa.py:14`
- Modify: `example/otlp/asyncpg.py:14`
- Modify: `example/otlp/asyncsqlalchemy.py:18`
- Modify: `example/otlp/psycopg3.py:17`

- [x] **Step 1: Fix all 6 example files**

| File | Old | New |
|---|---|---|
| `example/simple_web_server.py:9` | `from hasql.aiopg import PoolManager` | `from hasql.driver.aiopg import PoolManager` |
| `example/otlp/aiopg.py:14` | `from hasql.aiopg import PoolManager` | `from hasql.driver.aiopg import PoolManager` |
| `example/otlp/aiopg_sa.py:14` | `from hasql.aiopg_sa import PoolManager` | `from hasql.driver.aiopg_sa import PoolManager` |
| `example/otlp/asyncpg.py:14` | `from hasql.asyncpg import PoolManager` | `from hasql.driver.asyncpg import PoolManager` |
| `example/otlp/asyncsqlalchemy.py:18` | `from hasql.asyncsqlalchemy import PoolManager` | `from hasql.driver.asyncsqlalchemy import PoolManager` |
| `example/otlp/psycopg3.py:17` | `from hasql.psycopg3 import PoolManager` | `from hasql.driver.psycopg3 import PoolManager` |

- [x] **Step 2: Verify no old import paths remain in examples**

Run: `grep -rn "from hasql\.\(aiopg\|asyncpg\|psycopg3\|asyncsqlalchemy\|aiopg_sa\|asyncpgsa\) import" example/`
Expected: No output (0 matches)

- [x] **Step 3: Commit**

```bash
git add example/
git commit -m "fix(examples): update dead import paths to hasql.driver.*"
```

---

## Task 3: Fix asyncpg version parsing crash on pre-release versions

**Files:**
- Modify: `hasql/driver/asyncpg.py:12-15`
- Test: `tests/test_asyncpg.py`

- [x] **Step 1: Write the failing test**

Add to `tests/test_asyncpg.py`:

```python
from hasql.driver.asyncpg import _asyncpg_version


def test_asyncpg_version_parsing_release():
    """Standard release version parses correctly."""
    import asyncpg
    version = _asyncpg_version()
    assert isinstance(version, tuple)
    assert all(isinstance(x, int) for x in version)
    assert len(version) <= 3


def test_asyncpg_version_parsing_prerelease(monkeypatch):
    """Pre-release versions like '0.29.0rc1' don't crash."""
    import asyncpg
    monkeypatch.setattr(asyncpg, "__version__", "0.29.0rc1")
    version = _asyncpg_version()
    assert version == (0, 29, 0)


def test_asyncpg_version_parsing_dev(monkeypatch):
    """Dev versions like '0.30.0.dev0' don't crash."""
    import asyncpg
    monkeypatch.setattr(asyncpg, "__version__", "0.30.0.dev0")
    version = _asyncpg_version()
    assert version == (0, 30, 0)
```

- [x] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_asyncpg.py::test_asyncpg_version_parsing_prerelease -v`
Expected: FAIL with `ValueError: invalid literal for int() with base 10: '0rc1'`

- [x] **Step 3: Fix the version parser**

In `hasql/driver/asyncpg.py`, replace:

```python
def _asyncpg_version() -> tuple[int, ...]:
    return tuple(
        int(x) for x in asyncpg.__version__.split(".")[:3]
    )
```

With:

```python
import re

def _asyncpg_version() -> tuple[int, ...]:
    return tuple(
        int(re.match(r"\d+", part).group())
        for part in asyncpg.__version__.split(".")[:3]
        if re.match(r"\d+", part)
    )
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_asyncpg.py -k "version_parsing" -v`
Expected: 3 PASSED

- [x] **Step 5: Run full lint + type check**

Run: `uv run ruff check hasql/driver/asyncpg.py && uv run mypy hasql/driver/asyncpg.py`
Expected: All checks passed, Success

- [x] **Step 6: Commit**

```bash
git add hasql/driver/asyncpg.py tests/test_asyncpg.py
git commit -m "fix(asyncpg): handle pre-release version strings in _asyncpg_version()"
```

---

## Task 4: Fix balancer probability edge case

**Files:**
- Modify: `hasql/balancer_policy/base.py:28-29`
- Test: `tests/test_balancer_policy.py`

- [x] **Step 1: Write the failing test**

Add to `tests/test_balancer_policy.py`:

```python
async def test_master_as_replica_weight_zero_always_false(make_pool_manager):
    """weight=0 should never choose master as replica, even when rand=0."""
    pool_manager = await make_pool_manager(master_as_replica_weight=0.0)
    # With weight=0, should never get master when requesting replica
    # (unless fallback_master is True)
    # This is a property test: run multiple iterations
    for _ in range(20):
        pool = await pool_manager._balancer.get_pool(
            read_only=True,
            master_as_replica_weight=0.0,
        )
        # Should only get replica pools, never master
        assert pool is None or pool_manager._pool_state.pool_is_replica(pool)
```

- [x] **Step 2: Run test to verify current behavior**

Run: `uv run pytest tests/test_balancer_policy.py::test_master_as_replica_weight_zero_always_false -v`
Expected: PASS (edge case is rare enough to not trigger in 20 iterations, but the fix is still correct)

- [x] **Step 3: Fix the probability expression**

In `hasql/balancer_policy/base.py`, replace:

```python
            rand = random.random()
            choose_master_as_replica = 0 < rand <= master_as_replica_weight
```

With:

```python
            choose_master_as_replica = random.random() < master_as_replica_weight
```

This is the standard probability check:
- `weight=0.0`: `rand < 0.0` → always False (correct)
- `weight=1.0`: `rand < 1.0` → always True since `random.random()` returns `[0.0, 1.0)` (correct)
- `weight=0.5`: True ~50% of the time (correct)

- [x] **Step 4: Run balancer policy tests**

Run: `uv run pytest tests/test_balancer_policy.py -v`
Expected: All PASSED

- [x] **Step 5: Commit**

```bash
git add hasql/balancer_policy/base.py tests/test_balancer_policy.py
git commit -m "fix(balancer): use standard probability check for master_as_replica_weight"
```

---

## Task 5: Fix psycopg3 acquire_from_pool kwargs pass-through

**Files:**
- Modify: `hasql/driver/psycopg3.py:46-50`

- [ ] **Step 1: Fix by dropping kwargs silently (they're unused)**

The `Psycopg3AcquireContext` constructor only accepts `pool` and `timeout`. The `**kwargs` from `acquire_from_pool` would cause a `TypeError` if any were passed. Since psycopg3 pools don't support extra acquire kwargs, just remove the pass-through:

In `hasql/driver/psycopg3.py`, replace:

```python
    def acquire_from_pool(
        self, pool: AsyncConnectionPool,
        *, timeout=None, **kwargs,
    ):
        return Psycopg3AcquireContext(pool, timeout=timeout, **kwargs)
```

With:

```python
    def acquire_from_pool(
        self, pool: AsyncConnectionPool,
        *, timeout=None, **kwargs,
    ):
        return Psycopg3AcquireContext(pool, timeout=timeout)
```

- [ ] **Step 2: Run lint + tests**

Run: `uv run ruff check hasql/driver/psycopg3.py && uv run pytest tests/test_psycopg3.py -k "not pg_dsn" -v`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add hasql/driver/psycopg3.py
git commit -m "fix(psycopg3): stop passing unsupported kwargs to Psycopg3AcquireContext"
```

---

## Task 6: Normalize import style in balancer policies

**Files:**
- Modify: `hasql/balancer_policy/greedy.py:3`
- Modify: `hasql/balancer_policy/random_weighted.py:4`
- Modify: `hasql/balancer_policy/round_robin.py:4-5`

- [ ] **Step 1: Fix imports to use relative style**

In `hasql/balancer_policy/greedy.py`, replace:

```python
from hasql.balancer_policy.base import AbstractBalancerPolicy, PoolT
```

With:

```python
from .base import AbstractBalancerPolicy, PoolT
```

In `hasql/balancer_policy/random_weighted.py`, replace:

```python
from hasql.balancer_policy.base import AbstractBalancerPolicy, PoolT
```

With:

```python
from .base import AbstractBalancerPolicy, PoolT
```

In `hasql/balancer_policy/round_robin.py`, replace:

```python
from hasql.balancer_policy.base import AbstractBalancerPolicy, PoolT
from hasql.pool_state import PoolStateProvider
```

With:

```python
from .base import AbstractBalancerPolicy, PoolT
from ..pool_state import PoolStateProvider
```

- [ ] **Step 2: Run lint + type check + tests**

Run: `uv run ruff check hasql/balancer_policy/ && uv run mypy hasql/balancer_policy/ && uv run pytest tests/test_balancer_policy.py tests/test_policy.py -v`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add hasql/balancer_policy/
git commit -m "refactor(balancer): use relative imports consistent with rest of codebase"
```

---

## Task 7: Final validation

- [ ] **Step 1: Run full lint suite**

Run: `uv run ruff check hasql tests && uv run mypy --install-types --non-interactive hasql tests`
Expected: All checks passed, Success: no issues found

- [ ] **Step 2: Run full test suite**

Run: `uv run pytest tests/ -q --aiomisc-test-timeout=30 -k "not pg_dsn"`
Expected: All tests pass (integration tests requiring DB are skipped)

- [ ] **Step 3: Verify no dead import paths remain anywhere**

Run: `grep -rn "from hasql\.\(aiopg\|asyncpg\|psycopg3\|asyncsqlalchemy\|aiopg_sa\|asyncpgsa\) import" --include="*.py" --include="*.rst" .`
Expected: Only `example/otlp/common.py:29` should match (it imports `from hasql.pool_manager` which is correct)

Wait — the grep pattern would also match `hasql.asyncpg` in test files that use the driver directly. Let me refine:

Run: `grep -rn "from hasql\.\(aiopg\|asyncpg\|psycopg3\|asyncsqlalchemy\|aiopg_sa\|asyncpgsa\) import" --include="*.py" --include="*.rst" . | grep -v "hasql\.driver\."`
Expected: No matches (all imports should use `hasql.driver.*` path)

---

## Known Issues NOT Fixed (out of scope for this MR)

These are pre-existing design decisions or low-priority items:

| Issue | Why not fixed |
|---|---|
| asyncpg accesses private attrs (`_holders`, `_con`, `_addr`, `_queue`, `_minsize`, `_maxsize`) | No public API available in asyncpg; version-gated already |
| asyncpg `cached_hosts` dict never cleaned | Pools are long-lived; leak is negligible in practice |
| psycopg3 `terminate_pool` is a no-op | psycopg3 `AsyncConnectionPool` has no terminate method |
| `PoolState` set mutations not locked | Runs in single asyncio event loop; no data race possible |
| `TODO.md` sparse | Not related to this MR |
| `PoolT`/`ConnT` TypeVar redefined in multiple files | Standard Python pattern; no runtime impact |
