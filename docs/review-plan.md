# План ревью: рефакторинг hasql 0.10.0

## Общая структура

6 PR образуют линейную цепочку. Каждый следующий базируется на предыдущем:

```
master → PR #36 → PR #37 → PR #38 → PR #39 → PR #31 → PR #32
         foundations  contracts  core split  drivers   staleness  docs
         +257/-26     +727/-148  +1664/-937  +830/-615 +734/-10   docs only
```

**Рекомендуемый порядок ревью: снизу вверх (PR #36 → #32).**
Каждый PR самодостаточен на уровне компиляции, но тесты PR #37 частично зависят от PR #38 (см. примечания).

---

## PR #36 — refactor(foundations): exception hierarchy, constants, enriched metrics

**Ветка:** `refactor/1-foundations` → `master`
**Объём:** +257/-26 (5 файлов hasql/tests)
**Статус:** ruff OK, 98 тестов проходят

### Что смотреть

| Файл | На что обратить внимание |
|------|-------------------------|
| `hasql/exceptions.py` | Иерархия исключений: `HasqlError` → 4 подкласса. Полнота, именование, нужен ли `PoolManagerClosingError` как публичный. |
| `hasql/constants.py` | Значения по умолчанию. Дублируются с `base.py` — wiring в PR #38. |
| `hasql/metrics.py` | Новые dataclass-ы: `PoolRole`, `PoolStats`, `PoolMetrics`, `HasqlGauges`. Обратная совместимость `DriverMetrics`. |
| `tests/test_calculate_metrics.py` | Покрытие `CalculateMetrics`. |
| `tests/test_exceptions.py` | Иерархия + экспорты из модулей. |

### Известные замечания из ревью

- `frozen=True` есть, `slots=True` нет (гайдлайн проекта требует оба)
- `PoolRole` использует `str, Enum` вместо `IntEnum` — нужен комментарий почему
- Константы пока дублируются в `base.py` — устраняется в PR #38

---

## PR #37 — refactor(contracts): PoolDriver ABC, acquire contexts, balancer refactoring

**Ветка:** `refactor/2-contracts` → `refactor/1-foundations`
**Объём:** +727/-148 (11 файлов)
**Статус:** ruff OK, 100 тестов проходят, 18 падают (зависят от PR #38)

### Что смотреть

| Файл | На что обратить внимание |
|------|-------------------------|
| `hasql/abc.py` | `PoolDriver` ABC — полнота интерфейса, generic-параметры `[PoolT, ConnT]`. |
| `hasql/acquire.py` | `TimeoutAcquireContext`, `PoolAcquireContext` — корректность deadline/timeout бюджета, edge cases (0, отрицательный). |
| `hasql/pool_state.py` | `PoolStateProvider` Protocol — минимальность, достаточность для балансировщиков. |
| `hasql/balancer_policy/*.py` | Переход на Protocol. Изменение формулы весов в `RandomWeightedBalancerPolicy` — это **bugfix**, а не просто упрощение. |
| `tests/test_acquire.py` | 256 строк тестов на acquire-контексты. |

### Известные замечания

- 3 теста в `test_abc.py` импортируют `TestDriver` из PR #38 — падают отдельно, проходят при merge в стек
- `test_balancer_policy.py` использует `TestPoolManager` из PR #38 — аналогично
- `PoolAcquireContext` использует методы, не описанные в `PoolStateProvider` (полный тип — `PoolState` из PR #38)
- `defaultdict(lambda: 0)` → лучше `defaultdict(int)` в `round_robin.py`

---

## PR #38 — refactor(core): split base.py into pool_state, pool_manager, health

**Ветка:** `refactor/3-core` → `refactor/2-contracts`
**Объём:** +1664/-937 (14 файлов) — **самый крупный и важный PR**
**Статус:** ruff — 1 ошибка (line length, исправлена в PR #31), 163 теста проходят

### Что смотреть

| Файл | На что обратить внимание |
|------|-------------------------|
| `hasql/pool_state.py` | `PoolState` — управление master/replica sets, ожидание через `Condition`. Корректность asyncio-примитивов. |
| `hasql/pool_manager.py` | `BasePoolManager` — тонкий оркестратор. **Все proxy-методы** (`release`, `terminate`, `dsn`, `pools`, `closing`, `closed`, `balancer` и т.д.) на месте. |
| `hasql/health.py` | `PoolHealthMonitor` — получает `pool_state`, `refresh_delay`, `refresh_timeout`, `closing_getter` через конструктор (DI, **без** ссылки на manager). |
| `hasql/base.py` | 23 строки — re-export shim. Все старые имена доступны. |
| `hasql/utils.py` | `Stopwatch[KeyT]` — generic, `Dsn.with_()` — сохранение scheme, defensive copy params. |
| `tests/test_backward_compat.py` | Проверка что re-export shim-ы работают (`is` identity). |

### Ключевые вопросы для ревьюера

1. **Разделение ответственности:** `PoolState` — и контейнер состояния, и фасад к `PoolDriver`. Стоит ли выделить driver delegation в отдельный объект?
2. **Concurrency:** Condition-ы (`_master_cond`, `_replica_cond`) правильно используют acquire/notify паттерн?
3. **Backward compat:** Все ли публичные API из старого `base.py` доступны?

### Известные замечания

- Тесты часто обращаются к `_pool_state.*` вместо proxy-методов на `BasePoolManager` — стоит поправить
- `PoolState.ready()` содержит unreachable ветку `if replicas_count is None` (строка ~250)

---

## PR #39 — refactor(drivers): extract drivers into hasql/driver/ package

**Ветка:** `refactor/4-drivers` → `refactor/3-core`
**Объём:** +830/-615 (19 файлов)
**Статус:** ruff — 1 ошибка (та же, исправлена в PR #31), 163 теста проходят

### Что смотреть

| Файл | На что обратить внимание |
|------|-------------------------|
| `hasql/driver/aiopg.py` | Эталонный драйвер — смотреть первым, остальные по аналогии. |
| `hasql/driver/asyncpg.py` | Ручной парсер версий вместо `packaging`. `cached_hosts: ClassVar` — memory leak при пересоздании пулов. |
| `hasql/driver/psycopg3.py` | `Psycopg3AcquireContext` — renamed из `PoolAcquireContext`, алиас в shim **отсутствует**. |
| `hasql/driver/asyncsqlalchemy.py` | Самый сложный (151 строка). `_AsyncGeneratorContextManager` — приватный API `contextlib`. |
| `hasql/aiopg.py` и др. shim-ы | 4-9 строк re-export. Все ли имена проброшены? |

### Ключевые вопросы

1. **Консистентность `is_master` при `None` row:** только psycopg3 кидает `UnexpectedDatabaseResponseError`. Остальные — `TypeError` или неверный результат. Унифицировать?
2. **`AiopgSaDriver(AiopgDriver)`** — наследование vs composition (гайдлайн проекта — composition). Допустимо для driver specialization?
3. **Deprecation warnings** в shim-ах отсутствуют — стоит ли добавить?

---

## PR #31 — feat(staleness): add replication lag detection with tiered fallback

**Ветка:** `mr/2-staleness` → `refactor/4-drivers`
**Объём:** +734/-10 (9 файлов)
**Статус:** ruff OK, 191 тест проходит

### Что смотреть

| Файл | На что обратить внимание |
|------|-------------------------|
| `hasql/staleness.py` | `BaseStalenessChecker` ABC, `BytesStalenessChecker` (WAL byte lag), `TimeStalenessChecker` (replay timestamp), `StalenessPolicy` (grace period). |
| `hasql/pool_state.py` (+82) | `_stale_pool_set`, `check_replica_staleness`, `collect_master_state`, `mark_pool_stale`. Переходы stale↔replica. |
| `hasql/health.py` | `_full_pool_check` — оркестрация role refresh + staleness check. **Живёт в PoolHealthMonitor** (не в manager). |
| `hasql/balancer_policy/base.py` | Tiered fallback: fresh replicas → master → stale replicas → wait. |
| `hasql/metrics.py` | `PoolStaleness` enum, `staleness`/`lag` поля, `stale_count` gauge. |
| `tests/test_staleness.py` | 28 unit-тестов на checkers и policy. |
| `tests/test_staleness_integration.py` | Интеграция с `TestPoolManager`. |

### Ключевые вопросы

1. **SQL injection:** `BytesStalenessChecker.check()` интерполирует `_master_lsn` через f-string. Есть regex-валидация `_WAL_LSN_PATTERN`, но нет параметризованных запросов. Достаточно ли?
2. **`StalenessCheckResult.lag: dict[str, Any]`** — mutable dict внутри frozen dataclass. Контракт на неизменяемость нарушен. Рассмотреть `MappingProxyType`?
3. **Tiered fallback при `fallback_master=False`:** stale replicas приоритетнее ожидания мастера. Это осознанное решение?
4. **Race window:** между `refresh_pool_role` (помечает как replica) и `check_replica_staleness` пул кратковременно в replica_set, хотя может быть stale.
5. **Grace period cleanup:** `clear_sets()` не чистит `_staleness._last_fresh_at`.

---

## PR #32 — docs: add migration guide, architecture docs, and OTLP examples

**Ветка:** `mr/3-docs` → `mr/2-staleness`
**Объём:** docs only (не меняет hasql/ или tests/)
**Статус:** ruff OK, 191 тест проходит

### Что смотреть

| Файл | На что обратить внимание |
|------|-------------------------|
| `docs/migration-0.9.0-to-0.10.0.md` | Гайд миграции. **Исправлен:** shim-ы существуют, proxy-методы сохранены, типы обновлены. |
| `docs/class-scheme.md` | Архитектурная диаграмма классов. Соответствие коду. |
| `docs/metrics-dashboard-example.md` | Grafana/OTLP setup с PromQL и alerting rules. |
| `docs/types.md` | Документация типовой системы. |
| `example/otlp/*.py` | **Исправлены:** `pool.ready()` вместо `pool.pool_state.ready()`. |
| `README.rst` | Обновлённая структура модулей. Проверить что import paths в Overview section актуальны. |
| `CLAUDE.md` | Архитектурные детали — проверить соответствие коду. |

### Известные замечания

- README Overview (строки ~817-827) всё ещё использует старые import paths — стоит обновить на `hasql.driver.*`
- `docs/timeouts.md` ссылается на удалённый `_prepare_acquire_kwargs`
- `CLAUDE.md` упоминает `PoolHealthMonitor` использует `manager._pool_state` — **неверно** после исправления, теперь чистый DI

---

## Матрица валидации

| Ветка | ruff | Тесты | Примечание |
|-------|------|-------|------------|
| `refactor/1-foundations` | OK | 98 pass | — |
| `refactor/2-contracts` | OK | 100 pass, 18 fail | Падения — тесты зависят от mock-ов из PR #38 |
| `refactor/3-core` | 1 E501 | 163 pass | Line length fix в PR #31 |
| `refactor/4-drivers` | 1 E501 | 163 pass | Та же ошибка, fix в PR #31 |
| `mr/2-staleness` | OK | 191 pass | — |
| `mr/3-docs` | OK | 191 pass | — |

**Тесты, требующие PostgreSQL** (`test_asyncsqlalchemy`, `test_metrics`, `test_timeout_handling`, `test_trouble`) и **libpq** (`test_exceptions` с psycopg3) — исключены, это pre-existing ограничения окружения.

---

## Рекомендация по порядку merge

1. **PR #36** — merge в master (чистые additions)
2. **PR #37** — merge в refactor/1 (после #36 merge в master, перебазировать на master)
3. **PR #38** — merge в refactor/2
4. **PR #39** — merge в refactor/3
5. **PR #31** — merge в refactor/4
6. **PR #32** — merge в mr/2-staleness

Либо: merge всей цепочки последовательно, каскадно обновляя базовые ветки.
