import asyncio
from collections.abc import Generator
from types import TracebackType
from typing import Any, Generic, Protocol, TypeVar

ConnT = TypeVar("ConnT")
ConnT_co = TypeVar("ConnT_co", covariant=True)


class AcquireContext(Protocol[ConnT_co]):
    async def __aenter__(self) -> ConnT_co: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None: ...
    def __await__(self) -> Generator[Any, None, ConnT_co]: ...


class TimeoutAcquireContext(Generic[ConnT]):
    __slots__ = ("_context", "_timeout")

    def __init__(self, context: AcquireContext[ConnT], timeout: float):
        self._context = context
        self._timeout = timeout

    async def __aenter__(self) -> ConnT:
        return await asyncio.wait_for(
            self._context.__aenter__(),
            timeout=self._timeout,
        )

    async def __aexit__(self, *exc):
        # TODO: consider adding a bounded timeout here. Currently if the
        #  underlying driver hangs during connection release this will block
        #  indefinitely. A timeout risks leaking the connection (not returned
        #  to pool), so this needs careful design.
        return await self._context.__aexit__(*exc)

    def __await__(self) -> Generator[Any, None, ConnT]:
        return asyncio.wait_for(
            self._context.__aenter__(),
            timeout=self._timeout,
        ).__await__()


__all__ = (
    "AcquireContext",
    "TimeoutAcquireContext",
)
