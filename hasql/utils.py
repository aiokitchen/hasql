import io
import re
import statistics
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from typing import (
    Any, DefaultDict, Deque, Dict, Generator, Iterable, List, Optional, Tuple,
    Union,
)
from urllib.parse import unquote, urlencode


def host_is_ipv6_address(netloc: str) -> bool:
    return netloc.count(":") > 1


class Dsn:
    __slots__ = (
        "_netloc", "_user", "_password", "_dbname", "_kwargs",
        "_scheme", "_compiled_dsn",
    )

    URL_EXP = re.compile(
        r"^(?P<scheme>[^\:]+):\/\/"
        r"((((?P<user>[^:^@]+))?"
        r"((\:(?P<password>[^@]+)?))?\@)?"
        r"(?P<netloc>([^\/^\?]+|\[([^\/]+)\])))?"
        r"(((?P<path>\/[^\?]*)?"
        r"(\?(?P<query>[^\#]+)?)?"
        r"(\#(?P<fragment>.*))?)?)?$",
    )

    def __init__(
        self,
        netloc: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        scheme: str = "postgresql",
        **kwargs: Any,
    ):
        self._netloc = netloc
        self._user = user
        self._password = password
        self._dbname = dbname
        self._kwargs = kwargs
        self._scheme = scheme
        self._compiled_dsn = self._compile_dsn()

    @classmethod
    def parse(cls, dsn: str) -> "Dsn":
        match = cls.URL_EXP.match(dsn)

        if match is None:
            raise ValueError("Bad DSN")

        groupdict = match.groupdict()
        scheme = groupdict["scheme"]
        user = groupdict.get("user")
        password = groupdict.get("password")
        netloc: str = groupdict["netloc"]
        dbname = (groupdict.get("path") or "").lstrip("/")
        query = groupdict.get("query") or ""

        params = {}
        for item in query.split("&"):
            if not item:
                continue
            key, value = item.split("=", 1)
            params[key] = unquote(value)

        return cls(
            scheme=scheme,
            netloc=netloc,
            user=user,
            password=password,
            dbname=dbname,
            **params
        )

    def _compile_dsn(self) -> str:
        with io.StringIO() as fp:
            fp.write(self._scheme)
            fp.write("://")

            if self._user is not None:
                fp.write(self._user)

            if self._password is not None:
                fp.write(":")
                fp.write(self._password)

            if self._user is not None or self._password is not None:
                fp.write("@")

            fp.write(self._netloc)

            if self._dbname is not None:
                fp.write("/")
                fp.write(self._dbname)

            if self._kwargs:
                fp.write("?")
                fp.write(urlencode(self._kwargs, safe="/~.\"'"))

            return fp.getvalue()

    def with_(
        self,
        netloc: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
    ) -> "Dsn":
        params = {
            "netloc": netloc if netloc is not None else self._netloc,
            "user": user if user is not None else self._user,
            "password": password if password is not None else self._password,
            "dbname": dbname if dbname is not None else self._dbname,
            **self._kwargs,
        }
        return self.__class__(**params)

    def __str__(self) -> str:
        return self._compiled_dsn

    def __eq__(self, other: Any) -> bool:
        return str(self) == str(other)

    def __hash__(self) -> int:
        return hash(str(self))

    @property
    def netloc(self) -> str:
        return self._netloc

    @property
    def user(self) -> Optional[str]:
        return self._user

    @property
    def password(self) -> Optional[str]:
        return self._password

    @property
    def dbname(self) -> Optional[str]:
        return self._dbname

    @property
    def params(self) -> Dict[str, str]:
        return self._kwargs

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def compiled_dsn(self) -> str:
        return self._compiled_dsn


def split_dsn(dsn: Union[Dsn, str], default_port: int = 5432) -> List[Dsn]:
    if not isinstance(dsn, Dsn):
        dsn = Dsn.parse(dsn)

    host_port_pairs: List[Tuple[str, Optional[int]]] = []
    port_count = 0
    port: Optional[int]
    for host in dsn.netloc.split(","):
        if ":" in host:
            host, port_str = host.rsplit(":", 1)
            port = int(port_str)
            port_count += 1
        else:
            host = host
            port = None
        host_port_pairs.append((host, port))

    def deduplicate(dsns: Iterable[Dsn]) -> List[Dsn]:
        cache = set()
        result = []
        for dsn in dsns:
            if dsn in cache:
                continue
            result.append(dsn)
            cache.add(dsn)
        return result

    if port_count == len(host_port_pairs):
        return deduplicate(
            dsn.with_(netloc=f"{host}:{port}")
            for host, port in host_port_pairs
        )

    if port_count == 1 and host_port_pairs[-1][1] is not None:
        port = host_port_pairs[-1][1]
        return deduplicate(
            dsn.with_(netloc=f"{host}:{port}")
            for host, _ in host_port_pairs
        )

    return deduplicate(
        dsn.with_(netloc=f"{host}:{port or default_port}")
        for host, port in host_port_pairs
    )


class Stopwatch:
    def __init__(self, window_size: int):
        self._times: DefaultDict[Any, Deque] = defaultdict(
            lambda: deque(maxlen=window_size),
        )
        self._cache: Dict[Any, Optional[int]] = {}

    def get_time(self, obj: Any) -> Optional[float]:
        if obj not in self._times:
            return None
        if self._cache.get(obj) is None:
            self._cache[obj] = statistics.median(self._times[obj])
        return self._cache[obj]

    @contextmanager
    def __call__(self, obj: Any) -> Generator[None, None, None]:
        start_at = time.monotonic()
        yield
        self._times[obj].append(time.monotonic() - start_at)
        self._cache[obj] = None


__all__ = ("Dsn", "split_dsn", "Stopwatch", "host_is_ipv6_address")
