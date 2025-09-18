from io import StringIO
from typing import Iterable, Optional, Union

import pytest

from hasql.utils import Dsn, host_is_ipv6_address, split_dsn


FORMAT_DSN_TEST_CASES = [
    [
        "localhost",
        5432,
        None,
        None,
        None,
        "postgresql://localhost:5432",
    ],
    [
        "localhost",
        5432,
        "user",
        None,
        None,
        "postgresql://user@localhost:5432",
    ],
    [
        "localhost",
        5432,
        "user",
        "pwd",
        None,
        "postgresql://user:pwd@localhost:5432",
    ],
    [
        "localhost",
        5432,
        None,
        None,
        "testdb",
        "postgresql://localhost:5432/testdb",
    ],
    [
        "localhost",
        "5432",
        "user",
        None,
        "testdb",
        "postgresql://user@localhost:5432/testdb",
    ],
    [
        "localhost",
        5432,
        "user",
        "pwd",
        "testdb",
        "postgresql://user:pwd@localhost:5432/testdb",
    ],
]


@pytest.mark.parametrize(
    ["host", "port", "user", "password", "dbname", "expected_result"],
    FORMAT_DSN_TEST_CASES,
)
def test_format_dsn(
    host: str,
    port: Union[str, int],
    user: Optional[str],
    password: Optional[str],
    dbname: Optional[str],
    expected_result: str,
):
    result_dsn = Dsn(
        netloc=f"{host}:{port}",
        user=user,
        password=password,
        dbname=dbname,
    )
    assert str(result_dsn) == expected_result


def build_url(*, host, user="", password="", dbname="test"):
    with StringIO() as fp:
        fp.write("postgresql://")

        if user:
            fp.write(user)
        if password:
            fp.write(":")
            fp.write(password)
        if user or password:
            fp.write("@")
        fp.write(host)
        if dbname:
            fp.write("/")
            fp.write(dbname)

        return fp.getvalue()


def make_examples():
    cases = [
        dict(),
        dict(user="test"),
        dict(password="secret"),
        dict(user="test", password="secret"),
    ]

    hosts_cases = [
        ["host1,host2", ["host1:5432", "host2:5432"]],
        ["host1:6432,host2", ["host1:6432", "host2:5432"]],
        ["host1,host2:6432", ["host1:6432", "host2:6432"]],
        ["host1,host2,host3", ["host1:5432", "host2:5432", "host3:5432"]],
        ["host1:6432,host2,host3", ["host1:6432", "host2:5432", "host3:5432"]],
        ["host1,host2:6432,host3", ["host1:5432", "host2:6432", "host3:5432"]],
        ["host1,host2,host3:6432", ["host1:6432", "host2:6432", "host3:6432"]],
    ]

    for case in cases:
        for (hosts, expected) in hosts_cases:
            yield [
                build_url(host=hosts, **case),
                [build_url(host=host, **case) for host in expected],
            ]


MULTI_DSN_PORT_CASES = list(make_examples())


@pytest.mark.parametrize(
    ["dsn", "expected_dsns"],
    MULTI_DSN_PORT_CASES,
    ids=[dsn for dsn, _ in MULTI_DSN_PORT_CASES],
)
def test_multi_dsn_port(dsn: str, expected_dsns: Iterable[str]):
    for host_dsn, expected in zip(split_dsn(Dsn.parse(dsn)), expected_dsns):
        assert str(host_dsn) == expected


def test_replace_dsn_params():
    dsn = Dsn(
        netloc="localhost:5432",
        user="user",
        password="password",
        dbname="testdb",
    )
    replaced_dsn = dsn.with_(password="***")
    assert str(replaced_dsn) == "postgresql://user:***@localhost:5432/testdb"


def test_split_single_host_dsn():
    source_dsn = "postgresql://user:pwd@localhost:5432/testdb"
    result_dsn = split_dsn(source_dsn)
    assert len(result_dsn) == 1
    assert str(result_dsn[0]) == source_dsn


def test_split_single_host_dsn_without_port():
    source_dsn = "postgresql://user:pwd@localhost/testdb"
    result_dsn = split_dsn(source_dsn, default_port=1)
    assert len(result_dsn) == 1
    assert str(result_dsn[0]) == "postgresql://user:pwd@localhost:1/testdb"


def test_split_multi_host_dsn():
    hosts = ",".join(["master:5432", "replica:5432", "replica:6432"])
    source_dsn = f"postgresql://user:pwd@{hosts}/testdb"
    result_dsn = split_dsn(source_dsn)
    assert len(result_dsn) == 3
    master_dsn, fst_replica_dsn, snd_replica_dsn = result_dsn
    assert str(master_dsn) == "postgresql://user:pwd@master:5432/testdb"
    assert str(fst_replica_dsn) == "postgresql://user:pwd@replica:5432/testdb"
    assert str(snd_replica_dsn) == "postgresql://user:pwd@replica:6432/testdb"


def test_split_dsn_skip_same_addreses():
    source_dsn = "postgresql://user:pwd@localhost:5432,localhost:5432/testdb"
    result_dsn = split_dsn(source_dsn)
    assert len(result_dsn) == 1
    assert str(result_dsn[0]) == "postgresql://user:pwd@localhost:5432/testdb"


def test_split_dsn_with_default_port():
    source_dsn = "postgresql://user:pwd@master:6432,replica/testdb"
    result_dsn = split_dsn(source_dsn, default_port=15432)
    assert len(result_dsn) == 2
    master_dsn, replica_dsn = result_dsn
    assert str(master_dsn) == "postgresql://user:pwd@master:6432/testdb"
    assert str(replica_dsn) == "postgresql://user:pwd@replica:15432/testdb"


@pytest.mark.parametrize(
    ["hosts_count"],
    [[1024]],
)
def test_split_large_dsn(hosts_count: int):
    hosts = [f"host-{i}" for i in range(hosts_count)]
    large_dsn = "postgresql://user:pwd@" + ",".join(hosts) + "/testdb"
    result_dsn = split_dsn(large_dsn, default_port=5432)
    for i, dsn in enumerate(result_dsn):
        assert str(dsn) == f"postgresql://user:pwd@host-{i}:5432/testdb"


def test_split_dsn_with_params():
    dsn = (
        "postgresql://user:password@master:5432,replica:5432/testdb?"
        "sslmode=verify-full&sslcert=/root/.postgresql/aa/postgresql.crt&"
        "sslkey=/root/.postgresql/aa/postgresql.key"
    )
    expected_master_dsn = (
        "postgresql://user:password@master:5432/testdb?"
        "sslmode=verify-full&sslcert=/root/.postgresql/aa/postgresql.crt&"
        "sslkey=/root/.postgresql/aa/postgresql.key"
    )
    expected_replica_dsn = (
        "postgresql://user:password@replica:5432/testdb?"
        "sslmode=verify-full&sslcert=/root/.postgresql/aa/postgresql.crt&"
        "sslkey=/root/.postgresql/aa/postgresql.key"
    )
    master_dsn, replica_dsn = split_dsn(dsn)
    assert str(master_dsn) == expected_master_dsn
    assert str(replica_dsn) == expected_replica_dsn


def test_replace_dsn_part():
    dsn = "postgresql://user:password@localhost:5432/testdb"
    expected_dsn = "postgresql://user:***@localhost:5432/testdb"
    result_dsn, *_ = split_dsn(dsn)
    dsn_with_hidden_password = result_dsn.with_(password="***")
    assert str(dsn_with_hidden_password) == expected_dsn


@pytest.mark.parametrize(
    ["host", "expected_result"],
    [
        ["yandex.ru", False],
        ["127.0.0.1", False],
        ["2001:DB8:3C4D:7777:260:3EFF:FE15:9501", True],
        ["2001:dead:beef::1", True],
    ],
)
def test_host_is_ipv6_address(host: str, expected_result: bool):
    result = host_is_ipv6_address(host)
    assert result == expected_result


def test_ipv6_host_in_dsn():
    dsn = (
        "postgresql://"
        "user:password@["
        "2001:DB8:3C4D:7777:260:3EFF:FE15:9501"
        "]:5432/testdb"
    )
    result_dsn, *_ = split_dsn(dsn)
    assert str(result_dsn) == dsn


# Connection string format tests
def test_parse_connection_string_basic():
    """Test basic connection string parsing."""
    conn_str = "host=localhost port=5432 dbname=mydb user=testuser"
    dsn = Dsn.parse(conn_str)
    assert dsn.netloc == "localhost:5432"
    assert dsn.user == "testuser"
    assert dsn.dbname == "mydb"
    assert dsn.scheme == "postgresql"


def test_parse_connection_string_multiple_hosts():
    """Test connection string with comma-separated hosts."""
    conn_str = "host=localhost,replica port=5432,5433 dbname=mydb"
    dsn = Dsn.parse(conn_str)
    assert dsn.netloc == "localhost:5432,replica:5433"
    assert dsn.dbname == "mydb"


def test_parse_connection_string_single_port_multiple_hosts():
    """Test connection string with single port for multiple hosts."""
    conn_str = "host=localhost,replica port=5432 dbname=mydb"
    dsn = Dsn.parse(conn_str)
    assert dsn.netloc == "localhost:5432,replica:5432"


def test_parse_connection_string_with_password():
    """Test connection string with password."""
    conn_str = "host=localhost port=5432 dbname=mydb user=testuser password=secret"
    dsn = Dsn.parse(conn_str)
    assert dsn.user == "testuser"
    assert dsn.password == "secret"


def test_parse_connection_string_with_extra_params():
    """Test connection string with additional parameters."""
    conn_str = "host=localhost port=5432 dbname=mydb connect_timeout=10 sslmode=require"
    dsn = Dsn.parse(conn_str)
    assert dsn.params["connect_timeout"] == "10"
    assert dsn.params["sslmode"] == "require"


def test_parse_connection_string_quoted_values():
    """Test connection string with quoted values."""
    conn_str = "host=localhost port=5432 dbname='my database' user='test user'"
    dsn = Dsn.parse(conn_str)
    assert dsn.dbname == "my database"
    assert dsn.user == "test user"


def test_split_dsn_from_connection_string():
    """Test that split_dsn works with connection string format."""
    conn_str = "host=localhost,replica port=5432,5433 dbname=mydb user=testuser"
    dsns = split_dsn(conn_str)
    assert len(dsns) == 2
    assert str(dsns[0]) == "postgresql://testuser@localhost:5432/mydb"
    assert str(dsns[1]) == "postgresql://testuser@replica:5433/mydb"


def test_connection_string_example_format():
    """Test the exact example format from the user request."""
    conn_str = "host=localhost,localhost port=5432,5432 dbname=mydb connect_timeout=10"
    dsn = Dsn.parse(conn_str)
    assert dsn.netloc == "localhost:5432,localhost:5432"
    assert dsn.dbname == "mydb"
    assert dsn.params["connect_timeout"] == "10"

    # Test that split_dsn handles duplicates correctly
    dsns = split_dsn(conn_str)
    assert len(dsns) == 1  # Should deduplicate identical host:port pairs
    assert str(dsns[0]) == "postgresql://localhost:5432/mydb?connect_timeout=10"
