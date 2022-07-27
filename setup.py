import os
from importlib.machinery import SourceFileLoader

from setuptools import setup, find_packages

module = SourceFileLoader(
    "version", os.path.join("hasql", "__init__.py")
).load_module()

setup(
    name="hasql",
    version=module.__version__,
    author=module.__author__,
    author_email=module.authors_email,
    license=module.__license__,
    description=module.package_info,
    long_description=open("README.md").read(),
    platforms="all",
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: Russian",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Microsoft",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    packages=find_packages(exclude=["tests", "example"]),
    install_requires=[
        "async_timeout",
        "psycopg2-binary",
    ],
    extras_require={
        "aiopg": [
            "aiopg"
        ],
        "aiopg_sa": [
            "aiopg[sa]"
        ],
        "asyncpg": [
            "asyncpg"
        ],
        "asyncpgsa": [
            "asyncpgsa"
        ],
        "psycopg": [
            "psycopg"
        ],
        "test": [
            "psycopg[binary,pool]==3.0",
            "aiopg[sa]~=1.3.2",
            "asyncpg~=0.24.0",
            "pytest~=6.2.5",
            "pytest-cov~=3.0.0",
            "aiomisc~=15.2.4",
            "mock~=4.0.1",
            "sqlalchemy[asyncio]~=1.4.27",
        ],
        "develop": [
            "psycopg[binary,pool]==3.0",
            "aiopg[sa]~=1.3.2",
            "asyncpg~=0.24.0",
            "pytest~=6.2.5",
            "pytest-cov~=3.0.0",
            "pylama~=7.7.1",
            "aiomisc~=15.2.4",
            "mock~=4.0.3",
            "sqlalchemy[asyncio]~=1.4.27",
            "black~=21.9b0",
            "tox~=3.24",
            "twine",
            "wheel",
        ],
    },
)
