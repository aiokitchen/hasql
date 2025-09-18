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
    long_description=open("README.rst").read(),

    platforms="all",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: Russian",
        "Operating System :: MacOS",
        "Operating System :: Microsoft",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python",
    ],
    packages=find_packages(exclude=["tests", "example"]),
    package_data={'hasql': ['py.typed']},
    install_requires=[],
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
            "psycopg[pool]>=3,<4"
        ],
        "test": [
            "async_timeout",
            "psycopg[pool]>=3.0,<4",
            "aiopg[sa]~=1.4.0",
            "asyncpg~=0.30.0",
            "pytest~=8.2.0",
            "pytest-cov~=3.0.0",
            "pytest-asyncio~=0.26.0",
            "aiomisc~=17.7.7",
            "aiomisc-pytest~=1.2.1",
            "mock~=4.0.1",
            "sqlalchemy[asyncio]~=1.4.27",
        ],
        "develop": [
            "async_timeout",
            "psycopg[pool]>=3.0,<4",
            "aiopg[sa]~=1.4.0",
            "asyncpg~=0.30.0",
            "pytest~=8.2.0",
            "pytest-cov~=3.0.0",
            "pytest-asyncio~=0.26.0",
            "aiomisc~=17.7.7",
            "aiomisc-pytest~=1.2.1",
            "mock~=4.0.3",
            "sqlalchemy[asyncio]~=1.4.27",
            "black~=21.9b0",
            "tox~=3.24",
            "ruff~=0.11.9",
            "twine",
            "wheel",
            "types-psycopg2",
        ],
    },
    project_urls={
        "Source": "https://github.com/aiokitchen/hasql",
        "Tracker": "https://github.com/aiokitchen/hasql/issues",
        "Documentation": "https://github.com/aiokitchen/hasql/blob/master/README.rst",
    },
)
