__version__ = "0.5.0"

package_info = (
    "hasql is a module for acquiring actual connections "
    "with masters and replicas"
)

authors = (
    ("Vladislav Bakaev", "vlad@bakaev.tech"),
    ("Dmitry Orlov", "me@mosquito.su"),
)

authors_email = ", ".join(email for _, email in authors)

__license__ = "Apache 2"
__author__ = ", ".join(f"{name} <{email}>" for name, email in authors)

__maintainer__ = __author__

__all__ = (
    "__author__",
    "__license__",
    "__maintainer__",
    "__version__",
)
