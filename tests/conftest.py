import sys
from pathlib import Path
from typing import AsyncGenerator, Optional
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.settings import (
    PREFECT_API_DATABASE_CONNECTION_URL,
    PREFECT_HOME,
    temporary_settings,
)
from prefect.testing.utilities import prefect_test_harness
from sqlalchemy.dialects.postgresql.asyncpg import dialect as postgres_dialect


@pytest.fixture(scope="session", autouse=True)
async def generate_test_database_connection_url(
    worker_id: str,
) -> AsyncGenerator[Optional[str], None]:
    """Prepares an alternative test database URL, if necessary, for the current
    connection URL.

    For databases without a server (i.e. SQLite), produces `None`, indicating we should
    just use the currently configured value.

    For databases with a server (i.e. Postgres), creates an additional database on the
    server for each test worker, using the provided connection URL as the starting
    point.  Requires that the given database user has permission to connect to the
    server and create new databases."""
    original_url = PREFECT_API_DATABASE_CONNECTION_URL.value()
    if not original_url:
        yield None
        return

    print(f"Generating test database connection URL from {original_url!r}")
    scheme, netloc, database, query, fragment = urlsplit(original_url)
    if scheme == "sqlite+aiosqlite":
        # SQLite databases will be scoped by the PREFECT_HOME setting, which will
        # be in an isolated temporary directory
        test_db_path = Path(PREFECT_HOME.value()) / f"prefect_{worker_id}.db"
        yield f"sqlite+aiosqlite:///{test_db_path}"
        return

    elif scheme == "postgresql+asyncpg":
        test_db_name = database.strip("/") + f"_tests_{worker_id}"
        quoted_db_name = postgres_dialect().identifier_preparer.quote(test_db_name)

        postgres_url = urlunsplit(("postgres", netloc, "postgres", query, fragment))

        # Create an empty temporary database for use in the tests

        print(f"Connecting to postgres at {postgres_url!r}")
        connection = await asyncpg.connect(postgres_url)
        try:
            print(f"Creating test postgres database {quoted_db_name!r}")
            # remove any connections to the test database. For example if a SQL IDE
            # is being used to investigate it, it will block the drop database command.
            await connection.execute(
                f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{quoted_db_name}'
                AND pid <> pg_backend_pid();
                """
            )

            await connection.execute(f"DROP DATABASE IF EXISTS {quoted_db_name}")
            await connection.execute(f"CREATE DATABASE {quoted_db_name}")
        finally:
            await connection.close()

        new_url = urlunsplit((scheme, netloc, test_db_name, query, fragment))

        print(f"Using test database connection URL {new_url!r}")
        yield new_url

        print("Cleaning up test postgres database")
        # Now drop the temporary database we created
        connection = await asyncpg.connect(postgres_url)
        try:
            await connection.execute(f"DROP DATABASE IF EXISTS {quoted_db_name}")
        except asyncpg.exceptions.ObjectInUseError:
            # If we aren't able to drop the database because there's still a connection,
            # open, that's okay.  If we're in CI, then this DB is going away permanently
            # anyway, and if we're testing locally, in the beginning of this fixture,
            # we drop the database prior to creating it.  The worst case is that we
            # leave a DB catalog lying around on your local Postgres, which will get
            # cleaned up before the next test suite run.
            pass
        finally:
            await connection.close()
    else:
        raise ValueError(
            f"Unknown scheme {scheme!r} parsed from database url {original_url!r}."
        )


@pytest.fixture(scope="session", autouse=True)
def test_database_connection_url(generate_test_database_connection_url):
    """
    Update the setting for the database connection url to the generated value from
    `generate_test_database_connection_url`

    This _must_ be separate from the generation of the test url because async fixtures
    are run in a separate context from the test suite.
    """
    url = generate_test_database_connection_url
    if url is None:
        yield None
    else:
        with temporary_settings({PREFECT_API_DATABASE_CONNECTION_URL: url}):
            yield url


@pytest.fixture(autouse=True)
def reset_sys_modules():
    import importlib

    original_modules = sys.modules.copy()

    # Workaround for weird behavior on Linux where some of our "expected
    # failure" tests succeed because '.' is in the path.
    if sys.platform == "linux" and "." in sys.path:
        sys.path.remove(".")

    yield

    # Delete all of the module objects that were introduced so they are not
    # cached.
    for module in set(sys.modules.keys()):
        if module not in original_modules:
            del sys.modules[module]

    importlib.invalidate_caches()


@pytest.fixture(autouse=True, scope="module")
def leaves_no_extraneous_files():
    """This fixture will fail a test if it seems to have left new files or directories
    in the root of the local working tree.  For performance, it only checks for changes
    at the test module level, but that should generally be enough to narrow down what
    is happening.  If you're having trouble isolating the problematic test, you can
    switch it to scope="function" temporarily.  It may also help to run the test suite
    with one process (-n0) so that unrelated tests won't fail."""
    before = set(Path(".").iterdir())
    yield
    after = set(Path(".").iterdir())
    new_files = after - before

    ignored_file_prefixes = {".coverage"}

    new_files = {
        f
        for f in new_files
        if not any(f.name.startswith(prefix) for prefix in ignored_file_prefixes)
    }

    if new_files:
        raise AssertionError(
            "One of the tests in this module left new files in the "
            f"working directory: {new_files}"
        )


@pytest.fixture(autouse=True)  # for all tests in the test suite
def prefect_db():
    with prefect_test_harness():
        """sets up a temp sandbox prefect database/server for running tests against"""
        yield


@pytest.fixture
async def prefect_client(
    test_database_connection_url: str,
) -> AsyncGenerator[PrefectClient, None]:
    async with get_client() as client:
        yield client
