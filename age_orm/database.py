"""Database connection and pool management for Apache AGE."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from psycopg import Connection
from psycopg_pool import ConnectionPool, AsyncConnectionPool

from age_orm.exceptions import GraphNotFoundError, GraphExistsError

if TYPE_CHECKING:
    from age_orm.graph import Graph, AsyncGraph

log = logging.getLogger(__name__)


def _configure_age_connection(conn: Connection) -> None:
    """Configure a connection for AGE: load extension and set search path.

    Uses autocommit to avoid leaving the connection in INTRANS state,
    which would cause psycopg_pool to discard the connection.
    """
    conn.autocommit = True
    conn.execute("LOAD 'age'")
    conn.execute('SET search_path = ag_catalog, "$user", public')
    conn.autocommit = False


class Database:
    """Synchronous database connection manager for Apache AGE.

    Manages a connection pool and provides graph-level operations.

    Usage:
        db = Database("postgresql://user:pass@localhost:5433/agedb")
        graph = db.graph("my_graph", create=True)
        # ... use graph ...
        db.close()
    """

    def __init__(self, dsn: str, **pool_kwargs):
        self._dsn = dsn
        pool_kwargs.setdefault("min_size", 1)
        pool_kwargs.setdefault("max_size", 10)
        self._pool = ConnectionPool(
            dsn,
            configure=_configure_age_connection,
            **pool_kwargs,
        )

    def graph(self, name: str, create: bool = False) -> "Graph":
        """Get a Graph handle for the named graph.

        Args:
            name: The graph name.
            create: If True, create the graph if it doesn't exist.
        """
        from age_orm.graph import Graph

        if create and not self.graph_exists(name):
            return self.create_graph(name)

        if not self.graph_exists(name):
            raise GraphNotFoundError(f"Graph '{name}' does not exist")

        return Graph(name=name, db=self)

    def create_graph(self, name: str) -> "Graph":
        """Create a new graph and return a Graph handle."""
        from age_orm.graph import Graph

        if self.graph_exists(name):
            raise GraphExistsError(f"Graph '{name}' already exists")

        with self._pool.connection() as conn:
            conn.execute("SELECT create_graph(%s)", (name,))
        log.info("Created graph: %s", name)
        return Graph(name=name, db=self)

    def drop_graph(self, name: str, cascade: bool = True) -> None:
        """Drop a graph."""
        if not self.graph_exists(name):
            raise GraphNotFoundError(f"Graph '{name}' does not exist")

        with self._pool.connection() as conn:
            conn.execute("SELECT drop_graph(%s, %s)", (name, cascade))
        log.info("Dropped graph: %s", name)

    def graph_exists(self, name: str) -> bool:
        """Check if a graph exists."""
        with self._pool.connection() as conn:
            result = conn.execute(
                "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s", (name,)
            ).fetchone()
        return result is not None

    def list_graphs(self) -> list[str]:
        """List all graph names."""
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT name FROM ag_catalog.ag_graph"
            ).fetchall()
        return [row[0] for row in rows]

    def close(self) -> None:
        """Close the connection pool."""
        self._pool.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *args) -> None:
        self.close()


class AsyncDatabase:
    """Asynchronous database connection manager for Apache AGE.

    Usage:
        async with AsyncDatabase("postgresql://...") as db:
            graph = await db.graph("my_graph", create=True)
    """

    def __init__(self, dsn: str, **pool_kwargs):
        self._dsn = dsn
        pool_kwargs.setdefault("min_size", 1)
        pool_kwargs.setdefault("max_size", 10)
        self._pool = AsyncConnectionPool(
            dsn,
            configure=self._configure_connection,
            **pool_kwargs,
        )

    @staticmethod
    async def _configure_connection(conn) -> None:
        """Configure a connection for AGE (async version)."""
        await conn.set_autocommit(True)
        await conn.execute("LOAD 'age'")
        await conn.execute('SET search_path = ag_catalog, "$user", public')
        await conn.set_autocommit(False)

    async def graph(self, name: str, create: bool = False) -> "AsyncGraph":
        """Get an AsyncGraph handle for the named graph."""
        from age_orm.graph import AsyncGraph

        if create and not await self.graph_exists(name):
            return await self.create_graph(name)

        if not await self.graph_exists(name):
            raise GraphNotFoundError(f"Graph '{name}' does not exist")

        return AsyncGraph(name=name, db=self)

    async def create_graph(self, name: str) -> "AsyncGraph":
        """Create a new graph and return an AsyncGraph handle."""
        from age_orm.graph import AsyncGraph

        if await self.graph_exists(name):
            raise GraphExistsError(f"Graph '{name}' already exists")

        async with self._pool.connection() as conn:
            await conn.execute("SELECT create_graph(%s)", (name,))
        log.info("Created graph: %s", name)
        return AsyncGraph(name=name, db=self)

    async def drop_graph(self, name: str, cascade: bool = True) -> None:
        """Drop a graph."""
        if not await self.graph_exists(name):
            raise GraphNotFoundError(f"Graph '{name}' does not exist")

        async with self._pool.connection() as conn:
            await conn.execute("SELECT drop_graph(%s, %s)", (name, cascade))
        log.info("Dropped graph: %s", name)

    async def graph_exists(self, name: str) -> bool:
        """Check if a graph exists."""
        async with self._pool.connection() as conn:
            result = await conn.execute(
                "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s", (name,)
            )
            row = await result.fetchone()
        return row is not None

    async def list_graphs(self) -> list[str]:
        """List all graph names."""
        async with self._pool.connection() as conn:
            result = await conn.execute("SELECT name FROM ag_catalog.ag_graph")
            rows = await result.fetchall()
        return [row[0] for row in rows]

    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()

    async def __aenter__(self) -> "AsyncDatabase":
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
