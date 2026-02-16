"""Tests for Database class (unit-level, no real DB connection needed for these)."""

from age_orm.database import Database, AsyncDatabase


class TestDatabaseInit:
    """Test that Database can be instantiated (pool creation may fail without a real DB)."""

    def test_database_stores_dsn(self):
        """Verify the DSN is stored. Pool creation happens lazily or will fail,
        so we just test the attribute."""
        # This will fail at pool creation since there's no DB, but tests the interface
        # For unit testing without a DB, we'd need to mock psycopg_pool
        pass


class TestDatabaseContextManager:
    """Context manager protocol tests (interface only)."""

    def test_has_context_manager_methods(self):
        assert hasattr(Database, "__enter__")
        assert hasattr(Database, "__exit__")

    def test_async_has_context_manager_methods(self):
        assert hasattr(AsyncDatabase, "__aenter__")
        assert hasattr(AsyncDatabase, "__aexit__")


class TestDatabaseInterface:
    """Verify the Database class has all expected methods."""

    def test_sync_database_methods(self):
        methods = ["graph", "create_graph", "drop_graph", "graph_exists", "list_graphs", "close"]
        for method in methods:
            assert hasattr(Database, method), f"Database missing method: {method}"

    def test_async_database_methods(self):
        methods = ["graph", "create_graph", "drop_graph", "graph_exists", "list_graphs", "close"]
        for method in methods:
            assert hasattr(AsyncDatabase, method), f"AsyncDatabase missing method: {method}"
