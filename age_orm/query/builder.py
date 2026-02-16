"""Fluent Cypher query builder for AGE."""

from __future__ import annotations

import logging
from typing import Any, Generic, Iterator, TypeVar, TYPE_CHECKING

from age_orm.exceptions import EntityNotFoundError, MultipleResultsError
from age_orm.models.base import AgeModel
from age_orm.utils.serialization import (
    dict_to_model,
    format_cypher_value,
    substitute_cypher_params,
)

if TYPE_CHECKING:
    from age_orm.graph import Graph, AsyncGraph

log = logging.getLogger(__name__)

T = TypeVar("T", bound=AgeModel)


class Query(Generic[T]):
    """Fluent Cypher query builder for synchronous graph operations.

    Usage:
        people = graph.query(Person).filter("n.age > $min_age", min_age=20).sort("n.name").all()
        alice = graph.query(Person).filter_by(name="Alice").one()
    """

    def __init__(self, model_class: type[T], graph: "Graph"):
        self._model_class = model_class
        self._graph = graph
        self._label = getattr(model_class, "__label__", None) or model_class.__name__
        self._filters: list[dict] = []
        self._sort_columns: list[str] = []
        self._limit: int | None = None
        self._skip: int = 0
        self._return_fields: list[str] | None = None
        self._bind_vars: dict[str, Any] = {}

    def __str__(self) -> str:
        return self._build_cypher()

    def __iter__(self) -> Iterator[T]:
        return self.iterator()

    # === Filtering ===

    def filter(self, condition: str, _or: bool = False, **kwargs) -> "Query[T]":
        """Add a filter condition.

        Conditions use 'n' as the variable name for the matched node.
        Parameters are referenced with $name and provided as kwargs.

        Example:
            query.filter("n.age > $min_age AND n.name <> $excluded", min_age=20, excluded="Bob")
        """
        joiner = None
        if self._filters:
            joiner = "OR" if _or else "AND"

        self._filters.append({"condition": condition, "joiner": joiner})
        self._bind_vars.update(kwargs)
        return self

    def filter_by(self, _or: bool = False, **kwargs) -> "Query[T]":
        """Convenience filter for equality conditions.

        Example:
            query.filter_by(name="Alice", age=30)
            # Generates: WHERE n.name = 'Alice' AND n.age = 30
        """
        if not kwargs:
            return self

        conditions = [f"n.{k} = ${k}" for k in kwargs]
        condition = " AND ".join(conditions)
        if len(conditions) > 1:
            condition = f"({condition})"

        joiner = None
        if self._filters:
            joiner = "OR" if _or else "AND"

        self._filters.append({"condition": condition, "joiner": joiner})
        self._bind_vars.update(kwargs)
        return self

    # === Sorting / Limiting ===

    def sort(self, field: str) -> "Query[T]":
        """Add a sort clause.

        Use 'n.' prefix for the node variable. Append ' DESC' for descending.

        Example:
            query.sort("n.name")
            query.sort("n.age DESC")
        """
        self._sort_columns.append(field)
        return self

    def limit(self, count: int, skip: int = 0) -> "Query[T]":
        """Set limit and optional skip (offset)."""
        self._limit = count
        self._skip = skip
        return self

    def returns(self, *fields: str) -> "Query[T]":
        """Specify which fields to return (projections).

        Example:
            query.returns("n.name", "n.age")
        """
        self._return_fields = list(fields)
        return self

    # === Execution ===

    def all(self) -> list[T]:
        """Execute query and return all matching entities."""
        return list(self.iterator())

    def first(self) -> T | None:
        """Return the first matching entity, or None."""
        original_limit = self._limit
        self._limit = 1
        results = self.all()
        self._limit = original_limit
        return results[0] if results else None

    def one(self) -> T:
        """Return exactly one matching entity. Raises if not exactly one result."""
        results = self.all()
        if len(results) == 0:
            raise EntityNotFoundError(
                f"No {self._model_class.__name__} found matching query"
            )
        if len(results) > 1:
            raise MultipleResultsError(
                f"Expected 1 {self._model_class.__name__}, got {len(results)}"
            )
        return results[0]

    def count(self) -> int:
        """Return the count of matching entities."""
        cypher = self._build_match_where()
        cypher += "\nRETURN count(n)"
        results = self._graph._execute_cypher(cypher, return_type="raw")
        if results and "value" in results[0]:
            return results[0]["value"]
        return 0

    def iterator(self) -> Iterator[T]:
        """Execute query and yield results one at a time."""
        cypher = self._build_cypher()
        results = self._graph._execute_cypher(cypher, return_type="vertex")

        for r in results:
            yield dict_to_model(
                r, self._model_class, db=self._graph._db, graph=self._graph
            )

    # === Lookups ===

    def by_id(self, graph_id: int) -> T | None:
        """Look up an entity by its AGE graph ID."""
        cypher = (
            f"MATCH (n:{self._label}) WHERE id(n) = {graph_id} RETURN n"
        )
        results = self._graph._execute_cypher(cypher, return_type="vertex")
        if results:
            return dict_to_model(
                results[0], self._model_class, db=self._graph._db, graph=self._graph
            )
        return None

    def by_property(self, field: str, value: Any) -> T | None:
        """Look up an entity by a single property value."""
        cypher = (
            f"MATCH (n:{self._label}) "
            f"WHERE n.{field} = {format_cypher_value(value)} "
            f"RETURN n"
        )
        results = self._graph._execute_cypher(cypher, return_type="vertex")
        if results:
            return dict_to_model(
                results[0], self._model_class, db=self._graph._db, graph=self._graph
            )
        return None

    # === Bulk Mutations ===

    def update(self, **kwargs) -> int:
        """Update all matching entities with the given values. Returns count updated."""
        match_where = self._build_match_where()
        set_parts = ", ".join(
            f"n.{k} = {format_cypher_value(v)}" for k, v in kwargs.items()
        )
        cypher = f"{match_where}\nSET {set_parts}\nRETURN count(n)"
        results = self._graph._execute_cypher(cypher, return_type="raw")
        if results and "value" in results[0]:
            return results[0]["value"]
        return 0

    def delete(self) -> int:
        """Delete all matching entities. Returns count deleted."""
        match_where = self._build_match_where()
        cypher = f"{match_where}\nDETACH DELETE n"
        # DELETE doesn't return count in AGE, so we count first
        count = self.count()
        self._graph._execute_cypher(cypher, return_type="raw")
        return count

    # === Raw Cypher ===

    def cypher(self, statement: str, **kwargs) -> list:
        """Execute a raw Cypher query with the current collection binding."""
        return self._graph.cypher(statement, **kwargs)

    # === Internal ===

    def _build_match_where(self) -> str:
        """Build the MATCH + WHERE portion of the Cypher query."""
        cypher = f"MATCH (n:{self._label})"

        # WHERE clauses
        if self._filters:
            where_parts = []
            for fc in self._filters:
                if fc["joiner"] is None:
                    where_parts.append(fc["condition"])
                else:
                    where_parts.append(f"{fc['joiner']} {fc['condition']}")

            where_clause = " ".join(where_parts)
            # Substitute bind vars
            where_clause = substitute_cypher_params(where_clause, self._bind_vars)
            cypher += f"\nWHERE {where_clause}"

        return cypher

    def _build_cypher(self) -> str:
        """Build the full Cypher query string."""
        cypher = self._build_match_where()

        # RETURN
        if self._return_fields:
            cypher += f"\nRETURN {', '.join(self._return_fields)}"
        else:
            cypher += "\nRETURN n"

        # ORDER BY
        if self._sort_columns:
            cypher += f"\nORDER BY {', '.join(self._sort_columns)}"

        # SKIP
        if self._skip > 0:
            cypher += f"\nSKIP {self._skip}"

        # LIMIT
        if self._limit is not None:
            cypher += f"\nLIMIT {self._limit}"

        return cypher


class AsyncQuery(Generic[T]):
    """Fluent Cypher query builder for async graph operations.

    Same interface as Query but with async execution methods.
    """

    def __init__(self, model_class: type[T], graph: "AsyncGraph"):
        self._model_class = model_class
        self._graph = graph
        self._label = getattr(model_class, "__label__", None) or model_class.__name__
        self._filters: list[dict] = []
        self._sort_columns: list[str] = []
        self._limit: int | None = None
        self._skip: int = 0
        self._return_fields: list[str] | None = None
        self._bind_vars: dict[str, Any] = {}

    def __str__(self) -> str:
        return self._build_cypher()

    # === Filtering (sync — returns self) ===

    def filter(self, condition: str, _or: bool = False, **kwargs) -> "AsyncQuery[T]":
        """Add a filter condition."""
        joiner = None
        if self._filters:
            joiner = "OR" if _or else "AND"
        self._filters.append({"condition": condition, "joiner": joiner})
        self._bind_vars.update(kwargs)
        return self

    def filter_by(self, _or: bool = False, **kwargs) -> "AsyncQuery[T]":
        """Convenience filter for equality conditions."""
        if not kwargs:
            return self
        conditions = [f"n.{k} = ${k}" for k in kwargs]
        condition = " AND ".join(conditions)
        if len(conditions) > 1:
            condition = f"({condition})"
        joiner = None
        if self._filters:
            joiner = "OR" if _or else "AND"
        self._filters.append({"condition": condition, "joiner": joiner})
        self._bind_vars.update(kwargs)
        return self

    def sort(self, field: str) -> "AsyncQuery[T]":
        """Add a sort clause."""
        self._sort_columns.append(field)
        return self

    def limit(self, count: int, skip: int = 0) -> "AsyncQuery[T]":
        """Set limit and optional skip."""
        self._limit = count
        self._skip = skip
        return self

    def returns(self, *fields: str) -> "AsyncQuery[T]":
        """Specify fields to return."""
        self._return_fields = list(fields)
        return self

    # === Async Execution ===

    async def all(self) -> list[T]:
        """Execute query and return all matching entities."""
        cypher = self._build_cypher()
        results = await self._graph._execute_cypher(cypher, return_type="vertex")
        return [
            dict_to_model(r, self._model_class, db=self._graph._db, graph=self._graph)
            for r in results
        ]

    async def first(self) -> T | None:
        """Return first matching entity or None."""
        original_limit = self._limit
        self._limit = 1
        results = await self.all()
        self._limit = original_limit
        return results[0] if results else None

    async def one(self) -> T:
        """Return exactly one matching entity."""
        results = await self.all()
        if len(results) == 0:
            raise EntityNotFoundError(
                f"No {self._model_class.__name__} found matching query"
            )
        if len(results) > 1:
            raise MultipleResultsError(
                f"Expected 1 {self._model_class.__name__}, got {len(results)}"
            )
        return results[0]

    async def count(self) -> int:
        """Return the count of matching entities."""
        cypher = self._build_match_where()
        cypher += "\nRETURN count(n)"
        results = await self._graph._execute_cypher(cypher, return_type="raw")
        if results and "value" in results[0]:
            return results[0]["value"]
        return 0

    async def by_id(self, graph_id: int) -> T | None:
        """Look up by AGE graph ID."""
        cypher = f"MATCH (n:{self._label}) WHERE id(n) = {graph_id} RETURN n"
        results = await self._graph._execute_cypher(cypher, return_type="vertex")
        if results:
            return dict_to_model(
                results[0], self._model_class, db=self._graph._db, graph=self._graph
            )
        return None

    async def by_property(self, field: str, value: Any) -> T | None:
        """Look up by single property value."""
        cypher = (
            f"MATCH (n:{self._label}) "
            f"WHERE n.{field} = {format_cypher_value(value)} "
            f"RETURN n"
        )
        results = await self._graph._execute_cypher(cypher, return_type="vertex")
        if results:
            return dict_to_model(
                results[0], self._model_class, db=self._graph._db, graph=self._graph
            )
        return None

    async def update(self, **kwargs) -> int:
        """Update all matching entities."""
        match_where = self._build_match_where()
        set_parts = ", ".join(
            f"n.{k} = {format_cypher_value(v)}" for k, v in kwargs.items()
        )
        cypher = f"{match_where}\nSET {set_parts}\nRETURN count(n)"
        results = await self._graph._execute_cypher(cypher, return_type="raw")
        if results and "value" in results[0]:
            return results[0]["value"]
        return 0

    async def delete(self) -> int:
        """Delete all matching entities."""
        count = await self.count()
        match_where = self._build_match_where()
        cypher = f"{match_where}\nDETACH DELETE n"
        await self._graph._execute_cypher(cypher, return_type="raw")
        return count

    # === Internal ===

    def _build_match_where(self) -> str:
        cypher = f"MATCH (n:{self._label})"
        if self._filters:
            where_parts = []
            for fc in self._filters:
                if fc["joiner"] is None:
                    where_parts.append(fc["condition"])
                else:
                    where_parts.append(f"{fc['joiner']} {fc['condition']}")
            where_clause = " ".join(where_parts)
            where_clause = substitute_cypher_params(where_clause, self._bind_vars)
            cypher += f"\nWHERE {where_clause}"
        return cypher

    def _build_cypher(self) -> str:
        cypher = self._build_match_where()

        if self._return_fields:
            cypher += f"\nRETURN {', '.join(self._return_fields)}"
        else:
            cypher += "\nRETURN n"

        if self._sort_columns:
            cypher += f"\nORDER BY {', '.join(self._sort_columns)}"

        if self._skip > 0:
            cypher += f"\nSKIP {self._skip}"

        if self._limit is not None:
            cypher += f"\nLIMIT {self._limit}"

        return cypher
