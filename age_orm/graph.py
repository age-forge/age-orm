"""Graph class for CRUD operations, traversal, and schema management."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from age_orm.database import Database, AsyncDatabase
    from age_orm.query.builder import Query, AsyncQuery

from age_orm.event import dispatch
from age_orm.exceptions import EntityNotFoundError
from age_orm.models.base import AgeModel
from age_orm.models.vertex import Vertex
from age_orm.models.edge import Edge
from age_orm.utils.serialization import (
    dict_to_model,
    escape_sql_literal,
    format_cypher_value,
    model_to_cypher_properties,
    substitute_cypher_params,
    to_agtype_properties,
)

log = logging.getLogger(__name__)

T = TypeVar("T", bound=AgeModel)


class Graph:
    """Synchronous graph operations.

    The primary interface for all CRUD, query, and traversal operations
    on a named AGE graph.
    """

    def __init__(self, name: str, db: "Database"):
        self._name = name
        self._db = db

    @property
    def name(self) -> str:
        return self._name

    # === Cypher Execution ===

    def _execute_cypher(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        return_type: str = "vertex",
    ) -> list[dict]:
        """Execute Cypher within AGE's SQL wrapper.

        Args:
            cypher: Cypher query string with optional $param placeholders.
            params: Parameter values to substitute into the Cypher.
            columns: Column definitions for the AS clause. Defaults to single "result agtype".
            return_type: Hint for parsing results ("vertex", "edge", "scalar", "raw").

        Returns:
            List of parsed result dicts.
        """
        # Substitute parameters into Cypher
        resolved_cypher = substitute_cypher_params(cypher, params)

        # Build column clause
        if columns:
            col_clause = ", ".join(f"{c} agtype" for c in columns)
        else:
            col_clause = "result agtype"

        sql = f"SELECT * FROM cypher('{self._name}', $$ {resolved_cypher} $$) AS ({col_clause})"
        log.debug("Executing: %s", sql)

        with self._db._pool.connection() as conn:
            rows = conn.execute(sql).fetchall()

        return self._parse_results(rows, return_type, num_columns=len(columns) if columns else 1)

    def _parse_results(
        self, rows: list[tuple], return_type: str, num_columns: int = 1
    ) -> list[dict]:
        """Parse raw agtype result rows into dicts."""
        results = []
        for row in rows:
            if num_columns == 1:
                parsed = _parse_agtype_result(row[0], return_type)
                results.append(parsed)
            else:
                # Multiple columns
                parsed_row = {}
                for i, val in enumerate(row):
                    parsed_row[f"col_{i}"] = _parse_agtype_result(val, "raw")
                results.append(parsed_row)
        return results

    # === CRUD Operations ===

    def add(self, entity: Vertex) -> Vertex:
        """Add a vertex to the graph.

        The vertex must not already have a graph_id (i.e., must be new).
        """
        dispatch(entity, "pre_add", graph=self)
        self.ensure_label(type(entity))

        props = model_to_cypher_properties(entity)
        label = entity.label
        cypher = f"CREATE (n:{label} {props}) RETURN n"
        results = self._execute_cypher(cypher, return_type="vertex")

        if results:
            entity._graph_id = results[0].get("graph_id")
            entity._dirty.clear()
            entity._db = self._db
            entity._graph = self

        dispatch(entity, "post_add", graph=self)
        return entity

    def update(self, entity: Vertex | Edge, only_dirty: bool = False) -> Vertex | Edge:
        """Update an existing entity in the graph."""
        if entity.graph_id is None:
            raise EntityNotFoundError("Cannot update entity without graph_id")

        dispatch(entity, "pre_update", graph=self)

        if only_dirty:
            props = entity.dirty_fields_dump(mode="json")
        else:
            props = entity.model_dump(mode="json")

        if not props:
            return entity

        # Build SET clause
        set_parts = ", ".join(
            f"n.{k} = {format_cypher_value(v)}" for k, v in props.items()
        )
        cypher = f"MATCH (n) WHERE id(n) = {entity.graph_id} SET {set_parts} RETURN n"
        self._execute_cypher(cypher, return_type="vertex")

        entity._dirty.clear()
        dispatch(entity, "post_update", graph=self)
        return entity

    def delete(self, entity: Vertex | Edge) -> None:
        """Delete an entity from the graph (DETACH DELETE for vertices)."""
        if entity.graph_id is None:
            raise EntityNotFoundError("Cannot delete entity without graph_id")

        dispatch(entity, "pre_delete", graph=self)

        if isinstance(entity, Vertex):
            cypher = f"MATCH (n) WHERE id(n) = {entity.graph_id} DETACH DELETE n"
        else:
            cypher = f"MATCH ()-[e]->() WHERE id(e) = {entity.graph_id} DELETE e"

        self._execute_cypher(cypher, return_type="raw")
        entity._graph_id = None
        entity._db = None
        entity._graph = None

        dispatch(entity, "post_delete", graph=self)

    def connect(self, from_v: Vertex, edge: Edge, to_v: Vertex) -> Edge:
        """Create an edge between two vertices.

        Both vertices must be persisted (have graph_ids).
        """
        if from_v.graph_id is None or to_v.graph_id is None:
            raise EntityNotFoundError(
                "Both vertices must be persisted before creating an edge"
            )

        dispatch(edge, "pre_add", graph=self)
        self.ensure_label(type(edge), kind="e")

        props = model_to_cypher_properties(edge)
        label = edge.label
        cypher = (
            f"MATCH (a), (b) WHERE id(a) = {from_v.graph_id} AND id(b) = {to_v.graph_id} "
            f"CREATE (a)-[e:{label} {props}]->(b) RETURN e"
        )
        results = self._execute_cypher(cypher, return_type="edge")

        if results:
            edge._graph_id = results[0].get("graph_id")
            edge._start_id = from_v.graph_id
            edge._end_id = to_v.graph_id
            edge._dirty.clear()
            edge._db = self._db
            edge._graph = self

        dispatch(edge, "post_add", graph=self)
        return edge

    # === Bulk Operations ===

    def bulk_add(self, entities: list[Vertex], label: str | None = None) -> list[Vertex]:
        """Bulk insert vertices using direct SQL INSERT (much faster than Cypher).

        Args:
            entities: List of vertex instances to insert.
            label: Override label (defaults to entity class label).

        Returns:
            The same list with graph_ids populated.
        """
        if not entities:
            return entities

        model_class = type(entities[0])
        resolved_label = label or entities[0].label
        self.ensure_label(model_class)

        # Build batch INSERT values
        values_parts = []
        for entity in entities:
            props = entity.model_dump(mode="json")
            agtype_str = to_agtype_properties(props)
            agtype_sql = escape_sql_literal(agtype_str)
            values_parts.append(f"('{agtype_sql}'::agtype)")

        sql_stmt = (
            f'INSERT INTO {self._name}."{resolved_label}" (properties) '
            f'VALUES {", ".join(values_parts)}'
        )

        with self._db._pool.connection() as conn:
            conn.execute(sql_stmt)

            # Fetch the last N inserted rows to get graphids
            rows = conn.execute(
                f'SELECT id FROM {self._name}."{resolved_label}" '
                f"ORDER BY id DESC LIMIT {len(entities)}"
            ).fetchall()

        # Rows come in reverse order, reverse to match entity order
        rows.reverse()

        for entity, row in zip(entities, rows):
            entity._graph_id = int(row[0])
            entity._dirty.clear()
            entity._db = self._db
            entity._graph = self

        return entities

    def bulk_add_edges(
        self,
        triples: list[tuple[Vertex, Edge, Vertex]],
        label: str | None = None,
    ) -> list[Edge]:
        """Bulk insert edges using direct SQL INSERT.

        Args:
            triples: List of (from_vertex, edge, to_vertex) tuples.
            label: Override edge label.

        Returns:
            List of edge instances with graph_ids populated.
        """
        if not triples:
            return []

        resolved_label = label or triples[0][1].label
        self.ensure_label(type(triples[0][1]), kind="e")

        values_parts = []
        for from_v, edge, to_v in triples:
            if from_v.graph_id is None or to_v.graph_id is None:
                raise EntityNotFoundError(
                    "All vertices must be persisted before bulk edge insert"
                )
            props = edge.model_dump(mode="json")
            agtype_str = to_agtype_properties(props)
            agtype_sql = escape_sql_literal(agtype_str)
            values_parts.append(
                f"(ag_catalog.graphid_in('{from_v.graph_id}'), "
                f"ag_catalog.graphid_in('{to_v.graph_id}'), "
                f"'{agtype_sql}'::agtype)"
            )

        sql_stmt = (
            f'INSERT INTO {self._name}."{resolved_label}" (start_id, end_id, properties) '
            f'VALUES {", ".join(values_parts)}'
        )

        with self._db._pool.connection() as conn:
            conn.execute(sql_stmt)

            rows = conn.execute(
                f'SELECT id, start_id, end_id FROM {self._name}."{resolved_label}" '
                f"ORDER BY id DESC LIMIT {len(triples)}"
            ).fetchall()

        rows.reverse()

        edges = []
        for (from_v, edge, to_v), row in zip(triples, rows):
            edge._graph_id = int(row[0])
            edge._start_id = from_v.graph_id
            edge._end_id = to_v.graph_id
            edge._dirty.clear()
            edge._db = self._db
            edge._graph = self
            edges.append(edge)

        return edges

    # === Query ===

    def query(self, model_class: type[T]) -> "Query[T]":
        """Create a query builder for the given model class."""
        from age_orm.query.builder import Query

        return Query(model_class=model_class, graph=self)

    def _hydrate_result(self, data):
        """Auto-hydrate a parsed result dict into a model instance if possible.

        Converts vertex/edge dicts whose label matches a registered model class
        into model instances. Scalars and unknown labels pass through unchanged.
        """
        if not isinstance(data, dict):
            return data
        # Multi-column result: hydrate each column value
        if any(k.startswith("col_") for k in data):
            return {k: self._hydrate_result(v) for k, v in data.items()}
        # Only hydrate dicts that look like a vertex/edge (have label + properties)
        if "label" not in data or "properties" not in data:
            return data
        model_class = AgeModel._label_registry.get(data["label"])
        if model_class is None:
            return data
        return dict_to_model(data, model_class, db=self._db, graph=self)

    def cypher(
        self,
        statement: str,
        columns: list[str] | None = None,
        return_type: str = "raw",
        **params,
    ) -> list:
        """Execute raw Cypher and return results.

        Args:
            statement: Cypher query string.
            columns: Column names for the AS clause.
            return_type: How to parse results ("vertex", "edge", "raw").
            **params: Parameter values to substitute ($name placeholders).
        """
        results = self._execute_cypher(
            statement, params=params, columns=columns, return_type=return_type
        )
        return [self._hydrate_result(r) for r in results]

    # === Traversal ===

    def expand(
        self,
        vertex: Vertex,
        direction: str = "any",
        depth: int = 1,
        only: list[type[AgeModel] | str] | None = None,
    ) -> None:
        """Populate vertex._relations with connected entities.

        Edge and target data are auto-hydrated into model instances
        when their labels match registered model classes.

        Args:
            vertex: The vertex to expand from.
            direction: "outbound", "inbound", or "any".
            depth: Maximum traversal depth.
            only: Restrict to specific edge labels or model classes.
        """
        if vertex.graph_id is None:
            raise EntityNotFoundError("Cannot expand vertex without graph_id")

        dir_left = "<" if direction == "inbound" else ""
        dir_right = ">" if direction == "outbound" else ""
        if direction == "any":
            dir_left = ""
            dir_right = ""

        cypher = (
            f"MATCH (n){dir_left}-[e*1..{depth}]-{dir_right}(m) "
            f"WHERE id(n) = {vertex.graph_id} RETURN e, m"
        )
        results = self._execute_cypher(
            cypher, columns=["e", "m"], return_type="raw"
        )

        vertex._relations = {}
        for row in results:
            edge_data = row.get("col_0")
            target_data = row.get("col_1")
            if not edge_data or not target_data:
                continue

            # VLE returns edge paths as {"value": [edge_dict, ...]}
            # Extract the last edge (direct connection to target) for labeling
            if "value" in edge_data and isinstance(edge_data["value"], list):
                edge_list = edge_data["value"]
                if not edge_list:
                    continue
                edge_data = edge_list[-1]

            # Group by edge label
            edge_label = edge_data.get("label", "unknown") if isinstance(edge_data, dict) else "unknown"
            if edge_label not in vertex._relations:
                vertex._relations[edge_label] = []
            vertex._relations[edge_label].append({
                "edge": self._hydrate_result(edge_data),
                "target": self._hydrate_result(target_data),
            })

    def traverse(
        self,
        vertex: Vertex,
        edge_label: str,
        depth: int = 1,
        direction: str = "outbound",
        target_class: type[T] | None = None,
    ) -> list[T] | list[dict]:
        """Traverse from a vertex along edges with the given label.

        Args:
            vertex: Starting vertex.
            edge_label: Edge label to follow.
            depth: Maximum hops.
            direction: "outbound", "inbound", or "any".
            target_class: If provided, hydrate results into this model class.
                When omitted, results are auto-hydrated via the label registry.

        Returns:
            List of target vertices as model instances (or dicts for unknown labels).
        """
        if vertex.graph_id is None:
            raise EntityNotFoundError("Cannot traverse from vertex without graph_id")

        dir_left = "<" if direction == "inbound" else ""
        dir_right = ">" if direction == "outbound" else ""
        if direction == "any":
            dir_left = ""
            dir_right = ""

        cypher = (
            f"MATCH (n){dir_left}-[:{edge_label}*1..{depth}]-{dir_right}(m) "
            f"WHERE id(n) = {vertex.graph_id} RETURN m"
        )
        results = self._execute_cypher(cypher, return_type="vertex")

        if target_class:
            return [
                dict_to_model(r, target_class, db=self._db, graph=self) for r in results
            ]
        return [self._hydrate_result(r) for r in results]

    # === Schema Management ===

    def ensure_label(self, model_class: type[AgeModel], kind: str = "v") -> None:
        """Ensure a vertex or edge label exists in the graph, creating if needed.

        Args:
            model_class: The model class to create a label for.
            kind: "v" for vertex, "e" for edge.
        """
        label = getattr(model_class, "__label__", None) or model_class.__name__

        with self._db._pool.connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (self._name, label),
            ).fetchone()

            if row is None:
                if kind == "v":
                    conn.execute(
                        f"SELECT create_vlabel('{self._name}', '{label}')"
                    )
                else:
                    conn.execute(
                        f"SELECT create_elabel('{self._name}', '{label}')"
                    )
                log.info("Created %s label: %s", "vertex" if kind == "v" else "edge", label)

    def create_index(
        self, model_class: type[AgeModel], field: str, unique: bool = False
    ) -> None:
        """Create a PostgreSQL index on a property field.

        Args:
            model_class: The model class (determines the label/table).
            field: The property field name to index.
            unique: If True, create a unique index.
        """
        label = getattr(model_class, "__label__", None) or model_class.__name__
        idx_name = f"idx_{self._name}_{label}_{field}"
        unique_str = "UNIQUE " if unique else ""

        with self._db._pool.connection() as conn:
            conn.execute(
                f'CREATE {unique_str}INDEX IF NOT EXISTS {idx_name} '
                f'ON {self._name}."{label}" ((properties::json->>\'{field}\'))'
            )
        log.info("Created index: %s", idx_name)


class AsyncGraph:
    """Asynchronous graph operations.

    Mirror of Graph using async/await.
    """

    def __init__(self, name: str, db: "AsyncDatabase"):
        self._name = name
        self._db = db

    @property
    def name(self) -> str:
        return self._name

    async def _execute_cypher(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
        columns: list[str] | None = None,
        return_type: str = "vertex",
    ) -> list[dict]:
        """Execute Cypher within AGE's SQL wrapper (async)."""
        resolved_cypher = substitute_cypher_params(cypher, params)

        if columns:
            col_clause = ", ".join(f"{c} agtype" for c in columns)
        else:
            col_clause = "result agtype"

        sql = f"SELECT * FROM cypher('{self._name}', $$ {resolved_cypher} $$) AS ({col_clause})"
        log.debug("Executing: %s", sql)

        async with self._db._pool.connection() as conn:
            result = await conn.execute(sql)
            rows = await result.fetchall()

        return _parse_result_rows(rows, return_type, num_columns=len(columns) if columns else 1)

    async def add(self, entity: Vertex) -> Vertex:
        """Add a vertex to the graph (async)."""
        dispatch(entity, "pre_add", graph=self)
        await self.ensure_label(type(entity))

        props = model_to_cypher_properties(entity)
        label = entity.label
        cypher = f"CREATE (n:{label} {props}) RETURN n"
        results = await self._execute_cypher(cypher, return_type="vertex")

        if results:
            entity._graph_id = results[0].get("graph_id")
            entity._dirty.clear()
            entity._db = self._db
            entity._graph = self

        dispatch(entity, "post_add", graph=self)
        return entity

    async def update(self, entity: Vertex | Edge, only_dirty: bool = False) -> Vertex | Edge:
        """Update an existing entity (async)."""
        if entity.graph_id is None:
            raise EntityNotFoundError("Cannot update entity without graph_id")

        dispatch(entity, "pre_update", graph=self)

        if only_dirty:
            props = entity.dirty_fields_dump(mode="json")
        else:
            props = entity.model_dump(mode="json")

        if not props:
            return entity

        set_parts = ", ".join(
            f"n.{k} = {format_cypher_value(v)}" for k, v in props.items()
        )
        cypher = f"MATCH (n) WHERE id(n) = {entity.graph_id} SET {set_parts} RETURN n"
        await self._execute_cypher(cypher, return_type="vertex")

        entity._dirty.clear()
        dispatch(entity, "post_update", graph=self)
        return entity

    async def delete(self, entity: Vertex | Edge) -> None:
        """Delete an entity (async)."""
        if entity.graph_id is None:
            raise EntityNotFoundError("Cannot delete entity without graph_id")

        dispatch(entity, "pre_delete", graph=self)

        if isinstance(entity, Vertex):
            cypher = f"MATCH (n) WHERE id(n) = {entity.graph_id} DETACH DELETE n"
        else:
            cypher = f"MATCH ()-[e]->() WHERE id(e) = {entity.graph_id} DELETE e"

        await self._execute_cypher(cypher, return_type="raw")
        entity._graph_id = None
        entity._db = None
        entity._graph = None

        dispatch(entity, "post_delete", graph=self)

    async def connect(self, from_v: Vertex, edge: Edge, to_v: Vertex) -> Edge:
        """Create an edge between two vertices (async)."""
        if from_v.graph_id is None or to_v.graph_id is None:
            raise EntityNotFoundError(
                "Both vertices must be persisted before creating an edge"
            )

        dispatch(edge, "pre_add", graph=self)
        await self.ensure_label(type(edge), kind="e")

        props = model_to_cypher_properties(edge)
        label = edge.label
        cypher = (
            f"MATCH (a), (b) WHERE id(a) = {from_v.graph_id} AND id(b) = {to_v.graph_id} "
            f"CREATE (a)-[e:{label} {props}]->(b) RETURN e"
        )
        results = await self._execute_cypher(cypher, return_type="edge")

        if results:
            edge._graph_id = results[0].get("graph_id")
            edge._start_id = from_v.graph_id
            edge._end_id = to_v.graph_id
            edge._dirty.clear()
            edge._db = self._db
            edge._graph = self

        dispatch(edge, "post_add", graph=self)
        return edge

    async def query(self, model_class: type[T]) -> "AsyncQuery[T]":
        """Create an async query builder for the given model class."""
        from age_orm.query.builder import AsyncQuery

        return AsyncQuery(model_class=model_class, graph=self)

    def _hydrate_result(self, data):
        """Auto-hydrate a parsed result dict into a model instance if possible.

        Converts vertex/edge dicts whose label matches a registered model class
        into model instances. Scalars and unknown labels pass through unchanged.
        """
        if not isinstance(data, dict):
            return data
        if any(k.startswith("col_") for k in data):
            return {k: self._hydrate_result(v) for k, v in data.items()}
        if "label" not in data or "properties" not in data:
            return data
        model_class = AgeModel._label_registry.get(data["label"])
        if model_class is None:
            return data
        return dict_to_model(data, model_class, db=self._db, graph=self)

    async def cypher(
        self,
        statement: str,
        columns: list[str] | None = None,
        return_type: str = "raw",
        **params,
    ) -> list:
        """Execute raw Cypher and return results (async)."""
        results = await self._execute_cypher(
            statement, params=params, columns=columns, return_type=return_type
        )
        return [self._hydrate_result(r) for r in results]

    async def ensure_label(self, model_class: type[AgeModel], kind: str = "v") -> None:
        """Ensure a vertex or edge label exists (async)."""
        label = getattr(model_class, "__label__", None) or model_class.__name__

        async with self._db._pool.connection() as conn:
            result = await conn.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (self._name, label),
            )
            row = await result.fetchone()

            if row is None:
                if kind == "v":
                    await conn.execute(
                        f"SELECT create_vlabel('{self._name}', '{label}')"
                    )
                else:
                    await conn.execute(
                        f"SELECT create_elabel('{self._name}', '{label}')"
                    )
                log.info("Created %s label: %s", "vertex" if kind == "v" else "edge", label)


# === Result Parsing Helpers ===


def _parse_agtype_result(val: Any, return_type: str) -> dict:
    """Parse a single agtype result value into a dict.

    AGE returns results as strings in one of these formats:
      - Vertex: '{"id": N, "label": "L", "properties": {...}}::vertex'
      - Edge: '{"id": N, "label": "L", "start_id": N, "end_id": N, "properties": {...}}::edge'
      - Scalar: '42', '"text"', 'true', 'null'
      - JSON: '{"key": "value"}', '[1, 2]'

    For vertex/edge results, the 'id' key is renamed to 'graph_id'
    to match the ORM's internal convention.
    """
    if val is None:
        return {}

    # psycopg returns agtype as string if no custom loader is registered
    if isinstance(val, str):
        val_str = val.strip()

        # Strip AGE agtype suffixes (::vertex, ::edge, ::path, ::int, etc.)
        # First strip suffixes attached to objects/arrays (e.g., }::edge, ]::path)
        val_str = re.sub(r'([\]}])::\w+', r'\1', val_str)
        # Then strip a trailing suffix on scalars (e.g., 42::int, "text"::text)
        val_str = re.sub(r'::\w+$', '', val_str)
        val_str = val_str.strip()

        # JSON object/array/scalar
        if val_str.startswith(("{", "[", '"')) or val_str in ("null", "true", "false"):
            try:
                parsed = json.loads(val_str)
                if isinstance(parsed, dict):
                    # Normalize vertex/edge results: rename 'id' -> 'graph_id'
                    if "id" in parsed and "properties" in parsed:
                        parsed["graph_id"] = parsed.pop("id")
                    return parsed
                if isinstance(parsed, list):
                    # Normalize vertex/edge dicts inside arrays (e.g., VLE paths)
                    for item in parsed:
                        if isinstance(item, dict) and "id" in item and "properties" in item:
                            item["graph_id"] = item.pop("id")
                return {"value": parsed}
            except json.JSONDecodeError:
                pass

        # Numeric
        try:
            num = int(val_str)
            return {"value": num}
        except ValueError:
            try:
                num = float(val_str)
                return {"value": num}
            except ValueError:
                pass

        return {"raw": val_str}

    if isinstance(val, dict):
        return val

    if isinstance(val, (int, float, bool)):
        return {"value": val}

    # Fallback
    return {"raw": str(val)}


def _parse_result_rows(
    rows: list[tuple], return_type: str, num_columns: int = 1
) -> list[dict]:
    """Parse multiple result rows."""
    results = []
    for row in rows:
        if num_columns == 1:
            parsed = _parse_agtype_result(row[0], return_type)
            results.append(parsed)
        else:
            parsed_row = {}
            for i, val in enumerate(row):
                parsed_row[f"col_{i}"] = _parse_agtype_result(val, "raw")
            results.append(parsed_row)
    return results
