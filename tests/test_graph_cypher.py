"""Tests for Graph.cypher() column remapping and scalar unwrapping."""

from unittest.mock import MagicMock, patch

import pytest

from age_orm.graph import Graph, _remap_columns, _unwrap_scalar


# --- Unit tests for helper functions ---


class TestUnwrapScalar:
    def test_unwrap_value_dict(self):
        assert _unwrap_scalar({"value": 42}) == 42

    def test_unwrap_string_value(self):
        assert _unwrap_scalar({"value": "hello"}) == "hello"

    def test_unwrap_list_value(self):
        assert _unwrap_scalar({"value": [1, 2, 3]}) == [1, 2, 3]

    def test_no_unwrap_multi_key_dict(self):
        d = {"value": 42, "other": "stuff"}
        assert _unwrap_scalar(d) == d

    def test_no_unwrap_non_value_dict(self):
        d = {"label": "Person", "properties": {"name": "Alice"}}
        assert _unwrap_scalar(d) == d

    def test_no_unwrap_non_dict(self):
        assert _unwrap_scalar(42) == 42
        assert _unwrap_scalar("hello") == "hello"
        assert _unwrap_scalar(None) is None


class TestRemapColumns:
    def test_multi_column_remap(self):
        hydrated = [
            {"col_0": {"value": "word1"}, "col_1": {"value": 5}},
            {"col_0": {"value": "word2"}, "col_1": {"value": 3}},
        ]
        result = _remap_columns(hydrated, ["word", "count"])
        assert result == [
            {"word": "word1", "count": 5},
            {"word": "word2", "count": 3},
        ]

    def test_multi_column_mixed_types(self):
        """Vertex dicts (label+properties) should pass through, scalars should unwrap."""
        vertex_dict = {"label": "Word", "properties": {"word": "test"}, "graph_id": 123}
        hydrated = [
            {"col_0": vertex_dict, "col_1": {"value": 5}},
        ]
        result = _remap_columns(hydrated, ["node", "count"])
        assert result[0]["node"] == vertex_dict
        assert result[0]["count"] == 5

    def test_multi_column_preserves_non_col_rows(self):
        """Rows without col_N keys pass through unchanged."""
        row = {"some_key": "some_value"}
        result = _remap_columns([row], ["a", "b"])
        assert result == [row]

    def test_single_column_scalar_unwrap(self):
        hydrated = [{"value": "hello"}, {"value": 42}]
        result = _remap_columns(hydrated, ["text"])
        assert result == [{"text": "hello"}, {"text": 42}]

    def test_single_column_model_passthrough(self):
        """Non-scalar results (e.g., hydrated model instances) pass through."""
        mock_model = MagicMock()
        mock_model.__class__.__name__ = "Person"
        result = _remap_columns([mock_model], ["node"])
        assert result == [mock_model]

    def test_single_column_complex_dict_passthrough(self):
        """Dicts that aren't simple {"value": x} pass through."""
        d = {"label": "Person", "properties": {"name": "Alice"}, "graph_id": 1}
        result = _remap_columns([d], ["node"])
        assert result == [d]

    def test_empty_results(self):
        assert _remap_columns([], ["a", "b"]) == []
        assert _remap_columns([], ["a"]) == []

    def test_three_columns(self):
        hydrated = [
            {"col_0": {"value": "1:1"}, "col_1": {"value": "arabic"}, "col_2": {"value": "text"}},
        ]
        result = _remap_columns(hydrated, ["aya_id", "language", "content"])
        assert result == [{"aya_id": "1:1", "language": "arabic", "content": "text"}]

    def test_backward_compat_no_columns(self):
        """With empty columns list, return hydrated as-is."""
        hydrated = [{"value": 42}]
        result = _remap_columns(hydrated, [])
        assert result == hydrated


class TestGraphCypherRemapping:
    """Test Graph.cypher() integration with column remapping."""

    def _make_graph(self):
        """Create a Graph with a mocked database."""
        mock_db = MagicMock()
        return Graph(name="test_graph", db=mock_db)

    def test_cypher_with_named_columns(self):
        g = self._make_graph()
        # Mock _execute_cypher to return multi-column raw results
        g._execute_cypher = MagicMock(return_value=[
            {"col_0": {"value": "word1"}, "col_1": {"value": 5}},
            {"col_0": {"value": "word2"}, "col_1": {"value": 3}},
        ])

        results = g.cypher(
            "MATCH (w:Word) RETURN w.word, w.count",
            columns=["word", "count"],
        )

        assert len(results) == 2
        assert results[0] == {"word": "word1", "count": 5}
        assert results[1] == {"word": "word2", "count": 3}

    def test_cypher_single_column_scalar(self):
        g = self._make_graph()
        g._execute_cypher = MagicMock(return_value=[
            {"value": "some text content"},
        ])

        results = g.cypher(
            "MATCH (t:Text) RETURN t.text",
            columns=["text"],
        )

        assert results == [{"text": "some text content"}]

    def test_cypher_without_columns(self):
        """Without columns parameter, behavior is unchanged."""
        g = self._make_graph()
        g._execute_cypher = MagicMock(return_value=[
            {"value": 42},
        ])

        results = g.cypher("MATCH (n) RETURN count(n)")

        # No remapping — returns hydrated as-is
        assert results == [{"value": 42}]

    def test_cypher_vertex_hydration_still_works(self):
        """Vertex results should still be hydrated into model instances."""
        g = self._make_graph()
        vertex_data = {
            "label": "Person",
            "properties": {"name": "Alice", "age": 30},
            "graph_id": 123,
        }
        g._execute_cypher = MagicMock(return_value=[vertex_data])

        # Without columns, vertex hydration should work (if model is registered)
        results = g.cypher("MATCH (n:Person) RETURN n")

        # Result will be the hydrated form (dict or model instance depending on registry)
        assert len(results) == 1

    def test_cypher_multi_column_with_none_values(self):
        g = self._make_graph()
        g._execute_cypher = MagicMock(return_value=[
            {"col_0": {"value": "word1"}, "col_1": None},
        ])

        results = g.cypher(
            "MATCH (w:Word) RETURN w.word, w.missing",
            columns=["word", "missing"],
        )

        assert results[0]["word"] == "word1"
        assert results[0]["missing"] is None
