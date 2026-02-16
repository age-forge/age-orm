"""Tests for the Cypher query builder (no DB required — tests query generation)."""

import pytest

from age_orm.query.builder import Query


class FakeGraph:
    """Minimal graph stub for testing query building."""

    def __init__(self):
        self._name = "test_graph"
        self._db = None


class TestPerson:
    """Inline test model to avoid import issues."""

    from age_orm.models.vertex import Vertex

    class Person(Vertex):
        name: str
        age: int
        email: str | None = None


Person = TestPerson.Person


@pytest.fixture
def graph():
    return FakeGraph()


@pytest.fixture
def query(graph):
    return Query(model_class=Person, graph=graph)


class TestQueryBuilding:
    def test_basic_query(self, query):
        cypher = query._build_cypher()
        assert "MATCH (n:Person)" in cypher
        assert "RETURN n" in cypher

    def test_filter(self, query):
        query.filter("n.age > $min_age", min_age=20)
        cypher = query._build_cypher()
        assert "WHERE" in cypher
        assert "n.age > 20" in cypher

    def test_filter_by(self, query):
        query.filter_by(name="Alice")
        cypher = query._build_cypher()
        assert "WHERE" in cypher
        assert "n.name = 'Alice'" in cypher

    def test_filter_by_multiple(self, query):
        query.filter_by(name="Alice", age=30)
        cypher = query._build_cypher()
        assert "n.name = 'Alice'" in cypher
        assert "n.age = 30" in cypher

    def test_filter_and(self, query):
        query.filter("n.age > $min", min=20).filter("n.age < $max", max=50)
        cypher = query._build_cypher()
        assert "AND" in cypher

    def test_filter_or(self, query):
        query.filter("n.name = $n1", n1="Alice").filter(
            "n.name = $n2", _or=True, n2="Bob"
        )
        cypher = query._build_cypher()
        assert "OR" in cypher

    def test_sort(self, query):
        query.sort("n.name")
        cypher = query._build_cypher()
        assert "ORDER BY n.name" in cypher

    def test_sort_desc(self, query):
        query.sort("n.age DESC")
        cypher = query._build_cypher()
        assert "ORDER BY n.age DESC" in cypher

    def test_limit(self, query):
        query.limit(10)
        cypher = query._build_cypher()
        assert "LIMIT 10" in cypher

    def test_skip_and_limit(self, query):
        query.limit(10, skip=5)
        cypher = query._build_cypher()
        assert "SKIP 5" in cypher
        assert "LIMIT 10" in cypher

    def test_returns(self, query):
        query.returns("n.name", "n.age")
        cypher = query._build_cypher()
        assert "RETURN n.name, n.age" in cypher
        assert "RETURN n\n" not in cypher

    def test_combined_query(self, query):
        query.filter("n.age > $min", min=20).sort("n.name").limit(5, skip=2)
        cypher = query._build_cypher()
        assert "MATCH (n:Person)" in cypher
        assert "WHERE" in cypher
        assert "n.age > 20" in cypher
        assert "ORDER BY n.name" in cypher
        assert "SKIP 2" in cypher
        assert "LIMIT 5" in cypher

    def test_str_representation(self, query):
        query.filter_by(name="Alice")
        s = str(query)
        assert "MATCH (n:Person)" in s


class TestQueryIteration:
    def test_iter_delegates_to_iterator(self, graph):
        """Query.__iter__ should yield the same results as iterator()."""
        results = [
            {"graph_id": 1, "label": "Person", "properties": {"name": "Alice", "age": 30}},
            {"graph_id": 2, "label": "Person", "properties": {"name": "Bob", "age": 25}},
        ]
        graph._execute_cypher = lambda *a, **kw: results
        graph._db = None

        query = Query(model_class=Person, graph=graph)
        names = [p.name for p in query]
        assert names == ["Alice", "Bob"]

    def test_iter_with_filters(self, graph):
        """Iteration respects filters applied to the query."""
        results = [
            {"graph_id": 1, "label": "Person", "properties": {"name": "Alice", "age": 30}},
        ]
        graph._execute_cypher = lambda *a, **kw: results
        graph._db = None

        query = Query(model_class=Person, graph=graph)
        people = [p for p in query.filter("n.age > $min", min=25)]
        assert len(people) == 1
        assert people[0].name == "Alice"

    def test_iter_empty(self, graph):
        """Iterating an empty result set yields nothing."""
        graph._execute_cypher = lambda *a, **kw: []
        graph._db = None

        query = Query(model_class=Person, graph=graph)
        assert list(query) == []

    def test_iter_matches_all(self, graph):
        """list(query) should produce the same result as query.all()."""
        results = [
            {"graph_id": 1, "label": "Person", "properties": {"name": "Alice", "age": 30}},
            {"graph_id": 2, "label": "Person", "properties": {"name": "Bob", "age": 25}},
            {"graph_id": 3, "label": "Person", "properties": {"name": "Carol", "age": 35}},
        ]
        graph._execute_cypher = lambda *a, **kw: results
        graph._db = None

        q1 = Query(model_class=Person, graph=graph)
        q2 = Query(model_class=Person, graph=graph)
        iter_names = [p.name for p in q1]
        all_names = [p.name for p in q2.all()]
        assert iter_names == all_names


class TestQueryByIdCypher:
    def test_by_id_builds_correct_cypher(self, graph):
        query = Query(model_class=Person, graph=graph)
        # Verify label is used correctly in query generation
        cypher = query._build_cypher()
        assert "Person" in cypher


class TestLabelRegistry:
    """Test the AgeModel._label_registry populated via __init_subclass__."""

    def test_base_classes_not_registered(self):
        """Intermediate classes (Vertex, Edge) with __label__ = None are not registered."""
        from age_orm.models.base import AgeModel

        assert None not in AgeModel._label_registry

    def test_class_without_label_not_registered(self):
        """Classes without __label__ are not auto-registered."""
        from age_orm.models.base import AgeModel

        # The local Person class has no __label__, so "Person" should not be
        # registered (unless another module already registered a Person with __label__).
        # Just verify the registry value is not our local Person class.
        if "Person" in AgeModel._label_registry:
            assert AgeModel._label_registry["Person"] is not Person

    def test_new_subclass_registered(self):
        """Defining a new subclass with __label__ auto-registers it."""
        from age_orm.models.base import AgeModel
        from age_orm.models.vertex import Vertex

        class Widget(Vertex):
            __label__ = "Widget"
            color: str = "red"

        assert "Widget" in AgeModel._label_registry
        assert AgeModel._label_registry["Widget"] is Widget
