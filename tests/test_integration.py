"""Integration tests against a real Apache AGE database.

Prerequisites:
    - PostgreSQL with AGE extension installed
    - Database: age_tutorial
    - DSN: postgresql://kashif:compulife@localhost/age_tutorial

Run with:
    uv run pytest tests/test_integration.py -v -s
"""

import pytest
from age_orm import Database, Vertex, Edge, Graph, listen, listens_for

# ── Models ──────────────────────────────────────────────────────────

DSN = "postgresql://kashif:compulife@localhost/age_tutorial"
GRAPH_NAME = "test_social"


class Person(Vertex):
    __label__ = "Person"
    name: str
    age: int
    email: str | None = None


class Company(Vertex):
    __label__ = "Company"
    name: str
    industry: str


class Knows(Edge):
    __label__ = "KNOWS"
    since: int
    relationship_type: str = "friend"


class WorksAt(Edge):
    __label__ = "WORKS_AT"
    role: str
    start_year: int


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def db():
    """Create a Database connection for the test session."""
    database = Database(DSN)
    yield database
    # Cleanup: drop graph if it still exists
    if database.graph_exists(GRAPH_NAME):
        database.drop_graph(GRAPH_NAME)
    database.close()


@pytest.fixture(scope="module")
def graph(db):
    """Create a fresh graph for the test session."""
    if db.graph_exists(GRAPH_NAME):
        db.drop_graph(GRAPH_NAME)
    g = db.graph(GRAPH_NAME, create=True)
    return g


# ── Tests ───────────────────────────────────────────────────────────


class TestDatabaseOperations:
    """Test Database-level operations: connection, graph management."""

    def test_connection(self, db):
        """Database connects and can run a simple query."""
        assert db is not None

    def test_create_graph(self, db, graph):
        """graph() with create=True creates the graph."""
        assert graph is not None
        assert graph.name == GRAPH_NAME

    def test_graph_exists(self, db, graph):
        """graph_exists() returns True for an existing graph."""
        assert db.graph_exists(GRAPH_NAME)

    def test_graph_not_exists(self, db):
        """graph_exists() returns False for a non-existent graph."""
        assert not db.graph_exists("no_such_graph")

    def test_list_graphs(self, db, graph):
        """list_graphs() includes our test graph."""
        graphs = db.list_graphs()
        assert GRAPH_NAME in graphs


class TestVertexCRUD:
    """Test creating, reading, updating, and deleting vertices."""

    def test_add_vertex(self, graph):
        """graph.add() creates a vertex and sets graph_id."""
        alice = Person(name="Alice", age=30, email="alice@example.com")
        graph.add(alice)
        assert alice.graph_id is not None
        assert not alice.is_dirty

    def test_query_vertex(self, graph):
        """Query builder can find the vertex we just created."""
        results = graph.query(Person).filter_by(name="Alice").all()
        assert len(results) == 1
        alice = results[0]
        assert alice.name == "Alice"
        assert alice.age == 30
        assert alice.email == "alice@example.com"
        assert alice.graph_id is not None

    def test_query_one(self, graph):
        """filter_by().one() returns exactly one result."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        assert alice.name == "Alice"

    def test_query_first(self, graph):
        """first() returns the first result or None."""
        alice = graph.query(Person).filter_by(name="Alice").first()
        assert alice is not None
        assert alice.name == "Alice"

        nobody = graph.query(Person).filter_by(name="NoSuchPerson").first()
        assert nobody is None

    def test_update_vertex(self, graph):
        """graph.update() modifies an existing vertex."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        alice.age = 31
        assert alice.is_dirty
        graph.update(alice)
        assert not alice.is_dirty

        # Verify the update persisted
        alice_reloaded = graph.query(Person).filter_by(name="Alice").one()
        assert alice_reloaded.age == 31

    def test_update_only_dirty(self, graph):
        """graph.update(only_dirty=True) sends only changed fields."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        alice.email = "alice@newdomain.com"
        graph.update(alice, only_dirty=True)

        alice_reloaded = graph.query(Person).filter_by(name="Alice").one()
        assert alice_reloaded.email == "alice@newdomain.com"
        assert alice_reloaded.age == 31  # Unchanged

    def test_add_multiple_vertices(self, graph):
        """Add several more vertices for later tests."""
        bob = Person(name="Bob", age=25)
        charlie = Person(name="Charlie", age=35, email="charlie@example.com")
        diana = Person(name="Diana", age=28)

        for person in [bob, charlie, diana]:
            graph.add(person)
            assert person.graph_id is not None

    def test_query_count(self, graph):
        """count() returns the number of matching entities."""
        count = graph.query(Person).count()
        assert count == 4  # Alice, Bob, Charlie, Diana

    def test_query_filter(self, graph):
        """filter() with raw Cypher conditions works."""
        results = graph.query(Person).filter("n.age > $min_age", min_age=28).all()
        names = {p.name for p in results}
        assert "Alice" in names  # age 31
        assert "Charlie" in names  # age 35
        assert "Bob" not in names  # age 25

    def test_query_sort_and_limit(self, graph):
        """sort() and limit() work correctly."""
        results = graph.query(Person).sort("n.age").limit(2).all()
        assert len(results) == 2
        assert results[0].name == "Bob"  # youngest
        assert results[1].name == "Diana"

    def test_query_sort_desc(self, graph):
        """Descending sort works."""
        results = graph.query(Person).sort("n.age DESC").limit(1).all()
        assert len(results) == 1
        assert results[0].name == "Charlie"  # oldest at 35

    def test_query_skip(self, graph):
        """limit() with skip (offset) works."""
        results = graph.query(Person).sort("n.age").limit(2, skip=1).all()
        assert len(results) == 2
        assert results[0].name == "Diana"  # second youngest

    def test_query_by_property(self, graph):
        """by_property() lookup works."""
        alice = graph.query(Person).by_property("name", "Alice")
        assert alice is not None
        assert alice.age == 31

    def test_query_iter(self, graph):
        """Query objects are directly iterable."""
        names = [p.name for p in graph.query(Person).sort("n.name")]
        assert len(names) == 4
        assert names == sorted(names)

    def test_query_iter_with_filter(self, graph):
        """Iteration works with filters applied."""
        young = [p for p in graph.query(Person).filter("n.age < $max", max=30)]
        assert all(p.age < 30 for p in young)
        assert len(young) > 0


class TestEdgeCRUD:
    """Test creating and querying edges."""

    def test_connect(self, graph):
        """graph.connect() creates an edge between two vertices."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        bob = graph.query(Person).filter_by(name="Bob").one()

        knows = Knows(since=2020, relationship_type="colleague")
        graph.connect(alice, knows, bob)

        assert knows.graph_id is not None
        assert knows.start_id == alice.graph_id
        assert knows.end_id == bob.graph_id
        assert not knows.is_dirty

    def test_connect_multiple(self, graph):
        """Create several more edges for traversal tests."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        charlie = graph.query(Person).filter_by(name="Charlie").one()
        diana = graph.query(Person).filter_by(name="Diana").one()

        # Alice knows Charlie
        graph.connect(alice, Knows(since=2019, relationship_type="friend"), charlie)
        # Bob knows Diana
        bob = graph.query(Person).filter_by(name="Bob").one()
        graph.connect(bob, Knows(since=2021, relationship_type="friend"), diana)

    def test_add_company_and_works_at(self, graph):
        """Create Company vertices and WorksAt edges."""
        acme = Company(name="Acme Corp", industry="Technology")
        graph.add(acme)
        assert acme.graph_id is not None

        alice = graph.query(Person).filter_by(name="Alice").one()
        works = WorksAt(role="Engineer", start_year=2018)
        graph.connect(alice, works, acme)
        assert works.graph_id is not None


class TestTraversal:
    """Test graph traversal operations."""

    def test_traverse_outbound(self, graph):
        """traverse() finds outbound neighbors."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        friends = graph.traverse(alice, "KNOWS", direction="outbound", target_class=Person)
        names = {p.name for p in friends}
        assert "Bob" in names
        assert "Charlie" in names

    def test_traverse_inbound(self, graph):
        """traverse() finds inbound neighbors."""
        bob = graph.query(Person).filter_by(name="Bob").one()
        who_knows_bob = graph.traverse(bob, "KNOWS", direction="inbound", target_class=Person)
        names = {p.name for p in who_knows_bob}
        assert "Alice" in names

    def test_traverse_auto_hydrates_without_target_class(self, graph):
        """traverse() without target_class auto-hydrates via label registry."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        results = graph.traverse(alice, "KNOWS", direction="outbound")
        assert len(results) >= 2
        for r in results:
            assert isinstance(r, Person)
            assert r.graph_id is not None
            assert not r.is_dirty

    def test_expand_hydrates_relations(self, graph):
        """expand() auto-hydrates edge and target in vertex._relations."""
        alice = graph.query(Person).filter_by(name="Alice").one()
        graph.expand(alice, direction="outbound")
        assert "KNOWS" in alice._relations
        for entry in alice._relations["KNOWS"]:
            assert isinstance(entry["edge"], Knows)
            assert isinstance(entry["target"], Person)
            assert entry["target"].graph_id is not None


class TestRawCypher:
    """Test raw Cypher execution."""

    def test_cypher_count(self, graph):
        """Raw Cypher count query works."""
        results = graph.cypher("MATCH (n:Person) RETURN count(n)")
        assert len(results) == 1
        assert results[0].get("value") == 4

    def test_cypher_with_params(self, graph):
        """Raw Cypher with parameter substitution works."""
        results = graph.cypher(
            "MATCH (n:Person) WHERE n.name = $name RETURN n",
            name="Alice",
        )
        assert len(results) == 1

    def test_cypher_auto_hydrates_vertex(self, graph):
        """cypher() auto-hydrates vertex results into model instances."""
        results = graph.cypher(
            "MATCH (n:Person) WHERE n.name = $name RETURN n",
            name="Alice",
        )
        assert len(results) == 1
        alice = results[0]
        assert isinstance(alice, Person)
        assert alice.name == "Alice"
        assert alice.graph_id is not None
        assert not alice.is_dirty

    def test_cypher_auto_hydrates_edge(self, graph):
        """cypher() auto-hydrates edge results into model instances."""
        results = graph.cypher(
            "MATCH (:Person)-[e:KNOWS]->(:Person) RETURN e",
        )
        assert len(results) >= 1
        edge = results[0]
        assert isinstance(edge, Knows)
        assert edge.graph_id is not None

    def test_cypher_scalar_not_hydrated(self, graph):
        """cypher() returns raw dicts for scalar results (no label/properties)."""
        results = graph.cypher("MATCH (n:Person) RETURN count(n)")
        assert len(results) == 1
        assert isinstance(results[0], dict)
        assert results[0].get("value") == 4

    def test_cypher_multi_column_hydrates(self, graph):
        """cypher() hydrates individual columns in multi-column results with named keys."""
        results = graph.cypher(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a, e, b",
            columns=["a", "e", "b"],
        )
        assert len(results) >= 1
        row = results[0]
        assert isinstance(row["a"], Person)   # source vertex
        assert isinstance(row["e"], Knows)     # edge
        assert isinstance(row["b"], Person)    # target vertex


class TestBulkOperations:
    """Test bulk insert operations."""

    def test_bulk_add_vertices(self, graph):
        """bulk_add() inserts multiple vertices efficiently."""
        people = [
            Person(name="Eve", age=22),
            Person(name="Frank", age=40),
            Person(name="Grace", age=33),
        ]
        graph.bulk_add(people)
        for p in people:
            assert p.graph_id is not None
            assert not p.is_dirty

        # Verify they exist
        count = graph.query(Person).count()
        assert count == 7  # 4 original + 3 bulk

    def test_bulk_add_edges(self, graph):
        """bulk_add_edges() inserts multiple edges efficiently."""
        eve = graph.query(Person).filter_by(name="Eve").one()
        frank = graph.query(Person).filter_by(name="Frank").one()
        grace = graph.query(Person).filter_by(name="Grace").one()

        triples = [
            (eve, Knows(since=2022, relationship_type="friend"), frank),
            (frank, Knows(since=2023, relationship_type="colleague"), grace),
        ]
        edges = graph.bulk_add_edges(triples)
        assert len(edges) == 2
        for e in edges:
            assert e.graph_id is not None


class TestQueryBulkMutations:
    """Test bulk update and delete via query builder."""

    def test_bulk_update(self, graph):
        """Query.update() modifies matching entities."""
        # Update all people older than 35 to have email
        updated = graph.query(Person).filter("n.age > $age", age=35).update(email="senior@example.com")
        assert updated >= 1

        frank = graph.query(Person).filter_by(name="Frank").one()
        assert frank.email == "senior@example.com"

    def test_bulk_delete(self, graph):
        """Query.delete() removes matching entities."""
        # Delete Eve, Frank, Grace (the bulk-added ones)
        initial_count = graph.query(Person).count()
        deleted = graph.query(Person).filter("n.name IN $names", names=["Eve", "Frank", "Grace"]).delete()
        assert deleted == 3
        assert graph.query(Person).count() == initial_count - 3


class TestEvents:
    """Test the event system with real operations."""

    def test_pre_post_add_events(self, graph):
        """pre_add and post_add events fire during graph.add()."""
        events_fired = []

        @listens_for(Person, ["pre_add", "post_add"])
        def on_add(target, event, **kwargs):
            events_fired.append(event)

        person = Person(name="EventTest", age=99)
        graph.add(person)

        assert "pre_add" in events_fired
        assert "post_add" in events_fired

        # Cleanup
        graph.delete(person)

    def test_pre_post_update_events(self, graph):
        """pre_update and post_update events fire during graph.update()."""
        events_fired = []

        @listens_for(Person, ["pre_update", "post_update"])
        def on_update(target, event, **kwargs):
            events_fired.append(event)

        alice = graph.query(Person).filter_by(name="Alice").one()
        alice.age = 32
        graph.update(alice)

        assert "pre_update" in events_fired
        assert "post_update" in events_fired

    def test_pre_post_delete_events(self, graph):
        """pre_delete and post_delete events fire during graph.delete()."""
        events_fired = []

        @listens_for(Person, ["pre_delete", "post_delete"])
        def on_delete(target, event, **kwargs):
            events_fired.append(event)

        person = Person(name="ToDelete", age=1)
        graph.add(person)
        graph.delete(person)

        assert "pre_delete" in events_fired
        assert "post_delete" in events_fired
        assert person.graph_id is None


class TestDeleteVertex:
    """Test vertex deletion (detach delete removes connected edges)."""

    def test_delete_vertex(self, graph):
        """graph.delete() removes a vertex and its edges."""
        diana = graph.query(Person).filter_by(name="Diana").one()
        graph.delete(diana)
        assert diana.graph_id is None

        result = graph.query(Person).filter_by(name="Diana").first()
        assert result is None


class TestCleanup:
    """Cleanup tests - drop graph at the end."""

    def test_drop_graph(self, db):
        """db.drop_graph() removes the graph."""
        if db.graph_exists(GRAPH_NAME):
            db.drop_graph(GRAPH_NAME)
        assert not db.graph_exists(GRAPH_NAME)
