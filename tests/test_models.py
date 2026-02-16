"""Tests for model creation, dirty tracking, serialization, and field handling."""

import pytest

from age_orm.models.vertex import Vertex
from age_orm.exceptions import DetachedInstanceError


class TestVertex:
    def test_create_basic(self, alice):
        assert alice.name == "Alice"
        assert alice.age == 30
        assert alice.email == "alice@example.com"
        assert alice.graph_id is None
        assert alice.label == "Person"

    def test_label_from_class_name(self):
        class UnlabeledVertex(Vertex):
            value: str

        v = UnlabeledVertex(value="test")
        assert v.label == "UnlabeledVertex"

    def test_label_from_class_var(self, alice):
        assert alice.label == "Person"

    def test_new_instance_is_dirty(self, alice):
        assert alice.is_dirty
        assert "name" in alice._dirty
        assert "age" in alice._dirty
        assert "email" in alice._dirty

    def test_db_loaded_instance_is_clean(self):
        from tests.conftest import Person

        # Simulate loading from DB by passing _db
        p = Person(name="Test", age=1, _db=object())
        assert not p.is_dirty
        assert len(p._dirty) == 0

    def test_dirty_tracking_on_setattr(self):
        from tests.conftest import Person

        p = Person(name="Test", age=1, _db=object())
        assert not p.is_dirty

        p.name = "Changed"
        assert p.is_dirty
        assert "name" in p._dirty
        assert "age" not in p._dirty

    def test_model_dump_excludes_internal(self, alice):
        data = alice.model_dump()
        assert "name" in data
        assert "age" in data
        assert "_graph_id" not in data
        assert "_dirty" not in data
        assert "_db" not in data

    def test_model_dump_json_mode(self, alice):
        data = alice.model_dump(mode="json")
        assert data["name"] == "Alice"
        assert data["age"] == 30
        assert data["email"] == "alice@example.com"

    def test_dirty_fields_dump(self):
        from tests.conftest import Person

        p = Person(name="Test", age=1, _db=object())
        p.name = "Changed"
        dirty = p.dirty_fields_dump()
        assert dirty == {"name": "Changed"}

    def test_str_repr(self, alice):
        s = str(alice)
        assert "Person" in s
        assert "Alice" in s


class TestEdge:
    def test_create_basic(self, knows_edge):
        assert knows_edge.since == 2020
        assert knows_edge.relationship_type == "colleague"
        assert knows_edge.label == "KNOWS"
        assert knows_edge.start_id is None
        assert knows_edge.end_id is None
        assert knows_edge.graph_id is None

    def test_edge_dirty_tracking(self, knows_edge):
        assert knows_edge.is_dirty
        assert "since" in knows_edge._dirty

    def test_edge_model_dump(self, knows_edge):
        data = knows_edge.model_dump()
        assert "since" in data
        assert "relationship_type" in data
        assert "_start_id" not in data
        assert "_end_id" not in data

    def test_edge_endpoints(self):
        from tests.conftest import Knows

        e = Knows(since=2023, _start_id=100, _end_id=200)
        assert e.start_id == 100
        assert e.end_id == 200


class TestRelationshipField:
    def test_relationship_excluded_from_dump(self):
        from tests.conftest import PersonWithRels

        p = PersonWithRels(name="Test", age=25)
        data = p.model_dump()
        assert "name" in data
        assert "age" in data
        assert "friends" not in data
        assert "employer" not in data

    def test_relationship_access_without_db_raises(self):
        from tests.conftest import PersonWithRels

        p = PersonWithRels(name="Test", age=25)
        with pytest.raises(DetachedInstanceError):
            _ = p.friends
