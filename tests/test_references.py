"""Tests for the relationship descriptor system."""

import pytest

from age_orm.references import Relationship, relationship


class TestRelationship:
    def test_basic_creation(self):
        r = Relationship(
            target_class="some.module.Person",
            edge_label="KNOWS",
        )
        assert r.edge_label == "KNOWS"
        assert r.direction == "outbound"
        assert r.uselist is True
        assert r.cache is True
        assert r.depth == 1

    def test_custom_params(self):
        r = Relationship(
            target_class="some.module.Company",
            edge_label="WORKS_AT",
            direction="inbound",
            uselist=False,
            cache=False,
            depth=3,
        )
        assert r.direction == "inbound"
        assert r.uselist is False
        assert r.cache is False
        assert r.depth == 3


class TestRelationshipFactory:
    def test_factory_defaults(self):
        r = relationship("some.module.Person", "KNOWS")
        assert isinstance(r, Relationship)
        assert r.edge_label == "KNOWS"
        assert r.direction == "outbound"
        assert r.uselist is True

    def test_factory_custom(self):
        r = relationship(
            "some.module.Company",
            "WORKS_AT",
            direction="outbound",
            uselist=False,
        )
        assert r.uselist is False
        assert r.direction == "outbound"

    def test_resolve_target_class_with_actual_class(self):
        from age_orm.models.vertex import Vertex

        class MyVertex(Vertex):
            __label__ = "Test"
            name: str

        r = Relationship(target_class=MyVertex, edge_label="TEST")
        resolved = r.resolve_target_class()
        assert resolved is MyVertex

    def test_resolve_target_class_with_string_fails_gracefully(self):
        r = Relationship(target_class="nonexistent.module.Class", edge_label="TEST")
        with pytest.raises(ImportError, match="Cannot resolve"):
            r.resolve_target_class()
