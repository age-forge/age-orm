"""Shared test fixtures."""

import pytest

from age_orm.models.vertex import Vertex
from age_orm.models.edge import Edge
from age_orm.references import relationship


# --- Test Model Definitions ---


class Person(Vertex):
    __label__ = "Person"
    name: str
    age: int
    email: str | None = None


class Company(Vertex):
    __label__ = "Company"
    name: str
    industry: str = "tech"


class Knows(Edge):
    __label__ = "KNOWS"
    since: int
    relationship_type: str = "friend"


class WorksAt(Edge):
    __label__ = "WORKS_AT"
    role: str
    start_year: int


# Model with relationships
class PersonWithRels(Vertex):
    __label__ = "PersonRel"
    name: str
    age: int
    friends: list["PersonWithRels"] = relationship(
        "tests.conftest.PersonWithRels", "KNOWS", direction="outbound"
    )
    employer: "Company" = relationship(
        "tests.conftest.Company", "WORKS_AT", direction="outbound", uselist=False
    )


# --- Fixtures ---


@pytest.fixture
def alice():
    return Person(name="Alice", age=30, email="alice@example.com")


@pytest.fixture
def bob():
    return Person(name="Bob", age=25)


@pytest.fixture
def knows_edge():
    return Knows(since=2020, relationship_type="colleague")


@pytest.fixture
def company():
    return Company(name="Acme Corp", industry="manufacturing")
