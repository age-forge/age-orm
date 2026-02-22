# age-orm

A Python ORM for Apache AGE, providing SQLAlchemy-like abstractions for graph database operations.

**Status:** v0.2.0 — Core implemented (models, CRUD, query builder, relationships, async)

## Features

### Core
- [x] Pydantic v2-based model definitions for vertices and edges
- [x] Graph, Vertex, and Edge abstractions
- [x] Automatic schema creation (labels, indexes)
- [x] CRUD operations (create, read, update, delete)
- [x] Connection pooling via psycopg3

### Query Building
- [x] Fluent Cypher query builder (filter, sort, limit, skip)
- [x] Safe parameter substitution
- [x] Raw Cypher support
- [x] Bulk mutations (update/delete via query)

### Advanced
- [x] Sync + Async support (psycopg3's unified API)
- [x] Relationship descriptors with lazy loading
- [x] Event system (pre/post hooks for add, update, delete)
- [x] Bulk import (direct SQL INSERT for performance)
- [x] Graph traversal helpers (expand, traverse)
- [x] Dirty tracking (only update changed fields)
- [ ] Migrations and schema versioning
- [ ] Integration with AGEFreighter for bulk loads

## Installation

```bash
# With uv
uv add age-orm

# With pip
pip install age-orm
```

Requires Python 3.12+ and a running Apache AGE instance.

## Quick Start

```python
from age_orm import Database, Vertex, Edge, relationship

# Define models
class Person(Vertex):
    __label__ = "Person"
    name: str
    age: int
    email: str | None = None

class Knows(Edge):
    __label__ = "KNOWS"
    since: int
    relationship_type: str = "friend"

# Connect
db = Database("postgresql://ageuser:agepassword@localhost:5433/agedb")
graph = db.graph("social", create=True)

# Create vertices
alice = Person(name="Alice", age=30)
bob = Person(name="Bob", age=25)
graph.add(alice)
graph.add(bob)

# Create edge
knows = Knows(since=2020)
graph.connect(alice, knows, bob)

# Query
people = graph.query(Person).filter("n.age > $min_age", min_age=20).sort("n.name").all()
alice = graph.query(Person).filter_by(name="Alice").one()

# Traverse
friends = graph.traverse(alice, "KNOWS", depth=2, target_class=Person)

# Raw Cypher (columns are returned with named keys)
results = graph.cypher(
    "MATCH (n:Person)-[:KNOWS]->(m) RETURN n.name, m.name",
    columns=["source", "target"]
)
# results[0] == {"source": "Alice", "target": "Bob"}

# Cleanup
db.close()
```

## Async Usage

```python
from age_orm import AsyncDatabase

async with AsyncDatabase("postgresql://...") as db:
    graph = await db.graph("social", create=True)
    alice = Person(name="Alice", age=30)
    await graph.add(alice)

    q = await graph.query(Person)
    people = await q.filter("n.age > $min", min=20).all()
```

## Relationships

```python
class Person(Vertex):
    __label__ = "Person"
    name: str
    friends: list["Person"] = relationship("Person", "KNOWS", direction="outbound")
    employer: "Company" = relationship("Company", "WORKS_AT", uselist=False)
```

Relationships are lazy-loaded on access when the entity is bound to a graph.

## Event Hooks

```python
from age_orm import listen, listens_for

@listens_for(Person, "pre_add")
def validate_person(target, event, **kwargs):
    if target.age < 0:
        raise ValueError("Age cannot be negative")
```

## Dependencies

- `psycopg[binary,pool] >= 3.2` — PostgreSQL driver with connection pooling
- `pydantic >= 2.3` — Data validation and models

## Project Structure

```
age_orm/
├── __init__.py           # Public API exports
├── exceptions.py         # Custom exceptions
├── event.py              # Event system (pre/post hooks)
├── database.py           # Connection + pool management
├── graph.py              # Graph class + CRUD + traversal
├── references.py         # Relationship descriptors
├── models/
│   ├── base.py           # AgeModel base class
│   ├── vertex.py         # Vertex model
│   └── edge.py           # Edge model
├── query/
│   └── builder.py        # Cypher query builder
└── utils/
    └── serialization.py  # Agtype serialization helpers
```

## License

MIT
