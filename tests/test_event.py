"""Tests for the event dispatch system."""

import pytest

from age_orm.event import listen, listens_for, dispatch, _registrars
from age_orm.models.vertex import Vertex


class Person(Vertex):
    __label__ = "Person"
    name: str
    age: int


@pytest.fixture(autouse=True)
def clear_registrars():
    """Clear event registrars before each test."""
    _registrars.clear()
    yield
    _registrars.clear()


class TestListen:
    def test_listen_registers_handler(self):
        handler_called = []

        def handler(target, event, **kwargs):
            handler_called.append(event)

        listen(Person, "pre_add", handler)
        p = Person(name="Test", age=1)
        dispatch(p, "pre_add")
        assert handler_called == ["pre_add"]

    def test_listen_multiple_events(self):
        events_received = []

        def handler(target, event, **kwargs):
            events_received.append(event)

        listen(Person, ["pre_add", "post_add"], handler)
        p = Person(name="Test", age=1)
        dispatch(p, "pre_add")
        dispatch(p, "post_add")
        assert events_received == ["pre_add", "post_add"]

    def test_dispatch_wrong_type_not_called(self):
        called = []

        class Other(Vertex):
            __label__ = "Other"
            val: str

        def handler(target, event, **kwargs):
            called.append(True)

        listen(Other, "pre_add", handler)
        p = Person(name="Test", age=1)
        dispatch(p, "pre_add")
        assert called == []

    def test_dispatch_passes_kwargs(self):
        received_kwargs = {}

        def handler(target, event, **kwargs):
            received_kwargs.update(kwargs)

        listen(Person, "pre_add", handler)
        p = Person(name="Test", age=1)
        dispatch(p, "pre_add", graph="test_graph")
        assert received_kwargs["graph"] == "test_graph"


class TestListensFor:
    def test_decorator(self):
        called = []

        @listens_for(Person, "post_add")
        def on_post_add(target, event, **kwargs):
            called.append(target.name)

        p = Person(name="Alice", age=30)
        dispatch(p, "post_add")
        assert called == ["Alice"]

    def test_decorator_preserves_function(self):
        @listens_for(Person, "pre_delete")
        def my_handler(target, event, **kwargs):
            pass

        assert my_handler.__name__ == "my_handler"
