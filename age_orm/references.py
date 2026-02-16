"""Relationship descriptors for defining graph relationships on models."""

from __future__ import annotations

from pydoc import locate
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from age_orm.models.base import AgeModel


class Relationship:
    """Stores metadata about a graph relationship for lazy loading.

    Used as a field default on model classes:

        class Person(Vertex):
            friends: list["Person"] = relationship("Person", "KNOWS")
    """

    def __init__(
        self,
        target_class: type["AgeModel"] | str,
        edge_label: str,
        direction: str = "outbound",
        uselist: bool = True,
        cache: bool = True,
        depth: int = 1,
    ):
        self._target_class = target_class
        self.edge_label = edge_label
        self.direction = direction  # "outbound", "inbound", "any"
        self.uselist = uselist
        self.cache = cache
        self.depth = depth

    def resolve_target_class(self) -> type["AgeModel"]:
        """Resolve string class reference to actual class."""
        if isinstance(self._target_class, str):
            resolved = locate(self._target_class)
            if resolved is None:
                raise ImportError(
                    f"Cannot resolve relationship target class: {self._target_class!r}"
                )
            self._target_class = resolved
        return self._target_class


def relationship(
    target_class: type["AgeModel"] | str,
    edge_label: str,
    direction: str = "outbound",
    uselist: bool = True,
    cache: bool = True,
    depth: int = 1,
) -> Any:
    """Define a graph relationship for lazy loading.

    Args:
        target_class: The target vertex model class (or fully qualified name string).
        edge_label: The AGE edge label to traverse.
        direction: "outbound", "inbound", or "any".
        uselist: If True, returns a list. If False, returns a single instance or None.
        cache: If True, caches the loaded result for future access.
        depth: Maximum traversal depth (default 1).

    Usage:
        class Person(Vertex):
            __label__ = "Person"
            name: str
            friends: list["Person"] = relationship("Person", "KNOWS", direction="outbound")
            employer: "Company" = relationship("Company", "WORKS_AT", uselist=False)
    """
    return Relationship(target_class, edge_label, direction, uselist, cache, depth)
