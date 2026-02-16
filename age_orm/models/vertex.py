"""Vertex model for AGE graph vertices."""

from __future__ import annotations

from typing import ClassVar

from .base import AgeModel


class Vertex(AgeModel):
    """Base class for graph vertex models.

    Define vertex types by subclassing:

        class Person(Vertex):
            __label__ = "Person"
            name: str
            age: int
    """

    __label__: ClassVar[str | None] = None
