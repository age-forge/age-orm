"""Edge model for AGE graph edges."""

from __future__ import annotations

from typing import ClassVar

from .base import AgeModel


class Edge(AgeModel):
    """Base class for graph edge models.

    Define edge types by subclassing:

        class Knows(Edge):
            __label__ = "KNOWS"
            since: int
    """

    __label__: ClassVar[str | None] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        object.__setattr__(self, "_age_start_id", kwargs.get("_start_id", None))
        object.__setattr__(self, "_age_end_id", kwargs.get("_end_id", None))

    @property
    def start_id(self) -> int | None:
        """Source vertex graph ID."""
        return object.__getattribute__(self, "_age_start_id")

    @property
    def end_id(self) -> int | None:
        """Target vertex graph ID."""
        return object.__getattribute__(self, "_age_end_id")

    @property
    def _start_id(self) -> int | None:
        return object.__getattribute__(self, "_age_start_id")

    @_start_id.setter
    def _start_id(self, value: int | None):
        object.__setattr__(self, "_age_start_id", value)

    @property
    def _end_id(self) -> int | None:
        return object.__getattribute__(self, "_age_end_id")

    @_end_id.setter
    def _end_id(self, value: int | None):
        object.__setattr__(self, "_age_end_id", value)
