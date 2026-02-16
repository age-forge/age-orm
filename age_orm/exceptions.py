"""Custom exceptions for age-orm."""


class AgeORMError(Exception):
    """Base exception for all age-orm errors."""


class GraphNotFoundError(AgeORMError):
    """Raised when a graph does not exist."""


class GraphExistsError(AgeORMError):
    """Raised when trying to create a graph that already exists."""


class LabelNotFoundError(AgeORMError):
    """Raised when a vertex/edge label does not exist."""


class DetachedInstanceError(AgeORMError):
    """Raised when accessing a relationship on an entity not bound to a database."""


class EntityNotFoundError(AgeORMError):
    """Raised when an entity lookup returns no results."""


class MultipleResultsError(AgeORMError):
    """Raised when a single-result query returns multiple results."""
