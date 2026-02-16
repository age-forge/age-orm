"""Event system for pre/post hooks on graph operations."""

from collections import defaultdict

_registrars: dict = defaultdict(lambda: defaultdict(list))


def dispatch(target, event: str, *args, **kwargs):
    """Fire given event for all registered handlers matching the target's type."""
    by_event = _registrars[event]
    for target_class in by_event:
        if isinstance(target, target_class):
            for fn in by_event[target_class]:
                fn(target, event, *args, **kwargs)


def listen(target, event: str | list[str], fn):
    """Register fn to listen for event(s) on target class."""
    events = [event] if isinstance(event, str) else event
    for ev in events:
        _registrars[ev][target].append(fn)


def listens_for(target, event: str | list[str]):
    """Decorator to register fn to listen for event(s) on target class."""

    def decorator(fn):
        listen(target, event, fn)
        return fn

    return decorator
