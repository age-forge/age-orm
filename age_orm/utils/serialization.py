"""Agtype serialization helpers for converting between Python/Pydantic and AGE's agtype format."""

from __future__ import annotations

import json
import re
from typing import Any, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from age_orm.models.base import AgeModel

T = TypeVar("T", bound="AgeModel")


def escape_agtype_string(s: str) -> str:
    """Escape a string for use inside an agtype JSON value.

    Handles backslashes, quotes, newlines, tabs, carriage returns,
    and control characters (0x00-0x1F).
    """
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\\", "\\\\")
    s = s.replace('"', '\\"')
    s = s.replace("\n", "\\n")
    s = s.replace("\r", "\\r")
    s = s.replace("\t", "\\t")
    # Escape remaining control characters
    result = []
    for c in s:
        code = ord(c)
        if code < 0x20 and c not in "\n\r\t":
            result.append(f"\\u{code:04x}")
        else:
            result.append(c)
    return "".join(result)


def escape_sql_literal(s: str) -> str:
    """Escape a string for use in a PostgreSQL SQL literal (double single quotes)."""
    return s.replace("'", "''")


def to_agtype_value(val: Any) -> str:
    """Convert a Python value to an agtype-compatible string representation."""
    if val is None:
        return "null"
    elif isinstance(val, bool):
        return "true" if val else "false"
    elif isinstance(val, int):
        return str(val)
    elif isinstance(val, float):
        return str(val)
    elif isinstance(val, str):
        return f'"{escape_agtype_string(val)}"'
    elif isinstance(val, list):
        items = ", ".join(to_agtype_value(v) for v in val)
        return f"[{items}]"
    elif isinstance(val, dict):
        items = ", ".join(f'"{k}": {to_agtype_value(v)}' for k, v in val.items())
        return "{" + items + "}"
    else:
        return f'"{escape_agtype_string(str(val))}"'


def to_agtype_properties(props: dict) -> str:
    """Convert a properties dict to an agtype object string: {key: value, ...}."""
    items = ", ".join(f'"{k}": {to_agtype_value(v)}' for k, v in props.items())
    return "{" + items + "}"


def format_cypher_value(val: Any) -> str:
    """Format a Python value for safe inline use in a Cypher query string.

    AGE's Cypher doesn't support $param bind variables natively,
    so values must be safely interpolated into the Cypher text.
    """
    if val is None:
        return "null"
    elif isinstance(val, bool):
        return "true" if val else "false"
    elif isinstance(val, int):
        return str(val)
    elif isinstance(val, float):
        return str(val)
    elif isinstance(val, str):
        escaped = val.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    elif isinstance(val, list):
        items = ", ".join(format_cypher_value(v) for v in val)
        return f"[{items}]"
    elif isinstance(val, dict):
        items = ", ".join(
            f"{k}: {format_cypher_value(v)}" for k, v in val.items()
        )
        return "{" + items + "}"
    else:
        return format_cypher_value(str(val))


def substitute_cypher_params(cypher: str, params: dict[str, Any] | None) -> str:
    """Replace $param placeholders in a Cypher string with formatted values.

    Parameters are referenced as $name in Cypher and replaced with safely
    formatted literal values.
    """
    if not params:
        return cypher
    result = cypher
    # Sort by key length descending to avoid partial replacements
    for key in sorted(params.keys(), key=len, reverse=True):
        pattern = re.compile(r"\$" + re.escape(key) + r"(?![a-zA-Z0-9_])")
        result = pattern.sub(format_cypher_value(params[key]), result)
    return result


def model_to_agtype(model: "AgeModel") -> str:
    """Serialize a model's properties to an agtype string."""
    props = model.model_dump(mode="json")
    return to_agtype_properties(props)


def model_to_cypher_properties(model: "AgeModel", only: set[str] | None = None) -> str:
    """Serialize model properties as inline Cypher map: {key: value, ...}.

    If `only` is given, only include those field names.
    """
    props = model.model_dump(mode="json")
    if only:
        props = {k: v for k, v in props.items() if k in only}
    items = ", ".join(f"{k}: {format_cypher_value(v)}" for k, v in props.items())
    return "{" + items + "}"


# --- Agtype result parsing ---

_AGTYPE_VERTEX_RE = re.compile(
    r"""(\w+)\[(\d+\.\d+)\]\{(.*)\}""", re.DOTALL
)
_AGTYPE_EDGE_RE = re.compile(
    r"""(\w+)\[(\d+\.\d+)\]\[(\d+\.\d+),(\d+\.\d+)\]\{(.*)\}""", re.DOTALL
)


def parse_agtype_vertex(agtype_str: str) -> dict:
    """Parse an agtype vertex string like: Label[graphid]{props} into a dict.

    Returns dict with keys: label, graph_id, properties.
    """
    m = _AGTYPE_VERTEX_RE.match(agtype_str.strip())
    if not m:
        raise ValueError(f"Cannot parse agtype vertex: {agtype_str!r}")
    label = m.group(1)
    graph_id = m.group(2)
    props_str = m.group(3)
    props = json.loads("{" + props_str + "}") if props_str.strip() else {}
    return {"label": label, "graph_id": int(graph_id.replace(".", "")), "properties": props}


def parse_agtype_edge(agtype_str: str) -> dict:
    """Parse an agtype edge string into a dict.

    Returns dict with keys: label, graph_id, start_id, end_id, properties.
    """
    m = _AGTYPE_EDGE_RE.match(agtype_str.strip())
    if not m:
        raise ValueError(f"Cannot parse agtype edge: {agtype_str!r}")
    label = m.group(1)
    graph_id = m.group(2)
    start_id = m.group(3)
    end_id = m.group(4)
    props_str = m.group(5)
    props = json.loads("{" + props_str + "}") if props_str.strip() else {}
    return {
        "label": label,
        "graph_id": int(graph_id.replace(".", "")),
        "start_id": int(start_id.replace(".", "")),
        "end_id": int(end_id.replace(".", "")),
        "properties": props,
    }


def dict_to_model(data: dict, model_class: type[T], db=None, graph=None) -> T:
    """Hydrate a model instance from a properties dict.

    Sets internal fields (_graph_id, _db, _graph) and marks the instance as clean.
    """
    instance = model_class(**data.get("properties", data))
    instance._graph_id = data.get("graph_id")
    instance._label = data.get("label", getattr(model_class, "__label__", None))
    instance._dirty = set()
    instance._db = db
    instance._graph = graph
    if "start_id" in data:
        instance._start_id = data["start_id"]
    if "end_id" in data:
        instance._end_id = data["end_id"]
    return instance
