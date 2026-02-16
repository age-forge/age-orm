"""Base model for all AGE graph entities (vertices and edges)."""

from __future__ import annotations

from typing import Any, ClassVar, Literal, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from age_orm.exceptions import DetachedInstanceError
from age_orm.references import Relationship

if TYPE_CHECKING:
    type IncEx = set[int] | set[str] | dict[int, Any] | dict[str, Any] | None

# Internal attributes stored in __dict__ via object.__setattr__, NOT declared
# as Pydantic fields or private attributes. This avoids Pydantic's
# __pydantic_private__ mechanism which conflicts with __getattribute__ overrides.
_INTERNAL_ATTRS = frozenset({
    "_age_graph_id", "_age_label", "_age_dirty", "_age_db", "_age_graph",
    "_age_fields", "_age_refs", "_age_refs_vals", "_age_relations",
})


class AgeModel(BaseModel):
    """Base for all graph entities (vertices and edges)."""

    model_config: ClassVar[dict] = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        ignored_types=(Relationship,),
    )

    __label__: ClassVar[str | None] = None
    _label_registry: ClassVar[dict[str, type["AgeModel"]]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        label = getattr(cls, "__label__", None)
        if label is not None:
            AgeModel._label_registry[label] = cls

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Separate data fields from relationship fields
        fields = {}
        refs = {}
        for fname, finfo in type(self).model_fields.items():
            if finfo.default.__class__ is Relationship:
                refs[fname] = finfo
                continue
            fields[fname] = finfo

        # Store internal state directly in __dict__, bypassing Pydantic
        _set = object.__setattr__
        _set(self, "_age_fields", fields)
        _set(self, "_age_refs", refs)
        _set(self, "_age_refs_vals", {})
        _set(self, "_age_graph_id", kwargs.get("_graph_id", None))
        _set(self, "_age_label", kwargs.get("_label", None) or self.__label__ or type(self).__name__)
        _set(self, "_age_db", kwargs.get("_db", None))
        _set(self, "_age_graph", kwargs.get("_graph", None))
        _set(self, "_age_relations", {})

        # Dirty tracking: new objects start fully dirty, DB-loaded objects start clean
        if kwargs.get("_db") is not None:
            _set(self, "_age_dirty", set())
        else:
            _set(self, "_age_dirty", set(fields.keys()))

    def __str__(self):
        return f"{type(self).__name__}({super().__str__()})"

    def __repr__(self):
        return self.__str__()

    def __setattr__(self, attr: str, value: Any):
        if attr in _INTERNAL_ATTRS:
            object.__setattr__(self, attr, value)
            return
        super().__setattr__(attr, value)
        if attr.startswith("_") or attr == "model_config":
            return
        refs = object.__getattribute__(self, "_age_refs")
        dirty = object.__getattribute__(self, "_age_dirty")
        if attr in type(self).model_fields and attr not in refs:
            dirty.add(attr)

    def __getattribute__(self, item: str):
        # Fast path for truly internal attrs stored in __dict__
        if item in _INTERNAL_ATTRS:
            return object.__getattribute__(self, item)

        # Fast path for private/dunder/model_ attributes
        if item.startswith("_") or item.startswith("model_"):
            return super().__getattribute__(item)

        # Check if this is a relationship field
        try:
            refs = object.__getattribute__(self, "_age_refs")
        except AttributeError:
            # Model not fully initialized yet
            return super().__getattribute__(item)

        if item not in refs:
            return super().__getattribute__(item)

        # Item is a relationship — return cached or lazy-load
        refs_vals = object.__getattribute__(self, "_age_refs_vals")
        if item in refs_vals:
            return refs_vals[item]

        db = object.__getattribute__(self, "_age_db")
        graph = object.__getattribute__(self, "_age_graph")
        if db is None or graph is None:
            raise DetachedInstanceError(
                f"Cannot load relationship '{item}': entity is not bound to a database/graph. "
                "Save the entity first or pass _db and _graph."
            )

        relationship: Relationship = refs[item].default
        target_class = relationship.resolve_target_class()
        target_label = getattr(target_class, "__label__", None) or target_class.__name__

        graph_id = object.__getattribute__(self, "_age_graph_id")
        dir_left = "<" if relationship.direction == "inbound" else ""
        dir_right = ">" if relationship.direction == "outbound" else ""

        depth_str = f"*1..{relationship.depth}" if relationship.depth > 1 else ""
        cypher = (
            f"MATCH (n){dir_left}-[:{relationship.edge_label}{depth_str}]-{dir_right}(m:{target_label}) "
            f"WHERE id(n) = {graph_id} RETURN m"
        )

        results = graph._execute_cypher(cypher, return_type="vertex")

        from age_orm.utils.serialization import dict_to_model

        models = [dict_to_model(r, target_class, db=db, graph=graph) for r in results]

        if relationship.uselist:
            r_val = models
        else:
            r_val = models[0] if models else None

        if relationship.cache:
            refs_vals[item] = r_val

        return r_val

    @property
    def graph_id(self) -> int | None:
        """AGE internal graph ID (read-only after creation)."""
        return object.__getattribute__(self, "_age_graph_id")

    @property
    def label(self) -> str:
        """The AGE label for this entity."""
        return object.__getattribute__(self, "_age_label") or self.__label__ or type(self).__name__

    @property
    def is_dirty(self) -> bool:
        return len(object.__getattribute__(self, "_age_dirty")) > 0

    @property
    def _label(self) -> str | None:
        return object.__getattribute__(self, "_age_label")

    @_label.setter
    def _label(self, value: str | None):
        object.__setattr__(self, "_age_label", value)

    @property
    def _dirty(self) -> set[str]:
        return object.__getattribute__(self, "_age_dirty")

    @_dirty.setter
    def _dirty(self, value: set[str]):
        object.__setattr__(self, "_age_dirty", value)

    @property
    def _graph_id(self) -> int | None:
        return object.__getattribute__(self, "_age_graph_id")

    @_graph_id.setter
    def _graph_id(self, value: int | None):
        object.__setattr__(self, "_age_graph_id", value)

    @property
    def _db(self):
        return object.__getattribute__(self, "_age_db")

    @_db.setter
    def _db(self, value):
        object.__setattr__(self, "_age_db", value)

    @property
    def _graph(self):
        return object.__getattribute__(self, "_age_graph")

    @_graph.setter
    def _graph(self, value):
        object.__setattr__(self, "_age_graph", value)

    @property
    def _relations(self) -> dict:
        return object.__getattribute__(self, "_age_relations")

    @_relations.setter
    def _relations(self, value: dict):
        object.__setattr__(self, "_age_relations", value)

    @property
    def _fields(self) -> dict:
        return object.__getattribute__(self, "_age_fields")

    @property
    def _refs(self) -> dict:
        return object.__getattribute__(self, "_age_refs")

    @property
    def _refs_vals(self) -> dict:
        return object.__getattribute__(self, "_age_refs_vals")

    def model_dump(
        self,
        *,
        mode: Literal["json", "python"] | str = "python",
        include: "IncEx" = None,
        exclude: "IncEx" = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool = True,
    ) -> dict[str, Any]:
        # Always exclude relationship fields
        fields = object.__getattribute__(self, "_age_fields")
        exclude_fields: set[str] = set()
        for fname in type(self).model_fields:
            if fname not in fields:
                exclude_fields.add(fname)

        if exclude:
            exclude = set(exclude)  # type: ignore
            exclude.update(exclude_fields)
        else:
            exclude = exclude_fields

        return super().model_dump(
            mode=mode,
            include=include,
            exclude=exclude or None,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=warnings,
        )

    def dirty_fields_dump(self, mode: str = "json") -> dict[str, Any]:
        """Return only the dirty (modified) fields as a dict."""
        all_props = self.model_dump(mode=mode)
        dirty = object.__getattribute__(self, "_age_dirty")
        return {k: v for k, v in all_props.items() if k in dirty}
