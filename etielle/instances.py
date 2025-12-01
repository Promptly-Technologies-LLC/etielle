from dataclasses import dataclass, field
from types import NoneType
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    TYPE_CHECKING,
    cast,
)
from typing import get_args, get_origin, Union as _Union

from .core import Transform, field_of


K = Tuple[Any, ...]
T = TypeVar("T", covariant=True)


# -----------------------------
# Merge policies
# -----------------------------


class MergePolicy:
    def merge(self, old: Any, new: Any) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError


class AddPolicy(MergePolicy):
    def merge(self, old: Any, new: Any) -> Any:
        return (old or 0) + (new or 0)


class AppendPolicy(MergePolicy):
    def merge(self, old: Any, new: Any) -> Any:
        base = [] if old is None else list(old)
        return base + ([] if new is None else [new])


class ExtendPolicy(MergePolicy):
    def merge(self, old: Any, new: Any) -> Any:
        base = [] if old is None else list(old)
        if new is None:
            return base
        if isinstance(new, (list, tuple)):
            return base + list(new)
        # Fallback: append single item
        return base + [new]


class MinPolicy(MergePolicy):
    def merge(self, old: Any, new: Any) -> Any:
        if old is None:
            return new
        if new is None:
            return old
        try:
            return new if new < old else old
        except Exception:
            # On incomparable types, prefer old to keep determinism
            return old


class MaxPolicy(MergePolicy):
    def merge(self, old: Any, new: Any) -> Any:
        if old is None:
            return new
        if new is None:
            return old
        try:
            return new if new > old else old
        except Exception:
            return old


class FirstNonNullPolicy(MergePolicy):
    def merge(self, old: Any, new: Any) -> Any:
        return old if old is not None else new


# -----------------------------
# Builder protocol
# -----------------------------


class InstanceBuilder(Generic[T]):
    def update(
        self, key: K, updates: Mapping[str, Any]
    ) -> None:  # pragma: no cover - interface only
        raise NotImplementedError

    def finalize_all(self) -> Dict[K, T]:  # pragma: no cover - interface only
        raise NotImplementedError

    def get(self, key: K) -> Optional[T]:  # pragma: no cover - interface only
        return None

    def errors(self) -> Dict[K, list[str]]:  # pragma: no cover - interface only
        # Back-compat alias for update_errors
        return self.update_errors()

    # Known field names for this builder (used for strict field checks and suggestions)
    def known_fields(self) -> set[str]:  # pragma: no cover - interface only
        return set()

    # Update-time errors (incremental validation)
    def update_errors(self) -> Dict[K, list[str]]:  # pragma: no cover - interface only
        return {}

    # Finalize-time errors (full validation)
    def finalize_errors(
        self,
    ) -> Dict[K, list[str]]:  # pragma: no cover - interface only
        return {}

    # Helpers to record errors from the executor (e.g., unknown fields)
    def record_update_error(
        self, key: K, reason: str
    ) -> None:  # pragma: no cover - interface only
        pass

    def record_finalize_error(
        self, key: K, reason: str
    ) -> None:  # pragma: no cover - interface only
        pass


@dataclass(frozen=True)
class FieldSpec(Generic[T]):
    # Accept string field name or callable selector (via field_of)
    selector: Union[str, Callable[[T], Any]]
    transform: Transform[Any]


@dataclass(frozen=True)
class InstanceEmit(Generic[T]):
    table: str
    join_keys: Sequence[Transform[Any]]
    fields: Sequence[FieldSpec[T]]
    builder: InstanceBuilder[T]
    policies: Mapping[str, MergePolicy] = field(default_factory=dict)
    strict_fields: bool = True
    allow_extras: bool = False
    # strictness mode: 'collect_all' (default) or 'fail_fast'
    strict_mode: str = "collect_all"
    # Field names that should be stored in shadow but NOT persisted to instances
    # Used for TempFields that are needed for relationship linking
    temp_fields: frozenset[str] = field(default_factory=frozenset)


# -----------------------------
# Pydantic-based builders (optional dependency)
# -----------------------------


if TYPE_CHECKING:
    try:
        pass  # noqa: F401
    except Exception:  # pragma: no cover - optional for type checking
        pass


def _import_pydantic():
    try:
        from pydantic import (
            BaseModel as PydBaseModel,
            TypeAdapter as PydTypeAdapter,
            create_model as pyd_create_model,
        )  # type: ignore

        return PydBaseModel, PydTypeAdapter, pyd_create_model
    except Exception:  # pragma: no cover - optional
        return None, None, None


class PydanticBuilder(InstanceBuilder[T]):
    def __init__(self, model: type[T]):
        self.model = model
        self.acc: Dict[K, Dict[str, Any]] = {}
        # Per-field adapters for incremental validation
        self._field_adapters: Dict[str, Any] = {}
        _BaseModel, _TypeAdapter, _create_model = _import_pydantic()
        if _TypeAdapter is not None and hasattr(model, "model_fields"):
            self._field_adapters = {
                name: _TypeAdapter(f.annotation)
                for name, f in getattr(model, "model_fields").items()
            }  # type: ignore[misc]
        self._update_errors: Dict[K, list[str]] = {}
        self._finalize_errors: Dict[K, list[str]] = {}

    def known_fields(self) -> set[str]:
        fields = getattr(self.model, "model_fields", None)
        if fields is None:
            return set()
        return set(fields.keys())

    def update(self, key: K, updates: Mapping[str, Any]) -> None:
        bucket = self.acc.setdefault(key, {})
        for k, v in updates.items():
            if k in self._field_adapters:
                try:
                    bucket[k] = self._field_adapters[k].validate_python(v)
                except Exception as e:  # pragma: no cover - defensive
                    self._update_errors.setdefault(key, []).append(f"field {k}: {e}")
                    bucket[k] = v
            else:
                bucket[k] = v

    def finalize_all(self) -> Dict[K, T]:
        # At finalize, rely on full model validation per-key; accumulate errors
        out: Dict[K, T] = {}
        for k, payload in self.acc.items():
            try:
                out[k] = cast(Any, self.model).model_validate(payload)
            except Exception as e:  # pragma: no cover - defensive
                self._finalize_errors.setdefault(k, []).append(str(e))
        return out

    def get(self, key: K) -> Optional[T]:
        return None

    def update_errors(self) -> Dict[K, list[str]]:
        return self._update_errors

    def finalize_errors(self) -> Dict[K, list[str]]:
        return self._finalize_errors

    def record_update_error(self, key: K, reason: str) -> None:
        self._update_errors.setdefault(key, []).append(reason)

    def record_finalize_error(self, key: K, reason: str) -> None:
        self._finalize_errors.setdefault(key, []).append(reason)


class PydanticPartialBuilder(InstanceBuilder[T]):
    def __init__(self, model: type[T]):
        _BaseModel, _TypeAdapter, _create_model = _import_pydantic()
        if _create_model is None:  # pragma: no cover - optional
            raise RuntimeError("pydantic is required for PydanticPartialBuilder")
        self.model = model
        # Build a Partial[T] with all fields optional
        partial_fields: Dict[str, tuple[Any, Any]] = {}
        for name, f in getattr(model, "model_fields").items():
            ann = f.annotation
            # If already Optional[...], keep as-is; else wrap in Optional
            if get_origin(ann) is _Union and type(None) in get_args(ann):
                optional_ann = ann
            else:
                # Build Optional[ann] dynamically as Union[ann, NoneType]
                optional_ann = Union[ann, NoneType]
            partial_fields[name] = (optional_ann, None)
        self.partial = _create_model(f"{model.__name__}Partial", **partial_fields)
        self.acc: Dict[K, Dict[str, Any]] = {}
        self._update_errors: Dict[K, list[str]] = {}
        self._finalize_errors: Dict[K, list[str]] = {}

    def known_fields(self) -> set[str]:
        fields = getattr(self.model, "model_fields", None)
        if fields is None:
            return set()
        return set(fields.keys())

    def update(self, key: K, updates: Mapping[str, Any]) -> None:
        bucket = self.acc.setdefault(key, {})
        merged = {**bucket, **updates}
        try:
            cast(Any, self.partial).model_validate(merged)
        except Exception as e:  # pragma: no cover - defensive
            self._update_errors.setdefault(key, []).append(str(e))
        bucket.update(updates)

    def finalize_all(self) -> Dict[K, T]:
        out: Dict[K, T] = {}
        for k, payload in self.acc.items():
            try:
                out[k] = cast(Any, self.model).model_validate(payload)
            except Exception as e:  # pragma: no cover - defensive
                self._finalize_errors.setdefault(k, []).append(str(e))
        return out

    def get(self, key: K) -> Optional[T]:
        return None

    def update_errors(self) -> Dict[K, list[str]]:
        return self._update_errors

    def finalize_errors(self) -> Dict[K, list[str]]:
        return self._finalize_errors

    def record_update_error(self, key: K, reason: str) -> None:
        self._update_errors.setdefault(key, []).append(reason)

    def record_finalize_error(self, key: K, reason: str) -> None:
        self._finalize_errors.setdefault(key, []).append(reason)


# -----------------------------
# TypedDict/Factory-based builder (no external deps)
# -----------------------------


class TypedDictBuilder(InstanceBuilder[T]):
    def __init__(
        self,
        factory: Callable[[Dict[str, Any]], T],
        *,
        field_type_checkers: Optional[Mapping[str, Callable[[Any], Any]]] = None,
    ) -> None:
        self.factory = factory
        self.acc: Dict[K, Dict[str, Any]] = {}
        self._update_errors: Dict[K, list[str]] = {}
        self._finalize_errors: Dict[K, list[str]] = {}
        # Optional per-field validators (callables that may raise)
        self._checkers: Mapping[str, Callable[[Any], Any]] = field_type_checkers or {}

    def update(self, key: K, updates: Mapping[str, Any]) -> None:
        bucket = self.acc.setdefault(key, {})
        for k, v in updates.items():
            checker = self._checkers.get(k)
            if checker is not None:
                try:
                    v = checker(v)
                except Exception as e:  # pragma: no cover - defensive
                    self._update_errors.setdefault(key, []).append(f"field {k}: {e}")
            bucket[k] = v

    def finalize_all(self) -> Dict[K, T]:
        out: Dict[K, T] = {}
        for k, payload in self.acc.items():
            try:
                out[k] = self.factory(payload)
            except Exception as e:  # pragma: no cover - defensive
                self._finalize_errors.setdefault(k, []).append(str(e))
        return out

    def get(self, key: K) -> Optional[T]:
        return None

    def known_fields(self) -> set[str]:
        # We only know about fields with explicit type checkers; treat others as allowed
        return set(self._checkers.keys())

    def update_errors(self) -> Dict[K, list[str]]:
        return self._update_errors

    def finalize_errors(self) -> Dict[K, list[str]]:
        return self._finalize_errors

    def record_update_error(self, key: K, reason: str) -> None:
        self._update_errors.setdefault(key, []).append(reason)

    def record_finalize_error(self, key: K, reason: str) -> None:
        self._finalize_errors.setdefault(key, []).append(reason)


class ConstructorBuilder(InstanceBuilder[T]):
    """
    Simplified builder for classes that accept keyword arguments in their constructor.
    Perfect for SQLAlchemy/SQLModel ORM models.

    Usage:
        builder = ConstructorBuilder(User)  # Just pass the class

    Equivalent to:
        builder = TypedDictBuilder(lambda d: User(**d))
    """

    def __init__(self, constructor: Callable[..., T]) -> None:
        self.constructor = constructor
        self.acc: Dict[K, Dict[str, Any]] = {}
        self._update_errors: Dict[K, list[str]] = {}
        self._finalize_errors: Dict[K, list[str]] = {}

    def update(self, key: K, updates: Mapping[str, Any]) -> None:
        bucket = self.acc.setdefault(key, {})
        bucket.update(updates)

    def finalize_all(self) -> Dict[K, T]:
        out: Dict[K, T] = {}
        for k, payload in self.acc.items():
            try:
                out[k] = self.constructor(**payload)
            except Exception as e:  # pragma: no cover - defensive
                self._finalize_errors.setdefault(k, []).append(str(e))
        return out

    def get(self, key: K) -> Optional[T]:
        return None

    def known_fields(self) -> set[str]:
        # No introspection, treat all fields as allowed
        return set()

    def update_errors(self) -> Dict[K, list[str]]:
        return self._update_errors

    def finalize_errors(self) -> Dict[K, list[str]]:
        return self._finalize_errors

    def record_update_error(self, key: K, reason: str) -> None:
        self._update_errors.setdefault(key, []).append(reason)

    def record_finalize_error(self, key: K, reason: str) -> None:
        self._finalize_errors.setdefault(key, []).append(reason)


# -----------------------------
# Helpers
# -----------------------------


def resolve_field_name_for_builder(
    builder: InstanceBuilder[T], spec: FieldSpec[T]
) -> str:
    if isinstance(spec.selector, str):
        return spec.selector
    # Try to resolve from typed selector via builder.model if available
    model = getattr(builder, "model", None)
    if model is None:
        raise ValueError(
            "Typed selector requires a builder with a 'model' attribute; pass a string field name instead"
        )
    return field_of(cast(Any, model), cast(Callable[[Any], Any], spec.selector))
