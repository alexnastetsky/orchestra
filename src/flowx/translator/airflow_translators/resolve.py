"""Shared Jinja-resolution helpers for the Airflow operator translators.

Wrap :func:`flowx.parser.jinja_resolver.resolve_template` and accumulate the job
parameters and approximation notes a resolved field implies, so each translator can
fold them into the IR ``Activity`` (``required_parameters`` drives DAB job-parameter
declarations; ``parameter_approximations`` is surfaced in SETUP.md).
"""

from __future__ import annotations

from typing import Any

from flowx.parser.jinja_resolver import resolve_template


class FieldResolver:
    """Resolves templated fields while collecting required params + approximation notes."""

    def __init__(self) -> None:
        self.required_parameters: dict[str, str] = {}
        self.approximations: list[dict[str, str]] = []

    def field(self, value: Any) -> Any:
        """Resolve a single field value, recording params/notes as a side effect."""
        if not isinstance(value, str):
            return value
        result = resolve_template(value)
        self.required_parameters.update(result.required_parameters)
        for note in result.notes:
            self.approximations.append({"raw_expression": value, "replacement": result.value, "note": note})
        return result.value

    def mapping(self, value: Any) -> dict[str, Any] | None:
        """Resolve every string value in a dict; non-dicts return ``None``."""
        if not isinstance(value, dict):
            return None
        resolved = {key: self.field(val) for key, val in value.items()}
        return resolved or None

    def sequence(self, value: Any) -> list[Any]:
        """Resolve every string element of a list; non-lists return ``[]``."""
        if not isinstance(value, list):
            return []
        return [self.field(item) for item in value]

    def apply(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Merge the collected params/notes into an Activity kwargs dict."""
        if self.required_parameters:
            kwargs["required_parameters"] = dict(self.required_parameters)
        if self.approximations:
            kwargs["parameter_approximations"] = list(self.approximations)
        return kwargs


def field_of(task_fields: dict[str, Any], raw: dict[str, Any] | None, *keys: str) -> Any:
    """Return the first present value among *keys*, preferring template_fields over raw."""
    raw = raw or {}
    for key in keys:
        if key in task_fields and task_fields[key] is not None:
            return task_fields[key]
        if key in raw and raw[key] is not None:
            return raw[key]
    return None
