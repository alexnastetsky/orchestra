"""Resolves Apache Airflow Jinja templates into the source-neutral :class:`ExpressionResult`.

This is the Airflow analog of :mod:`flowx.parser.expression_parser` (which handles ADF
``@{...}`` expressions).  Airflow operators carry Jinja-templated fields such as
``"echo {{ ds }}"`` or ``"{{ params.env }}"``; this module lowers the common, safely
mappable templates to Databricks job dynamic-value references or job parameters and
returns the same :class:`ExpressionResult` taxonomy the rest of the pipeline consumes:

- ``literal`` — no templating, or fully reduced to a constant string.
- ``dab_ref`` — reduced to (or interpolated with) Databricks dynamic-value references
  such as ``{{job.start_time.iso_date}}`` / ``{{job.parameters.X}}``.
- ``unresolved`` — contains Jinja the resolver can't safely lower (filters, loops,
  arbitrary macros, connections); the caller routes these to the agentic path.

Datetime mappings are documented as approximations (Airflow's ``logical_date`` /
``ds`` are the *scheduled* interval, whereas ``job.start_time`` is the actual run
start) and surfaced via ``ExpressionResult.notes`` so the bundler lists them in SETUP.md.
"""

from __future__ import annotations

import re
from typing import Any

from flowx.models.ir import ExpressionResult

# A single ``{{ ... }}`` Jinja block.
_MACRO_RE = re.compile(r"\{\{\s*(.+?)\s*\}\}")

# Airflow date/datetime context vars -> Databricks dynamic-value reference.
# Date-like vars map to iso_date; datetime-like to iso_datetime.  All are approximations
# (scheduled interval vs. actual run start), flagged in notes.
_DATE_VARS: dict[str, str] = {
    "ds": "{{job.start_time.iso_date}}",
    "ds_nodash": "{{job.start_time.iso_date}}",
    "prev_ds": "{{job.start_time.iso_date}}",
    "next_ds": "{{job.start_time.iso_date}}",
}
_DATETIME_VARS: dict[str, str] = {
    "ts": "{{job.start_time.iso_datetime}}",
    "ts_nodash": "{{job.start_time.iso_datetime}}",
    "execution_date": "{{job.start_time.iso_datetime}}",
    "logical_date": "{{job.start_time.iso_datetime}}",
    "data_interval_start": "{{job.start_time.iso_datetime}}",
    "data_interval_end": "{{job.start_time.iso_datetime}}",
}

_APPROX_NOTE = (
    "Airflow schedule-time variable approximated by {ref}; "
    "Databricks job.start_time is the actual run start, not the scheduled interval."
)


def resolve_template(value: Any) -> ExpressionResult:
    """Resolve an Airflow Jinja-templated *value* to an :class:`ExpressionResult`.

    Non-string values are returned verbatim as literals (translators handle structured
    op_kwargs / lists directly).  Strings are scanned for ``{{ ... }}`` blocks:

    - none found -> ``literal``;
    - all blocks reducible -> ``dab_ref`` (single-block values become the bare ref;
      embedded blocks are substituted inline within the surrounding text);
    - any block irreducible -> ``unresolved`` (carrying the original text + notes).

    Args:
        value: The raw templated field value.

    Returns:
        Resolved :class:`ExpressionResult`.
    """
    if not isinstance(value, str):
        return ExpressionResult(kind="literal", value=value if isinstance(value, str) else str(value))

    blocks = _MACRO_RE.findall(value)
    if not blocks:
        return ExpressionResult(kind="literal", value=value, was_string_literal=True)

    notes: list[str] = []
    required_parameters: dict[str, str] = {}
    resolved_any_ref = False
    unresolved = False

    def _sub(match: re.Match[str]) -> str:
        nonlocal resolved_any_ref, unresolved
        token, note, params = _resolve_macro(match.group(1))
        if note:
            notes.append(note)
        if token is None:
            unresolved = True
            return match.group(0)
        resolved_any_ref = resolved_any_ref or token != match.group(0)
        required_parameters.update(params)
        return token

    substituted = _MACRO_RE.sub(_sub, value)

    if unresolved:
        return ExpressionResult(
            kind="unresolved",
            value=value,
            notes=notes or [f"Unresolved Airflow Jinja template: {value!r}"],
            required_parameters=required_parameters,
        )

    kind = "dab_ref" if resolved_any_ref else "literal"
    return ExpressionResult(
        kind=kind,
        value=substituted,
        notes=notes,
        required_parameters=required_parameters,
    )


def _resolve_macro(expr: str) -> tuple[str | None, str | None, dict[str, str]]:
    """Resolve a single Jinja expression (the text inside ``{{ }}``).

    Returns ``(token, note, required_parameters)`` where *token* is the replacement
    string (a Databricks dynamic-value ref or job-parameter ref), or ``None`` when the
    expression can't be safely lowered.
    """
    # Jinja filters (``{{ ds | foo }}``) can't be reproduced; treat as unresolved.
    if "|" in expr:
        return None, None, {}

    expr = expr.strip()

    if expr in _DATE_VARS:
        ref = _DATE_VARS[expr]
        return ref, _APPROX_NOTE.format(ref=ref), {}
    if expr in _DATETIME_VARS:
        ref = _DATETIME_VARS[expr]
        return ref, _APPROX_NOTE.format(ref=ref), {}

    # params.X  /  dag_run.conf.X  ->  job parameter
    param_match = re.fullmatch(r"(?:params|dag_run\.conf)(?:\.([A-Za-z_][A-Za-z0-9_]*)|\[['\"]([^'\"]+)['\"]\])", expr)
    if param_match:
        name = param_match.group(1) or param_match.group(2)
        ref = f"{{{{job.parameters.{name}}}}}"
        return ref, None, {name: ref}

    # var.value.X / var.json.X  ->  job parameter sourced from a migrated Airflow Variable
    var_match = re.fullmatch(r"var\.(?:value|json)\.([A-Za-z_][A-Za-z0-9_]*)", expr)
    if var_match:
        name = var_match.group(1)
        ref = f"{{{{job.parameters.{name}}}}}"
        note = f"Airflow Variable '{name}' migrated to job parameter '{name}'; set its value in the bundle."
        return ref, note, {name: ref}

    # conn.X... -> Databricks secret; can't inline, flag for manual setup.
    if expr.startswith("conn."):
        return None, f"Airflow connection reference '{{{{ {expr} }}}}' -> migrate to a Databricks secret.", {}

    return None, None, {}
