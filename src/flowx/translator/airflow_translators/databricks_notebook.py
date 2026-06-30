"""Translates Airflow DatabricksNotebookOperator -> NotebookActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.airflow_ast import AirflowDefinitions, AirflowTask
from flowx.models.ir import Activity, NotebookActivity, TranslationContext
from flowx.translator.airflow_translators.resolve import FieldResolver, field_of


def translate(
    task: AirflowTask,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AirflowDefinitions,
) -> Activity:
    """Translate a DatabricksNotebookOperator into a notebook task."""
    resolver = FieldResolver()
    tf, raw = task.template_fields, task.raw

    notebook_path = resolver.field(field_of(tf, raw, "notebook_path") or "")
    base_parameters = resolver.mapping(field_of(tf, raw, "notebook_params", "base_parameters"))

    return NotebookActivity(
        **resolver.apply(base_kwargs),
        notebook_path=notebook_path,
        base_parameters=base_parameters,
    )
