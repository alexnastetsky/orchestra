"""Translates Airflow DatabricksRunNowOperator -> RunJobActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.airflow_ast import AirflowDefinitions, AirflowTask
from flowx.models.ir import Activity, RunJobActivity, TranslationContext
from flowx.translator.airflow_translators.resolve import FieldResolver, field_of

_PARAM_KEYS = (
    "notebook_params",
    "python_params",
    "jar_params",
    "spark_submit_params",
    "job_parameters",
    "python_named_params",
)


def translate(
    task: AirflowTask,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AirflowDefinitions,
) -> Activity:
    """Translate a DatabricksRunNowOperator into a run-job task.

    The operator triggers an existing Databricks job by id/name; its ``*_params``
    bags become the run-job parameters.
    """
    resolver = FieldResolver()
    tf, raw = task.template_fields, task.raw

    job_id = field_of(tf, raw, "job_id")
    job_name = resolver.field(field_of(tf, raw, "job_name") or "") or task.task_id

    job_parameters: dict[str, Any] = {}
    for key in _PARAM_KEYS:
        params = resolver.mapping(field_of(tf, raw, key))
        if params:
            job_parameters.update(params)

    return RunJobActivity(
        **resolver.apply(base_kwargs),
        job_name=job_name,
        existing_job_id=str(job_id) if job_id is not None else None,
        job_parameters=job_parameters or None,
    )
