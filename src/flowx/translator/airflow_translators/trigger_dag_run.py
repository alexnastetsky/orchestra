"""Translates Airflow TriggerDagRunOperator -> RunJobActivity IR.

A triggered DAG becomes a triggered Databricks job (the target DAG migrates to its own
job of the same name); the operator's ``conf`` becomes the run-job parameters.
"""

from __future__ import annotations

from typing import Any

from flowx.models.airflow_ast import AirflowDefinitions, AirflowTask
from flowx.models.ir import Activity, RunJobActivity, TranslationContext
from flowx.translator.airflow_translators.resolve import FieldResolver, field_of


def translate(
    task: AirflowTask,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AirflowDefinitions,
) -> Activity:
    """Translate a TriggerDagRunOperator into a run-job task targeting the triggered DAG."""
    resolver = FieldResolver()
    tf, raw = task.template_fields, task.raw

    trigger_dag_id = field_of(tf, raw, "trigger_dag_id")
    conf = resolver.mapping(field_of(tf, raw, "conf"))

    return RunJobActivity(
        **resolver.apply(base_kwargs),
        job_name=str(trigger_dag_id) if trigger_dag_id else task.task_id,
        job_parameters=conf,
    )
