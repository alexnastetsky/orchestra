"""Translates Airflow DatabricksSubmitRunOperator -> the matching IR task.

DatabricksSubmitRunOperator submits a one-off run described by a task spec. We map by
the spec present: ``notebook_task`` -> NotebookActivity, ``spark_python_task`` ->
SparkPythonActivity, ``spark_jar_task`` -> SparkJarActivity, else a run of an existing
job by id.
"""

from __future__ import annotations

from typing import Any

from flowx.models.airflow_ast import AirflowDefinitions, AirflowTask
from flowx.models.ir import (
    Activity,
    NotebookActivity,
    RunJobActivity,
    SparkJarActivity,
    SparkPythonActivity,
    TranslationContext,
)
from flowx.translator.airflow_translators.resolve import FieldResolver, field_of


def translate(
    task: AirflowTask,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AirflowDefinitions,
) -> Activity:
    """Translate a DatabricksSubmitRunOperator by inspecting its run spec."""
    resolver = FieldResolver()
    tf, raw = task.template_fields, task.raw

    notebook_task = field_of(tf, raw, "notebook_task")
    if isinstance(notebook_task, dict):
        return NotebookActivity(
            **resolver.apply(base_kwargs),
            notebook_path=resolver.field(notebook_task.get("notebook_path", "")),
            base_parameters=resolver.mapping(notebook_task.get("base_parameters")),
        )

    spark_python_task = field_of(tf, raw, "spark_python_task")
    if isinstance(spark_python_task, dict):
        return SparkPythonActivity(
            **resolver.apply(base_kwargs),
            python_file=resolver.field(spark_python_task.get("python_file", "")),
            parameters=resolver.sequence(spark_python_task.get("parameters")) or None,
        )

    spark_jar_task = field_of(tf, raw, "spark_jar_task")
    if isinstance(spark_jar_task, dict):
        return SparkJarActivity(
            **resolver.apply(base_kwargs),
            main_class_name=spark_jar_task.get("main_class_name", ""),
            parameters=resolver.sequence(spark_jar_task.get("parameters")) or None,
        )

    job_id = field_of(tf, raw, "job_id")
    return RunJobActivity(
        **resolver.apply(base_kwargs),
        job_name=field_of(tf, raw, "run_name") or task.task_id,
        existing_job_id=str(job_id) if job_id is not None else None,
    )
