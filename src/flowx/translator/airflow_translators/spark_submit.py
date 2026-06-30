"""Translates Airflow SparkSubmitOperator -> SparkPythonActivity / SparkJarActivity IR."""

from __future__ import annotations

from typing import Any

from flowx.models.airflow_ast import AirflowDefinitions, AirflowTask
from flowx.models.ir import Activity, SparkJarActivity, SparkPythonActivity, TranslationContext
from flowx.translator.airflow_translators.resolve import FieldResolver, field_of


def translate(
    task: AirflowTask,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AirflowDefinitions,
) -> Activity:
    """Translate a SparkSubmitOperator, branching on the application artifact type.

    A ``.jar`` application maps to a Spark JAR task (with ``java_class`` as the main
    class); anything else is treated as a PySpark script.
    """
    resolver = FieldResolver()
    tf, raw = task.template_fields, task.raw

    application = resolver.field(field_of(tf, raw, "application") or "")
    parameters = resolver.sequence(field_of(tf, raw, "application_args")) or None

    if application.lower().endswith(".jar"):
        return SparkJarActivity(
            **resolver.apply(base_kwargs),
            main_class_name=field_of(tf, raw, "java_class") or "",
            parameters=parameters,
        )

    return SparkPythonActivity(
        **resolver.apply(base_kwargs),
        python_file=application,
        parameters=parameters,
    )
