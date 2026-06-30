"""Translates Airflow EmptyOperator / DummyOperator -> a no-op WaitActivity(0) IR.

Empty operators are pure DAG-structure nodes (fan-in / fan-out markers). Databricks jobs
require every node to be a real task, so we preserve the edge by emitting a zero-second
wait — reusing the existing Wait preparer/codegen rather than inventing a new task type.
"""

from __future__ import annotations

from typing import Any

from flowx.models.airflow_ast import AirflowDefinitions, AirflowTask
from flowx.models.ir import Activity, TranslationContext, WaitActivity


def translate(
    task: AirflowTask,
    base_kwargs: dict[str, Any],
    context: TranslationContext,
    definitions: AirflowDefinitions,
) -> Activity:
    """Translate an EmptyOperator/DummyOperator into a zero-second wait task."""
    return WaitActivity(**base_kwargs, wait_time_seconds=0)
