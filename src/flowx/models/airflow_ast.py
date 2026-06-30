"""Typed Apache Airflow AST nodes.

Mirrors :mod:`flowx.models.adf_ast` for a second source system.  An Airflow DAG is
imperative Python, so the loader consumes the *serialized* DAG JSON that Airflow
itself emits (``SerializedDAG.to_dict``) for the orchestration skeleton, plus the raw
``.py`` source for recovering ``python_callable`` / custom-operator bodies that the
serialized form only references by name.

The classification taxonomy (:class:`TranslationStrategy`) and the inventory
containers (:class:`Inventory`, :class:`InventoryItem`) are source-neutral and reused
verbatim from :mod:`flowx.models.adf_ast` so the discover-phase output schema and the
downstream tooling stay identical across source systems.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Reuse the source-neutral classification + inventory types.
from flowx.models.adf_ast import (  # noqa: F401  (re-exported for parity with adf_ast)
    Inventory,
    InventoryItem,
    TranslationStrategy,
)

# ---------------------------------------------------------------------------
# Task-level AST node
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AirflowTask:
    """Single Airflow task (an operator instance) within a DAG.

    Attributes:
        task_id: Task identifier, unique within the DAG.
        operator: Operator class name (e.g. ``"PythonOperator"``,
            ``"DatabricksRunNowOperator"``).
        operator_module: Fully-qualified module the operator class lives in
            (e.g. ``"airflow.operators.python"``), when known.
        template_fields: The operator's templated field values, keyed by field
            name (e.g. ``{"bash_command": "echo {{ ds }}"}``).  Carried through
            so the Jinja resolver and translators can read them.
        downstream_task_ids: ``task_id`` s this task points at (DAG edges as
            serialized by Airflow).
        upstream_task_ids: Inverse of ``downstream_task_ids``, computed by the
            loader so the translator can build IR ``depends_on`` edges (which are
            upstream-oriented) without re-walking the whole DAG.
        trigger_rule: Airflow trigger rule (``"all_success"``, ``"all_done"``,
            ``"one_failed"``, ...) used to derive the dependency outcome.
        retries: Retry limit, if declared.
        retry_delay_seconds: Delay between retries in seconds.
        execution_timeout_seconds: Per-task timeout in seconds.
        pool: Airflow pool name (informational; surfaced in SETUP notes).
        python_callable_name: For PythonOperator-family tasks, the qualified
            name of the callable the serialized DAG references.
        python_callable_source: Best-effort source code of ``python_callable_name``
            recovered from the raw ``.py`` DAG file, for the agentic converter.
        task_group: Owning TaskGroup id, if any.
        raw: The verbatim serialized task dict, retained so agentic handlers can
            translate directly from source.
    """

    task_id: str
    operator: str
    operator_module: str | None = None
    template_fields: dict[str, Any] = field(default_factory=dict)
    downstream_task_ids: list[str] = field(default_factory=list)
    upstream_task_ids: list[str] = field(default_factory=list)
    trigger_rule: str = "all_success"
    retries: int | None = None
    retry_delay_seconds: int | None = None
    execution_timeout_seconds: int | None = None
    pool: str | None = None
    python_callable_name: str | None = None
    python_callable_source: str | None = None
    task_group: str | None = None
    raw: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# DAG node
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AirflowDag:
    """Top-level Airflow DAG definition.

    Attributes:
        dag_id: DAG identifier (becomes the Databricks job name).
        tasks: Tasks that make up the DAG.
        schedule: Schedule expression as serialized by Airflow — a cron string,
            a preset (``"@daily"``), a ``timedelta`` descriptor, or ``None``.
        timezone: DAG timezone string, if declared.
        catchup: Whether Airflow backfills missed intervals.
        default_args: The DAG's ``default_args`` bag (retries, retry_delay, ...).
        params: DAG-level params (become job parameters).
        tags: DAG tags.
        source_file: Path to the raw ``.py`` DAG file, used to recover callable
            source for the agentic phase.
        raw: Verbatim serialized DAG dict, retained for provenance export.
    """

    dag_id: str
    tasks: list[AirflowTask]
    schedule: str | None = None
    timezone: str | None = None
    catchup: bool = False
    default_args: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    source_file: str | None = None
    raw: dict[str, Any] | None = None

    def task_by_id(self, task_id: str) -> AirflowTask | None:
        """Return the task with *task_id*, or ``None``."""
        for task in self.tasks:
            if task.task_id == task_id:
                return task
        return None


# ---------------------------------------------------------------------------
# Aggregate container
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AirflowDefinitions:
    """Complete set of Airflow definitions loaded from a source directory.

    Attributes:
        dags: All parsed DAG definitions.
        variables: Airflow Variables (name -> value), if a ``variables.json``
            export was provided.  Become job parameters / secret references.
        connections: Airflow Connections (conn_id -> definition), if a
            ``connections.json`` export was provided.  Become Databricks
            secret-scope references in the bundle's setup phase.
    """

    dags: list[AirflowDag]
    variables: dict[str, Any] = field(default_factory=dict)
    connections: dict[str, Any] = field(default_factory=dict)

    def get_dag(self, dag_id: str | None) -> AirflowDag | None:
        """Return the DAG with *dag_id*, or ``None``."""
        if not dag_id:
            return None
        for dag in self.dags:
            if dag.dag_id == dag_id:
                return dag
        return None
