"""Loads Apache Airflow serialized-DAG JSON exports and produces typed AST (``AirflowDefinitions``).

This is the Airflow analog of :mod:`flowx.parser.adf_loader`.  Airflow DAGs are imperative
Python, so the supported source layout is a directory containing:

- ``dags_serialized/*.json`` — the serialized DAG skeleton emitted by Airflow's own
  ``SerializedDAG.to_dict`` (see ``scripts/export_airflow_dags.py``).  This carries the
  task graph, operators, templated args, schedule, and dependencies.
- ``dags/*.py`` (optional) — the raw DAG source, used only to recover ``python_callable``
  / custom-operator bodies that the serialized form references by name.  The agentic
  converter consumes these.
- ``variables.json`` / ``connections.json`` (optional) — Airflow Variables / Connections
  exports, surfaced into the bundle's parameter + secret setup.

The discover-phase output (``metadata/inventory.json``) uses the **same schema** the ADF
loader writes, with the DAG id in the ``pipeline`` slot and the operator class in the
activity ``type`` slot, so the existing coverage/reporting tooling works unchanged.
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flowx.models.adf_ast import Inventory, InventoryItem, TranslationStrategy
from flowx.models.airflow_ast import AirflowDag, AirflowDefinitions, AirflowTask

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Operator classification registries
# ---------------------------------------------------------------------------

# Operators that map structurally onto an existing IR Activity subclass without needing
# to read embedded user code (handled by deterministic translators).
DETERMINISTIC_OPERATORS: set[str] = {
    "DatabricksRunNowOperator",
    "DatabricksSubmitRunOperator",
    "DatabricksNotebookOperator",
    "DatabricksRunNowDeferrableOperator",
    "TriggerDagRunOperator",
    "SparkSubmitOperator",
    "EmptyOperator",
    "DummyOperator",
}

# Agentic skill namespace (mirrors the ADF ``adf-to-databricks:*`` convention).
_PYTHON_CONVERTER = "airflow-to-databricks:airflow-python-converter"
_OPERATOR_CONVERTER = "airflow-to-databricks:airflow-operator-converter"
_SENSOR_CONVERTER = "airflow-to-databricks:airflow-sensor-converter"

# Explicit operator -> agentic skill routing.  Anything not listed here and not
# deterministic falls through to a suffix-based heuristic (see ``_agentic_skill_for``),
# so custom operators still get an agentic conversion path rather than being dropped.
AGENTIC_OPERATORS: dict[str, str] = {
    "PythonOperator": _PYTHON_CONVERTER,
    "BranchPythonOperator": _PYTHON_CONVERTER,
    "ShortCircuitOperator": _PYTHON_CONVERTER,
    "PythonVirtualenvOperator": _PYTHON_CONVERTER,
    "_PythonDecoratedOperator": _PYTHON_CONVERTER,
    "BashOperator": _OPERATOR_CONVERTER,
    "KubernetesPodOperator": _OPERATOR_CONVERTER,
    "DockerOperator": _OPERATOR_CONVERTER,
    "EmailOperator": _OPERATOR_CONVERTER,
    "SlackAPIPostOperator": _OPERATOR_CONVERTER,
    "SimpleHttpOperator": _OPERATOR_CONVERTER,
}


def _agentic_skill_for(operator: str) -> str:
    """Return the agentic skill that should translate *operator*.

    Falls back on the operator-name suffix so custom and provider operators not in
    :data:`AGENTIC_OPERATORS` still route somewhere sensible: ``*Sensor`` to the sensor
    converter, ``*PythonOperator`` to the Python converter, everything else to the
    generic operator converter.
    """
    if operator in AGENTIC_OPERATORS:
        return AGENTIC_OPERATORS[operator]
    if operator.endswith("Sensor"):
        return _SENSOR_CONVERTER
    if "Python" in operator:
        return _PYTHON_CONVERTER
    return _OPERATOR_CONVERTER


def classify_operator(operator: str) -> tuple[TranslationStrategy, str | None]:
    """Classify an Airflow operator into a translation strategy.

    Args:
        operator: Operator class name (e.g. ``"PythonOperator"``).

    Returns:
        A ``(strategy, agentic_skill_name)`` tuple.  Because every Airflow operator
        carries recoverable Python source, unknown operators are classified
        :attr:`TranslationStrategy.AGENTIC` (via the generic converter) rather than
        unsupported — the agentic path always has something to translate.
    """
    if operator in DETERMINISTIC_OPERATORS:
        return TranslationStrategy.DETERMINISTIC, None
    return TranslationStrategy.AGENTIC, _agentic_skill_for(operator)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_airflow_definitions(source_dir: Path) -> AirflowDefinitions:
    """Load all Airflow serialized-DAG JSON files from *source_dir*.

    Args:
        source_dir: Root directory.  May contain a ``dags_serialized/`` subdir of
            serialized JSON, or serialized ``*.json`` files directly.  A sibling
            ``dags/`` directory of raw ``.py`` files (and optional
            ``variables.json`` / ``connections.json``) is picked up when present.

    Returns:
        Fully populated :class:`AirflowDefinitions`.
    """
    source_dir = Path(source_dir).resolve()

    serialized_dir = _find_dir(source_dir, "dags_serialized", "serialized_dags", "serialized")
    json_root = serialized_dir if serialized_dir is not None else source_dir

    raw_py_dir = _find_dir(source_dir, "dags", "dag_files")
    callable_index = _index_python_callables(raw_py_dir) if raw_py_dir is not None else {}

    dags: list[AirflowDag] = []
    for json_file in sorted(json_root.glob("*.json")):
        if json_file.name in ("variables.json", "connections.json"):
            continue
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            dag = _parse_serialized_dag(data, fallback_name=json_file.stem, callable_index=callable_index)
            if dag is not None:
                dags.append(dag)
        except Exception:
            logger.exception("Failed to parse serialized DAG file %s", json_file)

    variables = _load_optional_json(source_dir / "variables.json") or {}
    connections = _load_optional_json(source_dir / "connections.json") or {}

    return AirflowDefinitions(dags=dags, variables=variables, connections=connections)


def build_inventory(definitions: AirflowDefinitions) -> Inventory:
    """Walk all DAGs and classify every task.

    Args:
        definitions: Parsed Airflow definitions.

    Returns:
        :class:`Inventory` with one :class:`InventoryItem` per task.  The DAG id
        occupies the ``pipeline_name`` slot for parity with the ADF inventory.
    """
    items: list[InventoryItem] = []
    deterministic = agentic = unsupported = 0

    for dag in definitions.dags:
        for task in dag.tasks:
            strategy, skill = classify_operator(task.operator)
            items.append(
                InventoryItem(
                    pipeline_name=dag.dag_id,
                    activity_name=task.task_id,
                    activity_type=task.operator,
                    strategy=strategy,
                    agentic_skill=skill,
                    depends_on=list(task.upstream_task_ids) or None,
                )
            )
            if strategy is TranslationStrategy.DETERMINISTIC:
                deterministic += 1
            elif strategy is TranslationStrategy.AGENTIC:
                agentic += 1
            else:
                unsupported += 1

    return Inventory(
        items=items,
        deterministic_count=deterministic,
        agentic_count=agentic,
        unsupported_count=unsupported,
        pipeline_count=len(definitions.dags),
    )


# ---------------------------------------------------------------------------
# Internal helpers — parsing
# ---------------------------------------------------------------------------


def _find_dir(source_dir: Path, *candidate_names: str) -> Path | None:
    """Return the first existing subdirectory matching one of *candidate_names*."""
    for name in candidate_names:
        candidate = source_dir / name
        if candidate.is_dir():
            return candidate
    lowered = {name.lower() for name in candidate_names}
    if source_dir.is_dir():
        for child in source_dir.iterdir():
            if child.is_dir() and child.name.lower() in lowered:
                return child
    return None


def _load_optional_json(path: Path) -> Any | None:
    """Load *path* as JSON, returning ``None`` if it is missing or unreadable."""
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Could not parse optional JSON file %s", path)
        return None


def _parse_serialized_dag(
    data: dict[str, Any],
    *,
    fallback_name: str,
    callable_index: dict[str, str],
) -> AirflowDag | None:
    """Parse one serialized-DAG payload into an :class:`AirflowDag`.

    Tolerates both the ``{"dag": {...}}`` wrapper Airflow's ``SerializedDAG.to_dict``
    emits and a bare DAG dict, and both the underscore-prefixed serialized keys
    (``_dag_id``, ``_task_type``) and their plain forms.
    """
    dag_dict = data.get("dag", data) if isinstance(data, dict) else {}
    if not isinstance(dag_dict, dict):
        return None

    dag_id = dag_dict.get("_dag_id") or dag_dict.get("dag_id") or fallback_name
    default_args = _coerce_dict(dag_dict.get("default_args"))
    raw_tasks = dag_dict.get("tasks") or []

    tasks: list[AirflowTask] = []
    for raw_task in raw_tasks:
        task = _parse_serialized_task(raw_task, default_args=default_args, callable_index=callable_index)
        if task is not None:
            tasks.append(task)

    _populate_upstream(tasks)

    return AirflowDag(
        dag_id=dag_id,
        tasks=tasks,
        schedule=_extract_schedule(dag_dict),
        timezone=_extract_timezone(dag_dict),
        catchup=bool(dag_dict.get("catchup", False)),
        default_args=default_args,
        params=_extract_params(dag_dict.get("params")),
        tags=list(dag_dict.get("tags") or []),
        source_file=dag_dict.get("fileloc") or dag_dict.get("_processor_dags_folder"),
        raw=data,
    )


def _parse_serialized_task(
    raw_task: Any,
    *,
    default_args: dict[str, Any],
    callable_index: dict[str, str],
) -> AirflowTask | None:
    """Parse a single serialized task dict into an :class:`AirflowTask`."""
    if not isinstance(raw_task, dict):
        return None

    task_id = raw_task.get("task_id") or raw_task.get("_task_id") or "unnamed"
    operator = raw_task.get("_task_type") or raw_task.get("task_type") or raw_task.get("operator_class") or "Unknown"
    operator_module = raw_task.get("_task_module") or raw_task.get("task_module")

    template_field_names = raw_task.get("template_fields") or []
    template_fields: dict[str, Any] = {}
    for field_name in template_field_names:
        if field_name in raw_task:
            template_fields[field_name] = raw_task[field_name]
    # Some serializers inline the rendered values under "template_fields_renders" / op_kwargs.
    for extra_key in ("op_kwargs", "op_args", "bash_command", "sql", "application", "notebook_task"):
        if extra_key in raw_task and extra_key not in template_fields:
            template_fields[extra_key] = raw_task[extra_key]

    python_callable_name = _extract_callable_name(raw_task)
    python_callable_source = callable_index.get(python_callable_name) if python_callable_name else None

    return AirflowTask(
        task_id=task_id,
        operator=operator,
        operator_module=operator_module,
        template_fields=template_fields,
        downstream_task_ids=list(raw_task.get("downstream_task_ids") or []),
        trigger_rule=raw_task.get("trigger_rule") or default_args.get("trigger_rule") or "all_success",
        retries=_coerce_int(raw_task.get("retries", default_args.get("retries"))),
        retry_delay_seconds=_coerce_seconds(raw_task.get("retry_delay", default_args.get("retry_delay"))),
        execution_timeout_seconds=_coerce_seconds(raw_task.get("execution_timeout")),
        pool=raw_task.get("pool"),
        python_callable_name=python_callable_name,
        python_callable_source=python_callable_source,
        task_group=raw_task.get("task_group") or raw_task.get("_task_group"),
        raw=raw_task,
    )


def _populate_upstream(tasks: list[AirflowTask]) -> None:
    """Fill each task's ``upstream_task_ids`` by inverting ``downstream_task_ids``."""
    upstream: dict[str, list[str]] = {task.task_id: [] for task in tasks}
    for task in tasks:
        for downstream_id in task.downstream_task_ids:
            if downstream_id in upstream:
                upstream[downstream_id].append(task.task_id)
    for task in tasks:
        task.upstream_task_ids = sorted(upstream.get(task.task_id, []))


def _extract_callable_name(raw_task: dict[str, Any]) -> str | None:
    """Best-effort extraction of the python_callable name from a serialized task."""
    callable_ref = raw_task.get("python_callable") or raw_task.get("python_callable_name")
    if isinstance(callable_ref, str):
        return callable_ref.rsplit(".", 1)[-1]
    if isinstance(callable_ref, dict):
        name = callable_ref.get("__var") or callable_ref.get("name") or callable_ref.get("_name")
        if isinstance(name, str):
            return name.rsplit(".", 1)[-1]
    return None


def _index_python_callables(dags_dir: Path) -> dict[str, str]:
    """Index top-level + nested function defs across all ``.py`` files in *dags_dir*.

    Returns a mapping of function name -> source code, used to recover
    ``python_callable`` bodies the serialized DAG only references by name.
    """
    index: dict[str, str] = {}
    for py_file in sorted(dags_dir.rglob("*.py")):
        try:
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except Exception:
            logger.warning("Could not parse Python DAG file %s for callable recovery", py_file)
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                try:
                    index.setdefault(node.name, ast.get_source_segment(source, node) or "")
                except Exception:
                    continue
    return index


def _extract_schedule(dag_dict: dict[str, Any]) -> str | None:
    """Extract a schedule expression from a serialized DAG dict.

    Handles ``schedule_interval`` (older) and ``timetable`` (newer), reducing common
    timetable encodings to their cron / preset string.
    """
    schedule = dag_dict.get("schedule_interval")
    if isinstance(schedule, str):
        return schedule
    if isinstance(schedule, dict):
        # CronTriggerTimetable / cron-based interval encodings carry the cron under value/expression.
        value = schedule.get("value") or schedule.get("expression") or schedule.get("__var")
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            inner = value.get("expression") or value.get("cron")
            if isinstance(inner, str):
                return inner
    timetable = dag_dict.get("timetable")
    if isinstance(timetable, dict):
        var = timetable.get("__var") or timetable
        if isinstance(var, dict):
            expr = var.get("expression") or var.get("cron")
            if isinstance(expr, str):
                return expr
    return None


def _extract_timezone(dag_dict: dict[str, Any]) -> str | None:
    """Extract the DAG timezone string, if present."""
    tz = dag_dict.get("timezone")
    if isinstance(tz, str):
        return tz
    if isinstance(tz, dict):
        name = tz.get("__var") or tz.get("name")
        if isinstance(name, str):
            return name
    return None


def _extract_params(raw_params: Any) -> dict[str, Any]:
    """Reduce a serialized ``params`` bag to a flat name -> default-value map."""
    if not isinstance(raw_params, dict):
        return {}
    result: dict[str, Any] = {}
    for name, value in raw_params.items():
        if isinstance(value, dict):
            # Serialized Param objects nest the default under "value"/"default"/__var.
            result[name] = value.get("value", value.get("default", value.get("__var")))
        else:
            result[name] = value
    return result


def _coerce_dict(value: Any) -> dict[str, Any]:
    """Return *value* as a dict, unwrapping the serialized ``{"__var": {...}}`` shape."""
    if isinstance(value, dict):
        if "__var" in value and isinstance(value["__var"], dict):
            return value["__var"]
        return value
    return {}


def _coerce_int(value: Any) -> int | None:
    """Best-effort int coercion; returns ``None`` for missing/invalid values."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_seconds(value: Any) -> int | None:
    """Coerce a serialized duration to whole seconds.

    Airflow serializes ``timedelta`` as a number of seconds (float) or as a
    ``{"__type": "timedelta", "__var": <seconds>}`` envelope.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, dict):
        value = value.get("__var", value.get("seconds"))
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Serialisation helpers (inventory.json — identical schema to adf_loader)
# ---------------------------------------------------------------------------


def _inventory_to_dict(inventory: Inventory, source_dir: str) -> dict[str, Any]:
    """Serialise an :class:`Inventory` to the same dict schema the ADF loader emits."""
    pipeline_map: dict[str, list[dict[str, Any]]] = {}
    for item in inventory.items:
        entry: dict[str, Any] = {
            "name": item.activity_name,
            "type": item.activity_type,
            "strategy": item.strategy.value,
        }
        if item.agentic_skill:
            entry["skill"] = item.agentic_skill
        if item.depends_on:
            entry["depends_on"] = item.depends_on
        pipeline_map.setdefault(item.pipeline_name, []).append(entry)

    total = inventory.deterministic_count + inventory.agentic_count + inventory.unsupported_count
    coverage_pct = round((inventory.deterministic_count + inventory.agentic_count) / total * 100, 1) if total else 0.0

    return {
        "source_dir": source_dir,
        "source_type": "airflow",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pipelines": [{"name": name, "activities": acts} for name, acts in pipeline_map.items()],
        "summary": {
            "pipeline_count": inventory.pipeline_count,
            "activity_count": total,
            "deterministic_count": inventory.deterministic_count,
            "agentic_count": inventory.agentic_count,
            "unsupported_count": inventory.unsupported_count,
            "coverage_pct": coverage_pct,
        },
    }


# ---------------------------------------------------------------------------
# CLI entry point (parity with adf_loader.main)
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Discover-phase entry point for Airflow: load DAGs and build the inventory."""
    parser = argparse.ArgumentParser(description="Load Airflow serialized DAGs and build a translation inventory.")
    parser.add_argument(
        "--source-dir",
        required=True,
        type=Path,
        help="Root directory containing dags_serialized/*.json (and optionally dags/*.py).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./flowx_output"),
        help="Migration output directory; inventory.json is written into its metadata/ subfolder.",
    )
    parser.add_argument("--dag", "--pipeline", dest="dag", type=str, default=None, help="Filter to a single DAG by id.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    definitions = load_airflow_definitions(args.source_dir)
    logger.info("Loaded %d DAG(s) from %s", len(definitions.dags), args.source_dir)

    if args.dag:
        matched = [dag for dag in definitions.dags if dag.dag_id == args.dag]
        if not matched:
            available = ", ".join(dag.dag_id for dag in definitions.dags) or "(none)"
            logger.error("DAG %r not found. Available DAGs: %s", args.dag, available)
            return 1
        definitions = AirflowDefinitions(
            dags=matched,
            variables=definitions.variables,
            connections=definitions.connections,
        )

    inventory = build_inventory(definitions)

    metadata_dir = args.output_dir.resolve() / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    inventory_path = metadata_dir / "inventory.json"
    inventory_dict = _inventory_to_dict(inventory, str(args.source_dir))
    inventory_path.write_text(json.dumps(inventory_dict, indent=2), encoding="utf-8")
    logger.info("Wrote inventory to %s", inventory_path)

    summary = inventory_dict["summary"]
    print("\nAirflow Profile Summary")
    print("=======================")
    print(f"DAGs parsed:          {summary['pipeline_count']}")
    print(f"Total tasks:          {summary['activity_count']}")
    print("\nStrategy Breakdown:")
    print(f"  Deterministic:      {summary['deterministic_count']}")
    print(f"  Agentic:            {summary['agentic_count']}")
    print(f"  Unsupported:        {summary['unsupported_count']}")
    print(f"\nCoverage:             {summary['coverage_pct']}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
