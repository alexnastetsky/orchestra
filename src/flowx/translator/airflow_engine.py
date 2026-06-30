"""Core translation engine for Airflow-DAG -> Databricks IR conversion.

The Airflow analog of :mod:`flowx.translator.engine`.  It dispatches each task to a
deterministic operator translator (registry below) or, for code-bearing / unknown
operators, emits a :class:`PlaceholderActivity` carrying the recommended agentic skill
and the recovered ``python_callable`` source so the agentic converter can fill it in.

Crucially it emits the *same* :class:`TranslationReport` / :class:`Pipeline` IR the ADF
engine produces, so the convert serialization, the package phase, the bundler, and the
reporting/validate modules are reused unchanged.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable

from flowx.models.adf_ast import TranslationStrategy
from flowx.models.airflow_ast import AirflowDag, AirflowDefinitions, AirflowTask
from flowx.models.ir import (
    Activity,
    AgenticGap,
    Dependency,
    Pipeline,
    PlaceholderActivity,
    TranslationContext,
    TranslationReport,
)
from flowx.parser.airflow_loader import classify_operator, load_airflow_definitions
from flowx.parser.jinja_resolver import resolve_template
from flowx.translator.airflow_translators import (
    databricks_notebook,
    databricks_run_now,
    databricks_submit_run,
    empty,
    spark_submit,
    trigger_dag_run,
)

# Reuse the source-neutral IR serialization + sanitization from the ADF engine so the
# convert artifacts (translation_report.json, per-pipeline IR) are byte-for-byte compatible.
from flowx.translator.engine import _pipeline_to_dict, _sanitize_task_key

logger = logging.getLogger(__name__)

AIRFLOW_TRANSLATOR_REGISTRY: dict[str, Callable[..., Activity]] = {
    "DatabricksRunNowOperator": databricks_run_now.translate,
    "DatabricksRunNowDeferrableOperator": databricks_run_now.translate,
    "DatabricksSubmitRunOperator": databricks_submit_run.translate,
    "DatabricksNotebookOperator": databricks_notebook.translate,
    "TriggerDagRunOperator": trigger_dag_run.translate,
    "SparkSubmitOperator": spark_submit.translate,
    "EmptyOperator": empty.translate,
    "DummyOperator": empty.translate,
}

# Airflow trigger_rule -> the single dependency outcome the IR/DAB edge encodes.
_TRIGGER_RULE_TO_OUTCOME: dict[str, str] = {
    "all_success": "Succeeded",
    "all_done": "Completed",
    "all_failed": "Failed",
    "one_success": "Succeeded",
    "one_failed": "Failed",
    "none_failed": "Succeeded",
    "none_failed_min_one_success": "Succeeded",
    "none_failed_or_skipped": "Succeeded",
    "none_skipped": "Succeeded",
    "always": "Completed",
}


def translate_dag(dag: AirflowDag, definitions: AirflowDefinitions) -> TranslationReport:
    """Translate an Airflow DAG into a Databricks pipeline IR.

    Args:
        dag: Parsed Airflow DAG AST.
        definitions: Full Airflow definitions (variables/connections for context).

    Returns:
        :class:`TranslationReport` containing the translated :class:`Pipeline` and any
        agentic gaps — the identical type the ADF engine returns.
    """
    context = TranslationContext(registry=MappingProxyType(AIRFLOW_TRANSLATOR_REGISTRY))

    translated: list[Activity] = []
    deterministic_count = agentic_count = unsupported_count = 0

    for task in _topological_visit(dag.tasks):
        activity = _dispatch_task(task, context, definitions)
        translated.append(activity)
        strategy, _ = classify_operator(task.operator)
        if strategy is TranslationStrategy.DETERMINISTIC:
            deterministic_count += 1
        elif strategy is TranslationStrategy.AGENTIC:
            agentic_count += 1
        else:
            unsupported_count += 1

    warnings: list[str] = []
    gaps = _collect_agentic_gaps(dag.tasks, warnings)

    parameter_entries = _build_parameter_entries(dag, gaps)
    schedule = _compile_dag_schedule(dag, warnings)

    pipeline_ir = Pipeline(
        name=dag.dag_id,
        parameters=parameter_entries or None,
        tasks=translated,
        tags={"source": "airflow", "dag": dag.dag_id},
        schedule=schedule,
    )

    return TranslationReport(
        pipeline=pipeline_ir,
        deterministic_count=deterministic_count,
        agentic_count=agentic_count,
        unsupported_count=unsupported_count,
        gaps=gaps,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Dispatch + gaps
# ---------------------------------------------------------------------------


def _dispatch_task(task: AirflowTask, context: TranslationContext, definitions: AirflowDefinitions) -> Activity:
    """Translate a single task via its deterministic translator, else a placeholder."""
    base_kwargs = _build_base_kwargs(task)

    translator = AIRFLOW_TRANSLATOR_REGISTRY.get(task.operator)
    if translator is not None:
        return translator(task, base_kwargs, context, definitions)

    _, skill = classify_operator(task.operator)
    # Enrich the raw definition with the recovered callable source so the agentic skill
    # can translate the actual code, not just the operator's name/reference.
    raw_definition: dict[str, Any] = dict(task.raw or {})
    if task.python_callable_source:
        raw_definition["python_callable_source"] = task.python_callable_source
    if task.python_callable_name:
        raw_definition["python_callable_name"] = task.python_callable_name

    return PlaceholderActivity(
        **base_kwargs,
        original_type=task.operator,
        comment=f"Agentic skill: {skill}" if skill else f"No translator for operator '{task.operator}'",
        agentic_skill=skill,
        raw_definition=raw_definition,
    )


def _collect_agentic_gaps(tasks: list[AirflowTask], warnings: list[str]) -> list[AgenticGap]:
    """Emit a gap for every agentic/unsupported task, carrying recoverable source."""
    gaps: list[AgenticGap] = []
    for task in tasks:
        strategy, skill = classify_operator(task.operator)
        if strategy is TranslationStrategy.DETERMINISTIC:
            continue
        raw_definition: dict[str, Any] = dict(task.raw or {})
        if task.python_callable_source:
            raw_definition["python_callable_source"] = task.python_callable_source
        if task.python_callable_name:
            raw_definition["python_callable_name"] = task.python_callable_name
        gaps.append(
            AgenticGap(
                activity_name=task.task_id,
                activity_type=task.operator,
                recommended_skill=skill,
                raw_definition=raw_definition,
            )
        )
        if strategy is TranslationStrategy.UNSUPPORTED:
            warnings.append(f"Task '{task.task_id}' (operator={task.operator}) has no translation path.")
    return gaps


# ---------------------------------------------------------------------------
# Common-field extraction
# ---------------------------------------------------------------------------


def _build_base_kwargs(task: AirflowTask) -> dict[str, Any]:
    """Extract the common Activity fields shared by every IR subclass."""
    outcome = _TRIGGER_RULE_TO_OUTCOME.get(task.trigger_rule, "Succeeded")
    depends_on: list[Dependency] | None = None
    if task.upstream_task_ids:
        depends_on = [Dependency(task_key=_sanitize_task_key(up), outcome=outcome) for up in task.upstream_task_ids]

    return {
        "name": task.task_id,
        "task_key": _sanitize_task_key(task.task_id),
        "description": None,
        "timeout_seconds": task.execution_timeout_seconds,
        "max_retries": task.retries,
        "min_retry_interval_millis": (task.retry_delay_seconds * 1000) if task.retry_delay_seconds else None,
        "depends_on": depends_on,
        "cluster": None,
        "existing_cluster_id": None,
    }


def _build_parameter_entries(dag: AirflowDag, gaps: list[AgenticGap]) -> list[dict[str, Any]]:
    """Build job parameter entries from DAG params + Jinja-referenced params/variables."""
    entries: dict[str, dict[str, Any]] = {}
    for name, default in dag.params.items():
        entries[name] = {"name": name, "type": "String"}
        if default is not None:
            entries[name]["default"] = default

    # Collect {{ params.X }} / {{ var.value.X }} references surfaced by the resolver so
    # every job-parameter reference in the IR has a matching declaration.
    for task in dag.tasks:
        for value in task.template_fields.values():
            for ref_name in _referenced_param_names(value):
                entries.setdefault(ref_name, {"name": ref_name, "type": "String"})

    return list(entries.values())


def _referenced_param_names(value: Any) -> set[str]:
    """Return job-parameter names a (possibly nested) templated value resolves to."""
    names: set[str] = set()
    if isinstance(value, str):
        result = resolve_template(value)
        names.update(result.required_parameters.keys())
    elif isinstance(value, dict):
        for inner in value.values():
            names |= _referenced_param_names(inner)
    elif isinstance(value, list):
        for inner in value:
            names |= _referenced_param_names(inner)
    return names


# ---------------------------------------------------------------------------
# Topological ordering (uses upstream edges)
# ---------------------------------------------------------------------------


def _topological_visit(tasks: list[AirflowTask]) -> list[AirflowTask]:
    """Return tasks in dependency-first order using their upstream edges."""
    if not tasks:
        return []

    by_id = {task.task_id: task for task in tasks}
    in_degree = {task.task_id: 0 for task in tasks}
    dependents: dict[str, list[str]] = defaultdict(list)

    for task in tasks:
        for upstream in task.upstream_task_ids:
            if upstream in by_id:
                in_degree[task.task_id] += 1
                dependents[upstream].append(task.task_id)

    queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
    result: list[AirflowTask] = []
    while queue:
        queue.sort()
        current = queue.pop(0)
        result.append(by_id[current])
        for dependent in dependents.get(current, []):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(result) < len(tasks):
        visited = {task.task_id for task in result}
        for task in tasks:
            if task.task_id not in visited:
                logger.warning("Cycle detected: task '%s' has unresolved dependencies.", task.task_id)
                result.append(task)
    return result


# ---------------------------------------------------------------------------
# Schedule compilation (Airflow cron/preset -> Databricks Quartz cron)
# ---------------------------------------------------------------------------

_PRESET_TO_QUARTZ: dict[str, str | None] = {
    "@once": None,
    "@hourly": "0 0 * * * ?",
    "@daily": "0 0 0 * * ?",
    "@midnight": "0 0 0 * * ?",
    "@weekly": "0 0 0 ? * 1",
    "@monthly": "0 0 0 1 * ?",
    "@quarterly": "0 0 0 1 1,4,7,10 ?",
    "@yearly": "0 0 0 1 1 ?",
    "@annually": "0 0 0 1 1 ?",
}


def _compile_dag_schedule(dag: AirflowDag, warnings: list[str]) -> dict[str, Any] | None:
    """Compile a DAG schedule into a Databricks ``schedule`` dict, or ``None``."""
    schedule = dag.schedule
    if not schedule:
        return None

    quartz = _airflow_schedule_to_quartz(schedule, warnings)
    if quartz is None:
        return None

    return {
        "quartz_cron_expression": quartz,
        "timezone_id": dag.timezone or "UTC",
        "pause_status": "UNPAUSED",
    }


def _airflow_schedule_to_quartz(schedule: str, warnings: list[str]) -> str | None:
    """Convert an Airflow schedule (preset or 5-field Unix cron) to a Quartz cron string.

    Returns ``None`` for schedules that have no cron equivalent (``@once``, timedelta
    intervals), recording a warning so the migration surfaces the manual step.
    """
    schedule = schedule.strip()
    if schedule in _PRESET_TO_QUARTZ:
        if _PRESET_TO_QUARTZ[schedule] is None:
            warnings.append(
                f"DAG schedule '{schedule}' has no recurring cron equivalent; configure the trigger manually."
            )
        return _PRESET_TO_QUARTZ[schedule]

    fields = schedule.split()
    if len(fields) != 5:
        warnings.append(
            f"DAG schedule {schedule!r} is not a 5-field cron (likely a timedelta); set the job trigger manually."
        )
        return None

    minute, hour, dom, month, dow = fields
    # Quartz requires exactly one of day-of-month / day-of-week to be '?'.
    if dom == "*" and dow == "*":
        dow = "?"
    elif dom != "*" and dow != "*":
        # Both constrained: Quartz disallows; keep day-of-month, drop dow with a warning.
        warnings.append(
            f"Cron {schedule!r} constrains both day-of-month and day-of-week; day-of-week dropped for Quartz."
        )
        dow = "?"
    elif dow != "*":
        dom = "?"

    return f"0 {minute} {hour} {dom} {month} {dow}"


# ---------------------------------------------------------------------------
# CLI entry point (convert phase for Airflow; parity with engine.main)
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Convert-phase entry point: translate Airflow DAGs to IR under ``.work/``."""
    parser = argparse.ArgumentParser(description="Translate Airflow DAGs to Databricks IR.")
    parser.add_argument("--source-dir", required=True, type=Path, help="Root dir with dags_serialized/*.json.")
    parser.add_argument("--output-dir", type=Path, default=Path("./flowx_output"), help="Migration output directory.")
    parser.add_argument(
        "--dag", "--pipeline", dest="dag", type=str, default=None, help="Translate only the named DAG (default: all)."
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    definitions = load_airflow_definitions(args.source_dir)
    logger.info("Loaded %d DAG(s) from %s", len(definitions.dags), args.source_dir)

    work_dir = args.output_dir.resolve() / ".work"
    work_dir.mkdir(parents=True, exist_ok=True)

    all_pipeline_dicts: list[dict[str, Any]] = []
    all_gaps: list[dict[str, Any]] = []
    totals = {"deterministic": 0, "agentic": 0, "unsupported": 0}

    for dag in definitions.dags:
        if args.dag and dag.dag_id != args.dag:
            continue
        report = translate_dag(dag, definitions)
        totals["deterministic"] += report.deterministic_count
        totals["agentic"] += report.agentic_count
        totals["unsupported"] += report.unsupported_count

        pipeline_dict = _pipeline_to_dict(report.pipeline)
        all_pipeline_dicts.append(pipeline_dict)
        (work_dir / f"{_sanitize_task_key(dag.dag_id)}.json").write_text(
            json.dumps(pipeline_dict, indent=2, default=str), encoding="utf-8"
        )
        all_gaps.extend(asdict(gap) for gap in report.gaps)
        for warning in report.warnings:
            logger.warning(warning)

    report_payload = all_pipeline_dicts[0] if len(all_pipeline_dicts) == 1 else {"pipelines": all_pipeline_dicts}
    (work_dir / "translation_report.json").write_text(
        json.dumps(report_payload, indent=2, default=str), encoding="utf-8"
    )
    if all_gaps:
        (work_dir / "gaps.json").write_text(json.dumps(all_gaps, indent=2, default=str), encoding="utf-8")

    print("\nAirflow Convert Summary")
    print("=======================")
    print(f"Deterministic: {totals['deterministic']}")
    print(f"Agentic:       {totals['agentic']}")
    print(f"Unsupported:   {totals['unsupported']}")
    print(f"Agentic gaps:  {len(all_gaps)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
