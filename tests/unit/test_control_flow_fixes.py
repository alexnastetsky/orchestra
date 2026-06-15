"""Regression tests for three ADF -> Databricks translation fixes.

1. ForEach inputs-bridge notebooks must carry the ``# Databricks notebook
   source`` marker or ``bundle validate`` rejects the notebook_task.
2. A join after an ``IfCondition`` must use ``run_if: NONE_FAILED`` so a
   failure in the *taken* branch propagates (``AT_LEAST_ONE_SUCCESS`` masks it).
3. Synthesised ``_init_<var>`` tasks that nothing references must be pruned.
"""

from __future__ import annotations

from orchestra.bundler.dab_writer import _rewrite_post_branch_dependencies
from orchestra.preparer.activity_preparers.for_each import _render_for_each_inputs_bridge
from orchestra.translator.engine import _prune_dead_variable_inits


# ---------------------------------------------------------------------------
# Bug 1 — inputs-bridge notebook header
# ---------------------------------------------------------------------------


def test_inputs_bridge_starts_with_notebook_source_marker() -> None:
    source = _render_for_each_inputs_bridge(
        notebook_code="['a']",
        imports=[],
        widget_names=[],
        value_key="items",
    )
    assert source.splitlines()[0] == "# Databricks notebook source"


def test_inputs_bridge_marker_precedes_imports() -> None:
    source = _render_for_each_inputs_bridge(
        notebook_code="[1, 2]",
        imports=["import json"],
        widget_names=["item"],
        value_key="items",
    )
    lines = source.splitlines()
    assert lines[0] == "# Databricks notebook source"
    # The bridge still publishes the computed value.
    assert "dbutils.jobs.taskValues.set(key='items', value=_bridge_value)" in source


# ---------------------------------------------------------------------------
# Bug 2 — IfCondition join run_if
# ---------------------------------------------------------------------------


def _condition_join_tasks() -> list[dict]:
    """A condition task, a true-branch terminal, and a join that depended on the
    condition plus an always-on sibling (mirrors a ForEach inputs-bridge)."""
    return [
        {"task_key": "CheckRunMode", "condition_task": {"op": "EQUAL_TO"}},
        {
            "task_key": "NotifyFull",
            "depends_on": [{"task_key": "CheckRunMode", "outcome": "true"}],
        },
        {"task_key": "items_bridge"},
        {
            "task_key": "ProcessItems",
            "depends_on": [
                {"task_key": "CheckRunMode"},  # outcome-less -> rewritten to terminals
                {"task_key": "items_bridge"},
            ],
        },
    ]


def test_condition_join_uses_none_failed_not_at_least_one_success() -> None:
    tasks = _condition_join_tasks()
    _rewrite_post_branch_dependencies(tasks)
    join = next(t for t in tasks if t["task_key"] == "ProcessItems")

    assert join["run_if"] == "NONE_FAILED"
    # The outcome-less condition edge is replaced by the branch terminal,
    # the always-on sibling is preserved.
    dep_keys = {d["task_key"] for d in join["depends_on"]}
    assert dep_keys == {"NotifyFull", "items_bridge"}


# ---------------------------------------------------------------------------
# Bug 3 — dead _init_<var> pruning
# ---------------------------------------------------------------------------


def test_prune_drops_init_when_explicit_setter_dominates() -> None:
    tasks = [
        {"task_key": "_init_runMode", "variable_name": "runMode", "variable_value": "full"},
        {"task_key": "SetRunMode", "variable_name": "runMode", "variable_value": "full"},
        {
            "task_key": "CheckRunMode",
            "condition_task": {"left": "{{tasks.SetRunMode.values.runMode}}", "right": "full"},
        },
    ]
    pruned = _prune_dead_variable_inits(tasks)
    keys = [t["task_key"] for t in pruned]
    assert "_init_runMode" not in keys
    assert keys == ["SetRunMode", "CheckRunMode"]


def test_prune_keeps_init_when_value_is_read() -> None:
    tasks = [
        {"task_key": "_init_runMode", "variable_name": "runMode", "variable_value": "full"},
        {
            "task_key": "CheckRunMode",
            "condition_task": {"left": "{{tasks._init_runMode.values.runMode}}", "right": "full"},
        },
    ]
    pruned = _prune_dead_variable_inits(tasks)
    assert any(t["task_key"] == "_init_runMode" for t in pruned)


def test_prune_keeps_init_when_referenced_by_depends_on() -> None:
    tasks = [
        {"task_key": "_init_runMode", "variable_name": "runMode", "variable_value": "full"},
        {"task_key": "Next", "depends_on": [{"task_key": "_init_runMode"}]},
    ]
    pruned = _prune_dead_variable_inits(tasks)
    assert any(t["task_key"] == "_init_runMode" for t in pruned)


def test_prune_noop_without_init_tasks() -> None:
    tasks = [{"task_key": "SetRunMode"}, {"task_key": "Next"}]
    assert _prune_dead_variable_inits(tasks) == tasks
