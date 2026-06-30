---
name: airflow-python-converter
description: >
  Convert an Airflow PythonOperator / BranchPythonOperator / ShortCircuitOperator (or
  @task-decorated callable) into a Databricks notebook task. Invoked by the flowx Airflow
  convert phase for agentic gaps; reads the recovered python_callable source and emits a
  notebook plus an IR task dict for merge_agentic.
triggers:
  - "convert airflow python operator"
  - "translate python_callable"
  - "airflow python converter"
---

# Convert Airflow PythonOperator → Databricks notebook task

This skill resolves one agentic gap emitted by the Airflow convert phase
(`flowx.translator.airflow_engine`) for a Python-callable-bearing operator.

## Input

The gap appears in `<output_dir>/.work/gaps.json` (and the placeholder task in
`translation_report.json`). Each gap carries:

- `activity_name` — the Airflow `task_id`.
- `activity_type` — e.g. `PythonOperator`, `BranchPythonOperator`, `ShortCircuitOperator`.
- `raw_definition` — the serialized task, enriched with:
  - `python_callable_name` — the callable's name.
  - `python_callable_source` — the callable's source code, recovered from the raw `.py`.
  - templated `op_kwargs` / `op_args`.

## What to produce

1. A Databricks notebook (`# Databricks notebook source`) that reproduces the callable's
   behaviour:
   - Port the body of `python_callable_source` into the notebook.
   - Read each `op_kwargs` key from a widget: `dbutils.widgets.get("<key>")`. Where the
     value was a Jinja template the resolver already lowered (e.g.
     `{{job.start_time.iso_date}}` / `{{job.parameters.env}}`), wire it as a
     `base_parameter` so the job passes it in.
   - Replace `xcom_pull` / `xcom_push` with Databricks task values
     (`dbutils.jobs.taskValues.get/set`).
   - For **BranchPythonOperator / ShortCircuitOperator**: the callable returns the next
     `task_id`(s) / a bool. Model this as an **If/else condition task** instead — set a
     task value to the decision and translate to an `IfConditionActivity`-shaped task, or
     emit the branch decision as a task value the downstream `depends_on` outcomes gate on.

2. An IR task dict — a `NotebookActivity` (or condition task) shaped like the other tasks
   in `translation_report.json`: `{"task_key", "notebook_task"/"condition_task", ...}`,
   preserving the gap's `depends_on` from the placeholder it replaces.

## How to return it

Write one JSON file per gap to `<output_dir>/agentic_results/<task_key>.json`:

```json
{ "activity_name": "<task_id>", "pipeline": "<dag_id>", "task": { ...IR task dict... } }
```

Then the convert phase merges them:

```
python -m flowx.adapter convert --merge-agentic \
  --report <output_dir>/.work/translation_report.json \
  --agentic-results <output_dir>/agentic_results
```

## Notes / caveats to surface

- Flag Python dependencies the callable imports (they must be available on the task's
  compute — a serverless environment or a job cluster with libraries).
- Note any `xcom` semantics that don't map cleanly to task values (large payloads should
  go through a UC table/volume, not task values).
