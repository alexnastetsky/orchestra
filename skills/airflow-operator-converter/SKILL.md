---
name: airflow-operator-converter
description: >
  Convert a code-bearing or provider Airflow operator (BashOperator, SQL operators,
  KubernetesPodOperator, DockerOperator, EmailOperator, HTTP, or a custom operator) into a
  Databricks task. Invoked by the flowx Airflow convert phase for agentic gaps; emits a
  notebook plus an IR task dict for merge_agentic.
triggers:
  - "convert airflow operator"
  - "translate bash operator"
  - "airflow operator converter"
---

# Convert a generic Airflow operator → Databricks task

Resolves one agentic gap for an operator that isn't deterministically mappable. The gap
(`<output_dir>/.work/gaps.json`) carries `activity_name`, `activity_type`, and
`raw_definition` (the serialized task with its templated fields, and
`python_callable_source` when the operator subclasses carry one).

## Mapping guidance by operator family

- **BashOperator** → notebook task running the command via `%sh` (or a Python
  `subprocess` cell). Wire templated `{{ ... }}` segments the resolver lowered as
  `base_parameters`/widgets. Flag host-specific commands that won't exist on Databricks.
- **SQL operators** (`PostgresOperator`, `SnowflakeOperator`, `MySqlOperator`, ...) →
  a SQL task or a notebook running the `sql` against the appropriate connection. Map the
  Airflow connection to a Databricks connection / secret-scoped credentials.
- **KubernetesPodOperator / DockerOperator** → a notebook or python_wheel task running the
  containerized logic, or document that the image must be reproduced on Databricks compute.
- **EmailOperator / SlackAPIPostOperator** → a notebook that sends the notification, or map
  to the job's task/run notifications where possible.
- **SimpleHttpOperator** → reuse the HTTP/Web pattern (a notebook issuing the request).
- **Custom operators** → port the operator's `execute()` logic (in `python_callable_source`
  when recoverable) into a notebook.

## Output + return contract

Produce a Databricks notebook plus an IR task dict (usually a `NotebookActivity`), and write
`<output_dir>/agentic_results/<task_key>.json`:

```json
{ "activity_name": "<task_id>", "pipeline": "<dag_id>", "task": { ...IR task dict... } }
```

preserving the placeholder's `depends_on`. The convert phase merges via
`--merge-agentic` (see the airflow-python-converter skill). Surface connection→secret and
compute/library requirements as caveats for SETUP.md.
