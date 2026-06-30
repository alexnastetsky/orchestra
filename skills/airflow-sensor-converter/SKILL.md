---
name: airflow-sensor-converter
description: >
  Convert an Airflow sensor (S3KeySensor, FileSensor, ExternalTaskSensor, SqlSensor,
  PythonSensor, ...) into a Databricks trigger or polling task. Invoked by the flowx Airflow
  convert phase for agentic gaps; emits the appropriate IR (job file-arrival trigger or a
  polling notebook) for merge_agentic.
triggers:
  - "convert airflow sensor"
  - "translate sensor"
  - "airflow sensor converter"
---

# Convert an Airflow sensor → Databricks trigger or polling task

Resolves one agentic gap for a `*Sensor`. The gap (`<output_dir>/.work/gaps.json`) carries
`activity_name`, `activity_type`, and `raw_definition` (the sensor's templated config, e.g.
`bucket_key`, `filepath`, `external_dag_id`, `sql`).

## Mapping guidance — prefer native triggers over polling

- **S3KeySensor / GCSObjectSensor / FileSensor** that gate the *start* of the DAG → a
  Databricks **file-arrival trigger** on the job (`schedule`/`trigger.file_arrival` over a
  UC external location / volume). When the sensor gates a mid-DAG task instead, emit a
  polling notebook (loop with `time.sleep`, bounded by the sensor's `timeout`).
- **ExternalTaskSensor** (cross-DAG wait) → model as an ordering dependency: a **Run Job**
  task on the upstream DAG's migrated job, or document a `depends_on` across jobs.
- **SqlSensor / PythonSensor** → a polling notebook that evaluates the condition on an
  interval until true or `timeout`, then succeeds/fails the task.

## Decision: trigger vs. task

- If the sensor is a **root** task (no upstream) gating the whole DAG → recommend converting
  it to a job-level trigger and **dropping the task**, rewiring its downstream tasks to the
  trigger. Note this restructuring in the result so the merge/SETUP can reflect it.
- Otherwise → emit a `NotebookActivity` polling task preserving the placeholder's
  `depends_on`.

## Return contract

Write `<output_dir>/agentic_results/<task_key>.json`:

```json
{ "activity_name": "<task_id>", "pipeline": "<dag_id>", "task": { ...IR task dict... } }
```

(or, for the trigger case, a result describing the trigger + the tasks to rewire). Merge via
`--merge-agentic` (see the airflow-python-converter skill). Surface the polling interval,
timeout, and any external-location setup as SETUP.md caveats.
