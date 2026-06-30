---
name: flowx-airflow
description: >
  Migrate Apache Airflow DAGs to Databricks Lakeflow Jobs. Exports serialized DAGs, then
  runs the flowx discover → convert → package phases with --source airflow to produce a
  deployable Databricks Asset Bundle. Use for "migrate Airflow", "Airflow to Databricks",
  "convert DAG to Databricks job".
triggers:
  - "migrate airflow"
  - "airflow to databricks"
  - "convert airflow dag"
  - "airflow dag migration"
  - "translate airflow"
---

# Migrate Apache Airflow DAGs → Databricks Lakeflow Jobs

The Airflow source connector reuses the same three-phase flowx pipeline as the ADF path
(discover → convert → package). Every phase takes `--source airflow`; the package phase is
source-agnostic and unchanged. Output is a deployable Databricks Asset Bundle.

## Prerequisite — environment

Run the **`flowx-setup`** skill (or `bash <plugin_dir>/scripts/bootstrap.sh`) to create the
`.venv`. Run every command below with `PYTHONPATH=<plugin_dir>/src` and
`<plugin_dir>/.venv/bin/python`.

## Step 0 — Export the Airflow DAGs to serialized JSON

Airflow DAGs are imperative Python, so flowx consumes the *serialized* DAG JSON Airflow
itself emits, plus the raw `.py` for recovering `python_callable` bodies. In the user's
Airflow environment:

```bash
python <plugin_dir>/scripts/export_airflow_dags.py --output-dir ./airflow_export
```

This writes `airflow_export/dags_serialized/<dag_id>.json` and `airflow_export/dags/*.py`.
(Alternative with a running Airflow: the REST API `GET /api/v1/dags/{id}/details` + `/tasks`.)

## Step 1 — Discover

```bash
python -m flowx.adapter discover --source airflow \
  --source-dir ./airflow_export --output-dir ./flowx_output
```

Writes `metadata/inventory.json` classifying every task **deterministic** (mapped to a
built-in translator) or **agentic** (routed to an `airflow-to-databricks:*` converter
skill). Present the coverage summary to the user.

## Step 2 — Convert

```bash
python -m flowx.adapter convert --source airflow \
  --source-dir ./airflow_export --output-dir ./flowx_output
```

Translates DAGs to Databricks IR under `.work/`. Deterministic operators
(DatabricksRunNow/SubmitRun/Notebook, TriggerDagRun, SparkSubmit, Empty/Dummy) become job
tasks directly; code-bearing operators (PythonOperator, BashOperator, sensors, custom) are
emitted as placeholders with an agentic gap in `.work/gaps.json`.

### Resolve agentic gaps

For each gap, invoke the recommended converter skill — **airflow-python-converter**,
**airflow-operator-converter**, or **airflow-sensor-converter** — which reads the recovered
source and writes a result to `agentic_results/<task_key>.json`. Then merge:

```bash
python -m flowx.adapter convert --merge-agentic \
  --report ./flowx_output/.work/translation_report.json \
  --agentic-results ./flowx_output/agentic_results
```

## Step 3 — (optional) Configure compute, then Package

Optionally run `inspect` / `modify` to choose compute (serverless vs classic). Spark
python/jar tasks are bound to a classic job cluster automatically (they can't run
serverless). Then:

```bash
python -m flowx.adapter package --output-dir ./flowx_output --catalog <cat> --schema <schema>
```

Produces `databricks.yml`, `resources/<dag>.yml`, `src/notebooks/`, and `SETUP.md`.

## Step 4 — Validate & deploy

```bash
cd ./flowx_output && databricks bundle validate -t dev && databricks bundle deploy -t dev
```

## What maps to what

| Airflow | Databricks Lakeflow Job |
|---|---|
| DAG | Job |
| Task / operator | Job task |
| `>>` / upstream edges | task `depends_on` (outcome from `trigger_rule`) |
| `schedule_interval` (cron / preset) | job Quartz cron schedule |
| `params` / `var.value.*` | job parameters |
| `{{ ds }}` / `{{ execution_date }}` | `{{job.start_time.iso_date/iso_datetime}}` (approximation, noted in SETUP.md) |
| DatabricksRunNow/TriggerDagRun | `run_job_task` |
| SparkSubmit | `spark_python_task` / `spark_jar_task` (classic cluster) |
| PythonOperator / Bash / sensors / custom | agentic → generated notebook / trigger |
