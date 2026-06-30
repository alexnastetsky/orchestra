"""Unit tests for the Airflow serialized-DAG loader and operator classification."""

from __future__ import annotations

from pathlib import Path

from flowx.models.adf_ast import TranslationStrategy
from flowx.parser.airflow_loader import (
    build_inventory,
    classify_operator,
    load_airflow_definitions,
)

AIRFLOW_FIXTURES = Path(__file__).parent.parent / "resources" / "airflow"


def test_loads_dag_skeleton():
    definitions = load_airflow_definitions(AIRFLOW_FIXTURES)
    assert len(definitions.dags) == 1
    dag = definitions.dags[0]
    assert dag.dag_id == "example_etl"
    assert dag.schedule == "0 6 * * *"
    assert dag.params == {"env": "prod"}
    assert {t.task_id for t in dag.tasks} == {
        "start",
        "wait_for_file",
        "extract",
        "transform",
        "run_databricks_job",
        "branch",
        "notify",
        "cleanup",
    }


def test_upstream_edges_inverted_from_downstream():
    dag = load_airflow_definitions(AIRFLOW_FIXTURES).dags[0]
    extract = dag.task_by_id("extract")
    assert extract is not None
    assert set(extract.upstream_task_ids) == {"start", "wait_for_file"}


def test_recovers_python_callable_source():
    dag = load_airflow_definitions(AIRFLOW_FIXTURES).dags[0]
    extract = dag.task_by_id("extract")
    assert extract.python_callable_name == "extract_data"
    assert extract.python_callable_source is not None
    assert "def extract_data" in extract.python_callable_source


def test_default_args_propagate_to_tasks():
    dag = load_airflow_definitions(AIRFLOW_FIXTURES).dags[0]
    # default_args.retries=2 applies to tasks without an explicit override...
    assert dag.task_by_id("transform").retries == 2
    # ...and an explicit task-level retries wins.
    assert dag.task_by_id("extract").retries == 3


def test_classify_operator():
    assert classify_operator("DatabricksRunNowOperator") == (TranslationStrategy.DETERMINISTIC, None)
    assert classify_operator("EmptyOperator") == (TranslationStrategy.DETERMINISTIC, None)
    strategy, skill = classify_operator("PythonOperator")
    assert strategy is TranslationStrategy.AGENTIC
    assert skill == "airflow-to-databricks:airflow-python-converter"
    # Unknown sensor / custom operators still route to an agentic skill, never unsupported.
    assert classify_operator("S3KeySensor")[1] == "airflow-to-databricks:airflow-sensor-converter"
    assert classify_operator("MyCustomOperator")[1] == "airflow-to-databricks:airflow-operator-converter"


def test_build_inventory_counts():
    definitions = load_airflow_definitions(AIRFLOW_FIXTURES)
    inventory = build_inventory(definitions)
    assert inventory.pipeline_count == 1
    assert inventory.deterministic_count == 4  # start, transform, run_databricks_job, cleanup
    assert inventory.agentic_count == 4  # wait_for_file, extract, branch, notify
    assert inventory.unsupported_count == 0
