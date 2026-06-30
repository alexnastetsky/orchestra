"""Unit tests for the Airflow translation engine (DAG -> IR)."""

from __future__ import annotations

from pathlib import Path

from flowx.models.ir import (
    PlaceholderActivity,
    RunJobActivity,
    SparkPythonActivity,
    WaitActivity,
)
from flowx.parser.airflow_loader import load_airflow_definitions
from flowx.translator.airflow_engine import translate_dag

AIRFLOW_FIXTURES = Path(__file__).parent.parent / "resources" / "airflow"


def _report():
    dag = load_airflow_definitions(AIRFLOW_FIXTURES).dags[0]
    return translate_dag(dag, load_airflow_definitions(AIRFLOW_FIXTURES))


def _task(report, key):
    return next(t for t in report.pipeline.tasks if t.task_key == key)


def test_pipeline_metadata():
    report = _report()
    assert report.pipeline.name == "example_etl"
    assert report.pipeline.tags == {"source": "airflow", "dag": "example_etl"}
    # cron 0 6 * * *  ->  Quartz with seconds + day-of-week '?'.
    assert report.pipeline.schedule["quartz_cron_expression"] == "0 0 6 * * ?"
    assert report.pipeline.schedule["timezone_id"] == "UTC"


def test_empty_operator_becomes_zero_wait():
    task = _task(_report(), "start")
    assert isinstance(task, WaitActivity)
    assert task.wait_time_seconds == 0


def test_spark_submit_becomes_spark_python_with_resolved_args():
    task = _task(_report(), "transform")
    assert isinstance(task, SparkPythonActivity)
    assert task.python_file == "/jobs/transform.py"
    assert task.parameters == ["--date", "{{job.start_time.iso_date}}"]
    assert task.depends_on[0].task_key == "extract"


def test_databricks_run_now_becomes_run_job():
    task = _task(_report(), "run_databricks_job")
    assert isinstance(task, RunJobActivity)
    assert task.existing_job_id == "4321"
    assert task.job_parameters == {"env": "{{job.parameters.env}}"}


def test_code_operators_become_agentic_placeholders():
    report = _report()
    for key, skill in [
        ("extract", "airflow-to-databricks:airflow-python-converter"),
        ("notify", "airflow-to-databricks:airflow-operator-converter"),
        ("wait_for_file", "airflow-to-databricks:airflow-sensor-converter"),
    ]:
        task = _task(report, key)
        assert isinstance(task, PlaceholderActivity)
        assert task.agentic_skill == skill
    # The PythonOperator placeholder carries recovered source for the agentic converter.
    extract_gap = next(g for g in report.gaps if g.activity_name == "extract")
    assert "def extract_data" in extract_gap.raw_definition["python_callable_source"]


def test_trigger_rule_maps_to_dependency_outcome():
    # notify has trigger_rule=all_done -> the dependency edge outcome is "Completed".
    task = _task(_report(), "notify")
    assert task.depends_on[0].outcome == "Completed"
