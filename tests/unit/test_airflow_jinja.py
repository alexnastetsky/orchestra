"""Unit tests for the Airflow Jinja template resolver."""

from __future__ import annotations

from flowx.parser.jinja_resolver import resolve_template


def test_plain_string_is_literal():
    result = resolve_template("just a constant")
    assert result.kind == "literal"
    assert result.value == "just a constant"


def test_ds_maps_to_iso_date_dab_ref():
    result = resolve_template("{{ ds }}")
    assert result.kind == "dab_ref"
    assert result.value == "{{job.start_time.iso_date}}"
    assert result.notes  # approximation note recorded


def test_execution_date_maps_to_iso_datetime():
    result = resolve_template("{{ execution_date }}")
    assert result.kind == "dab_ref"
    assert result.value == "{{job.start_time.iso_datetime}}"


def test_embedded_macro_is_interpolated():
    result = resolve_template("s3://bucket/{{ ds }}/_SUCCESS")
    assert result.kind == "dab_ref"
    assert result.value == "s3://bucket/{{job.start_time.iso_date}}/_SUCCESS"


def test_params_reference_becomes_job_parameter():
    result = resolve_template("{{ params.env }}")
    assert result.kind == "dab_ref"
    assert result.value == "{{job.parameters.env}}"
    assert result.required_parameters == {"env": "{{job.parameters.env}}"}


def test_variable_reference_becomes_job_parameter_with_note():
    result = resolve_template("{{ var.value.region }}")
    assert result.value == "{{job.parameters.region}}"
    assert any("Variable" in note for note in result.notes)


def test_filtered_template_is_unresolved():
    result = resolve_template("{{ ds | ds_add(7) }}")
    assert result.kind == "unresolved"
    assert result.value == "{{ ds | ds_add(7) }}"


def test_connection_reference_is_unresolved_with_secret_note():
    result = resolve_template("{{ conn.my_db.host }}")
    assert result.kind == "unresolved"
    assert any("secret" in note.lower() for note in result.notes)
