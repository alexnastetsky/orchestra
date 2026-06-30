"""Per-operator translators mapping Airflow tasks onto the source-neutral IR.

Each module exposes ``translate(task, base_kwargs, context, definitions) -> Activity``
mirroring the ADF activity translators, but consumes an :class:`AirflowTask` and resolves
Jinja templates via :mod:`flowx.parser.jinja_resolver`.  Operators that embed user code
(Python/Bash/sensors/custom) are not translated here — they are routed to the agentic
converter by :mod:`flowx.translator.airflow_engine`.
"""
