#!/usr/bin/env python3
"""Export Apache Airflow DAGs to the serialized-JSON layout the flowx Airflow connector reads.

Run this **inside your Airflow environment** (it imports your DAG files through Airflow's
own DagBag, so the same provider packages your DAGs import must be installed).  It does
NOT need a running scheduler or metadata database.

It produces, under ``--output-dir``:

    dags_serialized/<dag_id>.json   # SerializedDAG.to_dict(dag) -- the orchestration skeleton
    dags/<file>.py                  # a copy of each source DAG file (callable bodies)

Point the flowx Airflow discover phase at ``--output-dir``:

    python -m flowx.adapter discover --source airflow --source-dir <output-dir> --output-dir <migration-dir>

Alternative (no Airflow install): if you have a running Airflow 2.x+, its REST API exposes
the same structure at ``GET /api/v1/dags/{dag_id}/details`` plus ``/api/v1/dags/{dag_id}/tasks``;
save each response as ``dags_serialized/<dag_id>.json``.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export Airflow DAGs to flowx serialized-JSON layout.")
    parser.add_argument(
        "--dags-folder",
        type=Path,
        default=None,
        help="DAG folder to load (defaults to Airflow's configured dags_folder).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Destination directory; dags_serialized/ and dags/ are written here.",
    )
    parser.add_argument(
        "--dag-id",
        type=str,
        default=None,
        help="Export only this DAG id (default: all DAGs in the folder).",
    )
    args = parser.parse_args(argv)

    try:
        from airflow.models.dagbag import DagBag
        from airflow.serialization.serialized_objects import SerializedDAG
    except Exception as error:  # noqa: BLE001 - actionable message for users without Airflow installed
        print(
            "Could not import Airflow. Run this script inside an environment where Apache "
            f"Airflow and your DAGs' provider packages are installed.\n  ({error})",
            file=sys.stderr,
        )
        return 2

    serialized_dir = args.output_dir / "dags_serialized"
    raw_dir = args.output_dir / "dags"
    serialized_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    dagbag = DagBag(dag_folder=str(args.dags_folder) if args.dags_folder else None, include_examples=False)
    if dagbag.import_errors:
        for filename, message in dagbag.import_errors.items():
            print(f"WARNING: import error in {filename}: {message}", file=sys.stderr)

    exported = 0
    source_files: set[str] = set()
    for dag_id, dag in sorted(dagbag.dags.items()):
        if args.dag_id and dag_id != args.dag_id:
            continue
        try:
            blob = SerializedDAG.to_dict(dag)
        except Exception as error:  # noqa: BLE001
            print(f"WARNING: failed to serialize DAG {dag_id!r}: {error}", file=sys.stderr)
            continue
        (serialized_dir / f"{dag_id}.json").write_text(json.dumps(blob, indent=2, default=str), encoding="utf-8")
        exported += 1
        fileloc = getattr(dag, "fileloc", None)
        if fileloc:
            source_files.add(fileloc)

    # Copy the raw .py source so the agentic converter can recover python_callable bodies.
    for fileloc in sorted(source_files):
        src = Path(fileloc)
        if src.is_file():
            try:
                shutil.copy2(src, raw_dir / src.name)
            except Exception as error:  # noqa: BLE001
                print(f"WARNING: could not copy DAG source {src}: {error}", file=sys.stderr)

    print(f"Exported {exported} DAG(s) to {serialized_dir}")
    print(f"Copied {len(source_files)} DAG source file(s) to {raw_dir}")
    return 0 if exported else 1


if __name__ == "__main__":
    raise SystemExit(main())
