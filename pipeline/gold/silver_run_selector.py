# Gold must know which Silver run it should process.

# We will add support for:

# Default:
# Gold uses the latest completed Silver run

# Optional:
# Gold uses a specific Silver run




import json
from typing import Any, Dict, List

from pipeline.common.config import (
    MANIFEST_PREFIX,
    MINIO_BUCKET,
)


SILVER_JOB_NAME = "silver_field_observations"


def read_json_object(
    minio_client: Any,
    object_name: str,
) -> Dict[str, Any]:
    """
    Read one JSON object from MinIO.
    """

    response = minio_client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    try:
        content = response.read().decode("utf-8")
        return json.loads(content)

    finally:
        response.close()
        response.release_conn()


def list_completed_silver_manifests(
    minio_client: Any,
) -> List[str]:
    """
    Return all successfully completed Silver manifest paths.
    """

    prefix = (
        f"{MANIFEST_PREFIX}/"
        f"{SILVER_JOB_NAME}/"
    )

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    return sorted(
        obj.object_name
        for obj in objects
        if obj.object_name.endswith(
            "manifest_completed.json"
        )
    )


def list_completed_silver_runs(
    minio_client: Any,
) -> List[Dict[str, Any]]:
    """
    Return information about all completed Silver runs.
    """

    completed_runs = []

    manifest_objects = list_completed_silver_manifests(
        minio_client
    )

    for manifest_object in manifest_objects:
        manifest = read_json_object(
            minio_client=minio_client,
            object_name=manifest_object,
        )

        output_objects = manifest.get(
            "output_objects",
            [],
        )

        accepted_objects = [
            object_name
            for object_name in output_objects
            if (
                object_name.startswith(
                    "silver/accepted/"
                )
                and object_name.endswith(".parquet")
            )
        ]

        # Gold can only use a Silver run that produced
        # an accepted Parquet dataset.
        if not accepted_objects:
            continue

        metrics = manifest.get("metrics", {})

        completed_runs.append(
            {
                "silver_run_id": manifest.get(
                    "run_id"
                ),
                "completed_at": manifest.get(
                    "completed_at"
                ),
                "accepted_objects": accepted_objects,
                "manifest_object": manifest_object,
                "bronze_run_id": metrics.get(
                    "bronze_run_id"
                ),
                "accepted_records": metrics.get(
                    "accepted_records",
                    0,
                ),
                "quarantined_records": metrics.get(
                    "quarantined_records",
                    0,
                ),
                "forced_reprocessing": metrics.get(
                    "forced_reprocessing",
                    False,
                ),
            }
        )

    return sorted(
        completed_runs,
        key=lambda run: run["silver_run_id"] or "",
    )


def select_latest_silver_run(
    minio_client: Any,
) -> Dict[str, Any]:
    """
    Select the latest completed Silver run that has
    an accepted Parquet output.
    """

    completed_runs = list_completed_silver_runs(
        minio_client
    )

    if not completed_runs:
        raise RuntimeError(
            "No completed Silver run with an accepted "
            "Parquet dataset was found."
        )

    return completed_runs[-1]


def select_silver_run_by_id(
    minio_client: Any,
    silver_run_id: str,
) -> Dict[str, Any]:
    """
    Select one completed Silver run by its exact run ID.
    """

    completed_runs = list_completed_silver_runs(
        minio_client
    )

    for run in completed_runs:
        if run["silver_run_id"] == silver_run_id:
            return run

    raise RuntimeError(
        "No completed Silver run with an accepted "
        f"dataset was found for run ID: {silver_run_id}"
    )