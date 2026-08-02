# What this file does

# It searches completed Silver manifests under:

# metadata/manifests/silver_field_observations/

# It checks whether their metrics contain:

# {
#   "bronze_run_id": "the-selected-bronze-run"
# }

# If a match exists, normal execution stops.


import json
from typing import Any, Dict, List

from pipeline.common.config import (
    MANIFEST_PREFIX,
    MINIO_BUCKET,
)


SILVER_JOB_NAME = "silver_field_observations"


def list_completed_silver_manifests(
    minio_client: Any,
) -> List[str]:
    """
    Return all completed Silver manifest object names.
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


def find_silver_runs_for_bronze_run(
    minio_client: Any,
    bronze_run_id: str,
) -> List[Dict[str, str]]:
    """
    Find successful Silver runs that processed a given Bronze run.
    """

    matching_runs = []

    manifest_objects = (
        list_completed_silver_manifests(
            minio_client
        )
    )

    for manifest_object in manifest_objects:
        manifest = read_json_object(
            minio_client=minio_client,
            object_name=manifest_object,
        )

        metrics = manifest.get("metrics", {})

        processed_bronze_run_id = metrics.get(
            "bronze_run_id"
        )

        if processed_bronze_run_id != bronze_run_id:
            continue

        matching_runs.append(
            {
                "silver_run_id": manifest.get(
                    "run_id",
                    "",
                ),
                "manifest_object": manifest_object,
                "completed_at": manifest.get(
                    "completed_at",
                    "",
                ),
            }
        )

    return matching_runs


def check_bronze_run_processing(
    minio_client: Any,
    bronze_run_id: str,
    force: bool,
) -> List[Dict[str, str]]:
    """
    Stop accidental reprocessing unless force=True.

    Returns previous matching Silver runs.
    """

    previous_runs = find_silver_runs_for_bronze_run(
        minio_client=minio_client,
        bronze_run_id=bronze_run_id,
    )

    if previous_runs and not force:
        previous_ids = [
            run["silver_run_id"]
            for run in previous_runs
        ]

        raise RuntimeError(
            "The selected Bronze run has already been "
            "processed successfully.\n"
            f"Bronze run: {bronze_run_id}\n"
            f"Previous Silver runs: {previous_ids}\n"
            "Use --force only when you intentionally want "
            "to process this Bronze run again."
        )

    return previous_runs