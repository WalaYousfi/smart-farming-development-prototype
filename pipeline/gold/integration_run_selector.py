import json
from typing import Any, Dict, List

from pipeline.common.config import (
    MANIFEST_PREFIX,
    MINIO_BUCKET,
)


INTEGRATION_JOB_NAME = (
    "silver_field_weather_integration"
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


def list_completed_integration_runs(
    minio_client: Any,
) -> List[Dict[str, Any]]:
    """
    Return completed integration runs that produced
    an integrated Silver Parquet dataset.
    """

    prefix = (
        f"{MANIFEST_PREFIX}/"
        f"{INTEGRATION_JOB_NAME}/"
    )

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    completed_runs = []

    for obj in objects:
        if not obj.object_name.endswith(
            "manifest_completed.json"
        ):
            continue

        manifest = read_json_object(
            minio_client=minio_client,
            object_name=obj.object_name,
        )

        output_objects = manifest.get(
            "output_objects",
            [],
        )

        integrated_objects = [
            object_name
            for object_name in output_objects
            if (
                object_name.startswith(
                    "silver/integrated/"
                    "field_weather_observations/"
                )
                and object_name.endswith(".parquet")
            )
        ]

        if not integrated_objects:
            continue

        metrics = manifest.get("metrics", {})

        completed_runs.append(
            {
                "integration_run_id": manifest.get(
                    "run_id"
                ),
                "completed_at": manifest.get(
                    "completed_at"
                ),
                "integrated_objects": (
                    integrated_objects
                ),
                "field_silver_run_id": metrics.get(
                    "field_silver_run_id"
                ),
                "weather_silver_run_id": metrics.get(
                    "weather_silver_run_id"
                ),
                "field_input_records": metrics.get(
                    "field_input_records",
                    0,
                ),
                "weather_input_records": metrics.get(
                    "weather_input_records",
                    0,
                ),
                "matched_field_records": metrics.get(
                    "matched_field_records",
                    0,
                ),
                "weather_match_rate": metrics.get(
                    "weather_match_rate",
                    0.0,
                ),
                "manifest_object": obj.object_name,
            }
        )

    return sorted(
        completed_runs,
        key=lambda run: (
            run["integration_run_id"] or ""
        ),
    )


def select_latest_integration_run(
    minio_client: Any,
) -> Dict[str, Any]:
    """
    Select the newest completed integration run.
    """

    runs = list_completed_integration_runs(
        minio_client
    )

    if not runs:
        raise RuntimeError(
            "No completed Field-Weather integration "
            "run with Parquet output was found."
        )

    return runs[-1]


def select_integration_run_by_id(
    minio_client: Any,
    run_id: str,
) -> Dict[str, Any]:
    """
    Select one integration run by its exact ID.
    """

    runs = list_completed_integration_runs(
        minio_client
    )

    for run in runs:
        if run["integration_run_id"] == run_id:
            return run

    raise RuntimeError(
        "No completed Field-Weather integration "
        f"run was found for ID: {run_id}"
    )