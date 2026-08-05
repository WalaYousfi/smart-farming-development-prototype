import json
from typing import Any, Dict, List

from pipeline.common.config import (
    MANIFEST_PREFIX,
    MINIO_BUCKET,
)


WEATHER_SILVER_JOB_NAME = "silver_weather_observations"


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


def list_completed_weather_silver_runs(
    minio_client: Any,
) -> List[Dict[str, Any]]:
    """
    Return completed Weather Silver runs that produced
    accepted Parquet data.
    """

    prefix = (
        f"{MANIFEST_PREFIX}/"
        f"{WEATHER_SILVER_JOB_NAME}/"
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

        accepted_objects = [
            object_name
            for object_name in output_objects
            if (
                object_name.startswith(
                    "silver/accepted/weather_observations/"
                )
                and object_name.endswith(".parquet")
            )
        ]

        if not accepted_objects:
            continue

        metrics = manifest.get("metrics", {})

        completed_runs.append(
            {
                "weather_silver_run_id": manifest.get(
                    "run_id"
                ),
                "accepted_objects": accepted_objects,
                "completed_at": manifest.get(
                    "completed_at"
                ),
                "weather_bronze_run_id": metrics.get(
                    "bronze_run_id"
                ),
                "accepted_records": metrics.get(
                    "accepted_records",
                    0,
                ),
                "manifest_object": obj.object_name,
            }
        )

    return sorted(
        completed_runs,
        key=lambda run: (
            run["weather_silver_run_id"] or ""
        ),
    )


def select_latest_weather_silver_run(
    minio_client: Any,
) -> Dict[str, Any]:
    """
    Select the latest completed Weather Silver run.
    """

    runs = list_completed_weather_silver_runs(
        minio_client
    )

    if not runs:
        raise RuntimeError(
            "No completed Weather Silver run with "
            "accepted Parquet data was found."
        )

    return runs[-1]


def select_weather_silver_run_by_id(
    minio_client: Any,
    run_id: str,
) -> Dict[str, Any]:
    """
    Select one completed Weather Silver run by ID.
    """

    runs = list_completed_weather_silver_runs(
        minio_client
    )

    for run in runs:
        if run["weather_silver_run_id"] == run_id:
            return run

    raise RuntimeError(
        "No completed Weather Silver run was found "
        f"for run ID: {run_id}"
    )