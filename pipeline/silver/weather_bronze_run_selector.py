from typing import Any, List

from pipeline.common.config import (
    BRONZE_PREFIX,
    MINIO_BUCKET,
    WEATHER_SOURCE_SYSTEM,
)


def list_weather_bronze_run_ids(
    minio_client: Any,
) -> List[str]:
    """
    Return all Weather Bronze run IDs.
    """

    prefix = (
        f"{BRONZE_PREFIX}/"
        f"source={WEATHER_SOURCE_SYSTEM}/"
    )

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    run_ids = set()

    for obj in objects:
        path_parts = obj.object_name.split("/")

        for part in path_parts:
            if part.startswith("run_id="):
                run_ids.add(
                    part.replace("run_id=", "", 1)
                )

    return sorted(run_ids)


def select_latest_weather_bronze_run_id(
    minio_client: Any,
) -> str:
    """
    Select the newest Weather Bronze run.
    """

    run_ids = list_weather_bronze_run_ids(
        minio_client
    )

    if not run_ids:
        raise RuntimeError(
            "No Weather Bronze ingestion runs were found."
        )

    return run_ids[-1]


def list_objects_for_weather_bronze_run(
    minio_client: Any,
    bronze_run_id: str,
) -> List[str]:
    """
    Return the JSONL files belonging to one
    Weather Bronze run.
    """

    prefix = (
        f"{BRONZE_PREFIX}/"
        f"source={WEATHER_SOURCE_SYSTEM}/"
    )

    run_marker = f"run_id={bronze_run_id}/"

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    object_names = [
        obj.object_name
        for obj in objects
        if (
            run_marker in obj.object_name
            and obj.object_name.endswith(".jsonl")
        )
    ]

    if not object_names:
        raise RuntimeError(
            "No Weather Bronze objects found for run: "
            f"{bronze_run_id}"
        )

    return sorted(object_names)