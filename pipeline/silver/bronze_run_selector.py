from typing import Any, List, Optional

from pipeline.common.config import (
    BRONZE_PREFIX,
    MINIO_BUCKET,
    SOURCE_SYSTEM,
)


def list_bronze_run_ids(
    minio_client: Any,
) -> List[str]:
    """
    Find all Bronze run IDs stored for the configured source.
    """

    prefix = (
        f"{BRONZE_PREFIX}/"
        f"source={SOURCE_SYSTEM}/"
    )

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    run_ids = set()

    for obj in objects:
        parts = obj.object_name.split("/")

        for part in parts:
            if part.startswith("run_id="):
                run_ids.add(
                    part.replace("run_id=", "", 1)
                )

    return sorted(run_ids)


def select_latest_bronze_run_id(
    minio_client: Any,
) -> str:
    """
    Select the latest Bronze run ID.

    Run IDs begin with UTC timestamps, so alphabetical sorting
    also sorts them chronologically.
    """

    run_ids = list_bronze_run_ids(
        minio_client
    )

    if not run_ids:
        raise RuntimeError(
            "No Bronze ingestion runs were found."
        )

    return run_ids[-1]


def list_objects_for_bronze_run(
    minio_client: Any,
    bronze_run_id: str,
) -> List[str]:
    """
    Return JSONL objects belonging only to one Bronze run.
    """

    prefix = (
        f"{BRONZE_PREFIX}/"
        f"source={SOURCE_SYSTEM}/"
    )

    objects = minio_client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    run_marker = f"run_id={bronze_run_id}/"

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
            f"No Bronze objects found for run: "
            f"{bronze_run_id}"
        )

    return sorted(object_names)