# Role
# Writes execution manifests to MinIO.
# A manifest is a small JSON file that records what happened during one pipeline run.

# It will answer questions like:

# Which job ran?
# When did it start and finish?
# Did it succeed or fail?
# How many records did it process?
# Which input and output objects were involved?


from datetime import datetime, timezone
from io import BytesIO
import json
from typing import Any, Dict, List, Optional

from pipeline.common.config import (
    MANIFEST_PREFIX,
    MINIO_BUCKET,
)
from pipeline.common.minio_client import (
    create_minio_client,
    ensure_bucket_exists,
)
from pipeline.common.run_context import RunContext


def create_manifest(
    run_context: RunContext,
    status: str,
    input_zone: Optional[str] = None,
    output_zone: Optional[str] = None,
    input_objects: Optional[List[str]] = None,
    output_objects: Optional[List[str]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a manifest dictionary for one pipeline execution.
    """

    if status not in {"started", "completed", "failed"}:
        raise ValueError(
            "status must be 'started', 'completed', or 'failed'"
        )

    return {
        **run_context.to_dict(),
        "status": status,
        "completed_at": (
            datetime.now(timezone.utc).isoformat()
            if status in {"completed", "failed"}
            else None
        ),
        "input_zone": input_zone,
        "output_zone": output_zone,
        "input_objects": input_objects or [],
        "output_objects": output_objects or [],
        "metrics": metrics or {},
        "error_message": error_message,
    }


def write_manifest(
    manifest: Dict[str, Any],
) -> str:
    """
    Store a manifest as JSON in MinIO.

    Returns the created MinIO object name.
    """

    required_fields = [
        "run_id",
        "job_name",
        "status",
    ]

    missing_fields = [
        field
        for field in required_fields
        if not manifest.get(field)
    ]

    if missing_fields:
        raise ValueError(
            f"Manifest is missing required fields: {missing_fields}"
        )

    client = create_minio_client()
    ensure_bucket_exists(client)

    job_name = manifest["job_name"]
    run_id = manifest["run_id"]
    status = manifest["status"]

    object_name = (
        f"{MANIFEST_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"manifest_{status}.json"
    )

    json_content = json.dumps(
        manifest,
        indent=2,
        ensure_ascii=False,
    )

    encoded_content = json_content.encode("utf-8")

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(encoded_content),
        length=len(encoded_content),
        content_type="application/json",
    )

    print(f"Manifest stored: {MINIO_BUCKET}/{object_name}")

    return object_name


if __name__ == "__main__":
    from pipeline.common.run_context import create_run_context

    test_context = create_run_context(
        job_name="manifest_test",
    )

    test_manifest = create_manifest(
        run_context=test_context,
        status="completed",
        input_zone="bronze",
        output_zone="silver",
        input_objects=[
            "bronze/test/input.jsonl",
        ],
        output_objects=[
            "silver/test/output.parquet",
        ],
        metrics={
            "input_records": 100,
            "accepted_records": 95,
            "quarantined_records": 5,
        },
    )

    print("\nGenerated manifest:")
    print(
        json.dumps(
            test_manifest,
            indent=2,
        )
    )

    write_manifest(test_manifest)