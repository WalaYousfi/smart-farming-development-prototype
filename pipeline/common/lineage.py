from datetime import datetime, timezone
from io import BytesIO
import json
from typing import Any, Dict, List

from pipeline.common.config import (
    LINEAGE_PREFIX,
    MINIO_BUCKET,
)
from pipeline.common.minio_client import (
    create_minio_client,
    ensure_bucket_exists,
)


def create_lineage_record(
    run_id: str,
    job_name: str,
    input_zone: str,
    output_zone: str,
    input_objects: List[str],
    output_objects: List[str],
    parent_run_ids: List[str],
    metrics: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create a lineage record connecting input data to output data.
    """

    return {
        "run_id": run_id,
        "job_name": job_name,
        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "input_zone": input_zone,
        "output_zone": output_zone,
        "input_objects": input_objects,
        "output_objects": output_objects,
        "parent_run_ids": parent_run_ids,
        "metrics": metrics,
    }


def write_lineage_record(
    lineage_record: Dict[str, Any],
) -> str:
    """
    Store the lineage record as JSON in MinIO.
    """

    client = create_minio_client()
    ensure_bucket_exists(client)

    run_id = lineage_record["run_id"]
    job_name = lineage_record["job_name"]

    object_name = (
        f"{LINEAGE_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"lineage.json"
    )

    json_content = json.dumps(
        lineage_record,
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

    print(
        f"Lineage stored: "
        f"{MINIO_BUCKET}/{object_name}"
    )

    return object_name