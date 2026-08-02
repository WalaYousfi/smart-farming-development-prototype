import json
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List

from pipeline.common.config import (
    LINEAGE_PREFIX,
    MANIFEST_PREFIX,
    METADATA_PREFIX,
    MINIO_BUCKET,
)
from pipeline.common.minio_client import (
    create_minio_client,
    ensure_bucket_exists,
)


SILVER_JOB_NAME = "silver_field_observations"
GOLD_JOB_NAME = "gold_field_anomaly_detection"


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


def object_exists(
    minio_client: Any,
    object_name: str,
) -> bool:
    """
    Check whether one MinIO object exists.
    """

    try:
        minio_client.stat_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

        return True

    except Exception:
        return False


def get_completed_manifest(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Dict[str, Any]:
    """
    Load the completed manifest for one pipeline run.
    """

    object_name = (
        f"{MANIFEST_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"manifest_completed.json"
    )

    if not object_exists(
        minio_client,
        object_name,
    ):
        raise RuntimeError(
            "Completed manifest not found.\n"
            f"Job: {job_name}\n"
            f"Run ID: {run_id}\n"
            f"Expected object: {object_name}"
        )

    return read_json_object(
        minio_client=minio_client,
        object_name=object_name,
    )


def get_lineage_record(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Dict[str, Any]:
    """
    Load the lineage record for one pipeline run.
    """

    object_name = (
        f"{LINEAGE_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"lineage.json"
    )

    if not object_exists(
        minio_client,
        object_name,
    ):
        raise RuntimeError(
            "Lineage record not found.\n"
            f"Job: {job_name}\n"
            f"Run ID: {run_id}\n"
            f"Expected object: {object_name}"
        )

    return read_json_object(
        minio_client=minio_client,
        object_name=object_name,
    )


def verify_declared_objects(
    minio_client: Any,
    object_names: List[str],
) -> Dict[str, List[str]]:
    """
    Verify that objects declared by a manifest still exist.
    """

    existing_objects = []
    missing_objects = []

    for object_name in object_names:
        if object_exists(
            minio_client,
            object_name,
        ):
            existing_objects.append(object_name)

        else:
            missing_objects.append(object_name)

    return {
        "existing_objects": existing_objects,
        "missing_objects": missing_objects,
    }


def build_traceability_report(
    minio_client: Any,
    gold_run_id: str,
) -> Dict[str, Any]:
    """
    Build the complete Bronze-to-Silver-to-Gold chain.
    """

    gold_manifest = get_completed_manifest(
        minio_client=minio_client,
        job_name=GOLD_JOB_NAME,
        run_id=gold_run_id,
    )

    gold_lineage = get_lineage_record(
        minio_client=minio_client,
        job_name=GOLD_JOB_NAME,
        run_id=gold_run_id,
    )

    silver_parent_ids = gold_lineage.get(
        "parent_run_ids",
        [],
    )

    if len(silver_parent_ids) != 1:
        raise RuntimeError(
            "Gold lineage must contain exactly one "
            "Silver parent run."
        )

    silver_run_id = silver_parent_ids[0]

    silver_manifest = get_completed_manifest(
        minio_client=minio_client,
        job_name=SILVER_JOB_NAME,
        run_id=silver_run_id,
    )

    silver_lineage = get_lineage_record(
        minio_client=minio_client,
        job_name=SILVER_JOB_NAME,
        run_id=silver_run_id,
    )

    bronze_parent_ids = silver_lineage.get(
        "parent_run_ids",
        [],
    )

    if len(bronze_parent_ids) != 1:
        raise RuntimeError(
            "Silver lineage must contain exactly one "
            "Bronze parent run."
        )

    bronze_run_id = bronze_parent_ids[0]

    bronze_input_objects = silver_lineage.get(
        "input_objects",
        [],
    )

    silver_output_objects = silver_manifest.get(
        "output_objects",
        [],
    )

    gold_output_objects = gold_manifest.get(
        "output_objects",
        [],
    )

    bronze_verification = verify_declared_objects(
        minio_client,
        bronze_input_objects,
    )

    silver_verification = verify_declared_objects(
        minio_client,
        silver_output_objects,
    )

    gold_verification = verify_declared_objects(
        minio_client,
        gold_output_objects,
    )

    all_objects_exist = not (
        bronze_verification["missing_objects"]
        or silver_verification["missing_objects"]
        or gold_verification["missing_objects"]
    )

    return {
        "report_type": (
            "bronze_silver_gold_traceability"
        ),
        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status": (
            "valid"
            if all_objects_exist
            else "invalid"
        ),
        "run_chain": {
            "bronze_run_id": bronze_run_id,
            "silver_run_id": silver_run_id,
            "gold_run_id": gold_run_id,
        },
        "bronze": {
            "input_objects": bronze_input_objects,
            **bronze_verification,
        },
        "silver": {
            "output_objects": silver_output_objects,
            **silver_verification,
        },
        "gold": {
            "output_objects": gold_output_objects,
            **gold_verification,
        },
        "checks": {
            "gold_has_one_silver_parent": True,
            "silver_has_one_bronze_parent": True,
            "all_declared_objects_exist": (
                all_objects_exist
            ),
        },
    }


def write_traceability_report(
    minio_client: Any,
    report: Dict[str, Any],
) -> str:
    """
    Store the traceability report in MinIO.
    """

    gold_run_id = report["run_chain"][
        "gold_run_id"
    ]

    object_name = (
        f"{METADATA_PREFIX}/"
        f"traceability/"
        f"gold_run_id={gold_run_id}/"
        f"traceability_report.json"
    )

    encoded_content = json.dumps(
        report,
        indent=2,
        ensure_ascii=False,
    ).encode("utf-8")

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(encoded_content),
        length=len(encoded_content),
        content_type="application/json",
    )

    print(
        f"Traceability report stored: "
        f"{MINIO_BUCKET}/{object_name}"
    )

    return object_name