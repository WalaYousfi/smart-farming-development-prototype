import argparse
from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.common.config import (
    MANIFEST_PREFIX,
    METADATA_PREFIX,
    MINIO_BUCKET,
    PROJECT_ROOT,
)
from pipeline.common.minio_client import (
    create_minio_client,
    ensure_bucket_exists,
)


FIELD_SILVER_JOB = "silver_field_observations"
WEATHER_SILVER_JOB = "silver_weather_observations"
INTEGRATION_JOB = "silver_field_weather_integration"

FIELD_GOLD_JOB = "gold_field_anomaly_detection"

INTEGRATED_GOLD_JOB = (
    "gold_integrated_field_anomaly_detection"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        return json.loads(
            response.read().decode("utf-8")
        )

    finally:
        response.close()
        response.release_conn()


def list_completed_manifests(
    minio_client: Any,
    job_name: str,
) -> List[str]:
    """
    List completed manifests for one job.
    """

    prefix = (
        f"{MANIFEST_PREFIX}/"
        f"{job_name}/"
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


def get_latest_completed_manifest(
    minio_client: Any,
    job_name: str,
) -> Dict[str, Any]:
    """
    Load the latest successfully completed manifest.
    """

    manifest_objects = list_completed_manifests(
        minio_client=minio_client,
        job_name=job_name,
    )

    if not manifest_objects:
        raise RuntimeError(
            f"No completed manifest found for job: "
            f"{job_name}"
        )

    latest_object = manifest_objects[-1]

    manifest = read_json_object(
        minio_client=minio_client,
        object_name=latest_object,
    )

    manifest["_manifest_object"] = latest_object

    return manifest


def get_manifest_by_run_id(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Dict[str, Any]:
    """
    Load one completed manifest by exact run ID.
    """

    object_name = (
        f"{MANIFEST_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"manifest_completed.json"
    )

    try:
        manifest = read_json_object(
            minio_client=minio_client,
            object_name=object_name,
        )

    except Exception as error:
        raise RuntimeError(
            "Completed manifest could not be loaded.\n"
            f"Job: {job_name}\n"
            f"Run ID: {run_id}\n"
            f"Object: {object_name}"
        ) from error

    manifest["_manifest_object"] = object_name

    return manifest


def select_manifest(
    minio_client: Any,
    job_name: str,
    run_id: Optional[str],
) -> Dict[str, Any]:
    """
    Use an explicit run when provided; otherwise use
    the latest completed run.
    """

    if run_id:
        return get_manifest_by_run_id(
            minio_client=minio_client,
            job_name=job_name,
            run_id=run_id,
        )

    return get_latest_completed_manifest(
        minio_client=minio_client,
        job_name=job_name,
    )


def get_object_information(
    minio_client: Any,
    object_name: str,
) -> Dict[str, Any]:
    """
    Get the size and metadata of one MinIO object.
    """

    try:
        result = minio_client.stat_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

        return {
            "object_name": object_name,
            "exists": True,
            "size_bytes": int(result.size),
            "etag": result.etag,
            "content_type": result.content_type,
            "last_modified": (
                result.last_modified.isoformat()
                if result.last_modified
                else None
            ),
        }

    except Exception as error:
        return {
            "object_name": object_name,
            "exists": False,
            "size_bytes": 0,
            "error": str(error),
        }


def summarize_objects(
    minio_client: Any,
    object_names: List[str],
) -> Dict[str, Any]:
    """
    Calculate total storage used by declared objects.
    """

    objects = [
        get_object_information(
            minio_client=minio_client,
            object_name=object_name,
        )
        for object_name in object_names
    ]

    existing_objects = [
        item
        for item in objects
        if item["exists"]
    ]

    missing_objects = [
        item["object_name"]
        for item in objects
        if not item["exists"]
    ]

    total_size = sum(
        item["size_bytes"]
        for item in existing_objects
    )

    return {
        "declared_object_count": len(object_names),
        "existing_object_count": len(
            existing_objects
        ),
        "missing_object_count": len(
            missing_objects
        ),
        "missing_objects": missing_objects,
        "total_size_bytes": total_size,
        "total_size_megabytes": round(
            total_size / (1024 * 1024),
            4,
        ),
        "objects": objects,
    }


def summarize_manifest(
    minio_client: Any,
    manifest: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract evaluation information from one manifest.
    """

    input_objects = manifest.get(
        "input_objects",
        [],
    )

    output_objects = manifest.get(
        "output_objects",
        [],
    )

    return {
        "run_id": manifest.get("run_id"),
        "job_name": manifest.get("job_name"),
        "job_version": manifest.get(
            "job_version"
        ),
        "started_at": manifest.get(
            "started_at"
        ),
        "completed_at": manifest.get(
            "completed_at"
        ),
        "status": manifest.get("status"),
        "manifest_object": manifest.get(
            "_manifest_object"
        ),
        "metrics": manifest.get(
            "metrics",
            {},
        ),
        "input_storage": summarize_objects(
            minio_client=minio_client,
            object_names=input_objects,
        ),
        "output_storage": summarize_objects(
            minio_client=minio_client,
            object_names=output_objects,
        ),
    }


def calculate_architecture_metrics(
    field_silver: Dict[str, Any],
    weather_silver: Dict[str, Any],
    integration: Dict[str, Any],
    field_gold: Dict[str, Any],
    integrated_gold: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create paper-oriented comparison metrics.
    """

    field_metrics = field_silver["metrics"]
    weather_metrics = weather_silver["metrics"]
    integration_metrics = integration["metrics"]
    field_gold_metrics = field_gold["metrics"]
    integrated_gold_metrics = integrated_gold[
        "metrics"
    ]

    field_gold_anomalies = int(
        field_gold_metrics.get(
            "anomaly_records",
            0,
        )
    )

    integrated_gold_anomalies = int(
        integrated_gold_metrics.get(
            "anomaly_records",
            0,
        )
    )

    anomaly_difference = (
        integrated_gold_anomalies
        - field_gold_anomalies
    )

    proposed_output_size = sum(
        section["output_storage"][
            "total_size_bytes"
        ]
        for section in [
            field_silver,
            weather_silver,
            integration,
            field_gold,
            integrated_gold,
        ]
    )

    return {
        "source_count": 2,
        "source_formats": [
            "CSV",
            "JSON",
        ],
        "source_ingestion_modes": [
            "batch_file_simulated_as_stream",
            "streaming_sensor",
        ],
        "field_silver_records": int(
            field_metrics.get(
                "accepted_records",
                0,
            )
        ),
        "weather_silver_records": int(
            weather_metrics.get(
                "accepted_records",
                0,
            )
        ),
        "field_quarantined_records": int(
            field_metrics.get(
                "quarantined_records",
                0,
            )
        ),
        "weather_quarantined_records": int(
            weather_metrics.get(
                "quarantined_records",
                0,
            )
        ),
        "integrated_records": int(
            integration_metrics.get(
                "integrated_output_records",
                0,
            )
        ),
        "matched_field_records": int(
            integration_metrics.get(
                "matched_field_records",
                0,
            )
        ),
        "unmatched_field_records": int(
            integration_metrics.get(
                "unmatched_field_records",
                0,
            )
        ),
        "weather_match_rate": float(
            integration_metrics.get(
                "weather_match_rate",
                0.0,
            )
        ),
        "field_only_anomalies": (
            field_gold_anomalies
        ),
        "integrated_anomalies": (
            integrated_gold_anomalies
        ),
        "anomaly_count_difference": (
            anomaly_difference
        ),
        "weather_matched_anomalies": int(
            integrated_gold_metrics.get(
                "weather_matched_anomalies",
                0,
            )
        ),
        "proposed_architecture_output_size_bytes": (
            proposed_output_size
        ),
        "proposed_architecture_output_size_megabytes": (
            round(
                proposed_output_size
                / (1024 * 1024),
                4,
            )
        ),
        "traceable_processing_stages": 5,
        "manifest_enabled": True,
        "lineage_enabled": True,
        "quarantine_enabled": True,
        "canonical_schemas": 2,
        "multi_source_integration_enabled": True,
    }


def write_local_report(
    report: Dict[str, Any],
) -> Path:
    """
    Store the evaluation report in the repository.
    """

    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "prototype-v2"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / "evaluation_snapshot.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            report,
            output_file,
            indent=2,
            ensure_ascii=False,
        )

    print(
        f"Local evaluation report stored: "
        f"{output_path}"
    )

    return output_path


def write_minio_report(
    minio_client: Any,
    report: Dict[str, Any],
) -> str:
    """
    Store the same evaluation report in MinIO.
    """

    evaluation_id = report["evaluation_id"]

    object_name = (
        f"{METADATA_PREFIX}/"
        f"evaluations/"
        f"prototype-v2/"
        f"evaluation_id={evaluation_id}/"
        f"evaluation_snapshot.json"
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
        f"MinIO evaluation report stored: "
        f"{MINIO_BUCKET}/{object_name}"
    )

    return object_name


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect reproducible evaluation metrics "
            "for the proposed V2 architecture."
        )
    )

    parser.add_argument(
        "--field-silver-run-id",
        default=None,
    )

    parser.add_argument(
        "--weather-silver-run-id",
        default=None,
    )

    parser.add_argument(
        "--integration-run-id",
        default=None,
    )

    parser.add_argument(
        "--field-gold-run-id",
        default=None,
    )

    parser.add_argument(
        "--integrated-gold-run-id",
        default=None,
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    evaluation_id = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    print("Collecting Prototype V2 metrics...")
    print(f"Evaluation ID: {evaluation_id}")

    field_silver_manifest = select_manifest(
        minio_client=minio_client,
        job_name=FIELD_SILVER_JOB,
        run_id=arguments.field_silver_run_id,
    )

    weather_silver_manifest = select_manifest(
        minio_client=minio_client,
        job_name=WEATHER_SILVER_JOB,
        run_id=arguments.weather_silver_run_id,
    )

    integration_manifest = select_manifest(
        minio_client=minio_client,
        job_name=INTEGRATION_JOB,
        run_id=arguments.integration_run_id,
    )

    field_gold_manifest = select_manifest(
        minio_client=minio_client,
        job_name=FIELD_GOLD_JOB,
        run_id=arguments.field_gold_run_id,
    )

    integrated_gold_manifest = select_manifest(
        minio_client=minio_client,
        job_name=INTEGRATED_GOLD_JOB,
        run_id=arguments.integrated_gold_run_id,
    )

    field_silver = summarize_manifest(
        minio_client,
        field_silver_manifest,
    )

    weather_silver = summarize_manifest(
        minio_client,
        weather_silver_manifest,
    )

    integration = summarize_manifest(
        minio_client,
        integration_manifest,
    )

    field_gold = summarize_manifest(
        minio_client,
        field_gold_manifest,
    )

    integrated_gold = summarize_manifest(
        minio_client,
        integrated_gold_manifest,
    )

    architecture_metrics = (
        calculate_architecture_metrics(
            field_silver=field_silver,
            weather_silver=weather_silver,
            integration=integration,
            field_gold=field_gold,
            integrated_gold=integrated_gold,
        )
    )

    report = {
        "evaluation_id": evaluation_id,
        "evaluation_name": (
            "prototype_v2_architecture_snapshot"
        ),
        "created_at": utc_now(),
        "bucket": MINIO_BUCKET,
        "architecture": {
            "functional_layers": [
                "acquisition",
                "ingestion_and_buffering",
                "data_management",
                "intelligence_and_analytics",
                "serving_and_consumption",
            ],
            "medallion_zones": [
                "bronze",
                "silver",
                "gold",
            ],
        },
        "selected_runs": {
            "field_silver": field_silver,
            "weather_silver": weather_silver,
            "integration": integration,
            "field_gold": field_gold,
            "integrated_gold": integrated_gold,
        },
        "paper_metrics": architecture_metrics,
    }

    write_local_report(report)

    minio_object = write_minio_report(
        minio_client=minio_client,
        report=report,
    )

    print("\nEvaluation snapshot completed.")
    print(
        "Field Silver records: "
        f"{architecture_metrics['field_silver_records']}"
    )
    print(
        "Weather Silver records: "
        f"{architecture_metrics['weather_silver_records']}"
    )
    print(
        "Integrated records: "
        f"{architecture_metrics['integrated_records']}"
    )
    print(
        "Weather match rate: "
        f"{architecture_metrics['weather_match_rate']}"
    )
    print(
        "Field-only anomalies: "
        f"{architecture_metrics['field_only_anomalies']}"
    )
    print(
        "Integrated anomalies: "
        f"{architecture_metrics['integrated_anomalies']}"
    )
    print(
        "Output storage: "
        f"{architecture_metrics['proposed_architecture_output_size_megabytes']} MB"
    )
    print(f"MinIO report: {minio_object}")


if __name__ == "__main__":
    main()