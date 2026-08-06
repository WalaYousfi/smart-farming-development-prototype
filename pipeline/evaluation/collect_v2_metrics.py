import argparse
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.common.config import (
    LINEAGE_PREFIX,
    MANIFEST_PREFIX,
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
INTEGRATED_GOLD_JOB = (
    "gold_integrated_field_anomaly_detection"
)


def parse_arguments() -> argparse.Namespace:
    """
    Read the four run IDs forming one V2 experiment.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Collect evaluation metrics for one complete "
            "heterogeneous V2 pipeline execution."
        )
    )

    parser.add_argument(
        "--field-silver-run-id",
        required=True,
        help="Completed canonical Field Silver run ID.",
    )

    parser.add_argument(
        "--weather-silver-run-id",
        required=True,
        help="Completed canonical Weather Silver run ID.",
    )

    parser.add_argument(
        "--integration-run-id",
        required=True,
        help="Completed Field–Weather integration run ID.",
    )

    parser.add_argument(
        "--integrated-gold-run-id",
        required=True,
        help="Completed integrated Gold run ID.",
    )

    parser.add_argument(
        "--experiment-name",
        default="prototype-v2-evaluation",
        help=(
            "Name used for the generated evaluation file."
        ),
    )

    return parser.parse_args()


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


def get_completed_manifest(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Dict[str, Any]:
    """
    Read the completed manifest of one pipeline job.
    """

    object_name = (
        f"{MANIFEST_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"manifest_completed.json"
    )

    try:
        minio_client.stat_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

    except Exception as error:
        raise RuntimeError(
            "Completed manifest was not found.\n"
            f"Job: {job_name}\n"
            f"Run ID: {run_id}\n"
            f"Expected object: {object_name}"
        ) from error

    manifest = read_json_object(
        minio_client=minio_client,
        object_name=object_name,
    )

    manifest["_manifest_object"] = object_name

    return manifest


def get_lineage(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Read lineage when the job has a lineage record.
    """

    object_name = (
        f"{LINEAGE_PREFIX}/"
        f"{job_name}/"
        f"run_id={run_id}/"
        f"lineage.json"
    )

    try:
        minio_client.stat_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

    except Exception:
        return None

    lineage = read_json_object(
        minio_client=minio_client,
        object_name=object_name,
    )

    lineage["_lineage_object"] = object_name

    return lineage


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """
    Convert an ISO timestamp into a datetime.
    """

    if not value:
        return None

    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def calculate_duration_seconds(
    manifest: Dict[str, Any],
) -> Optional[float]:
    """
    Calculate elapsed time from manifest timestamps.
    """

    started_at = parse_timestamp(
        manifest.get("started_at")
    )

    completed_at = parse_timestamp(
        manifest.get("completed_at")
    )

    if started_at is None or completed_at is None:
        return None

    duration = (
        completed_at - started_at
    ).total_seconds()

    return round(duration, 4)


def get_object_size(
    minio_client: Any,
    object_name: str,
) -> int:
    """
    Return the size of one MinIO object in bytes.
    """

    try:
        result = minio_client.stat_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

        return int(result.size)

    except Exception:
        return 0


def calculate_objects_size(
    minio_client: Any,
    object_names: List[str],
) -> Dict[str, Any]:
    """
    Calculate total and per-object storage size.
    """

    object_details = []

    total_bytes = 0

    for object_name in object_names:
        size_bytes = get_object_size(
            minio_client=minio_client,
            object_name=object_name,
        )

        total_bytes += size_bytes

        object_details.append(
            {
                "object_name": object_name,
                "size_bytes": size_bytes,
                "size_kilobytes": round(
                    size_bytes / 1024,
                    3,
                ),
            }
        )

    return {
        "object_count": len(object_names),
        "total_bytes": total_bytes,
        "total_kilobytes": round(
            total_bytes / 1024,
            3,
        ),
        "total_megabytes": round(
            total_bytes / (1024 * 1024),
            4,
        ),
        "objects": object_details,
    }


def create_job_metrics(
    minio_client: Any,
    job_name: str,
    run_id: str,
) -> Dict[str, Any]:
    """
    Collect manifest, lineage and storage measurements
    for one job.
    """

    manifest = get_completed_manifest(
        minio_client=minio_client,
        job_name=job_name,
        run_id=run_id,
    )

    lineage = get_lineage(
        minio_client=minio_client,
        job_name=job_name,
        run_id=run_id,
    )

    input_objects = manifest.get(
        "input_objects",
        [],
    )

    output_objects = manifest.get(
        "output_objects",
        [],
    )

    return {
        "job_name": job_name,
        "run_id": run_id,
        "job_version": manifest.get(
            "job_version"
        ),
        "status": manifest.get("status"),
        "started_at": manifest.get(
            "started_at"
        ),
        "completed_at": manifest.get(
            "completed_at"
        ),
        "duration_seconds": (
            calculate_duration_seconds(
                manifest
            )
        ),
        "manifest_metrics": manifest.get(
            "metrics",
            {},
        ),
        "input_storage": (
            calculate_objects_size(
                minio_client=minio_client,
                object_names=input_objects,
            )
        ),
        "output_storage": (
            calculate_objects_size(
                minio_client=minio_client,
                object_names=output_objects,
            )
        ),
        "lineage": {
            "available": lineage is not None,
            "parent_run_count": (
                len(
                    lineage.get(
                        "parent_run_ids",
                        [],
                    )
                )
                if lineage
                else 0
            ),
            "parent_run_ids": (
                lineage.get(
                    "parent_run_ids",
                    [],
                )
                if lineage
                else []
            ),
        },
    }


def build_evaluation_report(
    minio_client: Any,
    arguments: argparse.Namespace,
) -> Dict[str, Any]:
    """
    Build the V2 experiment report.
    """

    jobs = {
        "field_silver": create_job_metrics(
            minio_client=minio_client,
            job_name=FIELD_SILVER_JOB,
            run_id=arguments.field_silver_run_id,
        ),
        "weather_silver": create_job_metrics(
            minio_client=minio_client,
            job_name=WEATHER_SILVER_JOB,
            run_id=arguments.weather_silver_run_id,
        ),
        "integration": create_job_metrics(
            minio_client=minio_client,
            job_name=INTEGRATION_JOB,
            run_id=arguments.integration_run_id,
        ),
        "integrated_gold": create_job_metrics(
            minio_client=minio_client,
            job_name=INTEGRATED_GOLD_JOB,
            run_id=(
                arguments.integrated_gold_run_id
            ),
        ),
    }

    durations = [
        job["duration_seconds"]
        for job in jobs.values()
        if job["duration_seconds"] is not None
    ]

    total_duration = round(
        sum(durations),
        4,
    )

    total_output_bytes = sum(
        job["output_storage"]["total_bytes"]
        for job in jobs.values()
    )

    integration_metrics = jobs[
        "integration"
    ]["manifest_metrics"]

    gold_metrics = jobs[
        "integrated_gold"
    ]["manifest_metrics"]

    return {
        "experiment_name": (
            arguments.experiment_name
        ),
        "architecture": (
            "dual-dimensional multi-layer "
            "and Medallion architecture"
        ),
        "bucket": MINIO_BUCKET,
        "generated_at": (
            datetime.now()
            .astimezone()
            .isoformat()
        ),
        "run_ids": {
            "field_silver": (
                arguments.field_silver_run_id
            ),
            "weather_silver": (
                arguments.weather_silver_run_id
            ),
            "integration": (
                arguments.integration_run_id
            ),
            "integrated_gold": (
                arguments.integrated_gold_run_id
            ),
        },
        "summary": {
            "measured_job_count": len(jobs),
            "total_measured_duration_seconds": (
                total_duration
            ),
            "total_output_storage_bytes": (
                total_output_bytes
            ),
            "total_output_storage_megabytes": round(
                total_output_bytes
                / (1024 * 1024),
                4,
            ),
            "field_records": (
                integration_metrics.get(
                    "field_input_records"
                )
            ),
            "weather_records": (
                integration_metrics.get(
                    "weather_input_records"
                )
            ),
            "integrated_records": (
                integration_metrics.get(
                    "integrated_output_records"
                )
            ),
            "matched_records": (
                integration_metrics.get(
                    "matched_field_records"
                )
            ),
            "weather_match_rate": (
                integration_metrics.get(
                    "weather_match_rate"
                )
            ),
            "normal_records": (
                gold_metrics.get(
                    "normal_records"
                )
            ),
            "anomaly_records": (
                gold_metrics.get(
                    "anomaly_records"
                )
            ),
            "anomaly_rate": (
                gold_metrics.get(
                    "anomaly_rate"
                )
            ),
            "weather_matched_anomalies": (
                gold_metrics.get(
                    "weather_matched_anomalies"
                )
            ),
            "all_jobs_have_lineage": all(
                job["lineage"]["available"]
                for job in jobs.values()
            ),
        },
        "jobs": jobs,
    }


def save_report_locally(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:
    """
    Save the evaluation report in the repository.
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

    safe_name = (
        experiment_name
        .strip()
        .replace(" ", "_")
    )

    output_path = (
        output_directory
        / f"{safe_name}.json"
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

    return output_path


def print_summary(
    report: Dict[str, Any],
) -> None:
    """
    Print the most important experiment measurements.
    """

    summary = report["summary"]

    print("\nV2 evaluation summary")
    print("---------------------")

    print(
        "Measured jobs: "
        f"{summary['measured_job_count']}"
    )

    print(
        "Total measured duration: "
        f"{summary['total_measured_duration_seconds']} "
        "seconds"
    )

    print(
        "Total output storage: "
        f"{summary['total_output_storage_megabytes']} MB"
    )

    print(
        "Field records: "
        f"{summary['field_records']}"
    )

    print(
        "Weather records: "
        f"{summary['weather_records']}"
    )

    print(
        "Integrated records: "
        f"{summary['integrated_records']}"
    )

    print(
        "Weather match rate: "
        f"{summary['weather_match_rate']}"
    )

    print(
        "Normal records: "
        f"{summary['normal_records']}"
    )

    print(
        "Anomaly records: "
        f"{summary['anomaly_records']}"
    )

    print(
        "All measured jobs have lineage: "
        f"{summary['all_jobs_have_lineage']}"
    )


def main() -> None:
    arguments = parse_arguments()

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    report = build_evaluation_report(
        minio_client=minio_client,
        arguments=arguments,
    )

    output_path = save_report_locally(
        report=report,
        experiment_name=(
            arguments.experiment_name
        ),
    )

    print_summary(report)

    print(
        "\nEvaluation report saved locally:"
    )
    print(output_path)


if __name__ == "__main__":
    main()