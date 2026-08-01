from datetime import datetime, timezone
from io import BytesIO
import json
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from pipeline.common.config import (
    BRONZE_PREFIX,
    MINIO_BUCKET,
    QUALITY_REPORT_PREFIX,
    SILVER_PREFIX,
    SOURCE_SYSTEM,
)
from pipeline.common.manifest import (
    create_manifest,
    write_manifest,
)
from pipeline.common.minio_client import (
    create_minio_client,
    ensure_bucket_exists,
)
from pipeline.common.run_context import (
    create_run_context,
)
from pipeline.common.schema_loader import (
    create_validator,
    get_validation_errors,
)
from pipeline.silver.canonical_mapper import (
    map_bronze_to_canonical,
)
from pipeline.silver.quarantine import (
    create_quarantine_record,
)


JOB_NAME = "silver_field_observations"
JOB_VERSION = "2.0.0"

SOURCE_SCHEMA_PATH = (
    "source/crop_yield_csv.schema.json"
)

CANONICAL_SCHEMA_PATH = (
    "canonical/field_observation_v1.schema.json"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_bronze_objects(
    minio_client: Any,
) -> List[str]:
    """
    Find all V2 Bronze JSONL objects for the current source.
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

    object_names = []

    for obj in objects:
        if obj.object_name.endswith(".jsonl"):
            object_names.append(obj.object_name)

    return sorted(object_names)


def read_bronze_object(
    minio_client: Any,
    object_name: str,
) -> List[Dict[str, Any]]:
    """
    Read one Bronze JSONL object from MinIO.
    """

    response = minio_client.get_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
    )

    records = []

    try:
        content = response.read().decode("utf-8")

        for line_number, line in enumerate(
            content.splitlines(),
            start=1,
        ):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                record["_bronze_object"] = object_name
                record["_bronze_line_number"] = line_number
                records.append(record)

            except json.JSONDecodeError as error:
                records.append(
                    {
                        "_read_error": str(error),
                        "_raw_line": line,
                        "_bronze_object": object_name,
                        "_bronze_line_number": line_number,
                    }
                )

    finally:
        response.close()
        response.release_conn()

    return records


def load_bronze_records(
    minio_client: Any,
    object_names: List[str],
) -> List[Dict[str, Any]]:
    """
    Load all selected Bronze objects.
    """

    all_records = []

    for object_name in object_names:
        print(f"Reading Bronze object: {object_name}")

        object_records = read_bronze_object(
            minio_client=minio_client,
            object_name=object_name,
        )

        all_records.extend(object_records)

    return all_records


def add_location_to_quarantine(
    quarantine_record: Dict[str, Any],
    bronze_record: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Add the original Bronze object and line number.
    """

    quarantine_record["bronze_object"] = (
        bronze_record.get("_bronze_object")
    )

    quarantine_record["bronze_line_number"] = (
        bronze_record.get("_bronze_line_number")
    )

    return quarantine_record


def process_records(
    bronze_records: List[Dict[str, Any]],
    silver_run_id: str,
) -> Tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    Dict[str, int],
]:
    """
    Validate, map and separate accepted and quarantined records.
    """

    source_validator = create_validator(
        SOURCE_SCHEMA_PATH
    )

    canonical_validator = create_validator(
        CANONICAL_SCHEMA_PATH
    )

    accepted_records = []
    quarantine_records = []

    seen_event_ids = set()  # type: Set[str]

    counters = {
        "input_records": 0,
        "accepted_records": 0,
        "quarantined_records": 0,
        "duplicate_records": 0,
        "json_read_failures": 0,
        "source_schema_failures": 0,
        "mapping_failures": 0,
        "canonical_schema_failures": 0,
    }

    for bronze_record in bronze_records:
        counters["input_records"] += 1

        # Failure while reading a JSONL line.
        if "_read_error" in bronze_record:
            quarantine_record = {
                "event_id": None,
                "source_system": SOURCE_SYSTEM,
                "bronze_run_id": None,
                "silver_run_id": silver_run_id,
                "failed_stage": "bronze_json_reading",
                "failure_reasons": [
                    {
                        "field": "$",
                        "message": bronze_record[
                            "_read_error"
                        ],
                    }
                ],
                "quarantine_timestamp": utc_now(),
                "bronze_object": bronze_record.get(
                    "_bronze_object"
                ),
                "bronze_line_number": bronze_record.get(
                    "_bronze_line_number"
                ),
                "original_record": bronze_record.get(
                    "_raw_line"
                ),
            }

            quarantine_records.append(
                quarantine_record
            )

            counters["json_read_failures"] += 1
            counters["quarantined_records"] += 1
            continue

        metadata = bronze_record.get("metadata")
        payload = bronze_record.get("payload")

        if not isinstance(metadata, dict):
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage="bronze_envelope_validation",
                    failure_reasons=[
                        {
                            "field": "metadata",
                            "message": (
                                "A valid metadata object "
                                "is required."
                            ),
                        }
                    ],
                )
            )

            quarantine_records.append(
                add_location_to_quarantine(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["quarantined_records"] += 1
            continue

        if not isinstance(payload, dict):
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage="bronze_envelope_validation",
                    failure_reasons=[
                        {
                            "field": "payload",
                            "message": (
                                "A valid payload object "
                                "is required."
                            ),
                        }
                    ],
                )
            )

            quarantine_records.append(
                add_location_to_quarantine(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["quarantined_records"] += 1
            continue

        event_id = metadata.get("event_id")

        if event_id and event_id in seen_event_ids:
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage="deduplication",
                    failure_reasons=[
                        {
                            "field": "metadata.event_id",
                            "message": (
                                "Duplicate event_id detected."
                            ),
                        }
                    ],
                )
            )

            quarantine_records.append(
                add_location_to_quarantine(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["duplicate_records"] += 1
            counters["quarantined_records"] += 1
            continue

        if event_id:
            seen_event_ids.add(event_id)

        # Validate the original source payload.
        source_errors = get_validation_errors(
            payload,
            source_validator,
        )

        if source_errors:
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage="source_schema_validation",
                    failure_reasons=source_errors,
                )
            )

            quarantine_records.append(
                add_location_to_quarantine(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["source_schema_failures"] += 1
            counters["quarantined_records"] += 1
            continue

        # Map source fields to the canonical model.
        try:
            canonical_record = (
                map_bronze_to_canonical(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                )
            )

        except (
            ValueError,
            TypeError,
            OverflowError,
        ) as error:
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage="canonical_mapping",
                    failure_reasons=[
                        {
                            "field": "$",
                            "message": str(error),
                        }
                    ],
                )
            )

            quarantine_records.append(
                add_location_to_quarantine(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["mapping_failures"] += 1
            counters["quarantined_records"] += 1
            continue

        # Validate the result against the canonical schema.
        canonical_errors = get_validation_errors(
            canonical_record,
            canonical_validator,
        )

        if canonical_errors:
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage="canonical_schema_validation",
                    failure_reasons=canonical_errors,
                )
            )

            quarantine_record[
                "candidate_canonical_record"
            ] = canonical_record

            quarantine_records.append(
                add_location_to_quarantine(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters[
                "canonical_schema_failures"
            ] += 1

            counters["quarantined_records"] += 1
            continue

        accepted_records.append(canonical_record)
        counters["accepted_records"] += 1

    return (
        accepted_records,
        quarantine_records,
        counters,
    )


def upload_accepted_records(
    minio_client: Any,
    accepted_records: List[Dict[str, Any]],
    run_id: str,
) -> str:
    """
    Store accepted canonical Silver records as Parquet.
    """

    if not accepted_records:
        return ""

    dataframe = pd.DataFrame(accepted_records)

    buffer = BytesIO()

    dataframe.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
    )

    content = buffer.getvalue()

    object_name = (
        f"{SILVER_PREFIX}/accepted/"
        f"field_observations/"
        f"run_id={run_id}/"
        f"field_observations.parquet"
    )

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(content),
        length=len(content),
        content_type="application/octet-stream",
    )

    print(
        f"Accepted Silver records stored: "
        f"{object_name}"
    )

    return object_name


def upload_quarantine_records(
    minio_client: Any,
    quarantine_records: List[Dict[str, Any]],
    run_id: str,
) -> str:
    """
    Store quarantined records as JSONL.

    JSONL is used because quarantine records contain nested
    original records and lists of failure reasons.
    """

    if not quarantine_records:
        return ""

    jsonl_content = "\n".join(
        json.dumps(
            record,
            ensure_ascii=False,
        )
        for record in quarantine_records
    )

    jsonl_content += "\n"

    encoded_content = jsonl_content.encode("utf-8")

    object_name = (
        f"{SILVER_PREFIX}/quarantine/"
        f"field_observations/"
        f"run_id={run_id}/"
        f"rejected_records.jsonl"
    )

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(encoded_content),
        length=len(encoded_content),
        content_type="application/x-ndjson",
    )

    print(
        f"Quarantine records stored: "
        f"{object_name}"
    )

    return object_name


def calculate_quality_metrics(
    counters: Dict[str, int],
) -> Dict[str, Any]:
    """
    Calculate the first operational Silver quality metrics.
    """

    input_records = counters["input_records"]
    accepted_records = counters["accepted_records"]
    quarantined_records = counters[
        "quarantined_records"
    ]
    duplicate_records = counters["duplicate_records"]

    if input_records == 0:
        acceptance_rate = 0.0
        quarantine_rate = 0.0
        uniqueness_score = 0.0
    else:
        acceptance_rate = (
            accepted_records / input_records
        )

        quarantine_rate = (
            quarantined_records / input_records
        )

        uniqueness_score = (
            input_records - duplicate_records
        ) / input_records

    overall_quality_score = (
        acceptance_rate + uniqueness_score
    ) / 2

    return {
        **counters,
        "acceptance_rate": round(
            acceptance_rate,
            4,
        ),
        "quarantine_rate": round(
            quarantine_rate,
            4,
        ),
        "uniqueness_score": round(
            uniqueness_score,
            4,
        ),
        "overall_quality_score": round(
            overall_quality_score,
            4,
        ),
    }


def upload_quality_report(
    minio_client: Any,
    run_id: str,
    metrics: Dict[str, Any],
    input_objects: List[str],
    output_objects: List[str],
) -> str:
    """
    Store the Silver quality report as JSON.
    """

    report = {
        "run_id": run_id,
        "job_name": JOB_NAME,
        "job_version": JOB_VERSION,
        "created_at": utc_now(),
        "input_zone": "bronze",
        "output_zone": "silver",
        "input_objects": input_objects,
        "output_objects": output_objects,
        "metrics": metrics,
    }

    json_content = json.dumps(
        report,
        indent=2,
        ensure_ascii=False,
    )

    encoded_content = json_content.encode("utf-8")

    object_name = (
        f"{QUALITY_REPORT_PREFIX}/silver/"
        f"run_id={run_id}/"
        f"quality_report.json"
    )

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(encoded_content),
        length=len(encoded_content),
        content_type="application/json",
    )

    print(
        f"Quality report stored: {object_name}"
    )

    return object_name


def main() -> None:
    run_context = create_run_context(
        job_name=JOB_NAME,
        job_version=JOB_VERSION,
    )

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    input_objects = []
    output_objects = []

    started_manifest = create_manifest(
        run_context=run_context,
        status="started",
        input_zone="bronze",
        output_zone="silver",
        metrics={
            "input_records": 0,
            "accepted_records": 0,
            "quarantined_records": 0,
        },
    )

    write_manifest(started_manifest)

    print("Silver V2 processor started.")
    print(f"Run ID: {run_context.run_id}")

    try:
        input_objects = list_bronze_objects(
            minio_client
        )

        if not input_objects:
            raise RuntimeError(
                "No V2 Bronze JSONL objects were found."
            )

        bronze_records = load_bronze_records(
            minio_client=minio_client,
            object_names=input_objects,
        )

        (
            accepted_records,
            quarantine_records,
            counters,
        ) = process_records(
            bronze_records=bronze_records,
            silver_run_id=run_context.run_id,
        )

        accepted_object = upload_accepted_records(
            minio_client=minio_client,
            accepted_records=accepted_records,
            run_id=run_context.run_id,
        )

        if accepted_object:
            output_objects.append(accepted_object)

        quarantine_object = (
            upload_quarantine_records(
                minio_client=minio_client,
                quarantine_records=quarantine_records,
                run_id=run_context.run_id,
            )
        )

        if quarantine_object:
            output_objects.append(
                quarantine_object
            )

        quality_metrics = (
            calculate_quality_metrics(counters)
        )

        quality_report_object = (
            upload_quality_report(
                minio_client=minio_client,
                run_id=run_context.run_id,
                metrics=quality_metrics,
                input_objects=input_objects,
                output_objects=output_objects,
            )
        )

        output_objects.append(
            quality_report_object
        )

        completed_manifest = create_manifest(
            run_context=run_context,
            status="completed",
            input_zone="bronze",
            output_zone="silver",
            input_objects=input_objects,
            output_objects=output_objects,
            metrics=quality_metrics,
        )

        write_manifest(completed_manifest)

        print("\nSilver V2 processing completed.")
        print(
            f"Input records: "
            f"{quality_metrics['input_records']}"
        )
        print(
            f"Accepted records: "
            f"{quality_metrics['accepted_records']}"
        )
        print(
            f"Quarantined records: "
            f"{quality_metrics['quarantined_records']}"
        )
        print(
            f"Overall quality score: "
            f"{quality_metrics['overall_quality_score']}"
        )

    except Exception as error:
        failed_manifest = create_manifest(
            run_context=run_context,
            status="failed",
            input_zone="bronze",
            output_zone="silver",
            input_objects=input_objects,
            output_objects=output_objects,
            error_message=str(error),
        )

        try:
            write_manifest(failed_manifest)
        except Exception as manifest_error:
            print(
                "Failed to store failure manifest:",
                manifest_error,
            )

        raise


if __name__ == "__main__":
    main()