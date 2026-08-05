import argparse
from datetime import datetime, timezone
from io import BytesIO
import json
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from pipeline.common.config import (
    MINIO_BUCKET,
    QUALITY_REPORT_PREFIX,
    SILVER_PREFIX,
    WEATHER_SOURCE_SYSTEM,
)
from pipeline.common.lineage import (
    create_lineage_record,
    write_lineage_record,
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
from pipeline.silver.quarantine import (
    create_quarantine_record,
)
from pipeline.silver.weather_bronze_run_selector import (
    list_objects_for_weather_bronze_run,
    select_latest_weather_bronze_run_id,
)
from pipeline.silver.weather_canonical_mapper import (
    map_weather_bronze_to_canonical,
)


JOB_NAME = "silver_weather_observations"
JOB_VERSION = "2.0.0"

SOURCE_SCHEMA_PATH = (
    "source/weather_observation_json.schema.json"
)

CANONICAL_SCHEMA_PATH = (
    "canonical/weather_observation_v1.schema.json"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_arguments() -> argparse.Namespace:
    """
    Read the optional Weather Bronze run ID.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Transform one Weather Bronze run "
            "into canonical Weather Silver outputs."
        )
    )

    parser.add_argument(
        "--bronze-run-id",
        type=str,
        default=None,
        help=(
            "Weather Bronze run ID to process. "
            "When omitted, the newest run is selected."
        ),
    )

    return parser.parse_args()


def read_bronze_object(
    minio_client: Any,
    object_name: str,
) -> List[Dict[str, Any]]:
    """
    Read one Weather Bronze JSONL object.
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
                record["_bronze_line_number"] = (
                    line_number
                )

                records.append(record)

            except json.JSONDecodeError as error:
                records.append(
                    {
                        "_read_error": str(error),
                        "_raw_line": line,
                        "_bronze_object": object_name,
                        "_bronze_line_number": (
                            line_number
                        ),
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
    Load all objects belonging to one Weather
    Bronze run.
    """

    records = []

    for object_name in object_names:
        print(
            f"Reading Weather Bronze object: "
            f"{object_name}"
        )

        records.extend(
            read_bronze_object(
                minio_client=minio_client,
                object_name=object_name,
            )
        )

    return records


def add_bronze_location(
    quarantine_record: Dict[str, Any],
    bronze_record: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Add the original MinIO object and line number.
    """

    quarantine_record["bronze_object"] = (
        bronze_record.get("_bronze_object")
    )

    quarantine_record["bronze_line_number"] = (
        bronze_record.get(
            "_bronze_line_number"
        )
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
    Validate, map and separate accepted and
    quarantined Weather records.
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
        "bronze_envelope_failures": 0,
        "source_schema_failures": 0,
        "mapping_failures": 0,
        "canonical_schema_failures": 0,
    }

    for bronze_record in bronze_records:
        counters["input_records"] += 1

        if "_read_error" in bronze_record:
            quarantine_records.append(
                {
                    "event_id": None,
                    "source_system": (
                        WEATHER_SOURCE_SYSTEM
                    ),
                    "bronze_run_id": None,
                    "silver_run_id": silver_run_id,
                    "failed_stage": (
                        "bronze_json_reading"
                    ),
                    "failure_reasons": [
                        {
                            "field": "$",
                            "message": bronze_record[
                                "_read_error"
                            ],
                        }
                    ],
                    "quarantine_timestamp": utc_now(),
                    "bronze_object": (
                        bronze_record.get(
                            "_bronze_object"
                        )
                    ),
                    "bronze_line_number": (
                        bronze_record.get(
                            "_bronze_line_number"
                        )
                    ),
                    "original_record": (
                        bronze_record.get(
                            "_raw_line"
                        )
                    ),
                }
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
                    failed_stage=(
                        "bronze_envelope_validation"
                    ),
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
                add_bronze_location(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters[
                "bronze_envelope_failures"
            ] += 1

            counters["quarantined_records"] += 1
            continue

        if not isinstance(payload, dict):
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage=(
                        "bronze_envelope_validation"
                    ),
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
                add_bronze_location(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters[
                "bronze_envelope_failures"
            ] += 1

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
                            "field": (
                                "metadata.event_id"
                            ),
                            "message": (
                                "Duplicate event_id "
                                "detected."
                            ),
                        }
                    ],
                )
            )

            quarantine_records.append(
                add_bronze_location(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["duplicate_records"] += 1
            counters["quarantined_records"] += 1
            continue

        if event_id:
            seen_event_ids.add(event_id)

        source_errors = get_validation_errors(
            payload,
            source_validator,
        )

        if source_errors:
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage=(
                        "weather_source_schema_validation"
                    ),
                    failure_reasons=source_errors,
                )
            )

            quarantine_records.append(
                add_bronze_location(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["source_schema_failures"] += 1
            counters["quarantined_records"] += 1
            continue

        try:
            canonical_record = (
                map_weather_bronze_to_canonical(
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
                    failed_stage=(
                        "weather_canonical_mapping"
                    ),
                    failure_reasons=[
                        {
                            "field": "$",
                            "message": str(error),
                        }
                    ],
                )
            )

            quarantine_records.append(
                add_bronze_location(
                    quarantine_record,
                    bronze_record,
                )
            )

            counters["mapping_failures"] += 1
            counters["quarantined_records"] += 1
            continue

        canonical_errors = get_validation_errors(
            canonical_record,
            canonical_validator,
        )

        if canonical_errors:
            quarantine_record = (
                create_quarantine_record(
                    bronze_record=bronze_record,
                    silver_run_id=silver_run_id,
                    failed_stage=(
                        "weather_canonical_schema_"
                        "validation"
                    ),
                    failure_reasons=canonical_errors,
                )
            )

            quarantine_record[
                "candidate_canonical_record"
            ] = canonical_record

            quarantine_records.append(
                add_bronze_location(
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
    Store trusted canonical Weather records
    as Parquet.
    """

    if not accepted_records:
        return ""

    dataframe = pd.DataFrame(
        accepted_records
    )

    buffer = BytesIO()

    dataframe.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
    )

    content = buffer.getvalue()

    object_name = (
        f"{SILVER_PREFIX}/accepted/"
        f"weather_observations/"
        f"run_id={run_id}/"
        f"weather_observations.parquet"
    )

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(content),
        length=len(content),
        content_type="application/octet-stream",
    )

    print(
        f"Accepted Weather Silver data stored: "
        f"{object_name}"
    )

    return object_name


def upload_quarantine_records(
    minio_client: Any,
    quarantine_records: List[Dict[str, Any]],
    run_id: str,
) -> str:
    """
    Store rejected Weather records as JSONL.
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

    encoded_content = jsonl_content.encode(
        "utf-8"
    )

    object_name = (
        f"{SILVER_PREFIX}/quarantine/"
        f"weather_observations/"
        f"run_id={run_id}/"
        f"rejected_weather_records.jsonl"
    )

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(encoded_content),
        length=len(encoded_content),
        content_type="application/x-ndjson",
    )

    print(
        f"Weather quarantine stored: "
        f"{object_name}"
    )

    return object_name


def calculate_quality_metrics(
    counters: Dict[str, int],
) -> Dict[str, Any]:
    """
    Calculate Weather Silver quality metrics.
    """

    input_records = counters["input_records"]
    accepted_records = counters[
        "accepted_records"
    ]
    quarantined_records = counters[
        "quarantined_records"
    ]
    duplicate_records = counters[
        "duplicate_records"
    ]

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
    Store the Weather Silver quality report.
    """

    report = {
        "run_id": run_id,
        "job_name": JOB_NAME,
        "job_version": JOB_VERSION,
        "source_system": WEATHER_SOURCE_SYSTEM,
        "created_at": utc_now(),
        "input_zone": "bronze",
        "output_zone": "silver",
        "input_objects": input_objects,
        "output_objects": output_objects,
        "metrics": metrics,
    }

    encoded_content = json.dumps(
        report,
        indent=2,
        ensure_ascii=False,
    ).encode("utf-8")

    object_name = (
        f"{QUALITY_REPORT_PREFIX}/silver/"
        f"weather_observations/"
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
        f"Weather quality report stored: "
        f"{object_name}"
    )

    return object_name


def main() -> None:
    arguments = parse_arguments()

    run_context = create_run_context(
        job_name=JOB_NAME,
        job_version=JOB_VERSION,
    )

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    input_objects = []  # type: List[str]
    output_objects = []  # type: List[str]

    bronze_run_id = ""

    started_manifest = create_manifest(
        run_context=run_context,
        status="started",
        input_zone="bronze",
        output_zone="silver",
        metrics={
            "input_records": 0,
            "accepted_records": 0,
            "quarantined_records": 0,
            "source_system": WEATHER_SOURCE_SYSTEM,
        },
    )

    write_manifest(started_manifest)

    print("Weather Silver processor started.")
    print(f"Silver run ID: {run_context.run_id}")

    try:
        if arguments.bronze_run_id:
            bronze_run_id = (
                arguments.bronze_run_id
            )

            selection_mode = "explicit"

            print(
                "Using explicitly requested Weather "
                f"Bronze run: {bronze_run_id}"
            )

        else:
            bronze_run_id = (
                select_latest_weather_bronze_run_id(
                    minio_client
                )
            )

            selection_mode = "latest"

            print(
                "Using latest Weather Bronze run: "
                f"{bronze_run_id}"
            )

        input_objects = (
            list_objects_for_weather_bronze_run(
                minio_client=minio_client,
                bronze_run_id=bronze_run_id,
            )
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
            output_objects.append(
                accepted_object
            )

        quarantine_object = (
            upload_quarantine_records(
                minio_client=minio_client,
                quarantine_records=(
                    quarantine_records
                ),
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

        manifest_metrics = {
            **quality_metrics,
            "bronze_run_id": bronze_run_id,
            "bronze_run_selection_mode": (
                selection_mode
            ),
            "source_system": (
                WEATHER_SOURCE_SYSTEM
            ),
        }

        completed_manifest = create_manifest(
            run_context=run_context,
            status="completed",
            input_zone="bronze",
            output_zone="silver",
            input_objects=input_objects,
            output_objects=output_objects,
            metrics=manifest_metrics,
        )

        write_manifest(completed_manifest)

        lineage_record = create_lineage_record(
            run_id=run_context.run_id,
            job_name=JOB_NAME,
            input_zone="bronze",
            output_zone="silver",
            input_objects=input_objects,
            output_objects=output_objects,
            parent_run_ids=[
                bronze_run_id,
            ],
            metrics=manifest_metrics,
        )

        write_lineage_record(lineage_record)

        print(
            "\nWeather Silver processing completed."
        )

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
            metrics={
                "bronze_run_id": bronze_run_id,
                "source_system": (
                    WEATHER_SOURCE_SYSTEM
                ),
            },
            error_message=str(error),
        )

        try:
            write_manifest(failed_manifest)

        except Exception as manifest_error:
            print(
                "Could not store failed Weather "
                "Silver manifest:",
                manifest_error,
            )

        raise


if __name__ == "__main__":
    main()