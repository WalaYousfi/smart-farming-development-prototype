import argparse
from datetime import datetime, timezone
from io import BytesIO
import json
from typing import Any, Dict, List

import pandas as pd

from pipeline.common.config import (
    MINIO_BUCKET,
    QUALITY_REPORT_PREFIX,
    SILVER_PREFIX,
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
from pipeline.gold.silver_run_selector import (
    select_latest_silver_run,
    select_silver_run_by_id,
)
from pipeline.integration.weather_silver_run_selector import (
    select_latest_weather_silver_run,
    select_weather_silver_run_by_id,
)


JOB_NAME = "silver_field_weather_integration"
JOB_VERSION = "2.0.0"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_arguments() -> argparse.Namespace:
    """
    Read optional Field and Weather Silver run IDs.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Integrate canonical Field Silver data with "
            "canonical Weather Silver data."
        )
    )

    parser.add_argument(
        "--field-silver-run-id",
        type=str,
        default=None,
        help=(
            "Field Silver run to use. "
            "When omitted, the latest usable run is selected."
        ),
    )

    parser.add_argument(
        "--weather-silver-run-id",
        type=str,
        default=None,
        help=(
            "Weather Silver run to use. "
            "When omitted, the latest usable run is selected."
        ),
    )

    return parser.parse_args()


def read_parquet_objects(
    minio_client: Any,
    object_names: List[str],
) -> pd.DataFrame:
    """
    Read and combine Parquet objects from MinIO.
    """

    dataframes = []

    for object_name in object_names:
        print(f"Reading Silver object: {object_name}")

        response = minio_client.get_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

        try:
            content = response.read()

            dataframe = pd.read_parquet(
                BytesIO(content)
            )

            dataframes.append(dataframe)

        finally:
            response.close()
            response.release_conn()

    if not dataframes:
        raise RuntimeError(
            "No Silver Parquet data could be loaded."
        )

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def prepare_field_data(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prepare canonical Field Silver data for integration.
    """

    required_columns = [
        "observation_id",
        "farm_id",
        "observed_at",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Field Silver data is missing columns: "
            f"{missing_columns}"
        )

    prepared = dataframe.copy()

    prepared["observed_at"] = pd.to_datetime(
        prepared["observed_at"],
        errors="coerce",
        utc=True,
    )

    prepared["observation_date"] = (
        prepared["observed_at"]
        .dt.strftime("%Y-%m-%d")
    )

    if prepared["observed_at"].isnull().any():
        raise ValueError(
            "Field Silver contains invalid observed_at values."
        )

    return prepared


def prepare_weather_data(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prepare canonical Weather Silver data and prefix
    Weather-specific measurement columns.
    """

    required_columns = [
        "weather_observation_id",
        "farm_id",
        "observed_at",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Weather Silver data is missing columns: "
            f"{missing_columns}"
        )

    prepared = dataframe.copy()

    prepared["observed_at"] = pd.to_datetime(
        prepared["observed_at"],
        errors="coerce",
        utc=True,
    )

    prepared["observation_date"] = (
        prepared["observed_at"]
        .dt.strftime("%Y-%m-%d")
    )

    if prepared["observed_at"].isnull().any():
        raise ValueError(
            "Weather Silver contains invalid observed_at values."
        )

    weather_column_mapping = {
        "observed_at": "weather_observed_at",
        "temperature_celsius": (
            "weather_temperature_celsius"
        ),
        "humidity_percentage": (
            "weather_humidity_percentage"
        ),
        "rainfall_millimeters": (
            "weather_rainfall_millimeters"
        ),
        "sunlight_hours": (
            "weather_sunlight_hours"
        ),
        "latitude": "weather_latitude",
        "longitude": "weather_longitude",
        "source_system": (
            "weather_source_system"
        ),
        "source_event_id": (
            "weather_source_event_id"
        ),
        "source_schema_version": (
            "weather_source_schema_version"
        ),
        "processing_run_id": (
            "weather_processing_run_id"
        ),
        "silver_processing_timestamp": (
            "weather_silver_processing_timestamp"
        ),
        "quality_status": (
            "weather_quality_status"
        ),
    }

    return prepared.rename(
        columns=weather_column_mapping
    )


def integrate_datasets(
    field_dataframe: pd.DataFrame,
    weather_dataframe: pd.DataFrame,
    integration_run_id: str,
) -> pd.DataFrame:
    """
    Join Field and Weather observations using farm ID
    and calendar date.
    """

    integrated = field_dataframe.merge(
        weather_dataframe,
        how="left",
        on=[
            "farm_id",
            "observation_date",
        ],
        suffixes=(
            "_field",
            "_weather",
        ),
        indicator=True,
    )

    integrated["weather_match_status"] = (
        integrated["_merge"]
        .map(
            {
                "both": "matched",
                "left_only": "not_matched",
                "right_only": "weather_only",
            }
        )
        .astype("string")
    )

    integrated = integrated.drop(
        columns=["_merge"]
    )

    integrated[
        "integration_processing_run_id"
    ] = integration_run_id

    integrated[
        "integration_processing_timestamp"
    ] = utc_now()

    return integrated


def upload_parquet(
    minio_client: Any,
    dataframe: pd.DataFrame,
    object_name: str,
) -> str:
    """
    Upload an integrated DataFrame as Parquet.
    """

    buffer = BytesIO()

    dataframe.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
    )

    content = buffer.getvalue()

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(content),
        length=len(content),
        content_type="application/octet-stream",
    )

    print(f"Integrated Silver stored: {object_name}")

    return object_name


def calculate_metrics(
    integrated: pd.DataFrame,
    field_count: int,
    weather_count: int,
) -> Dict[str, Any]:
    """
    Calculate integration coverage metrics.
    """

    matched_records = int(
        (
            integrated["weather_match_status"]
            == "matched"
        ).sum()
    )

    unmatched_records = int(
        (
            integrated["weather_match_status"]
            == "not_matched"
        ).sum()
    )

    match_rate = (
        matched_records / field_count
        if field_count
        else 0.0
    )

    return {
        "field_input_records": field_count,
        "weather_input_records": weather_count,
        "integrated_output_records": int(
            len(integrated)
        ),
        "matched_field_records": matched_records,
        "unmatched_field_records": unmatched_records,
        "weather_match_rate": round(
            match_rate,
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
    Store integration-quality metrics in MinIO.
    """

    report = {
        "run_id": run_id,
        "job_name": JOB_NAME,
        "job_version": JOB_VERSION,
        "created_at": utc_now(),
        "integration_keys": [
            "farm_id",
            "observation_date",
        ],
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
        f"field_weather_integration/"
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
        f"Integration quality report stored: "
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

    input_objects = []
    output_objects = []

    field_silver_run = None
    weather_silver_run = None

    print("Field-Weather integration started.")
    print(f"Integration run ID: {run_context.run_id}")

    try:
        if arguments.field_silver_run_id:
            field_silver_run = (
                select_silver_run_by_id(
                    minio_client=minio_client,
                    silver_run_id=(
                        arguments.field_silver_run_id
                    ),
                )
            )

            print(
                "Using requested Field Silver run: "
                f"{arguments.field_silver_run_id}"
            )

        else:
            field_silver_run = (
                select_latest_silver_run(
                    minio_client
                )
            )

            print(
                "Using latest Field Silver run: "
                f"{field_silver_run['silver_run_id']}"
            )

        if arguments.weather_silver_run_id:
            weather_silver_run = (
                select_weather_silver_run_by_id(
                    minio_client=minio_client,
                    run_id=(
                        arguments.weather_silver_run_id
                    ),
                )
            )

            print(
                "Using requested Weather Silver run: "
                f"{arguments.weather_silver_run_id}"
            )

        else:
            weather_silver_run = (
                select_latest_weather_silver_run(
                    minio_client
                )
            )

            print(
                "Using latest Weather Silver run: "
                f"{weather_silver_run['weather_silver_run_id']}"
            )

        field_objects = field_silver_run[
            "accepted_objects"
        ]

        weather_objects = weather_silver_run[
            "accepted_objects"
        ]

        input_objects = (
            field_objects + weather_objects
        )

        started_manifest = create_manifest(
            run_context=run_context,
            status="started",
            input_zone="silver",
            output_zone="silver",
            input_objects=input_objects,
            metrics={
                "field_silver_run_id": (
                    field_silver_run["silver_run_id"]
                ),
                "weather_silver_run_id": (
                    weather_silver_run[
                        "weather_silver_run_id"
                    ]
                ),
            },
        )

        write_manifest(started_manifest)

        field_dataframe = read_parquet_objects(
            minio_client=minio_client,
            object_names=field_objects,
        )

        weather_dataframe = read_parquet_objects(
            minio_client=minio_client,
            object_names=weather_objects,
        )

        field_dataframe = prepare_field_data(
            field_dataframe
        )

        weather_dataframe = prepare_weather_data(
            weather_dataframe
        )

        integrated_dataframe = integrate_datasets(
            field_dataframe=field_dataframe,
            weather_dataframe=weather_dataframe,
            integration_run_id=run_context.run_id,
        )

        integrated_object = (
            f"{SILVER_PREFIX}/integrated/"
            f"field_weather_observations/"
            f"run_id={run_context.run_id}/"
            f"integrated_observations.parquet"
        )

        upload_parquet(
            minio_client=minio_client,
            dataframe=integrated_dataframe,
            object_name=integrated_object,
        )

        output_objects.append(
            integrated_object
        )

        metrics = calculate_metrics(
            integrated=integrated_dataframe,
            field_count=len(field_dataframe),
            weather_count=len(weather_dataframe),
        )

        quality_report_object = (
            upload_quality_report(
                minio_client=minio_client,
                run_id=run_context.run_id,
                metrics=metrics,
                input_objects=input_objects,
                output_objects=output_objects,
            )
        )

        output_objects.append(
            quality_report_object
        )

        manifest_metrics = {
            **metrics,
            "field_silver_run_id": (
                field_silver_run["silver_run_id"]
            ),
            "weather_silver_run_id": (
                weather_silver_run[
                    "weather_silver_run_id"
                ]
            ),
        }

        completed_manifest = create_manifest(
            run_context=run_context,
            status="completed",
            input_zone="silver",
            output_zone="silver",
            input_objects=input_objects,
            output_objects=output_objects,
            metrics=manifest_metrics,
        )

        write_manifest(completed_manifest)

        lineage_record = create_lineage_record(
            run_id=run_context.run_id,
            job_name=JOB_NAME,
            input_zone="silver",
            output_zone="silver",
            input_objects=input_objects,
            output_objects=output_objects,
            parent_run_ids=[
                field_silver_run["silver_run_id"],
                weather_silver_run[
                    "weather_silver_run_id"
                ],
            ],
            metrics=manifest_metrics,
        )

        write_lineage_record(
            lineage_record
        )

        print("\nIntegration completed.")
        print(
            f"Field records: "
            f"{metrics['field_input_records']}"
        )
        print(
            f"Weather records: "
            f"{metrics['weather_input_records']}"
        )
        print(
            f"Matched field records: "
            f"{metrics['matched_field_records']}"
        )
        print(
            f"Unmatched field records: "
            f"{metrics['unmatched_field_records']}"
        )
        print(
            f"Weather match rate: "
            f"{metrics['weather_match_rate']}"
        )

    except Exception as error:
        failed_manifest = create_manifest(
            run_context=run_context,
            status="failed",
            input_zone="silver",
            output_zone="silver",
            input_objects=input_objects,
            output_objects=output_objects,
            metrics={
                "field_silver_run_id": (
                    field_silver_run[
                        "silver_run_id"
                    ]
                    if field_silver_run
                    else arguments.field_silver_run_id
                ),
                "weather_silver_run_id": (
                    weather_silver_run[
                        "weather_silver_run_id"
                    ]
                    if weather_silver_run
                    else arguments.weather_silver_run_id
                ),
            },
            error_message=str(error),
        )

        try:
            write_manifest(failed_manifest)

        except Exception as manifest_error:
            print(
                "Could not store failed integration "
                "manifest:",
                manifest_error,
            )

        raise


if __name__ == "__main__":
    main()