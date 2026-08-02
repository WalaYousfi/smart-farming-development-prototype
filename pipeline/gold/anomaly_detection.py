import argparse
from datetime import datetime, timezone
from io import BytesIO
import json
from typing import Any, Dict, List
from uuid import uuid4

import pandas as pd
from sklearn.ensemble import IsolationForest

from pipeline.common.config import (
    GOLD_PREFIX,
    MINIO_BUCKET,
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


JOB_NAME = "gold_field_anomaly_detection"
JOB_VERSION = "2.0.0"

MODEL_NAME = "IsolationForest"
MODEL_VERSION = "2.0.0"

DEFAULT_CONTAMINATION = 0.05
MINIMUM_TRAINING_RECORDS = 20


FEATURE_COLUMNS = [
    "soil_moisture_percentage",
    "soil_ph",
    "temperature_celsius",
    "rainfall_millimeters",
    "humidity_percentage",
    "sunlight_hours",
    "pesticide_usage_milliliters",
    "ndvi_index",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_arguments() -> argparse.Namespace:
    """
    Read Gold-processing command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Run Isolation Forest on one completed "
            "canonical Silver dataset."
        )
    )

    parser.add_argument(
        "--silver-run-id",
        type=str,
        default=None,
        help=(
            "Completed Silver run to process. "
            "When omitted, the latest usable Silver run "
            "is selected."
        ),
    )

    parser.add_argument(
        "--contamination",
        type=float,
        default=DEFAULT_CONTAMINATION,
        help=(
            "Expected anomaly proportion. "
            "Default: 0.05"
        ),
    )

    return parser.parse_args()


def validate_arguments(
    arguments: argparse.Namespace,
) -> None:
    """
    Validate command-line arguments.
    """

    if not 0 < arguments.contamination <= 0.5:
        raise ValueError(
            "--contamination must be greater than 0 "
            "and less than or equal to 0.5."
        )


def read_silver_data(
    minio_client: Any,
    object_names: List[str],
) -> pd.DataFrame:
    """
    Read and combine accepted Silver Parquet objects.
    """

    dataframes = []

    for object_name in object_names:
        print(
            f"Reading accepted Silver object: "
            f"{object_name}"
        )

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
            "No accepted Silver data could be loaded."
        )

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def validate_feature_data(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Verify that all required ML features are available
    and usable.
    """

    missing_columns = [
        column
        for column in FEATURE_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Silver data is missing required ML columns: "
            f"{missing_columns}"
        )

    if len(dataframe) < MINIMUM_TRAINING_RECORDS:
        raise ValueError(
            "The selected Silver run contains too few "
            "accepted records for this anomaly model.\n"
            f"Records found: {len(dataframe)}\n"
            f"Minimum required: {MINIMUM_TRAINING_RECORDS}\n"
            "Choose a larger Silver run using "
            "--silver-run-id."
        )

    feature_dataframe = dataframe[
        FEATURE_COLUMNS
    ].copy()

    for column in FEATURE_COLUMNS:
        feature_dataframe[column] = pd.to_numeric(
            feature_dataframe[column],
            errors="coerce",
        )

    missing_feature_rows = (
        feature_dataframe.isnull().any(axis=1)
    )

    if missing_feature_rows.any():
        invalid_count = int(
            missing_feature_rows.sum()
        )

        raise ValueError(
            f"{invalid_count} accepted Silver records "
            "contain missing or nonnumeric ML features."
        )

    return feature_dataframe


def run_isolation_forest(
    dataframe: pd.DataFrame,
    feature_dataframe: pd.DataFrame,
    contamination: float,
    gold_run_id: str,
) -> pd.DataFrame:
    """
    Train Isolation Forest and enrich every observation
    with anomaly results.
    """

    # NumPy avoids the older scikit-learn
    # feature-name warning you encountered previously.
    feature_matrix = feature_dataframe.to_numpy()

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
    )

    predictions = model.fit_predict(
        feature_matrix
    )

    scores = model.decision_function(
        feature_matrix
    )

    result = dataframe.copy()

    result["anomaly_prediction"] = predictions

    result["anomaly_label"] = (
        pd.Series(predictions)
        .map(
            {
                1: "normal",
                -1: "anomaly",
            }
        )
        .values
    )

    result["anomaly_score"] = scores
    result["model_name"] = MODEL_NAME
    result["model_version"] = MODEL_VERSION
    result["model_contamination"] = contamination
    result["gold_processing_run_id"] = gold_run_id
    result["gold_processing_timestamp"] = utc_now()

    return result


def create_alert_product(
    scored_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create a purpose-specific Gold anomaly-alert product.
    """

    anomalies = scored_dataframe[
        scored_dataframe["anomaly_label"]
        == "anomaly"
    ].copy()

    if anomalies.empty:
        return anomalies

    anomalies = anomalies.sort_values(
        "anomaly_score",
        ascending=True,
    )

    anomalies["alert_id"] = [
        str(uuid4())
        for _ in range(len(anomalies))
    ]

    anomalies["alert_type"] = (
        "multivariate_field_anomaly"
    )

    anomalies["alert_status"] = "open"

    alert_columns = [
        "alert_id",
        "alert_type",
        "alert_status",
        "observation_id",
        "farm_id",
        "sensor_id",
        "observed_at",
        "region",
        "crop_type",
        "soil_moisture_percentage",
        "soil_ph",
        "temperature_celsius",
        "rainfall_millimeters",
        "humidity_percentage",
        "sunlight_hours",
        "pesticide_usage_milliliters",
        "ndvi_index",
        "anomaly_score",
        "model_name",
        "model_version",
        "model_contamination",
        "source_system",
        "source_event_id",
        "processing_run_id",
        "gold_processing_run_id",
        "gold_processing_timestamp",
    ]

    existing_columns = [
        column
        for column in alert_columns
        if column in anomalies.columns
    ]

    return anomalies[existing_columns]


def upload_parquet(
    minio_client: Any,
    dataframe: pd.DataFrame,
    object_name: str,
) -> str:
    """
    Upload a DataFrame as Parquet.
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

    print(f"Gold Parquet stored: {object_name}")

    return object_name


def upload_json(
    minio_client: Any,
    content: Dict[str, Any],
    object_name: str,
) -> str:
    """
    Upload a dictionary as formatted JSON.
    """

    encoded_content = json.dumps(
        content,
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

    print(f"Gold JSON stored: {object_name}")

    return object_name


def create_summary(
    scored_dataframe: pd.DataFrame,
    silver_run: Dict[str, Any],
    gold_run_id: str,
    contamination: float,
) -> Dict[str, Any]:
    """
    Create a Gold anomaly-detection summary product.
    """

    normal_count = int(
        (
            scored_dataframe["anomaly_label"]
            == "normal"
        ).sum()
    )

    anomaly_count = int(
        (
            scored_dataframe["anomaly_label"]
            == "anomaly"
        ).sum()
    )

    total_records = int(
        len(scored_dataframe)
    )

    anomaly_rate = (
        anomaly_count / total_records
        if total_records
        else 0.0
    )

    return {
        "gold_run_id": gold_run_id,
        "silver_run_id": silver_run[
            "silver_run_id"
        ],
        "bronze_run_id": silver_run.get(
            "bronze_run_id"
        ),
        "created_at": utc_now(),
        "data_product": (
            "field_anomaly_detection_summary"
        ),
        "model": {
            "name": MODEL_NAME,
            "version": MODEL_VERSION,
            "contamination": contamination,
            "feature_columns": FEATURE_COLUMNS,
            "n_estimators": 200,
            "random_state": 42,
        },
        "metrics": {
            "total_records": total_records,
            "normal_records": normal_count,
            "anomaly_records": anomaly_count,
            "anomaly_rate": round(
                anomaly_rate,
                4,
            ),
            "minimum_anomaly_score": float(
                scored_dataframe[
                    "anomaly_score"
                ].min()
            ),
            "maximum_anomaly_score": float(
                scored_dataframe[
                    "anomaly_score"
                ].max()
            ),
            "average_anomaly_score": float(
                scored_dataframe[
                    "anomaly_score"
                ].mean()
            ),
        },
    }


def main() -> None:
    arguments = parse_arguments()
    validate_arguments(arguments)

    run_context = create_run_context(
        job_name=JOB_NAME,
        job_version=JOB_VERSION,
    )

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    input_objects = []  # type: List[str]
    output_objects = []  # type: List[str]

    selected_silver_run = None
    selection_mode = ""

    print("Gold V2 anomaly detection started.")
    print(f"Gold run ID: {run_context.run_id}")

    try:
        if arguments.silver_run_id:
            selected_silver_run = (
                select_silver_run_by_id(
                    minio_client=minio_client,
                    silver_run_id=(
                        arguments.silver_run_id
                    ),
                )
            )

            selection_mode = "explicit"

            print(
                "Using explicitly requested Silver run: "
                f"{arguments.silver_run_id}"
            )

        else:
            selected_silver_run = (
                select_latest_silver_run(
                    minio_client
                )
            )

            selection_mode = "latest"

            print(
                "No Silver run was specified."
            )
            print(
                "Using latest usable Silver run: "
                f"{selected_silver_run['silver_run_id']}"
            )

        input_objects = selected_silver_run[
            "accepted_objects"
        ]

        started_manifest = create_manifest(
            run_context=run_context,
            status="started",
            input_zone="silver",
            output_zone="gold",
            input_objects=input_objects,
            metrics={
                "silver_run_id": (
                    selected_silver_run[
                        "silver_run_id"
                    ]
                ),
                "silver_run_selection_mode": (
                    selection_mode
                ),
                "contamination": (
                    arguments.contamination
                ),
            },
        )

        write_manifest(started_manifest)

        silver_dataframe = read_silver_data(
            minio_client=minio_client,
            object_names=input_objects,
        )

        print(
            f"Accepted Silver records loaded: "
            f"{len(silver_dataframe)}"
        )

        feature_dataframe = validate_feature_data(
            silver_dataframe
        )

        scored_dataframe = run_isolation_forest(
            dataframe=silver_dataframe,
            feature_dataframe=feature_dataframe,
            contamination=arguments.contamination,
            gold_run_id=run_context.run_id,
        )

        alert_dataframe = create_alert_product(
            scored_dataframe
        )

        scored_object = (
            f"{GOLD_PREFIX}/analytical/"
            f"field_observation_scores/"
            f"run_id={run_context.run_id}/"
            f"scored_field_observations.parquet"
        )

        upload_parquet(
            minio_client=minio_client,
            dataframe=scored_dataframe,
            object_name=scored_object,
        )

        output_objects.append(scored_object)

        if not alert_dataframe.empty:
            alerts_object = (
                f"{GOLD_PREFIX}/data-products/"
                f"field_anomaly_alerts/"
                f"run_id={run_context.run_id}/"
                f"field_anomaly_alerts.parquet"
            )

            upload_parquet(
                minio_client=minio_client,
                dataframe=alert_dataframe,
                object_name=alerts_object,
            )

            output_objects.append(alerts_object)

        summary = create_summary(
            scored_dataframe=scored_dataframe,
            silver_run=selected_silver_run,
            gold_run_id=run_context.run_id,
            contamination=arguments.contamination,
        )

        summary_object = (
            f"{GOLD_PREFIX}/data-products/"
            f"anomaly_detection_summary/"
            f"run_id={run_context.run_id}/"
            f"summary.json"
        )

        upload_json(
            minio_client=minio_client,
            content=summary,
            object_name=summary_object,
        )

        output_objects.append(summary_object)

        manifest_metrics = {
            **summary["metrics"],
            "silver_run_id": (
                selected_silver_run[
                    "silver_run_id"
                ]
            ),
            "bronze_run_id": (
                selected_silver_run.get(
                    "bronze_run_id"
                )
            ),
            "silver_run_selection_mode": (
                selection_mode
            ),
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "contamination": (
                arguments.contamination
            ),
        }

        completed_manifest = create_manifest(
            run_context=run_context,
            status="completed",
            input_zone="silver",
            output_zone="gold",
            input_objects=input_objects,
            output_objects=output_objects,
            metrics=manifest_metrics,
        )

        write_manifest(completed_manifest)

        lineage_record = create_lineage_record(
            run_id=run_context.run_id,
            job_name=JOB_NAME,
            input_zone="silver",
            output_zone="gold",
            input_objects=input_objects,
            output_objects=output_objects,
            parent_run_ids=[
                selected_silver_run[
                    "silver_run_id"
                ]
            ],
            metrics=manifest_metrics,
        )

        write_lineage_record(lineage_record)

        print("\nGold V2 processing completed.")
        print(
            f"Total records: "
            f"{summary['metrics']['total_records']}"
        )
        print(
            f"Normal records: "
            f"{summary['metrics']['normal_records']}"
        )
        print(
            f"Anomaly records: "
            f"{summary['metrics']['anomaly_records']}"
        )
        print(
            f"Anomaly rate: "
            f"{summary['metrics']['anomaly_rate']}"
        )

    except Exception as error:
        failed_metrics = {
            "silver_run_id": (
                selected_silver_run[
                    "silver_run_id"
                ]
                if selected_silver_run
                else arguments.silver_run_id
            ),
            "silver_run_selection_mode": (
                selection_mode
            ),
            "contamination": (
                arguments.contamination
            ),
        }

        failed_manifest = create_manifest(
            run_context=run_context,
            status="failed",
            input_zone="silver",
            output_zone="gold",
            input_objects=input_objects,
            output_objects=output_objects,
            metrics=failed_metrics,
            error_message=str(error),
        )

        try:
            write_manifest(failed_manifest)

        except Exception as manifest_error:
            print(
                "Could not store failed Gold manifest:",
                manifest_error,
            )

        raise


if __name__ == "__main__":
    main()