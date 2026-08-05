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
from pipeline.gold.integration_run_selector import (
    select_integration_run_by_id,
    select_latest_integration_run,
)


JOB_NAME = (
    "gold_integrated_field_anomaly_detection"
)
JOB_VERSION = "2.0.0"

MODEL_NAME = "IsolationForest"
MODEL_VERSION = "2.1.0"

DEFAULT_CONTAMINATION = 0.05
MINIMUM_RECORDS = 20


# We use the complete Field feature set.
# Weather columns are currently enrichment context because
# most Field rows do not yet have matching Weather data.
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
    Read integration run and model settings.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Run Isolation Forest on an integrated "
            "Field-Weather Silver dataset."
        )
    )

    parser.add_argument(
        "--integration-run-id",
        type=str,
        default=None,
        help=(
            "Integration run to process. "
            "When omitted, the latest completed run "
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
    if not 0 < arguments.contamination <= 0.5:
        raise ValueError(
            "--contamination must be greater than 0 "
            "and less than or equal to 0.5."
        )


def read_integrated_data(
    minio_client: Any,
    object_names: List[str],
) -> pd.DataFrame:
    """
    Read integrated Silver Parquet objects.
    """

    dataframes = []

    for object_name in object_names:
        print(
            f"Reading integrated Silver object: "
            f"{object_name}"
        )

        response = minio_client.get_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
        )

        try:
            dataframe = pd.read_parquet(
                BytesIO(response.read())
            )

            dataframes.append(dataframe)

        finally:
            response.close()
            response.release_conn()

    if not dataframes:
        raise RuntimeError(
            "No integrated Silver data was loaded."
        )

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def prepare_features(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Validate and prepare model features.
    """

    missing_columns = [
        column
        for column in FEATURE_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Integrated Silver data is missing "
            f"required features: {missing_columns}"
        )

    if len(dataframe) < MINIMUM_RECORDS:
        raise ValueError(
            "The integrated dataset contains too few "
            f"records. Found: {len(dataframe)}. "
            f"Required: {MINIMUM_RECORDS}."
        )

    features = dataframe[
        FEATURE_COLUMNS
    ].copy()

    for column in FEATURE_COLUMNS:
        features[column] = pd.to_numeric(
            features[column],
            errors="coerce",
        )

    invalid_rows = features.isnull().any(
        axis=1
    )

    if invalid_rows.any():
        raise ValueError(
            f"{int(invalid_rows.sum())} integrated "
            "records contain invalid Field model features."
        )

    return features


def score_observations(
    dataframe: pd.DataFrame,
    feature_dataframe: pd.DataFrame,
    contamination: float,
    gold_run_id: str,
) -> pd.DataFrame:
    """
    Train Isolation Forest and score observations.
    """

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
    )

    feature_matrix = (
        feature_dataframe.to_numpy()
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
    result["gold_processing_run_id"] = (
        gold_run_id
    )
    result["gold_processing_timestamp"] = (
        utc_now()
    )

    return result


def create_alerts(
    scored_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build an integrated Field-Weather alert product.
    """

    alerts = scored_dataframe[
        scored_dataframe["anomaly_label"]
        == "anomaly"
    ].copy()

    if alerts.empty:
        return alerts

    alerts = alerts.sort_values(
        "anomaly_score",
        ascending=True,
    )

    alerts["alert_id"] = [
        str(uuid4())
        for _ in range(len(alerts))
    ]

    alerts["alert_type"] = (
        "integrated_field_weather_anomaly"
    )
    alerts["alert_status"] = "open"

    wanted_columns = [
        "alert_id",
        "alert_type",
        "alert_status",
        "observation_id",
        "farm_id",
        "sensor_id",
        "observed_at",
        "region",
        "crop_type",
        "weather_match_status",
        "weather_observation_id",
        "weather_station_id",
        "weather_observed_at",
        "soil_moisture_percentage",
        "soil_ph",
        "temperature_celsius",
        "weather_temperature_celsius",
        "humidity_percentage",
        "weather_humidity_percentage",
        "rainfall_millimeters",
        "weather_rainfall_millimeters",
        "sunlight_hours",
        "weather_sunlight_hours",
        "wind_speed_kmh",
        "atmospheric_pressure_hpa",
        "ndvi_index",
        "anomaly_score",
        "model_name",
        "model_version",
        "integration_processing_run_id",
        "gold_processing_run_id",
        "gold_processing_timestamp",
    ]

    existing_columns = [
        column
        for column in wanted_columns
        if column in alerts.columns
    ]

    return alerts[existing_columns]


def upload_parquet(
    minio_client: Any,
    dataframe: pd.DataFrame,
    object_name: str,
) -> str:
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
    value: Dict[str, Any],
    object_name: str,
) -> str:
    encoded_content = json.dumps(
        value,
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
    integration_run: Dict[str, Any],
    gold_run_id: str,
    contamination: float,
) -> Dict[str, Any]:
    total_records = int(
        len(scored_dataframe)
    )

    anomaly_records = int(
        (
            scored_dataframe["anomaly_label"]
            == "anomaly"
        ).sum()
    )

    normal_records = (
        total_records - anomaly_records
    )

    if "weather_match_status" in (
        scored_dataframe.columns
    ):
        matched_weather_records = int(
            (
                scored_dataframe[
                    "weather_match_status"
                ]
                == "matched"
            ).sum()
        )
    else:
        matched_weather_records = 0

    matched_anomalies = 0

    if (
        "weather_match_status"
        in scored_dataframe.columns
    ):
        matched_anomalies = int(
            (
                (
                    scored_dataframe[
                        "anomaly_label"
                    ]
                    == "anomaly"
                )
                & (
                    scored_dataframe[
                        "weather_match_status"
                    ]
                    == "matched"
                )
            ).sum()
        )

    return {
        "gold_run_id": gold_run_id,
        "integration_run_id": integration_run[
            "integration_run_id"
        ],
        "field_silver_run_id": integration_run[
            "field_silver_run_id"
        ],
        "weather_silver_run_id": integration_run[
            "weather_silver_run_id"
        ],
        "created_at": utc_now(),
        "data_product": (
            "integrated_field_weather_"
            "anomaly_summary"
        ),
        "model": {
            "name": MODEL_NAME,
            "version": MODEL_VERSION,
            "contamination": contamination,
            "feature_columns": FEATURE_COLUMNS,
            "weather_feature_policy": (
                "Weather fields are enrichment context "
                "until integration coverage is sufficient."
            ),
        },
        "metrics": {
            "total_records": total_records,
            "normal_records": normal_records,
            "anomaly_records": anomaly_records,
            "anomaly_rate": round(
                anomaly_records / total_records,
                4,
            ),
            "weather_matched_records": (
                matched_weather_records
            ),
            "weather_match_rate": round(
                matched_weather_records
                / total_records,
                4,
            ),
            "weather_matched_anomalies": (
                matched_anomalies
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

    selected_run = None
    selection_mode = ""
    input_objects = []  # type: List[str]
    output_objects = []  # type: List[str]

    print(
        "Integrated Gold anomaly detection started."
    )
    print(f"Gold run ID: {run_context.run_id}")

    try:
        if arguments.integration_run_id:
            selected_run = (
                select_integration_run_by_id(
                    minio_client=minio_client,
                    run_id=(
                        arguments.integration_run_id
                    ),
                )
            )

            selection_mode = "explicit"

            print(
                "Using requested integration run: "
                f"{arguments.integration_run_id}"
            )

        else:
            selected_run = (
                select_latest_integration_run(
                    minio_client
                )
            )

            selection_mode = "latest"

            print(
                "Using latest integration run: "
                f"{selected_run['integration_run_id']}"
            )

        input_objects = selected_run[
            "integrated_objects"
        ]

        started_manifest = create_manifest(
            run_context=run_context,
            status="started",
            input_zone="silver",
            output_zone="gold",
            input_objects=input_objects,
            metrics={
                "integration_run_id": (
                    selected_run[
                        "integration_run_id"
                    ]
                ),
                "selection_mode": selection_mode,
                "contamination": (
                    arguments.contamination
                ),
            },
        )

        write_manifest(started_manifest)

        integrated_dataframe = (
            read_integrated_data(
                minio_client=minio_client,
                object_names=input_objects,
            )
        )

        print(
            "Integrated records loaded: "
            f"{len(integrated_dataframe)}"
        )

        feature_dataframe = prepare_features(
            integrated_dataframe
        )

        scored_dataframe = score_observations(
            dataframe=integrated_dataframe,
            feature_dataframe=feature_dataframe,
            contamination=arguments.contamination,
            gold_run_id=run_context.run_id,
        )

        alerts_dataframe = create_alerts(
            scored_dataframe
        )

        scored_object = (
            f"{GOLD_PREFIX}/analytical/"
            f"integrated_field_weather_scores/"
            f"run_id={run_context.run_id}/"
            f"scored_integrated_observations.parquet"
        )

        upload_parquet(
            minio_client=minio_client,
            dataframe=scored_dataframe,
            object_name=scored_object,
        )

        output_objects.append(scored_object)

        if not alerts_dataframe.empty:
            alerts_object = (
                f"{GOLD_PREFIX}/data-products/"
                f"integrated_field_weather_alerts/"
                f"run_id={run_context.run_id}/"
                f"alerts.parquet"
            )

            upload_parquet(
                minio_client=minio_client,
                dataframe=alerts_dataframe,
                object_name=alerts_object,
            )

            output_objects.append(
                alerts_object
            )

        summary = create_summary(
            scored_dataframe=scored_dataframe,
            integration_run=selected_run,
            gold_run_id=run_context.run_id,
            contamination=arguments.contamination,
        )

        summary_object = (
            f"{GOLD_PREFIX}/data-products/"
            f"integrated_anomaly_summary/"
            f"run_id={run_context.run_id}/"
            f"summary.json"
        )

        upload_json(
            minio_client=minio_client,
            value=summary,
            object_name=summary_object,
        )

        output_objects.append(summary_object)

        manifest_metrics = {
            **summary["metrics"],
            "integration_run_id": selected_run[
                "integration_run_id"
            ],
            "field_silver_run_id": selected_run[
                "field_silver_run_id"
            ],
            "weather_silver_run_id": selected_run[
                "weather_silver_run_id"
            ],
            "selection_mode": selection_mode,
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
                selected_run[
                    "integration_run_id"
                ]
            ],
            metrics=manifest_metrics,
        )

        write_lineage_record(
            lineage_record
        )

        print(
            "\nIntegrated Gold processing completed."
        )
        print(
            "Total records: "
            f"{summary['metrics']['total_records']}"
        )
        print(
            "Normal records: "
            f"{summary['metrics']['normal_records']}"
        )
        print(
            "Anomaly records: "
            f"{summary['metrics']['anomaly_records']}"
        )
        print(
            "Weather-matched records: "
            f"{summary['metrics']['weather_matched_records']}"
        )
        print(
            "Weather-matched anomalies: "
            f"{summary['metrics']['weather_matched_anomalies']}"
        )

    except Exception as error:
        failed_manifest = create_manifest(
            run_context=run_context,
            status="failed",
            input_zone="silver",
            output_zone="gold",
            input_objects=input_objects,
            output_objects=output_objects,
            metrics={
                "integration_run_id": (
                    selected_run[
                        "integration_run_id"
                    ]
                    if selected_run
                    else arguments.integration_run_id
                ),
                "selection_mode": selection_mode,
                "contamination": (
                    arguments.contamination
                ),
            },
            error_message=str(error),
        )

        try:
            write_manifest(failed_manifest)

        except Exception as manifest_error:
            print(
                "Could not store failed integrated "
                "Gold manifest:",
                manifest_error,
            )

        raise


if __name__ == "__main__":
    main()