from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4


def clean_required_text(
    values: Dict[str, Any],
    field_name: str,
) -> str:
    """
    Return a required non-empty string.
    """

    value = values.get(field_name)

    if value is None:
        raise ValueError(
            f"Required field '{field_name}' is missing."
        )

    cleaned_value = str(value).strip()

    if not cleaned_value:
        raise ValueError(
            f"Required field '{field_name}' is empty."
        )

    return cleaned_value


def required_float(
    values: Dict[str, Any],
    field_name: str,
) -> float:
    """
    Convert a mandatory value to float.
    """

    value = values.get(field_name)

    if value is None or str(value).strip() == "":
        raise ValueError(
            f"Required numeric field '{field_name}' "
            "is missing."
        )

    try:
        return float(value)

    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Field '{field_name}' must be numeric. "
            f"Received: {value!r}"
        ) from error


def optional_float(
    value: Any,
) -> Optional[float]:
    """
    Convert an optional value to float.
    """

    if value is None or str(value).strip() == "":
        return None

    try:
        return float(value)

    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Optional numeric value is invalid: "
            f"{value!r}"
        ) from error


def normalize_timestamp(
    value: Any,
    field_name: str,
) -> str:
    """
    Convert a timestamp to UTC ISO-8601 format.
    """

    if value is None or not str(value).strip():
        raise ValueError(
            f"Required timestamp '{field_name}' "
            "is missing."
        )

    normalized_value = str(value).strip().replace(
        "Z",
        "+00:00",
    )

    try:
        parsed_timestamp = datetime.fromisoformat(
            normalized_value
        )

    except ValueError as error:
        raise ValueError(
            f"Field '{field_name}' contains an invalid "
            f"timestamp: {value!r}"
        ) from error

    if parsed_timestamp.tzinfo is None:
        parsed_timestamp = parsed_timestamp.replace(
            tzinfo=timezone.utc
        )

    return parsed_timestamp.astimezone(
        timezone.utc
    ).isoformat()


def map_weather_bronze_to_canonical(
    bronze_record: Dict[str, Any],
    silver_run_id: str,
) -> Dict[str, Any]:
    """
    Convert one Weather Bronze envelope into the
    canonical Silver weather schema.
    """

    metadata = bronze_record.get("metadata")
    payload = bronze_record.get("payload")

    if not isinstance(metadata, dict):
        raise ValueError(
            "Weather Bronze record requires a valid "
            "metadata object."
        )

    if not isinstance(payload, dict):
        raise ValueError(
            "Weather Bronze record requires a valid "
            "payload object."
        )

    return {
        "weather_observation_id": str(uuid4()),

        "farm_id": clean_required_text(
            payload,
            "farm_id",
        ),

        "weather_station_id": clean_required_text(
            payload,
            "weather_station_id",
        ),

        "observed_at": normalize_timestamp(
            payload.get("observed_at"),
            "observed_at",
        ),

        "temperature_celsius": required_float(
            payload,
            "air_temperature_celsius",
        ),

        "humidity_percentage": required_float(
            payload,
            "relative_humidity_percentage",
        ),

        "rainfall_millimeters": required_float(
            payload,
            "rainfall_millimeters",
        ),

        "sunlight_hours": required_float(
            payload,
            "sunlight_duration_hours",
        ),

        "wind_speed_kmh": optional_float(
            payload.get("wind_speed_kmh")
        ),

        "atmospheric_pressure_hpa": optional_float(
            payload.get(
                "atmospheric_pressure_hpa"
            )
        ),

        "latitude": required_float(
            payload,
            "latitude",
        ),

        "longitude": required_float(
            payload,
            "longitude",
        ),

        "source_system": clean_required_text(
            metadata,
            "source_system",
        ),

        "source_event_id": clean_required_text(
            metadata,
            "event_id",
        ),

        "source_schema_version": clean_required_text(
            metadata,
            "source_schema_version",
        ),

        "bronze_ingestion_timestamp": (
            normalize_timestamp(
                metadata.get(
                    "ingestion_timestamp"
                ),
                "ingestion_timestamp",
            )
        ),

        "processing_run_id": silver_run_id,

        "silver_processing_timestamp": (
            datetime.now(timezone.utc).isoformat()
        ),

        "quality_status": "accepted",
    }