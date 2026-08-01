# What this file does

# It performs 3 tasks.

# Renaming
# soil_moisture_%     → soil_moisture_percentage
# soil_pH             → soil_ph
# temperature_C       → temperature_celsius



# Type conversion

# Values arriving from CSV may be strings:

# {
#   "temperature_C": "27.5"
# }

# The mapper converts them to:

# {
#   "temperature_celsius": 27.5
# }



# Metadata preservation

# It keeps the Bronze traceability fields:

# source_system
# source_event_id
# source_schema_version
# bronze_ingestion_timestamp
# processing_run_id






from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4


def clean_optional_text(value: Any) -> Optional[str]:
    """
    Convert an optional value into clean text.

    Empty strings become None.
    """

    if value is None:
        return None

    cleaned_value = str(value).strip()

    if cleaned_value == "":
        return None

    return cleaned_value


def required_text(
    payload: Dict[str, Any],
    field_name: str,
) -> str:
    """
    Read a mandatory text value.

    Raises ValueError when it is missing or empty.
    """

    value = clean_optional_text(
        payload.get(field_name)
    )

    if value is None:
        raise ValueError(
            f"Required field '{field_name}' is missing."
        )

    return value


def optional_float(
    value: Any,
) -> Optional[float]:
    """
    Convert an optional value to float.

    Empty values become None.
    """

    if value is None:
        return None

    cleaned_value = str(value).strip()

    if cleaned_value == "":
        return None

    return float(cleaned_value)


def required_float(
    payload: Dict[str, Any],
    field_name: str,
) -> float:
    """
    Convert a mandatory field to float.
    """

    value = optional_float(
        payload.get(field_name)
    )

    if value is None:
        raise ValueError(
            f"Required numeric field '{field_name}' is missing."
        )

    return value


def optional_integer(
    value: Any,
) -> Optional[int]:
    """
    Convert an optional value to integer.
    """

    if value is None:
        return None

    cleaned_value = str(value).strip()

    if cleaned_value == "":
        return None

    return int(float(cleaned_value))


def normalize_timestamp(
    value: Any,
) -> str:
    """
    Convert the source timestamp into ISO-8601 format.

    This first version accepts the existing dataset timestamp.
    """

    cleaned_value = clean_optional_text(value)

    if cleaned_value is None:
        raise ValueError(
            "Required field 'timestamp' is missing."
        )

    # Accept an already valid ISO-style timestamp.
    normalized_value = cleaned_value.replace(
        "Z",
        "+00:00",
    )

    parsed_timestamp = datetime.fromisoformat(
        normalized_value
    )

    if parsed_timestamp.tzinfo is None:
        parsed_timestamp = parsed_timestamp.replace(
            tzinfo=timezone.utc
        )

    return parsed_timestamp.astimezone(
        timezone.utc
    ).isoformat()


def map_bronze_to_canonical(
    bronze_record: Dict[str, Any],
    silver_run_id: str,
) -> Dict[str, Any]:
    """
    Convert one Bronze envelope into the canonical Silver schema.
    """

    metadata = bronze_record.get("metadata")
    payload = bronze_record.get("payload")

    if not isinstance(metadata, dict):
        raise ValueError(
            "Bronze record is missing a valid metadata object."
        )

    if not isinstance(payload, dict):
        raise ValueError(
            "Bronze record is missing a valid payload object."
        )

    source_event_id = required_text(
        metadata,
        "event_id",
    )

    source_system = required_text(
        metadata,
        "source_system",
    )

    source_schema_version = required_text(
        metadata,
        "source_schema_version",
    )

    bronze_ingestion_timestamp = required_text(
        metadata,
        "ingestion_timestamp",
    )

    canonical_record = {
        "observation_id": str(uuid4()),

        "farm_id": required_text(
            payload,
            "farm_id",
        ),

        "sensor_id": required_text(
            payload,
            "sensor_id",
        ),

        "observed_at": normalize_timestamp(
            payload.get("timestamp")
        ),

        "region": clean_optional_text(
            payload.get("region")
        ),

        "crop_type": clean_optional_text(
            payload.get("crop_type")
        ),

        "latitude": optional_float(
            payload.get("latitude")
        ),

        "longitude": optional_float(
            payload.get("longitude")
        ),

        "soil_moisture_percentage": required_float(
            payload,
            "soil_moisture_%",
        ),

        "soil_ph": required_float(
            payload,
            "soil_pH",
        ),

        "temperature_celsius": required_float(
            payload,
            "temperature_C",
        ),

        "rainfall_millimeters": required_float(
            payload,
            "rainfall_mm",
        ),

        "humidity_percentage": required_float(
            payload,
            "humidity_%",
        ),

        "sunlight_hours": required_float(
            payload,
            "sunlight_hours",
        ),

        "irrigation_type": clean_optional_text(
            payload.get("irrigation_type")
        ),

        "fertilizer_type": clean_optional_text(
            payload.get("fertilizer_type")
        ),

        "pesticide_usage_milliliters": optional_float(
            payload.get("pesticide_usage_ml")
        ),

        "ndvi_index": required_float(
            payload,
            "NDVI_index",
        ),

        "crop_disease_status": clean_optional_text(
            payload.get("crop_disease_status")
        ),

        "total_days": optional_integer(
            payload.get("total_days")
        ),

        "yield_kg_per_hectare": optional_float(
            payload.get("yield_kg_per_hectare")
        ),

        "source_system": source_system,
        "source_event_id": source_event_id,
        "source_schema_version": source_schema_version,
        "bronze_ingestion_timestamp": (
            bronze_ingestion_timestamp
        ),
        "processing_run_id": silver_run_id,
        "silver_processing_timestamp": (
            datetime.now(timezone.utc).isoformat()
        ),
        "quality_status": "accepted",
    }

    return canonical_record