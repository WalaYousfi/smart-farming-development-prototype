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


def create_valid_bronze_record():
    return {
        "metadata": {
            "event_id": "event-001",
            "run_id": "bronze-run-001",
            "source_system": (
                "smart_farming_crop_yield_csv"
            ),
            "source_type": (
                "batch_file_simulated_as_stream"
            ),
            "source_format": "csv",
            "source_schema_version": "source-1.0.0",
            "ingestion_timestamp": (
                "2026-08-01T01:30:00+00:00"
            ),
            "event_timestamp": (
                "2024-03-19T10:00:00Z"
            ),
            "kafka_topic": "raw-field-readings",
            "kafka_partition": 0,
            "kafka_offset": 1,
        },
        "payload": {
            "farm_id": "FARM0001",
            "region": "North India",
            "crop_type": "Wheat",
            "soil_moisture_%": "35.95",
            "soil_pH": "5.99",
            "temperature_C": "17.79",
            "rainfall_mm": "75.62",
            "humidity_%": "77.03",
            "sunlight_hours": "7.27",
            "irrigation_type": "",
            "fertilizer_type": "Organic",
            "pesticide_usage_ml": "6.34",
            "total_days": "120",
            "yield_kg_per_hectare": "4200.5",
            "sensor_id": "SENS0001",
            "timestamp": "2024-03-19T10:00:00Z",
            "latitude": "28.61",
            "longitude": "77.20",
            "NDVI_index": "0.63",
            "crop_disease_status": "Mild",
        },
    }


def test_valid_mapping() -> None:
    bronze_record = create_valid_bronze_record()

    canonical_record = map_bronze_to_canonical(
        bronze_record=bronze_record,
        silver_run_id="silver-run-001",
    )

    validator = create_validator(
        "canonical/field_observation_v1.schema.json"
    )

    errors = get_validation_errors(
        canonical_record,
        validator,
    )

    assert errors == []
    assert canonical_record[
        "soil_moisture_percentage"
    ] == 35.95
    assert canonical_record[
        "temperature_celsius"
    ] == 17.79
    assert canonical_record[
        "irrigation_type"
    ] is None
    assert canonical_record[
        "processing_run_id"
    ] == "silver-run-001"


def test_failed_mapping_goes_to_quarantine() -> None:
    bronze_record = create_valid_bronze_record()

    bronze_record["payload"][
        "soil_moisture_%"
    ] = "not-a-number"

    try:
        map_bronze_to_canonical(
            bronze_record=bronze_record,
            silver_run_id="silver-run-002",
        )

        raise AssertionError(
            "The invalid mapping should have failed."
        )

    except ValueError as error:
        quarantine_record = create_quarantine_record(
            bronze_record=bronze_record,
            silver_run_id="silver-run-002",
            failed_stage="canonical_mapping",
            failure_reasons=[
                {
                    "field": "soil_moisture_%",
                    "message": str(error),
                }
            ],
        )

    assert quarantine_record[
        "event_id"
    ] == "event-001"

    assert quarantine_record[
        "failed_stage"
    ] == "canonical_mapping"

    assert len(
        quarantine_record["failure_reasons"]
    ) == 1


if __name__ == "__main__":
    test_valid_mapping()
    test_failed_mapping_goes_to_quarantine()

    print("Valid canonical mapping test passed.")
    print("Quarantine mapping test passed.")