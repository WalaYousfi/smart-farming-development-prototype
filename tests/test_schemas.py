from pipeline.common.schema_loader import (
    create_validator,
    get_validation_errors,
)


def test_source_schema() -> None:
    validator = create_validator(
        "source/crop_yield_csv.schema.json"
    )

    valid_source_record = {
        "farm_id": "FARM0001",
        "region": "North India",
        "crop_type": "Wheat",
        "soil_moisture_%": "35.95",
        "soil_pH": "5.99",
        "temperature_C": "17.79",
        "rainfall_mm": "75.62",
        "humidity_%": "77.03",
        "sunlight_hours": "7.27",
        "sensor_id": "SENS0001",
        "timestamp": "2024-03-19T10:00:00Z",
        "latitude": "28.61",
        "longitude": "77.20",
        "NDVI_index": "0.63",
    }

    errors = get_validation_errors(
        valid_source_record,
        validator,
    )

    assert errors == []


def test_canonical_schema() -> None:
    validator = create_validator(
        "canonical/field_observation_v1.schema.json"
    )

    valid_canonical_record = {
        "observation_id": "observation-001",
        "farm_id": "FARM0001",
        "sensor_id": "SENS0001",
        "observed_at": "2024-03-19T10:00:00+00:00",
        "region": "North India",
        "crop_type": "Wheat",
        "latitude": 28.61,
        "longitude": 77.20,
        "soil_moisture_percentage": 35.95,
        "soil_ph": 5.99,
        "temperature_celsius": 17.79,
        "rainfall_millimeters": 75.62,
        "humidity_percentage": 77.03,
        "sunlight_hours": 7.27,
        "irrigation_type": None,
        "fertilizer_type": "Organic",
        "pesticide_usage_milliliters": 6.34,
        "ndvi_index": 0.63,
        "crop_disease_status": "Mild",
        "total_days": 120,
        "yield_kg_per_hectare": 4200.5,
        "source_system": "smart_farming_crop_yield_csv",
        "source_event_id": "event-001",
        "source_schema_version": "source-1.0.0",
        "bronze_ingestion_timestamp": "2026-08-01T01:30:00+00:00",
        "processing_run_id": "run-001",
        "silver_processing_timestamp": "2026-08-01T01:31:00+00:00",
        "quality_status": "accepted"
    }

    errors = get_validation_errors(
        valid_canonical_record,
        validator,
    )

    assert errors == []


if __name__ == "__main__":
    test_source_schema()
    test_canonical_schema()

    print("Source schema test passed.")
    print("Canonical schema test passed.")