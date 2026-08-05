from pipeline.common.schema_loader import (
    create_validator,
    get_validation_errors,
)
from pipeline.silver.quarantine import (
    create_quarantine_record,
)
from pipeline.silver.weather_canonical_mapper import (
    map_weather_bronze_to_canonical,
)


def create_valid_weather_bronze_record():
    return {
        "metadata": {
            "event_id": "weather-event-001",
            "run_id": "weather-bronze-run-001",
            "source_system": "farm_weather_station",
            "source_type": "streaming_sensor",
            "source_format": "json",
            "source_schema_version": (
                "weather-source-1.0.0"
            ),
            "ingestion_timestamp": (
                "2026-08-02T20:00:00+00:00"
            ),
            "event_timestamp": (
                "2024-03-19T10:00:00Z"
            ),
            "kafka_topic": "raw-weather-readings",
            "kafka_partition": 0,
            "kafka_offset": 0,
        },
        "payload": {
            "weather_station_id": "WS-001",
            "farm_id": "FARM0001",
            "observed_at": (
                "2024-03-19T10:00:00Z"
            ),
            "air_temperature_celsius": 17.79,
            "relative_humidity_percentage": 77.03,
            "rainfall_millimeters": 75.62,
            "sunlight_duration_hours": 7.27,
            "wind_speed_kmh": 13.4,
            "atmospheric_pressure_hpa": 1012.6,
            "latitude": 28.61,
            "longitude": 77.20,
        },
    }


def test_valid_weather_mapping() -> None:
    bronze_record = (
        create_valid_weather_bronze_record()
    )

    canonical_record = (
        map_weather_bronze_to_canonical(
            bronze_record=bronze_record,
            silver_run_id="weather-silver-run-001",
        )
    )

    validator = create_validator(
        "canonical/"
        "weather_observation_v1.schema.json"
    )

    errors = get_validation_errors(
        canonical_record,
        validator,
    )

    assert errors == []

    assert (
        canonical_record["temperature_celsius"]
        == 17.79
    )

    assert (
        canonical_record["humidity_percentage"]
        == 77.03
    )

    assert (
        canonical_record["sunlight_hours"]
        == 7.27
    )

    assert (
        canonical_record["source_event_id"]
        == "weather-event-001"
    )


def test_invalid_weather_mapping() -> None:
    bronze_record = (
        create_valid_weather_bronze_record()
    )

    bronze_record["payload"][
        "relative_humidity_percentage"
    ] = "not-a-number"

    try:
        map_weather_bronze_to_canonical(
            bronze_record=bronze_record,
            silver_run_id="weather-silver-run-002",
        )

        raise AssertionError(
            "Invalid weather mapping should fail."
        )

    except ValueError as error:
        quarantine_record = (
            create_quarantine_record(
                bronze_record=bronze_record,
                silver_run_id=(
                    "weather-silver-run-002"
                ),
                failed_stage=(
                    "weather_canonical_mapping"
                ),
                failure_reasons=[
                    {
                        "field": (
                            "relative_humidity_"
                            "percentage"
                        ),
                        "message": str(error),
                    }
                ],
            )
        )

    assert (
        quarantine_record["event_id"]
        == "weather-event-001"
    )

    assert (
        quarantine_record["failed_stage"]
        == "weather_canonical_mapping"
    )


if __name__ == "__main__":
    test_valid_weather_mapping()
    test_invalid_weather_mapping()

    print(
        "Valid Weather canonical mapping "
        "test passed."
    )

    print(
        "Invalid Weather quarantine mapping "
        "test passed."
    )