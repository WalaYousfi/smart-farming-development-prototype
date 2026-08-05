import json

from pipeline.common.config import PROJECT_ROOT
from pipeline.common.schema_loader import (
    create_validator,
    get_validation_errors,
)


def test_weather_source_schema() -> None:
    data_path = (
        PROJECT_ROOT
        / "data"
        / "source"
        / "weather"
        / "weather_observations.json"
    )

    with data_path.open(
        "r",
        encoding="utf-8",
    ) as data_file:
        records = json.load(data_file)

    validator = create_validator(
        "source/weather_observation_json.schema.json"
    )

    assert len(records) == 500

    for index, record in enumerate(
        records,
        start=1,
    ):
        errors = get_validation_errors(
            record,
            validator,
        )

        assert errors == [], (
            f"Weather record {index} failed validation: "
            f"{errors}"
        )


if __name__ == "__main__":
    test_weather_source_schema()

    print(
        "All simulated weather records passed "
        "source-schema validation."
    )