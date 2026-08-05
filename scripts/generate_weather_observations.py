import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

FIELD_DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "Smart_Farming_Crop_Yield_2024.csv"
)

WEATHER_OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "weather"
    / "weather_observations.json"
)


def optional_float(
    value: Any,
) -> float:
    """
    Convert a source value into a float.
    """

    if pd.isna(value):
        raise ValueError(
            "A required weather source value is missing."
        )

    return float(value)


def create_weather_record(
    row: pd.Series,
    row_number: int,
) -> Dict[str, Any]:
    """
    Create one deterministic synthetic weather observation.

    Shared farm and timestamp fields allow it to be integrated
    with the corresponding field observation.
    """

    wind_speed = round(
        5.0 + ((row_number * 1.7) % 25),
        2,
    )

    atmospheric_pressure = round(
        995.0 + ((row_number * 2.3) % 30),
        2,
    )

    return {
        "weather_station_id": (
            f"WS-{row_number:04d}"
        ),
        "farm_id": str(row["farm_id"]).strip(),
        "observed_at": str(row["timestamp"]).strip(),
        "air_temperature_celsius": optional_float(
            row["temperature_C"]
        ),
        "relative_humidity_percentage": optional_float(
            row["humidity_%"]
        ),
        "rainfall_millimeters": optional_float(
            row["rainfall_mm"]
        ),
        "sunlight_duration_hours": optional_float(
            row["sunlight_hours"]
        ),
        "wind_speed_kmh": wind_speed,
        "atmospheric_pressure_hpa": (
            atmospheric_pressure
        ),
        "latitude": optional_float(
            row["latitude"]
        ),
        "longitude": optional_float(
            row["longitude"]
        ),
    }


def main() -> None:
    if not FIELD_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Field dataset not found: {FIELD_DATA_PATH}"
        )

    dataframe = pd.read_csv(
        FIELD_DATA_PATH
    )

    required_columns = [
        "farm_id",
        "timestamp",
        "temperature_C",
        "humidity_%",
        "rainfall_mm",
        "sunlight_hours",
        "latitude",
        "longitude",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Field dataset is missing columns: "
            f"{missing_columns}"
        )

    weather_records: List[Dict[str, Any]] = []

    for row_number, (_, row) in enumerate(
        dataframe.iterrows(),
        start=1,
    ):
        weather_records.append(
            create_weather_record(
                row=row,
                row_number=row_number,
            )
        )

    WEATHER_OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with WEATHER_OUTPUT_PATH.open(
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            weather_records,
            output_file,
            indent=2,
            ensure_ascii=False,
        )

    print(
        f"Generated {len(weather_records)} "
        "weather observations."
    )

    print(
        f"Output: {WEATHER_OUTPUT_PATH}"
    )

    print("\nFirst weather observation:")
    print(
        json.dumps(
            weather_records[0],
            indent=2,
        )
    )


if __name__ == "__main__":
    main()