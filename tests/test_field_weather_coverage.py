import json

import pandas as pd

from pipeline.common.config import PROJECT_ROOT


def main() -> None:
    field_path = (
        PROJECT_ROOT
        / "data"
        / "source"
        / "Smart_Farming_Crop_Yield_2024.csv"
    )

    weather_path = (
        PROJECT_ROOT
        / "data"
        / "source"
        / "weather"
        / "weather_observations.json"
    )

    field_dataframe = pd.read_csv(
        field_path
    )

    with weather_path.open(
        "r",
        encoding="utf-8",
    ) as weather_file:
        weather_records = json.load(
            weather_file
        )

    weather_dataframe = pd.DataFrame(
        weather_records
    )

    field_dataframe["integration_date"] = (
        pd.to_datetime(
            field_dataframe["timestamp"],
            errors="coerce",
            utc=True,
        )
        .dt.strftime("%Y-%m-%d")
    )

    weather_dataframe["integration_date"] = (
        pd.to_datetime(
            weather_dataframe["observed_at"],
            errors="coerce",
            utc=True,
        )
        .dt.strftime("%Y-%m-%d")
    )

    matched = field_dataframe.merge(
        weather_dataframe[
            [
                "farm_id",
                "integration_date",
            ]
        ],
        how="inner",
        on=[
            "farm_id",
            "integration_date",
        ],
    )

    field_count = len(field_dataframe)
    weather_count = len(weather_dataframe)
    matched_count = len(matched)

    match_rate = (
        matched_count / field_count
        if field_count
        else 0
    )

    print(f"Field records: {field_count}")
    print(f"Weather records: {weather_count}")
    print(f"Potential matches: {matched_count}")
    print(f"Potential match rate: {match_rate:.4f}")

    assert field_count == 500
    assert weather_count == 500
    assert matched_count == 500
    assert match_rate == 1.0

    print(
        "Field-Weather source coverage test passed."
    )


if __name__ == "__main__":
    main()