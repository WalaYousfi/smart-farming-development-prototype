from datetime import datetime, timezone
from io import BytesIO
import json

from minio import Minio
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = "smart-farming"

BRONZE_PREFIX = "bronze/smart-farming/"
SILVER_OBJECT = "silver/smart-farming/cleaned_field_readings.parquet"


minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def load_bronze_events() :
    """Read every Bronze JSONL object from MinIO."""

    records = []

    objects = minio_client.list_objects(
        BUCKET_NAME,
        prefix=BRONZE_PREFIX,
        recursive=True,
    )

    for obj in objects:
        if not obj.object_name.endswith(".jsonl"):
            continue

        print(f"Reading: {obj.object_name}")

        response = minio_client.get_object(
            BUCKET_NAME,
            obj.object_name,
        )

        try:
            content = response.read().decode("utf-8")

            for line in content.splitlines():
                if not line.strip():
                    continue

                bronze_record = json.loads(line)

                raw_event = bronze_record["raw_event"]

                raw_event["bronze_ingestion_timestamp"] = (
                    bronze_record["ingestion_timestamp"]
                )
                raw_event["kafka_partition"] = (
                    bronze_record["kafka_partition"]
                )
                raw_event["kafka_offset"] = (
                    bronze_record["kafka_offset"]
                )

                records.append(raw_event)

        finally:
            response.close()
            response.release_conn()

    return records


def clean_data(records) -> pd.DataFrame:
    """Convert Bronze records into Silver-quality data."""

    dataframe = pd.DataFrame(records)

    print(f"Bronze rows loaded: {len(dataframe)}")

    numeric_columns = [
        "soil_moisture_%",
        "soil_pH",
        "temperature_C",
        "rainfall_mm",
        "humidity_%",
        "sunlight_hours",
        "pesticide_usage_ml",
        "total_days",
        "yield_kg_per_hectare",
        "latitude",
        "longitude",
        "NDVI_index",
    ]

    for column in numeric_columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="coerce",
            )

    date_columns = [
        "sowing_date",
        "harvest_date",
        "timestamp",
        "bronze_ingestion_timestamp",
    ]

    for column in date_columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(
                dataframe[column],
                errors="coerce",
                utc=True,
            )

    text_columns = [
        "farm_id",
        "region",
        "crop_type",
        "irrigation_type",
        "fertilizer_type",
        "sensor_id",
        "crop_disease_status",
    ]

    for column in text_columns:
        if column in dataframe.columns:
            dataframe[column] = (
                dataframe[column]
                .astype("string")
                .str.strip()
            )

    # Replace empty strings with missing values.
    dataframe = dataframe.replace("", pd.NA)

    # Remove exact duplicate Kafka events.
    dataframe = dataframe.drop_duplicates(
        subset=["kafka_partition", "kafka_offset"]
    )

    # Add simple quality flags without deleting the records.
    dataframe["quality_soil_moisture"] = dataframe[
        "soil_moisture_%"
    ].between(0, 100)

    dataframe["quality_humidity"] = dataframe[
        "humidity_%"
    ].between(0, 100)

    dataframe["quality_soil_ph"] = dataframe[
        "soil_pH"
    ].between(0, 14)

    dataframe["quality_ndvi"] = dataframe[
        "NDVI_index"
    ].between(-1, 1)

    dataframe["data_quality_status"] = "valid"

    invalid_mask = ~(
        dataframe["quality_soil_moisture"]
        & dataframe["quality_humidity"]
        & dataframe["quality_soil_ph"]
        & dataframe["quality_ndvi"]
    )

    dataframe.loc[
        invalid_mask,
        "data_quality_status",
    ] = "invalid_range"

    dataframe["silver_processing_timestamp"] = (
        datetime.now(timezone.utc)
    )

    print(f"Silver rows after deduplication: {len(dataframe)}")

    return dataframe


def upload_silver(dataframe: pd.DataFrame) -> None:
    """Write the Silver DataFrame as Parquet to MinIO."""

    parquet_buffer = BytesIO()

    dataframe.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
    )

    parquet_buffer.seek(0)
    parquet_bytes = parquet_buffer.getvalue()

    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=SILVER_OBJECT,
        data=BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )

    print("Silver file uploaded successfully")
    print(f"Object: {BUCKET_NAME}/{SILVER_OBJECT}")


def main() -> None:
    records = load_bronze_events()

    if not records:
        print("No Bronze records found")
        return

    silver_dataframe = clean_data(records)

    print("\nSilver preview:")
    print(silver_dataframe.head(3).to_string(index=False))

    print("\nData-quality results:")
    print(
        silver_dataframe["data_quality_status"]
        .value_counts(dropna=False)
    )

    upload_silver(silver_dataframe)


if __name__ == "__main__":
    main()