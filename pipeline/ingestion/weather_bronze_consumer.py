from datetime import datetime, timezone
from io import BytesIO
import json
from typing import Any, Dict, List
from uuid import uuid4

from kafka import KafkaConsumer

from pipeline.common.config import (
    BRONZE_BATCH_SIZE,
    BRONZE_PREFIX,
    MINIO_BUCKET,
    WEATHER_CONSUMER_GROUP,
    WEATHER_KAFKA_TOPIC,
    WEATHER_SOURCE_FORMAT,
    WEATHER_SOURCE_SCHEMA_VERSION,
    WEATHER_SOURCE_SYSTEM,
    WEATHER_SOURCE_TYPE,
    KAFKA_SERVER,
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


JOB_NAME = "weather_bronze_ingestion"
JOB_VERSION = "2.0.0"


def utc_now() -> str:
    """Return the current UTC timestamp."""

    return datetime.now(timezone.utc).isoformat()


def decode_headers(message: Any) -> Dict[str, str]:
    """
    Convert Kafka headers from bytes into readable text.
    """

    decoded_headers = {}

    for key, value in message.headers or []:
        if value is None:
            decoded_headers[key] = None
        else:
            decoded_headers[key] = value.decode(
                "utf-8"
            )

    return decoded_headers


def create_weather_bronze_event(
    message: Any,
    run_id: str,
) -> Dict[str, Any]:
    """
    Wrap the original weather event with Bronze metadata.

    The original weather record remains unchanged
    inside payload.
    """

    payload = message.value

    return {
        "metadata": {
            "event_id": str(uuid4()),
            "run_id": run_id,
            "source_system": WEATHER_SOURCE_SYSTEM,
            "source_type": WEATHER_SOURCE_TYPE,
            "source_format": WEATHER_SOURCE_FORMAT,
            "source_schema_version": (
                WEATHER_SOURCE_SCHEMA_VERSION
            ),
            "ingestion_timestamp": utc_now(),
            "event_timestamp": payload.get(
                "observed_at"
            ),
            "kafka_topic": message.topic,
            "kafka_partition": message.partition,
            "kafka_offset": message.offset,
            "kafka_key": (
                message.key.decode("utf-8")
                if message.key
                else None
            ),
            "kafka_headers": decode_headers(
                message
            ),
        },
        "payload": payload,
    }


def upload_batch(
    minio_client: Any,
    events: List[Dict[str, Any]],
    run_id: str,
) -> str:
    """
    Upload one weather-event batch as JSONL
    into the MinIO Bronze zone.
    """

    if not events:
        raise ValueError(
            "Cannot upload an empty weather batch."
        )

    first_metadata = events[0]["metadata"]
    last_metadata = events[-1]["metadata"]

    first_offset = first_metadata["kafka_offset"]
    last_offset = last_metadata["kafka_offset"]
    partition = first_metadata["kafka_partition"]

    now = datetime.now(timezone.utc)

    object_name = (
        f"{BRONZE_PREFIX}/"
        f"source={WEATHER_SOURCE_SYSTEM}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"run_id={run_id}/"
        f"partition={partition}/"
        f"events_{first_offset}_{last_offset}.jsonl"
    )

    jsonl_content = "\n".join(
        json.dumps(
            event,
            ensure_ascii=False,
        )
        for event in events
    )

    jsonl_content += "\n"

    encoded_content = jsonl_content.encode(
        "utf-8"
    )

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=BytesIO(encoded_content),
        length=len(encoded_content),
        content_type="application/x-ndjson",
    )

    print(
        f"Uploaded {len(events)} weather events: "
        f"{object_name}"
    )

    return object_name


def main() -> None:
    run_context = create_run_context(
        job_name=JOB_NAME,
        job_version=JOB_VERSION,
    )

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    consumer = KafkaConsumer(
        WEATHER_KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=WEATHER_CONSUMER_GROUP,
        key_deserializer=lambda value: (
            value
            if value is None
            else value
        ),
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    batch = []  # type: List[Dict[str, Any]]
    output_objects = []  # type: List[str]

    consumed_records = 0
    uploaded_records = 0
    uploaded_batches = 0

    started_manifest = create_manifest(
        run_context=run_context,
        status="started",
        input_zone="kafka",
        output_zone="bronze",
        input_objects=[
            f"kafka://{WEATHER_KAFKA_TOPIC}",
        ],
        metrics={
            "consumed_records": 0,
            "uploaded_records": 0,
            "uploaded_batches": 0,
            "source_system": (
                WEATHER_SOURCE_SYSTEM
            ),
        },
    )

    write_manifest(started_manifest)

    print("Weather Bronze consumer started.")
    print(f"Run ID: {run_context.run_id}")
    print(
        f"Kafka topic: {WEATHER_KAFKA_TOPIC}"
    )
    print(
        f"Source system: {WEATHER_SOURCE_SYSTEM}"
    )
    print(
        f"Batch size: {BRONZE_BATCH_SIZE}"
    )
    print(
        "Press Ctrl+C after all weather events "
        "have been received."
    )

    try:
        for message in consumer:
            bronze_event = (
                create_weather_bronze_event(
                    message=message,
                    run_id=run_context.run_id,
                )
            )

            batch.append(bronze_event)
            consumed_records += 1

            print(
                f"Buffered weather offset "
                f"{message.offset} "
                f"({len(batch)}/{BRONZE_BATCH_SIZE})"
            )

            if len(batch) >= BRONZE_BATCH_SIZE:
                object_name = upload_batch(
                    minio_client=minio_client,
                    events=batch,
                    run_id=run_context.run_id,
                )

                output_objects.append(object_name)

                uploaded_records += len(batch)
                uploaded_batches += 1

                # Commit only after MinIO confirms upload.
                consumer.commit()

                batch.clear()

    except KeyboardInterrupt:
        print(
            "\nStopping Weather Bronze consumer..."
        )

        if batch:
            object_name = upload_batch(
                minio_client=minio_client,
                events=batch,
                run_id=run_context.run_id,
            )

            output_objects.append(object_name)

            uploaded_records += len(batch)
            uploaded_batches += 1

            consumer.commit()
            batch.clear()

        completed_manifest = create_manifest(
            run_context=run_context,
            status="completed",
            input_zone="kafka",
            output_zone="bronze",
            input_objects=[
                f"kafka://{WEATHER_KAFKA_TOPIC}",
            ],
            output_objects=output_objects,
            metrics={
                "consumed_records": (
                    consumed_records
                ),
                "uploaded_records": (
                    uploaded_records
                ),
                "uploaded_batches": (
                    uploaded_batches
                ),
                "batch_size": BRONZE_BATCH_SIZE,
                "source_system": (
                    WEATHER_SOURCE_SYSTEM
                ),
                "source_type": (
                    WEATHER_SOURCE_TYPE
                ),
                "source_format": (
                    WEATHER_SOURCE_FORMAT
                ),
                "source_schema_version": (
                    WEATHER_SOURCE_SCHEMA_VERSION
                ),
            },
        )

        write_manifest(completed_manifest)

    except Exception as error:
        failed_manifest = create_manifest(
            run_context=run_context,
            status="failed",
            input_zone="kafka",
            output_zone="bronze",
            input_objects=[
                f"kafka://{WEATHER_KAFKA_TOPIC}",
            ],
            output_objects=output_objects,
            metrics={
                "consumed_records": (
                    consumed_records
                ),
                "uploaded_records": (
                    uploaded_records
                ),
                "uploaded_batches": (
                    uploaded_batches
                ),
                "source_system": (
                    WEATHER_SOURCE_SYSTEM
                ),
            },
            error_message=str(error),
        )

        try:
            write_manifest(failed_manifest)

        except Exception as manifest_error:
            print(
                "Could not store the failed "
                "weather manifest:",
                manifest_error,
            )

        raise

    finally:
        consumer.close()

        print(
            "Weather Bronze consumer closed."
        )


if __name__ == "__main__":
    main()