from datetime import datetime, timezone
from io import BytesIO
import json

from kafka import KafkaConsumer
from minio import Minio
import os
from dotenv import load_dotenv


load_dotenv()

KAFKA_TOPIC = "raw-field-readings"
KAFKA_SERVER = os.getenv("KAFKA_SERVER")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = "smart-farming"

BATCH_SIZE = 100


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",

    # We commit only after the batch is safely stored.
    enable_auto_commit=False,

    group_id="bronze-minio-consumer",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)


minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"Created MinIO bucket: {MINIO_BUCKET}")


def upload_batch(events) :
    """Convert a batch to JSONL and upload it to MinIO."""

    if not events:
        return

    first_offset = events[0]["kafka_offset"]
    last_offset = events[-1]["kafka_offset"]

    now = datetime.now(timezone.utc)

    object_name = (
        f"bronze/smart-farming/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"events_{first_offset}_{last_offset}_"
        f"{now.strftime('%Y%m%dT%H%M%S%fZ')}.jsonl"
    )

    jsonl_content = "\n".join(
        json.dumps(event, ensure_ascii=False)
        for event in events
    )

    # Add a final line break.
    jsonl_content += "\n"

    encoded_content = jsonl_content.encode("utf-8")
    data_stream = BytesIO(encoded_content)

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=data_stream,
        length=len(encoded_content),
        content_type="application/x-ndjson",
    )

    print(
        f"Uploaded {len(events)} events to "
        f"{MINIO_BUCKET}/{object_name}"
    )


batch = []

print("Bronze consumer started")
print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
print(f"Uploading every {BATCH_SIZE} records")


try:
    for message in consumer:
        bronze_event = {
            "ingestion_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "source": "Smart_Farming_Crop_Yield_2024.csv",
            "source_topic": message.topic,
            "kafka_partition": message.partition,
            "kafka_offset": message.offset,
            "raw_event": message.value,
        }

        batch.append(bronze_event)

        print(
            f"Buffered offset {message.offset} "
            f"({len(batch)}/{BATCH_SIZE})"
        )

        if len(batch) >= BATCH_SIZE:
            upload_batch(batch)

            # Kafka remembers progress only after MinIO upload succeeds.
            consumer.commit()

            batch.clear()

except KeyboardInterrupt:
    print("\nStopping Bronze consumer...")

finally:
    # Upload any remaining records smaller than the batch size.
    if batch:
        upload_batch(batch)
        consumer.commit()

    consumer.close()
    print("Bronze consumer stopped")