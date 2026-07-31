# Role
# Creates the shared MinIO client.

from minio import Minio
from minio.error import S3Error

from pipeline.common.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)


def create_minio_client() -> Minio:
    """
    Create and return a MinIO client using the central configuration.
    """
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket_exists(
    client: Minio,
    bucket_name: str = MINIO_BUCKET,
) -> None:
    """
    Create the configured bucket when it does not already exist.
    """
    try:
        if client.bucket_exists(bucket_name):
            return

        client.make_bucket(bucket_name)

        print(f"Created MinIO bucket: {bucket_name}")

    except S3Error as error:
        raise RuntimeError(
            f"Could not verify or create MinIO bucket "
            f"'{bucket_name}': {error}"
        ) from error


def test_minio_connection() -> None:
    """
    Verify that MinIO is reachable and the configured bucket is available.
    """
    client = create_minio_client()
    ensure_bucket_exists(client)

    objects = list(
        client.list_objects(
            MINIO_BUCKET,
            recursive=True,
        )
    )

    print("MinIO connection successful.")
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: {MINIO_BUCKET}")
    print(f"Objects currently stored: {len(objects)}")


if __name__ == "__main__":
    test_minio_connection()