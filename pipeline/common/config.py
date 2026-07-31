# Role
# Stores bucket names, object prefixes and connection configuration.


import os
from pathlib import Path

from dotenv import load_dotenv


# Locate the project root regardless of where the script is launched.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load variables from the root .env file.
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE)


def get_required_env(name: str) -> str:
    """
    Return a required environment variable.

    Raises a clear error when the variable is missing or empty.
    """
    value = os.getenv(name)

    if value is None or not value.strip():
        raise RuntimeError(
            f"Required environment variable '{name}' is missing. "
            f"Check the file: {ENV_FILE}"
        )

    return value.strip()


def get_boolean_env(name: str, default: bool = False) -> bool:
    """
    Convert an environment variable to a Boolean value.

    Accepted true values:
    true, 1, yes, y, on

    Accepted false values:
    false, 0, no, n, off
    """
    raw_value = os.getenv(name)

    if raw_value is None:
        return default

    normalized_value = raw_value.strip().lower()

    if normalized_value in {"true", "1", "yes", "y", "on"}:
        return True

    if normalized_value in {"false", "0", "no", "n", "off"}:
        return False

    raise ValueError(
        f"Environment variable '{name}' must contain a Boolean value. "
        f"Received: {raw_value!r}"
    )


MINIO_ENDPOINT = get_required_env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_required_env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = get_required_env("MINIO_SECRET_KEY")
MINIO_BUCKET = get_required_env("MINIO_BUCKET")
MINIO_SECURE = get_boolean_env("MINIO_SECURE", default=False)

KAFKA_SERVER = get_required_env("KAFKA_SERVER")
KAFKA_TOPIC = get_required_env("KAFKA_TOPIC")


# Medallion and metadata prefixes inside the MinIO bucket.
BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"
METADATA_PREFIX = "metadata"

MANIFEST_PREFIX = f"{METADATA_PREFIX}/manifests"
QUALITY_REPORT_PREFIX = f"{METADATA_PREFIX}/quality-reports"
LINEAGE_PREFIX = f"{METADATA_PREFIX}/lineage"
SCHEMA_PREFIX = f"{METADATA_PREFIX}/schemas"