import json
from pathlib import Path
from typing import Any, Dict, List

from jsonschema import Draft202012Validator

from pipeline.common.config import PROJECT_ROOT


SCHEMA_ROOT = PROJECT_ROOT / "schemas"


def load_schema(relative_path: str) -> Dict[str, Any]:
    """
    Load a JSON schema from the project's schemas directory.
    """

    schema_path = SCHEMA_ROOT / relative_path

    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema file not found: {schema_path}"
        )

    with schema_path.open(
        "r",
        encoding="utf-8",
    ) as schema_file:
        return json.load(schema_file)


def create_validator(
    relative_path: str,
) -> Draft202012Validator:
    """
    Load a schema and create its validator.
    """

    schema = load_schema(relative_path)

    Draft202012Validator.check_schema(schema)

    return Draft202012Validator(schema)


def get_validation_errors(
    record: Dict[str, Any],
    validator: Draft202012Validator,
) -> List[Dict[str, str]]:
    """
    Return readable validation errors for one record.
    """

    errors = sorted(
        validator.iter_errors(record),
        key=lambda error: list(error.path),
    )

    readable_errors = []

    for error in errors:
        field_path = ".".join(
            str(part)
            for part in error.path
        )

        readable_errors.append(
            {
                "field": field_path or "$",
                "message": error.message,
            }
        )

    return readable_errors