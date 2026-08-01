# This does not delete a bad record. It preserves:

# the original Bronze record;
# why it failed;
# which Silver run rejected it;
# where the failure happened.







from datetime import datetime, timezone
from typing import Any, Dict, List


def create_quarantine_record(
    bronze_record: Dict[str, Any],
    silver_run_id: str,
    failed_stage: str,
    failure_reasons: List[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Create a traceable quarantine record.
    """

    metadata = bronze_record.get("metadata", {})

    return {
        "event_id": metadata.get("event_id"),
        "source_system": metadata.get("source_system"),
        "source_schema_version": metadata.get(
            "source_schema_version"
        ),
        "bronze_run_id": metadata.get("run_id"),
        "silver_run_id": silver_run_id,
        "failed_stage": failed_stage,
        "failure_reasons": failure_reasons,
        "quarantine_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "original_record": bronze_record,
    }