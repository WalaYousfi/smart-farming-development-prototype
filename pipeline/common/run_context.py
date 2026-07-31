# Role
# Generates:

# run_id;
# processing timestamp;
# job name;
# job version.

# This will let us connect:
# Bronze run
#    ↓
# Silver run
#    ↓
# Gold run

# and later answer:

# Which job created this file?
# When was it created?
# Which input data was used?
# Which Silver output came from which Bronze run?


from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(frozen=True)
class RunContext:
    run_id: str
    job_name: str
    job_version: str
    started_at: str

    def to_dict(self) -> dict:
        return asdict(self)


def create_run_context(
    job_name: str,
    job_version: str = "2.0.0",
) -> RunContext:
    """
    Create metadata that identifies one pipeline execution.
    """

    now = datetime.now(timezone.utc)

    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    short_uuid = uuid4().hex[:8]

    run_id = f"{timestamp}_{short_uuid}"

    return RunContext(
        run_id=run_id,
        job_name=job_name,
        job_version=job_version,
        started_at=now.isoformat(),
    )


if __name__ == "__main__":
    context = create_run_context(
        job_name="test_job",
    )

    print("Run context created:")
    print(context.to_dict())