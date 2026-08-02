import argparse
import json

from pipeline.common.minio_client import (
    create_minio_client,
    ensure_bucket_exists,
)
from pipeline.common.traceability import (
    build_traceability_report,
    write_traceability_report,
)


def parse_arguments() -> argparse.Namespace:
    """
    Read the Gold run ID from the command line.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Verify the complete Bronze-to-Silver-to-Gold "
            "traceability chain for one Gold run."
        )
    )

    parser.add_argument(
        "--gold-run-id",
        required=True,
        help=(
            "Gold processing run ID whose lineage "
            "should be verified."
        ),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client)

    report = build_traceability_report(
        minio_client=minio_client,
        gold_run_id=arguments.gold_run_id,
    )

    print("\nTraceability report:")
    print(
        json.dumps(
            report,
            indent=2,
        )
    )

    write_traceability_report(
        minio_client=minio_client,
        report=report,
    )

    if report["status"] != "valid":
        raise RuntimeError(
            "Traceability verification failed because "
            "one or more declared objects are missing."
        )

    print("\nTraceability verification passed.")
    print(
        "Bronze run: "
        f"{report['run_chain']['bronze_run_id']}"
    )
    print(
        "Silver run: "
        f"{report['run_chain']['silver_run_id']}"
    )
    print(
        "Gold run: "
        f"{report['run_chain']['gold_run_id']}"
    )


if __name__ == "__main__":
    main()