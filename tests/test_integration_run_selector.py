from pipeline.common.minio_client import (
    create_minio_client,
)
from pipeline.gold.integration_run_selector import (
    list_completed_integration_runs,
    select_latest_integration_run,
    select_integration_run_by_id,
)


def main() -> None:
    minio_client = create_minio_client()

    runs = list_completed_integration_runs(
        minio_client
    )

    print(
        f"Usable integration runs: {len(runs)}"
    )

    for run in runs:
        print("\nIntegration run:")
        print(
            f"  ID: {run['integration_run_id']}"
        )
        print(
            "  Field Silver parent: "
            f"{run['field_silver_run_id']}"
        )
        print(
            "  Weather Silver parent: "
            f"{run['weather_silver_run_id']}"
        )
        print(
            "  Weather match rate: "
            f"{run['weather_match_rate']}"
        )
        print(
            "  Integrated objects: "
            f"{run['integrated_objects']}"
        )

    latest_run = select_latest_integration_run(
        minio_client
    )

    selected_run = select_integration_run_by_id(
        minio_client=minio_client,
        run_id=latest_run[
            "integration_run_id"
        ],
    )

    assert (
        selected_run["integration_run_id"]
        == latest_run["integration_run_id"]
    )

    print("\nIntegration selector test passed.")


if __name__ == "__main__":
    main()