from pipeline.common.minio_client import (
    create_minio_client,
)
from pipeline.gold.silver_run_selector import (
    list_completed_silver_runs,
    select_latest_silver_run,
    select_silver_run_by_id,
)


def main() -> None:
    minio_client = create_minio_client()

    runs = list_completed_silver_runs(
        minio_client
    )

    print(
        f"Completed Silver runs with accepted data: "
        f"{len(runs)}"
    )

    for run in runs:
        print("\nSilver run:")
        print(
            f"  Run ID: {run['silver_run_id']}"
        )
        print(
            f"  Accepted records: "
            f"{run['accepted_records']}"
        )
        print(
            f"  Quarantined records: "
            f"{run['quarantined_records']}"
        )
        print(
            f"  Parent Bronze run: "
            f"{run['bronze_run_id']}"
        )
        print(
            f"  Accepted objects: "
            f"{run['accepted_objects']}"
        )

    latest_run = select_latest_silver_run(
        minio_client
    )

    print("\nLatest usable Silver run:")
    print(latest_run)

    selected_again = select_silver_run_by_id(
        minio_client=minio_client,
        silver_run_id=latest_run["silver_run_id"],
    )

    assert (
        selected_again["silver_run_id"]
        == latest_run["silver_run_id"]
    )

    assert selected_again["accepted_objects"]

    print("\nSilver run selector test passed.")


if __name__ == "__main__":
    main()