import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from pipeline.common.config import PROJECT_ROOT


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_arguments() -> argparse.Namespace:
    """
    Read the experiment settings.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Repeat the Field-Weather integration and "
            "integrated Gold processing experiment."
        )
    )

    parser.add_argument(
        "--field-silver-run-id",
        required=True,
        help=(
            "Completed Field Silver run containing "
            "the 500 accepted observations."
        ),
    )

    parser.add_argument(
        "--weather-silver-run-id",
        required=True,
        help=(
            "Completed Weather Silver run containing "
            "the 500 accepted observations."
        ),
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help=(
            "Number of experiment repetitions. "
            "Use 3 for testing and at least 5 "
            "for paper results."
        ),
    )

    parser.add_argument(
        "--contamination",
        type=float,
        default=0.05,
        help=(
            "Isolation Forest contamination rate. "
            "Default: 0.05."
        ),
    )

    parser.add_argument(
        "--experiment-name",
        default="repeated-full-coverage",
        help="Name of the experiment.",
    )

    return parser.parse_args()


def validate_arguments(
    arguments: argparse.Namespace,
) -> None:
    """
    Validate experiment arguments before running jobs.
    """

    if arguments.repetitions < 2:
        raise ValueError(
            "--repetitions must be at least 2."
        )

    if arguments.repetitions > 30:
        raise ValueError(
            "--repetitions cannot exceed 30."
        )

    if not 0 < arguments.contamination <= 0.5:
        raise ValueError(
            "--contamination must be greater than 0 "
            "and less than or equal to 0.5."
        )


def run_command(
    command: List[str],
    step_name: str,
) -> Dict[str, Any]:
    """
    Execute one command and measure its wall-clock time.

    Captures standard output and standard error so each
    experimental run remains auditable.
    """

    print("\nRunning step:")
    print(step_name)
    print("Command:")
    print(" ".join(command))

    started_at = utc_now()
    start_time = time.perf_counter()

    process = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )

    duration_seconds = round(
        time.perf_counter() - start_time,
        4,
    )

    completed_at = utc_now()

    result = {
        "step_name": step_name,
        "command": command,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_seconds": duration_seconds,
        "return_code": process.returncode,
        "stdout": process.stdout,
        "stderr": process.stderr,
        "status": (
            "completed"
            if process.returncode == 0
            else "failed"
        ),
    }

    print(process.stdout)

    if process.stderr.strip():
        print("Standard error:")
        print(process.stderr)

    if process.returncode != 0:
        raise RuntimeError(
            f"Experiment step failed: {step_name}\n"
            f"Return code: {process.returncode}\n"
            f"Error:\n{process.stderr}"
        )

    return result


def extract_run_id(
    output: str,
    label: str,
) -> str:
    """
    Find a run ID printed after a known label.

    Example:
    Integration run ID: 20260806T...
    """

    for line in output.splitlines():
        cleaned_line = line.strip()

        if cleaned_line.startswith(label):
            value = cleaned_line.split(
                ":",
                1,
            )[1].strip()

            if value:
                return value

    raise RuntimeError(
        f"Could not find run ID using label: {label}"
    )


def calculate_statistics(
    values: List[float],
) -> Dict[str, Optional[float]]:
    """
    Calculate descriptive statistics.
    """

    if not values:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "minimum": None,
            "maximum": None,
            "standard_deviation": None,
        }

    standard_deviation = (
        statistics.stdev(values)
        if len(values) >= 2
        else 0.0
    )

    return {
        "count": len(values),
        "mean": round(
            statistics.mean(values),
            4,
        ),
        "median": round(
            statistics.median(values),
            4,
        ),
        "minimum": round(
            min(values),
            4,
        ),
        "maximum": round(
            max(values),
            4,
        ),
        "standard_deviation": round(
            standard_deviation,
            4,
        ),
    }


def run_one_repetition(
    repetition_number: int,
    arguments: argparse.Namespace,
) -> Dict[str, Any]:
    """
    Run one complete integration and Gold experiment.
    """

    print("\n" + "=" * 60)
    print(
        f"Starting repetition "
        f"{repetition_number}/{arguments.repetitions}"
    )
    print("=" * 60)

    integration_command = [
        sys.executable,
        "-m",
        "pipeline.integration.field_weather_integration",
        "--field-silver-run-id",
        arguments.field_silver_run_id,
        "--weather-silver-run-id",
        arguments.weather_silver_run_id,
    ]

    integration_result = run_command(
        command=integration_command,
        step_name="field_weather_integration",
    )

    integration_run_id = extract_run_id(
        output=integration_result["stdout"],
        label="Integration run ID",
    )

    gold_command = [
        sys.executable,
        "-m",
        "pipeline.gold.integrated_anomaly_detection",
        "--integration-run-id",
        integration_run_id,
        "--contamination",
        str(arguments.contamination),
    ]

    gold_result = run_command(
        command=gold_command,
        step_name="integrated_gold_anomaly_detection",
    )

    gold_run_id = extract_run_id(
        output=gold_result["stdout"],
        label="Gold run ID",
    )

    total_duration = round(
        integration_result["duration_seconds"]
        + gold_result["duration_seconds"],
        4,
    )

    return {
        "repetition": repetition_number,
        "started_at": (
            integration_result["started_at"]
        ),
        "completed_at": (
            gold_result["completed_at"]
        ),
        "input_runs": {
            "field_silver_run_id": (
                arguments.field_silver_run_id
            ),
            "weather_silver_run_id": (
                arguments.weather_silver_run_id
            ),
        },
        "generated_runs": {
            "integration_run_id": (
                integration_run_id
            ),
            "integrated_gold_run_id": (
                gold_run_id
            ),
        },
        "durations": {
            "integration_seconds": (
                integration_result[
                    "duration_seconds"
                ]
            ),
            "gold_seconds": (
                gold_result[
                    "duration_seconds"
                ]
            ),
            "total_seconds": total_duration,
        },
        "steps": {
            "integration": integration_result,
            "integrated_gold": gold_result,
        },
        "status": "completed",
    }


def build_summary(
    repetitions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Aggregate timing values across repetitions.
    """

    integration_durations = [
        repetition["durations"][
            "integration_seconds"
        ]
        for repetition in repetitions
    ]

    gold_durations = [
        repetition["durations"]["gold_seconds"]
        for repetition in repetitions
    ]

    total_durations = [
        repetition["durations"]["total_seconds"]
        for repetition in repetitions
    ]

    return {
        "completed_repetitions": len(
            repetitions
        ),
        "integration_duration_seconds": (
            calculate_statistics(
                integration_durations
            )
        ),
        "gold_duration_seconds": (
            calculate_statistics(
                gold_durations
            )
        ),
        "total_duration_seconds": (
            calculate_statistics(
                total_durations
            )
        ),
    }


def save_report(
    report: Dict[str, Any],
    experiment_name: str,
) -> Path:
    """
    Store the complete repeated-experiment report.
    """

    output_directory = (
        PROJECT_ROOT
        / "experiments"
        / "prototype-v2"
        / "repeated-runs"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    safe_name = (
        experiment_name
        .strip()
        .replace(" ", "_")
    )

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

    output_path = (
        output_directory
        / f"{safe_name}_{timestamp}.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            report,
            output_file,
            indent=2,
            ensure_ascii=False,
        )

    return output_path


def print_summary(
    summary: Dict[str, Any],
) -> None:
    """
    Display the paper-relevant timing results.
    """

    print("\nRepeated experiment summary")
    print("---------------------------")

    print(
        "Completed repetitions: "
        f"{summary['completed_repetitions']}"
    )

    integration = summary[
        "integration_duration_seconds"
    ]

    print("\nIntegration duration:")
    print(f"  Mean: {integration['mean']} s")
    print(f"  Median: {integration['median']} s")
    print(f"  Minimum: {integration['minimum']} s")
    print(f"  Maximum: {integration['maximum']} s")
    print(
        "  Standard deviation: "
        f"{integration['standard_deviation']} s"
    )

    gold = summary["gold_duration_seconds"]

    print("\nIntegrated Gold duration:")
    print(f"  Mean: {gold['mean']} s")
    print(f"  Median: {gold['median']} s")
    print(f"  Minimum: {gold['minimum']} s")
    print(f"  Maximum: {gold['maximum']} s")
    print(
        "  Standard deviation: "
        f"{gold['standard_deviation']} s"
    )

    total = summary["total_duration_seconds"]

    print("\nCombined duration:")
    print(f"  Mean: {total['mean']} s")
    print(f"  Median: {total['median']} s")
    print(f"  Minimum: {total['minimum']} s")
    print(f"  Maximum: {total['maximum']} s")
    print(
        "  Standard deviation: "
        f"{total['standard_deviation']} s"
    )


def main() -> None:
    arguments = parse_arguments()
    validate_arguments(arguments)

    experiment_started_at = utc_now()
    completed_repetitions = []

    for repetition_number in range(
        1,
        arguments.repetitions + 1,
    ):
        result = run_one_repetition(
            repetition_number=repetition_number,
            arguments=arguments,
        )

        completed_repetitions.append(result)

    summary = build_summary(
        completed_repetitions
    )

    report = {
        "experiment_name": (
            arguments.experiment_name
        ),
        "experiment_type": (
            "repeated_integration_and_gold_timing"
        ),
        "architecture": (
            "proposed dual-dimensional V2"
        ),
        "started_at": experiment_started_at,
        "completed_at": utc_now(),
        "configuration": {
            "field_silver_run_id": (
                arguments.field_silver_run_id
            ),
            "weather_silver_run_id": (
                arguments.weather_silver_run_id
            ),
            "requested_repetitions": (
                arguments.repetitions
            ),
            "contamination": (
                arguments.contamination
            ),
            "python_executable": (
                sys.executable
            ),
        },
        "summary": summary,
        "repetitions": completed_repetitions,
    }

    output_path = save_report(
        report=report,
        experiment_name=(
            arguments.experiment_name
        ),
    )

    print_summary(summary)

    print("\nExperiment report saved:")
    print(output_path)


if __name__ == "__main__":
    main()