#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from perf.stress_test import StressConfig, run_stress_test


@dataclass(frozen=True)
class ExperimentConfig:
    worker_counts: list[int]
    paragraphs: list[int]
    runs_per_setting: int
    pause_seconds: float
    compose_file: Path
    project_name: str | None
    scale_workers: bool
    scale_wait: float
    output_csv: Path
    graphs_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repeated stress tests and collect metrics.")
    parser.add_argument("--worker-counts", type=int, nargs="+", required=True,
                        help="List of worker replica counts to test (e.g. 1 2 3 5 8)")
    parser.add_argument("--paragraphs", type=int, nargs="+", required=True,
                        help="List of paragraph counts per job (e.g. 50 200)")
    parser.add_argument("--runs", type=int, default=10, help="Number of repetitions per combination")
    parser.add_argument("--pause", type=float, default=1.0, help="Pause between runs in seconds")
    parser.add_argument("--scale-wait", type=float, default=5.0,
                        help="Seconds to wait after scaling workers before starting measurements")
    parser.add_argument("--no-scale", action="store_true",
                        help="Skip docker compose scaling (handle replicas manually)")
    parser.add_argument("--compose-file", default="docker-compose.yml",
                        help="Path to docker-compose file used for scaling")
    parser.add_argument("--project-name", default=None,
                        help="Optional docker compose project name")
    parser.add_argument("--output", default="perf/results.csv",
                        help="CSV file to append experiment rows")
    parser.add_argument("--graphs-dir", default="perf/graphs",
                        help="Directory for generated plots")
    parser.add_argument("--paragraph-sweep-workers", type=int, default=None,
                        help="Worker count to use during paragraph sweep (defaults to max from --worker-counts)")

    # Stress-test parameters
    parser.add_argument("--jobs", type=int, default=50, help="Jobs per stress test run")
    parser.add_argument("--sentences-per-section", type=int, default=3)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--poll-interval", type=float, default=0.1)
    parser.add_argument("--poll-timeout", type=float, default=300.0)
    parser.add_argument("--request-timeout", type=float, default=15.0)
    parser.add_argument("--producer-url", default="http://localhost:8080")
    parser.add_argument("--aggregator-url", default="http://localhost:8081")
    parser.add_argument("--seed", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    experiment_cfg = ExperimentConfig(
        worker_counts=sorted(set(args.worker_counts)),
        paragraphs=sorted(set(args.paragraphs)),
        runs_per_setting=args.runs,
        pause_seconds=args.pause,
        compose_file=Path(args.compose_file),
        project_name=args.project_name,
        scale_workers=not args.no_scale,
        scale_wait=args.scale_wait,
        output_csv=Path(args.output),
        graphs_dir=Path(args.graphs_dir),
    )

    stress_base = StressConfig(
        producer_url=args.producer_url,
        aggregator_url=args.aggregator_url,
        jobs=args.jobs,
        paragraphs=args.paragraphs[0],
        sentences_per_section=args.sentences_per_section,
        top_n=args.top_n,
        concurrency=args.concurrency,
        poll_interval=args.poll_interval,
        poll_timeout=args.poll_timeout,
        request_timeout=args.request_timeout,
        seed=args.seed,
    )

    results: list[dict] = []
    ensure_parent_dirs(experiment_cfg.output_csv, experiment_cfg.graphs_dir)
    write_csv_header_if_missing(experiment_cfg.output_csv)

    scaler_state = {"current": None}

    def ensure_scale(worker_count: int) -> None:
        if not experiment_cfg.scale_workers:
            return
        if scaler_state["current"] == worker_count:
            return
        scale_cluster(experiment_cfg, worker_count)
        print(f"Waiting {experiment_cfg.scale_wait}s for scaling to settle (workers={worker_count})...")
        time.sleep(experiment_cfg.scale_wait)
        scaler_state["current"] = worker_count

    results.extend(run_worker_sweep(experiment_cfg, stress_base, ensure_scale))
    fixed_workers = args.paragraph_sweep_workers or experiment_cfg.worker_counts[-1]
    results.extend(run_paragraph_sweep(experiment_cfg, stress_base, ensure_scale, fixed_workers=fixed_workers))

    if results:
        experiment_cfg.graphs_dir.mkdir(parents=True, exist_ok=True)
        plot_latency_vs_workers(results, experiment_cfg.graphs_dir / "latency_vs_workers.png")
        plot_latency_vs_paragraphs(results, experiment_cfg.graphs_dir / "latency_vs_paragraphs.png")
        summary = compute_summary(results)
        summary_path = experiment_cfg.graphs_dir / "summary.json"
        with summary_path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)

        print(f"\nSaved CSV rows to {experiment_cfg.output_csv}")
        print(f"Graphs + summary written to {experiment_cfg.graphs_dir}")
    else:
        print("No results recorded.")


def scale_cluster(cfg: ExperimentConfig, workers: int) -> None:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(cfg.compose_file),
    ]
    if cfg.project_name:
        cmd += ["-p", cfg.project_name]
    cmd += [
        "up",
        "-d",
        "--scale",
        f"worker={workers}",
        "rabbit",
        "producer",
        "aggregator",
        "worker",
    ]
    subprocess.run(cmd, check=True)


def ensure_parent_dirs(*paths: Path) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)


def write_csv_header_if_missing(csv_path: Path) -> None:
    if csv_path.exists():
        return
    header = ["timestamp", "workers", "paragraphs", "run_index", "jobs", "successes", "failures",
              "throughput", "avg_latency", "p95_latency", "total_duration",
              "concurrency", "sentences_per_section", "top_n"]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)


def append_csv_row(csv_path: Path, row: dict) -> None:
    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([row[key] for key in [
            "timestamp", "workers", "paragraphs", "run_index", "jobs", "successes", "failures",
            "throughput", "avg_latency", "p95_latency", "total_duration",
            "concurrency", "sentences_per_section", "top_n"
        ]])


def run_worker_sweep(experiment_cfg: ExperimentConfig,
                     stress_base: StressConfig,
                     ensure_scale) -> list[dict]:
    baseline_paragraph = experiment_cfg.paragraphs[0]
    print(f"\n--- Worker sweep (paragraphs/job={baseline_paragraph}) ---")
    results: list[dict] = []
    for worker_count in experiment_cfg.worker_counts:
        results.extend(execute_setting(
            worker_count,
            baseline_paragraph,
            experiment_cfg,
            stress_base,
            ensure_scale
        ))
    return results


def run_paragraph_sweep(experiment_cfg: ExperimentConfig,
                        stress_base: StressConfig,
                        ensure_scale,
                        fixed_workers: int) -> list[dict]:
    print(f"\n--- Paragraph sweep (workers fixed at {fixed_workers}) ---")
    results: list[dict] = []
    for paragraph_count in experiment_cfg.paragraphs:
        results.extend(execute_setting(
            fixed_workers,
            paragraph_count,
            experiment_cfg,
            stress_base,
            ensure_scale
        ))
    return results


def execute_setting(worker_count: int,
                    paragraph_count: int,
                    experiment_cfg: ExperimentConfig,
                    stress_base: StressConfig,
                    ensure_scale) -> list[dict]:
    ensure_scale(worker_count)
    print(f"\n=== Paragraphs/job={paragraph_count}, workers={worker_count} ===")

    setting_results: list[dict] = []
    for run_idx in range(experiment_cfg.runs_per_setting):
        cfg = stress_base.__class__(
            **{
                **stress_base.__dict__,
                "paragraphs": paragraph_count,
                "seed": _derive_seed(stress_base.seed, worker_count, paragraph_count, run_idx)
            }
        )
        result = run_stress_test(cfg)
        row = {
            "timestamp": time.time(),
            "workers": worker_count,
            "paragraphs": paragraph_count,
            "run_index": run_idx,
            "jobs": cfg.jobs,
            "successes": len(result.success_runs),
            "failures": len(result.failure_runs),
            "throughput": result.throughput,
            "avg_latency": result.avg_latency,
            "p95_latency": result.p95_latency,
            "total_duration": result.total_duration,
            "concurrency": cfg.concurrency,
            "sentences_per_section": cfg.sentences_per_section,
            "top_n": cfg.top_n,
        }
        append_csv_row(experiment_cfg.output_csv, row)
        setting_results.append(row)
        print(f"Run {run_idx + 1}/{experiment_cfg.runs_per_setting}: "
              f"throughput={row['throughput']:.2f} jobs/s, "
              f"latency(avg/p95)={row['avg_latency']:.2f}/{row['p95_latency']:.2f}s")
        time.sleep(experiment_cfg.pause_seconds)
    return setting_results


def plot_latency_vs_workers(results: Iterable[dict], output: Path) -> None:
    grouped: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in results:
        grouped[row["paragraphs"]][row["workers"]].append(row["avg_latency"])

    plt.figure(figsize=(8, 5))
    for paragraphs, series in sorted(grouped.items()):
        xs = sorted(series.keys())
        ys = [statistics.mean(series[w]) for w in xs]
        plt.plot(xs, ys, marker="o", label=f"{paragraphs} paragraphs")

    plt.title("Average job latency vs worker count")
    plt.xlabel("Worker replicas")
    plt.ylabel("Average latency (s)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    output.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output, dpi=150, bbox_inches="tight")
    plt.close()


def plot_latency_vs_paragraphs(results: Iterable[dict], output: Path) -> None:
    grouped: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in results:
        grouped[row["workers"]][row["paragraphs"]].append(row["avg_latency"])

    plt.figure(figsize=(8, 5))
    for workers, series in sorted(grouped.items()):
        xs = sorted(series.keys())
        ys = [statistics.mean(series[p]) for p in xs]
        plt.plot(xs, ys, marker="o", label=f"{workers} workers")

    plt.title("Average job latency vs paragraphs per job")
    plt.xlabel("Paragraphs per job")
    plt.ylabel("Average latency (s)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    output.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output, dpi=150, bbox_inches="tight")
    plt.close()


def compute_summary(results: Iterable[dict]) -> list[dict]:
    grouped: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for row in results:
        grouped[(row["workers"], row["paragraphs"])].append(row)

    summary = []
    for (workers, paragraphs), rows in sorted(grouped.items()):
        summary.append({
            "workers": workers,
            "paragraphs": paragraphs,
            "runs": len(rows),
            "avg_throughput": statistics.mean(r["throughput"] for r in rows),
            "avg_latency": statistics.mean(r["avg_latency"] for r in rows),
            "avg_p95_latency": statistics.mean(r["p95_latency"] for r in rows),
        })
    return summary


def _derive_seed(base_seed: int | None, workers: int, paragraphs: int, run_idx: int) -> int | None:
    if base_seed is None:
        return None
    return base_seed + workers * 1000 + paragraphs * 100 + run_idx


if __name__ == "__main__":
    main()

