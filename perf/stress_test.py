#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Iterable, Sequence


DEFAULT_SENTENCES = [
    "Alice loves distributed systems",
    "Bob hates flaky tests",
    "Charlie writes wonderful documentation",
    "Diana fixes horrible bugs quickly",
    "Eve enjoys analyzing performance traces",
    "Федор обожает масштабируемые архитектуры",
    "Галина ненавидит медленные запросы",
    "Иван счастлив, когда тесты зелёные",
    "Мария грустит от регрессий",
    "Система должна быть устойчивой и быстрой",
]


@dataclass(frozen=True)
class StressConfig:
    producer_url: str
    aggregator_url: str
    jobs: int
    paragraphs: int
    sentences_per_section: int
    top_n: int
    concurrency: int
    poll_interval: float
    poll_timeout: float
    request_timeout: float
    seed: int | None = None


@dataclass(frozen=True)
class JobRun:
    job_id: str
    submit_seconds: float
    completion_seconds: float
    success: bool
    error: str | None = None


@dataclass(frozen=True)
class StressResult:
    config: StressConfig
    total_duration: float
    runs: Sequence[JobRun]

    @property
    def success_runs(self) -> list[JobRun]:
        return [run for run in self.runs if run.success]

    @property
    def failure_runs(self) -> list[JobRun]:
        return [run for run in self.runs if not run.success]

    @property
    def throughput(self) -> float:
        successes = len(self.success_runs)
        return successes / self.total_duration if self.total_duration else 0.0

    @property
    def avg_latency(self) -> float:
        successes = self.success_runs
        if not successes:
            return 0.0
        return mean(run.completion_seconds for run in successes)

    @property
    def p95_latency(self) -> float:
        successes = self.success_runs
        if not successes:
            return 0.0
        durations = [run.completion_seconds for run in successes]
        return percentile(durations, 95)

    def to_dict(self) -> dict:
        return {
            "config": asdict(self.config),
            "total_duration": self.total_duration,
            "throughput": self.throughput,
            "avg_latency": self.avg_latency,
            "p95_latency": self.p95_latency,
            "successes": len(self.success_runs),
            "failures": len(self.failure_runs),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stress-test the text processing pipeline.")
    parser.add_argument("--producer-url", default="http://localhost:8080")
    parser.add_argument("--aggregator-url", default="http://localhost:8081")
    parser.add_argument("--jobs", type=int, default=10)
    parser.add_argument("--paragraphs", type=int, default=50)
    parser.add_argument("--sentences-per-section", type=int, default=3)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--poll-interval", type=float, default=0.1)
    parser.add_argument("--poll-timeout", type=float, default=180.0)
    parser.add_argument("--request-timeout", type=float, default=15.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--json-output", default=None,
                        help="Optional file to append JSONL metrics for this run")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = StressConfig(
        producer_url=args.producer_url,
        aggregator_url=args.aggregator_url,
        jobs=args.jobs,
        paragraphs=args.paragraphs,
        sentences_per_section=args.sentences_per_section,
        top_n=args.top_n,
        concurrency=args.concurrency,
        poll_interval=args.poll_interval,
        poll_timeout=args.poll_timeout,
        request_timeout=args.request_timeout,
        seed=args.seed,
    )
    result = run_stress_test(config)

    print(f"Submitting {config.jobs} jobs "
          f"(paragraphs/job={config.paragraphs}, sentences/section={config.sentences_per_section}, "
          f"concurrency={config.concurrency})")

    successes = len(result.success_runs)
    if successes:
        print("\n=== Success metrics ===")
        print(f"Jobs completed: {successes}/{config.jobs}")
        print(f"Throughput: {result.throughput:.2f} jobs/sec "
              f"(wall-clock {result.total_duration:.2f}s)")
        print(f"Avg latency: {result.avg_latency:.2f}s, "
              f"P95 latency: {result.p95_latency:.2f}s")
    else:
        print("\nNo successful jobs.")

    if result.failure_runs:
        print("\n=== Failures ===")
        for run in result.failure_runs:
            print(f"Job {run.job_id or 'N/A'} failed after "
                  f"{run.completion_seconds:.2f}s: {run.error}")

    # if args.json_output:
    #     path = Path(args.json_output)
    #     path.parent.mkdir(parents=True, exist_ok=True)
    #     with path.open("a", encoding="utf-8") as handle:
    #         handle.write(json.dumps(result.to_dict()) + "\n")


def run_stress_test(config: StressConfig) -> StressResult:
    if config.seed is not None:
        random.seed(config.seed)

    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.concurrency) as pool:
        futures = [pool.submit(run_job, config) for _ in range(config.jobs)]
        runs = [future.result() for future in concurrent.futures.as_completed(futures)]
    total_duration = time.perf_counter() - start
    return StressResult(config=config, total_duration=total_duration, runs=runs)


def run_job(config: StressConfig) -> JobRun:
    try:
        text = synthesize_text(config.paragraphs)
        submit_start = time.perf_counter()
        job_id = submit_job(
            base_url=config.producer_url,
            text=text,
            top_n=config.top_n,
            sentences_per_section=config.sentences_per_section,
            timeout=config.request_timeout,
        )
        submit_duration = time.perf_counter() - submit_start
        completed = wait_for_job(
            base_url=config.aggregator_url,
            job_id=job_id,
            top_n=config.top_n,
            poll_interval=config.poll_interval,
            poll_timeout=config.poll_timeout,
            timeout=config.request_timeout,
        )
        total = submit_duration + completed
        return JobRun(job_id=job_id, submit_seconds=submit_duration,
                      completion_seconds=total, success=True)
    except Exception as exc:  # pylint: disable=broad-except
        return JobRun(job_id="", submit_seconds=0.0,
                      completion_seconds=0.0, success=False, error=str(exc))


def submit_job(*, base_url: str, text: str, top_n: int,
               sentences_per_section: int, timeout: float) -> str:
    payload = json.dumps({
        "text": text,
        "topN": top_n,
        "sentencesPerSection": sentences_per_section,
    }).encode("utf-8")
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "api/process")
    request = urllib.request.Request(
        url=url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        data = json.loads(response.read().decode("utf-8"))
        job_id = data.get("jobId")
        if not job_id:
            raise RuntimeError(f"Producer response missing jobId: {data}")
        return job_id


def wait_for_job(*, base_url: str, job_id: str, top_n: int,
                 poll_interval: float, poll_timeout: float, timeout: float) -> float:
    url = urllib.parse.urljoin(
        base_url.rstrip("/") + "/", f"api/jobs/{job_id}?topN={top_n}"
    )
    start = time.perf_counter()
    deadline = start + poll_timeout
    while time.perf_counter() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                if response.status == 200:
                    return time.perf_counter() - start
        except urllib.error.HTTPError as err:
            if err.code not in (404, 503):
                raise
        except urllib.error.URLError:
            pass
        time.sleep(poll_interval)
    raise TimeoutError(f"Job {job_id} did not finish within {poll_timeout}s")


def synthesize_text(paragraphs: int) -> str:
    sentences_per_paragraph = 5
    chunks = []
    for _ in range(paragraphs):
        sentences = [
            random_sentence()
            for _ in range(sentences_per_paragraph)
        ]
        chunks.append(" ".join(sentences))
    return "\n\n".join(chunks)


def random_sentence() -> str:
    base = random.choice(DEFAULT_SENTENCES)
    modifier = random.choice(["quickly", "slowly", "gracefully", "under load", "with joy", "с любовью"])
    punctuation = random.choice([".", "!", "?"])
    return f"{base} {modifier}{punctuation}"


def percentile(values: Iterable[float], pct: float) -> float:
    seq = sorted(values)
    if not seq:
        return 0.0
    k = (len(seq) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(seq) - 1)
    if f == c:
        return seq[int(k)]
    d0 = seq[f] * (c - k)
    d1 = seq[c] * (k - f)
    return d0 + d1


if __name__ == "__main__":
    main()

