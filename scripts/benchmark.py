"""Measure the service under a real Binance WebSocket for a fixed duration.

The script starts a private service instance on a temporary data directory,
waits for the order book to synchronize, records Prometheus counters and process
statistics, and emits a Markdown report plus the raw JSON behind it.

Run it from the repository root with the development dependencies installed:

    python scripts/benchmark.py --duration 600 --report docs/benchmark.md

Every number in the report comes from the running process or from the files it
wrote. The only arithmetic is unit conversion (bytes per trade to bytes per
million trades, and the measured rate normalized to one hour).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import psutil

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PORT = 8765
DEFAULT_SYMBOL = "BTCUSDT"
METRIC_PREFIX = "mds_"


def parse_prometheus(text: str) -> dict[str, float]:
    """Flatten a Prometheus exposition into ``sample -> value``."""
    samples: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.rpartition(" ")
        if not name or not value:
            continue
        try:
            samples[name] = float(value)
        except ValueError:
            continue
    return samples


def _delta(samples: dict[str, float], start: dict[str, float], name: str) -> float:
    return samples.get(name, 0.0) - start.get(name, 0.0)


def histogram_window(
    start: dict[str, float],
    end: dict[str, float],
    name: str,
) -> tuple[dict[float, int], float, int]:
    """Return bucket counts, sum and count for one histogram over the window."""
    buckets: dict[float, int] = {}
    pattern = re.compile(rf"^{re.escape(name)}_bucket\{{le=\"([^\"]+)\"\}}$")
    for key, value in end.items():
        match = pattern.match(key)
        if match is None:
            continue
        le = float(match.group(1))
        buckets[le] = int(value - start.get(key, 0.0))
    total = int(_delta(end, start, name + "_count"))
    total_sum = _delta(end, start, name + "_sum")
    return buckets, total_sum, total


def histogram_quantile(buckets: dict[float, int], quantile: float) -> float | None:
    """Linear interpolation inside Prometheus histogram buckets."""
    ordered = sorted(buckets.items())
    if not ordered:
        return None
    total = ordered[-1][1]
    if total <= 0:
        return None
    target = quantile * total
    previous_le = ordered[0][0]
    previous_count = 0
    for le, count in ordered:
        if count >= target:
            if le == float("inf"):
                return previous_le
            if count == previous_count:
                return le
            fraction = (target - previous_count) / (count - previous_count)
            return previous_le + (le - previous_le) * fraction
        previous_le = le
        previous_count = count
    return previous_le


def measure_clock_offset() -> float:
    """Median ``Binance time - host time`` in seconds."""
    offsets: list[float] = []
    with httpx.Client(base_url="https://api.binance.com", timeout=10.0) as client:
        for _ in range(5):
            before = time.time()
            response = client.get("/api/v3/time")
            after = time.time()
            response.raise_for_status()
            payload = response.json()
            server_ms = float(payload["serverTime"])
            offsets.append(server_ms / 1000.0 - (before + after) / 2.0)
            time.sleep(0.2)
    return statistics.median(offsets)


def python_process_tree(root: psutil.Process) -> list[psutil.Process]:
    """The service process plus its Python children (venv launchers)."""
    processes = [root]
    try:
        for child in root.children(recursive=True):
            if "python" in child.name().lower():
                processes.append(child)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return processes


def process_tree_rss(processes: list[psutil.Process]) -> int:
    total = 0
    for process in processes:
        try:
            total += process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total


def kill_process_tree(pid: int) -> None:
    """Kill the launcher and every Python child it spawned."""
    try:
        root = psutil.Process(pid)
        children = root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return
    for process in [*children, root]:
        with contextlib.suppress(psutil.Error):
            process.kill()
    psutil.wait_procs([*children, root], timeout=10)


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float,
    description: str,
    interval: float = 0.5,
) -> None:
    """Poll until ``predicate`` is true or raise a descriptive error."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            last_error = exc
        time.sleep(interval)
    suffix = f" (last error: {last_error})" if last_error is not None else ""
    raise RuntimeError(f"timed out waiting for {description}{suffix}")


def fetch_json(client: httpx.Client, path: str) -> dict[str, Any]:
    response = client.get(path)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path} did not return a JSON object")
    return payload


def fetch_text(client: httpx.Client, path: str) -> str:
    response = client.get(path)
    response.raise_for_status()
    return response.text


def cpu_model() -> str:
    """Best-effort CPU model name, measured on the host."""
    if os.name == "nt":
        try:
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    "(Get-CimInstance Win32_Processor).Name",
                ],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            value = completed.stdout.strip()
            if value:
                return value
        except (OSError, subprocess.SubprocessError):
            pass
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor() or platform.machine()


def log_tail(path: Path, lines: int = 40) -> str:
    if not path.exists():
        return "(no service log)"
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])


def format_bytes(value: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(value) < 1024 or unit == "GiB":
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} GiB"


def format_seconds(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 1000:.1f} ms"


def format_offset(value: float | None) -> str:
    if value is None:
        return "not measured (raw host-clock values)"
    return f"{value:+.3f} s"


def build_report(data: dict[str, Any], command: str) -> str:
    hardware = data["hardware"]
    throughput = data["throughput"]
    latency = data["latency"]
    storage = data["storage"]
    process = data["process"]

    lines = [
        "# Benchmark results",
        "",
        f"Measured on {data['date_utc']} against the real Binance `aggTrade` and",
        f"`depth@100ms` streams for `{data['symbol']}`.",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| Date (UTC) | {data['date_utc']} |",
        f"| Duration | {data['duration_s']:.1f} s |",
        f"| CPU | {hardware['cpu_model']} |",
        f"| Cores / threads | {hardware['physical_cores']} / {hardware['logical_cores']} |",
        f"| RAM | {hardware['ram_gib']:.1f} GiB |",
        f"| OS | {hardware['os']} |",
        f"| Python | {hardware['python']} |",
        f"| Clock offset applied (Binance - host) | {format_offset(latency['clock_offset_s'])} |",
        f"| Command | `{command}` |",
        "",
        "## Throughput",
        "",
        "| Metric | Measured |",
        "|---|---|",
        f"| WebSocket messages | {throughput['messages']:,} "
        f"({throughput['messages_per_s']:.2f}/s) |",
        f"| Trade messages | {throughput['trade_messages']:,} "
        f"({throughput['trade_messages_per_s']:.2f}/s) |",
        f"| Depth messages | {throughput['depth_messages']:,} "
        f"({throughput['depth_messages_per_s']:.2f}/s) |",
        f"| Trades ingested | {throughput['trades_ingested']:,} |",
        f"| Trades persisted (whole run) | {storage['trades_stored']:,} |",
        f"| Gaps detected | {throughput['gaps_detected']:,} |",
        f"| Trades backfilled | {throughput['backfilled']:,} |",
        f"| Trades missing | {throughput['missing']:,} |",
        "",
        "## Latency",
        "",
        "| Metric | p50 | p95 | Mean |",
        "|---|---|---|---|",
        (
            f"| Exchange timestamp to WebSocket ingest | {format_seconds(latency['ingest_p50'])} "
            f"| {format_seconds(latency['ingest_p95'])} "
            f"| {format_seconds(latency['ingest_mean'])} |"
        ),
        (
            f"| Exchange timestamp to Parquet write | {format_seconds(latency['write_p50'])} "
            f"| {format_seconds(latency['write_p95'])} "
            f"| {format_seconds(latency['write_mean'])} |"
        ),
        "",
        "## Storage",
        "",
        "| Metric | Measured |",
        "|---|---|",
        f"| Trade data | {format_bytes(storage['trades_bytes'])} |",
        f"| Book data | {format_bytes(storage['book_bytes'])} |",
        f"| Trade bytes per row | {storage['trade_bytes_per_row']:.1f} B |",
        f"| Disk per million trades | {format_bytes(storage['bytes_per_million_trades'])} |",
        f"| Total disk growth | {format_bytes(storage['disk_bytes_per_hour'])}/h |",
        f"| Trade files | {storage['trades_files']} |",
        f"| Book files | {storage['book_files']} |",
        f"| Process uptime at final read | {storage['uptime_s']:.1f} s |",
        "",
        "## Process",
        "",
        "| Metric | Measured |",
        "|---|---|",
        f"| RSS average | {process['rss_average_mib']:.1f} MiB |",
        f"| RSS peak | {process['rss_peak_mib']:.1f} MiB |",
        f"| CPU average | {process['cpu_cores']:.3f} cores "
        f"({process['cpu_percent_one_core']:.1f}% of one core) |",
        f"| CPU seconds consumed | {process['cpu_seconds']:.1f} s |",
        "",
        "## Method",
        "",
        "- Throughput comes from the service's Prometheus counters, sampled at the start",
        "  and at the end of the window. Samples are not interpolated.",
        "- Latency percentiles come from the `mds_message_latency_seconds` and",
        "  `mds_write_lag_seconds` histograms exported by the process itself. They are",
        "  bucket-interpolated, so their resolution is the bucket width. The mean is",
        "  `_sum / _count`.",
        "- The host clock was compared with `GET /api/v3/time` five times and the",
        "  median offset was passed to the service as `MDS_CLOCK_OFFSET_S`. The",
        "  histograms are therefore already expressed in exchange time. If the offset",
        "  is `n/a`, the host could not reach Binance and the service ran with 0.",
        "- The service clamps negative histogram observations at zero; the benchmark",
        "  reports the offset separately so clock skew stays visible.",
        "- Disk numbers are the file sizes reported by `/api/v1/status` after a",
        "  seven-second drain that lets buffered rows reach Parquet. They cover every",
        "  file written by the process, including the warmup. The per-million figure",
        "  divides the measured trade bytes by the measured persisted trade count.",
        "- The hourly disk figure multiplies the measured growth by `3600 / uptime`,",
        "  where uptime comes from the same status payload. It is a unit conversion of",
        "  the measured rate, not an independent estimate.",
        "- Memory and CPU come from `psutil` sampling the service process tree.",
        "  RSS is the working set. CPU is the delta of `user + system` CPU time",
        "  divided by the window.",
        "- Exchange timestamps are compared after applying the measured host offset;",
        "  residual skew and network latency remain part of the reported values.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=600, help="measurement window in seconds")
    parser.add_argument(
        "--warmup", type=int, default=30, help="seconds to wait after the book syncs"
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Binance spot symbol")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="local service port")
    parser.add_argument("--interval", type=float, default=5.0, help="process sampling interval")
    parser.add_argument(
        "--report", type=Path, default=None, help="Markdown report path; JSON is written next to it"
    )
    args = parser.parse_args()

    if args.duration < 600:
        print("warning: the requested window is shorter than the 10-minute target", file=sys.stderr)

    clock_offset: float | None = None
    try:
        clock_offset = measure_clock_offset()
        print(f"host clock offset vs Binance: {clock_offset:+.3f} s")
    except (httpx.HTTPError, KeyError, ValueError, OSError) as exc:
        print(f"warning: could not measure the clock offset: {exc}")

    data_dir = Path(tempfile.mkdtemp(prefix="mds-benchmark-"))
    log_path = data_dir / "service.log"
    log_handle = log_path.open("w", encoding="utf-8")
    env = os.environ.copy()
    env.update(
        {
            "MDS_SYMBOL": args.symbol,
            "MDS_DATA_DIR": str(data_dir / "data"),
            "MDS_HOST": "127.0.0.1",
            "MDS_PORT": str(args.port),
            "MDS_LOG_LEVEL": "WARNING",
            "MDS_CLOCK_OFFSET_S": f"{clock_offset:.6f}" if clock_offset is not None else "0",
        }
    )
    creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    process = subprocess.Popen(
        [sys.executable, "-m", "market_data_service"],
        cwd=ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
    )
    base_url = f"http://127.0.0.1:{args.port}"
    success = False
    try:
        with httpx.Client(base_url=base_url, timeout=10.0) as client:
            wait_until(
                lambda: fetch_json(client, "/api/v1/health").get("book_ready") is True,
                timeout=60.0,
                description="order book synchronization",
            )
            wait_until(
                lambda: fetch_json(client, "/api/v1/status")["ingest"]["last_trade_id"] is not None,
                timeout=30.0,
                description="the first trade",
            )
            print(f"service ready; warming up for {args.warmup} s")
            time.sleep(args.warmup)

            start_metrics = parse_prometheus(fetch_text(client, "/metrics"))
            start_time = datetime.now(UTC)
            start_monotonic = time.monotonic()
            process_handle = psutil.Process(process.pid)
            service_processes = python_process_tree(process_handle)
            start_cpu = {process.pid: process.cpu_times() for process in service_processes}
            rss_samples: list[int] = []

            print(f"measuring for {args.duration} s")
            while True:
                elapsed = time.monotonic() - start_monotonic
                if elapsed >= args.duration:
                    break
                time.sleep(min(args.interval, max(0.1, args.duration - elapsed)))
                rss_samples.append(process_tree_rss(python_process_tree(process_handle)))

            window_metrics = parse_prometheus(fetch_text(client, "/metrics"))
            window_status = fetch_json(client, "/api/v1/status")
            window_end_time = datetime.now(UTC)
            window_end_monotonic = time.monotonic()
            print("measurement finished; waiting 7 s for the final buffers to reach Parquet")
            time.sleep(7.0)
            final_metrics = parse_prometheus(fetch_text(client, "/metrics"))
            final_status = fetch_json(client, "/api/v1/status")
            end_time = datetime.now(UTC)
            rss_samples.append(process_tree_rss(python_process_tree(process_handle)))
            cpu_seconds = 0.0
            for pid, start_times in start_cpu.items():
                try:
                    end_times = psutil.Process(pid).cpu_times()
                except psutil.NoSuchProcess:
                    continue
                cpu_seconds += (end_times.user + end_times.system) - (
                    start_times.user + start_times.system
                )

        duration = window_end_time.timestamp() - start_time.timestamp()
        monotonic_duration = window_end_monotonic - start_monotonic
        if duration <= 0:
            raise RuntimeError("invalid measurement window")

        def counter(name: str) -> int:
            return round(_delta(window_metrics, start_metrics, name))

        trade_messages = counter('mds_ws_messages_total{stream="trade"}')
        depth_messages = counter('mds_ws_messages_total{stream="depth"}')
        trades_ingested = counter("mds_trades_ingested_total")
        gaps = counter("mds_gaps_detected_total")
        backfilled = counter("mds_trades_backfilled_total")
        missing = counter("mds_trades_missing_total")

        ingest_buckets, ingest_sum, ingest_count = histogram_window(
            start_metrics, window_metrics, "mds_message_latency_seconds"
        )
        write_buckets, write_sum, write_count = histogram_window(
            start_metrics, final_metrics, "mds_write_lag_seconds"
        )
        storage = final_status["storage"]
        trades_stored = int(storage["trades_stored"])
        trades_bytes = float(storage["trades_bytes"])
        book_bytes = float(storage["book_bytes"])
        trades_files = int(storage["trades_files"])
        book_files = int(storage["book_files"])
        uptime = float(final_status["uptime_seconds"])
        if trades_stored == 0:
            raise RuntimeError("no trades were persisted")
        if trades_bytes <= 0:
            raise RuntimeError("no trade bytes were written")
        if write_count == 0:
            raise RuntimeError("no write-lag observations were recorded")
        if uptime <= 0:
            raise RuntimeError("invalid process uptime")

        cpu_seconds = 0.0
        for pid, start_times in start_cpu.items():
            try:
                end_times = psutil.Process(pid).cpu_times()
            except psutil.NoSuchProcess:
                continue
            cpu_seconds += (end_times.user + end_times.system) - (
                start_times.user + start_times.system
            )
        rss_mib = [value / (1024 * 1024) for value in rss_samples]
        cpu_cores = cpu_seconds / monotonic_duration

        ingest_p50 = histogram_quantile(ingest_buckets, 0.50)
        ingest_p95 = histogram_quantile(ingest_buckets, 0.95)
        ingest_mean = ingest_sum / ingest_count if ingest_count else None
        write_p50 = histogram_quantile(write_buckets, 0.50)
        write_p95 = histogram_quantile(write_buckets, 0.95)
        write_mean = write_sum / write_count

        data: dict[str, Any] = {
            "date_utc": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_utc": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_s": duration,
            "symbol": args.symbol,
            "connected_at_end": bool(window_status["ingest"]["connected"]),
            "clock_offset_s": clock_offset,
            "hardware": {
                "cpu_model": cpu_model(),
                "physical_cores": psutil.cpu_count(logical=False) or 0,
                "logical_cores": psutil.cpu_count() or 0,
                "ram_gib": psutil.virtual_memory().total / (1024**3),
                "os": platform.platform(),
                "python": platform.python_version(),
            },
            "throughput": {
                "messages": trade_messages + depth_messages,
                "messages_per_s": (trade_messages + depth_messages) / duration,
                "trade_messages": trade_messages,
                "trade_messages_per_s": trade_messages / duration,
                "depth_messages": depth_messages,
                "depth_messages_per_s": depth_messages / duration,
                "trades_ingested": trades_ingested,
                "gaps_detected": gaps,
                "backfilled": backfilled,
                "missing": missing,
            },
            "latency": {
                "clock_offset_s": clock_offset,
                "ingest_p50": ingest_p50,
                "ingest_p95": ingest_p95,
                "ingest_mean": ingest_mean,
                "write_p50": write_p50,
                "write_p95": write_p95,
                "write_mean": write_mean,
            },
            "storage": {
                "trades_stored": trades_stored,
                "trades_bytes": trades_bytes,
                "book_bytes": book_bytes,
                "trade_bytes_per_row": trades_bytes / trades_stored,
                "bytes_per_million_trades": trades_bytes / trades_stored * 1_000_000,
                "disk_bytes_per_hour": (trades_bytes + book_bytes) / uptime * 3600,
                "trade_bytes_per_hour": trades_bytes / uptime * 3600,
                "trades_files": trades_files,
                "book_files": book_files,
                "uptime_s": uptime,
            },
            "process": {
                "rss_average_mib": sum(rss_mib) / len(rss_mib),
                "rss_peak_mib": max(rss_mib),
                "cpu_seconds": cpu_seconds,
                "cpu_cores": cpu_cores,
                "cpu_percent_one_core": cpu_cores * 100,
            },
        }
        command = (
            f"python scripts/benchmark.py --duration {args.duration} "
            f"--symbol {args.symbol} --report {args.report or 'docs/benchmark.md'}"
        )
        report = build_report(data, command)
        print()
        print(report)

        if args.report is not None:
            report_path = args.report
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(report + "\n", encoding="utf-8")
            json_path = report_path.with_suffix(".json")
            json_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
            print(f"wrote {report_path} and {json_path}")

        duration_ok = duration >= 600
        if not duration_ok:
            print(f"warning: measured window was only {duration:.1f} s", file=sys.stderr)
        success = duration_ok
        return 0 if success else 1
    except Exception as exc:
        print(f"benchmark failed: {exc}", file=sys.stderr)
        print("--- service log tail ---", file=sys.stderr)
        print(log_tail(log_path), file=sys.stderr)
        return 1
    finally:
        if process.poll() is None:
            kill_process_tree(process.pid)
        log_handle.close()
        if success:
            shutil.rmtree(data_dir, ignore_errors=True)
        else:
            print(f"benchmark data kept at {data_dir}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
