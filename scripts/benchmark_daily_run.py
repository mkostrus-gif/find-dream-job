#!/usr/bin/env python3
"""Reproducible synthetic daily-run benchmark; never reads a candidate workspace."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable


def run_cli(
    jobctl: Path,
    engine_root: Path,
    env: dict[str, str],
    *args: str,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [sys.executable, str(jobctl), *args],
        cwd=engine_root,
        env=env,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"jobctl {' '.join(args)} failed ({result.returncode})\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def elapsed(operation: Callable[[], Any]) -> float:
    started = time.perf_counter()
    operation()
    return time.perf_counter() - started


def seed_synthetic_database(database: Path, rows: int) -> None:
    timestamp = "2026-01-01T00:00:00"
    vacancies = (
        (
            index,
            "company_site",
            "synthetic_benchmark",
            f"synthetic:{index:05d}",
            f"https://example.test/jobs/{index:05d}",
            f"Synthetic Role {index:05d}",
            f"Synthetic Employer {index % 250:03d}",
            "2026-01-01",
            "2026-01-01",
            "NO_ACTIVE_ACTION",
            "seen",
            50 + index % 40,
            "synthetic benchmark row",
            timestamp,
        )
        for index in range(1, rows + 1)
    )
    actions = (
        (
            index,
            timestamp,
            "none",
            "backlog",
            "",
            0,
            "synthetic inactive history",
            "synthetic_benchmark",
            f"synthetic-action-{index:05d}",
            timestamp,
        )
        for index in range(1, rows + 1)
    )
    with contextlib.closing(sqlite3.connect(database)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executemany(
            """
            INSERT INTO vacancies (
                id, channel, source, external_id, url, title, company,
                first_seen_date, last_seen_date, latest_status, latest_stage,
                score, reason, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            vacancies,
        )
        conn.executemany(
            """
            INSERT INTO action_events (
                vacancy_id, event_at, action_state, bucket, due_date, priority,
                reason, source, dedupe_key, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            actions,
        )
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='projection_state'"
        ).fetchone():
            conn.execute(
                """
                UPDATE projection_state
                SET dirty_revision = dirty_revision + 1,
                    dirty_at = ?
                WHERE singleton_id = 1
                """,
                (timestamp,),
            )
        conn.commit()


def output_metrics(workspace: Path, jobctl: Path, engine_root: Path, env: dict[str, str]) -> dict[str, Any]:
    dashboard = workspace / "dashboard" / "index.html"
    wip_files = sorted((workspace / "views").glob("wip_queue*.md"))
    wip = json.loads(run_cli(jobctl, engine_root, env, "wip-queue", "--json").stdout)
    status: dict[str, Any] | None = None
    status_result = subprocess.run(
        [sys.executable, str(jobctl), "projection-status", "--json"],
        cwd=engine_root,
        env=env,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if status_result.returncode == 0:
        status = json.loads(status_result.stdout)
    return {
        "dashboard_bytes": dashboard.stat().st_size,
        "wip_file_count": len(wip_files),
        "wip_items": wip["pagination"]["total_items"],
        "sqlite_bytes": (workspace / "data" / "job_search.sqlite").stat().st_size,
        "projection_status": status,
    }


def benchmark(
    *,
    engine_root: Path,
    workspace: Path,
    rows: int,
    workflow: str,
) -> dict[str, Any]:
    jobctl = engine_root / "scripts" / "jobctl.py"
    if not jobctl.is_file():
        raise FileNotFoundError(f"jobctl not found: {jobctl}")
    workspace.mkdir(parents=True, exist_ok=True)
    if any(workspace.iterdir()):
        raise RuntimeError(f"benchmark workspace must be empty: {workspace}")
    env = os.environ.copy()
    env["JOB_SEARCH_HOME"] = str(workspace)
    env.pop("JOB_SEARCH_CONFIG", None)
    env.pop("JOB_SEARCH_RUN_LEASE", None)
    env.pop("JOB_SEARCH_DEFER_RENDER", None)
    run_cli(jobctl, engine_root, env, "init", "--json")
    database = workspace / "data" / "job_search.sqlite"
    seed_synthetic_database(database, rows)
    setup_rebuild = elapsed(
        lambda: run_cli(jobctl, engine_root, env, "rebuild", "--json")
    )

    def write(vacancy_id: int, *, deferred: bool) -> None:
        args = [
            "set-current-action",
            "--id",
            str(vacancy_id),
            "--action-state",
            "review",
            "--bucket",
            "deep_review",
            "--at",
            f"2026-01-{(vacancy_id % 28) + 1:02d}T12:00:00",
            "--priority",
            str(vacancy_id % 101),
            "--source",
            "synthetic_benchmark",
        ]
        if deferred:
            args.append("--defer-render")
        run_cli(jobctl, engine_root, env, *args)

    deferred = workflow == "deferred"
    one_write = elapsed(lambda: write(1, deferred=deferred))
    if deferred:
        run_cli(jobctl, engine_root, env, "rebuild", "--json")

    def ten_write_workflow() -> None:
        for vacancy_id in range(2, 12):
            write(vacancy_id, deferred=deferred)
        if deferred:
            run_cli(jobctl, engine_root, env, "rebuild", "--json")

    ten_writes = elapsed(ten_write_workflow)
    full_rebuild = elapsed(
        lambda: run_cli(jobctl, engine_root, env, "rebuild", "--json")
    )
    return {
        "engine_root": str(engine_root),
        "workspace": str(workspace),
        "synthetic_rows": rows,
        "workflow": workflow,
        "python": sys.version.split()[0],
        "sqlite": sqlite3.sqlite_version,
        "setup_rebuild_seconds": round(setup_rebuild, 6),
        "one_write_seconds": round(one_write, 6),
        "ten_writes_workflow_seconds": round(ten_writes, 6),
        "full_rebuild_seconds": round(full_rebuild, 6),
        **output_metrics(workspace, jobctl, engine_root, env),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synthetic 25k benchmark for auto-render versus deferred daily-run writes"
    )
    parser.add_argument(
        "--engine-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--rows", type=int, default=25_000)
    parser.add_argument("--workflow", choices=("auto", "deferred"), required=True)
    parser.add_argument("--workspace", type=Path, default=None)
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be positive")
    engine_root = args.engine_root.expanduser().resolve()
    if args.workspace is not None:
        result = benchmark(
            engine_root=engine_root,
            workspace=args.workspace.expanduser().absolute(),
            rows=args.rows,
            workflow=args.workflow,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="find-dream-job-benchmark-") as temp:
            result = benchmark(
                engine_root=engine_root,
                workspace=Path(temp),
                rows=args.rows,
                workflow=args.workflow,
            )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
