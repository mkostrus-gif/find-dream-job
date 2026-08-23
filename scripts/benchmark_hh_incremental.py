#!/usr/bin/env python3
"""Reproducible synthetic P2 benchmark; never reads a candidate workspace."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ENGINE_ROOT = Path(__file__).resolve().parents[1]
JOBCTL = ENGINE_ROOT / "scripts" / "jobctl.py"
sys.path.insert(0, str(ENGINE_ROOT / "scripts"))

import benchmark_daily_run  # noqa: E402
import hh_acquisition as hh  # noqa: E402
from jobsearch_config import Settings, load_settings  # noqa: E402


RUN_DATE = "2026-08-19"
PREVIOUS_DATE = "2026-08-18"


@dataclass(frozen=True)
class SyntheticCard:
    vacancy_id: int
    title: str
    company: str
    publication: str
    promoted: bool = False

    def adapter_card(self, position: int) -> dict[str, Any]:
        return {
            "vacancy_id": str(self.vacancy_id),
            "canonical_url": f"https://example.test/vacancy/{self.vacancy_id}",
            "title": self.title,
            "company": self.company,
            "position": position,
            "publication_evidence": self.publication,
            "promoted": self.promoted,
            "pinned": False,
        }


def run_cli(workspace: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["JOB_SEARCH_HOME"] = str(workspace)
    env.pop("JOB_SEARCH_CONFIG", None)
    env.pop("JOB_SEARCH_RUN_LEASE", None)
    result = subprocess.run(
        [sys.executable, str(JOBCTL), *args],
        cwd=ENGINE_ROOT,
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


def configure_workspace(
    workspace: Path,
    *,
    streams: Sequence[str],
    page_size: int,
) -> Settings:
    workspace.mkdir(parents=True, exist_ok=True)
    if any(workspace.iterdir()):
        raise RuntimeError(f"Synthetic benchmark workspace must be empty: {workspace}")
    run_cli(workspace, "init", "--json")
    config = workspace / "config" / "settings.toml"
    text = config.read_text(encoding="utf-8")
    replacements: Mapping[str, str] = {
        "required_streams": json.dumps(list(streams)),
        "items_per_page": str(page_size),
        "incremental_mode": '"enabled"',
        "minimum_overlap_pages": "2",
        "consecutive_known_boundary_pages": "2",
        "guard_page_required": "true",
        "shadow_runs_required": "3",
        "full_audit_interval_days": "7",
        "max_returned_ids": "10",
    }
    for key, value in replacements.items():
        text, count = re.subn(
            rf"^{re.escape(key)}\s*=.*$",
            f"{key} = {value}",
            text,
            count=1,
            flags=re.MULTILINE,
        )
        if count != 1:
            raise RuntimeError(f"Synthetic config key not found exactly once: {key}")
    config.write_text(text, encoding="utf-8")
    return load_settings(ENGINE_ROOT, config)


def connect(database: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def query_fingerprint(stream_key: str) -> str:
    return hh.payload_hash(
        {
            "source": "synthetic_visible_dom",
            "stream_key": stream_key,
            "period_days": 30,
            "ordering": "publication_descending",
        }
    )


def begin_run(workspace: Path, run_id: str) -> str:
    payload = json.loads(
        run_cli(
            workspace,
            "begin-daily-run",
            "--run-id",
            run_id,
            "--run-date",
            RUN_DATE,
            "--timezone",
            "UTC",
            "--owner",
            "synthetic-p2-benchmark",
            "--json",
        ).stdout
    )
    return str(payload["run_lease"])


def synthetic_dataset(
    *, streams: Sequence[str], cards_total: int
) -> tuple[dict[str, list[SyntheticCard]], set[int], set[int], set[int]]:
    if cards_total % len(streams):
        raise ValueError("--cards must be divisible by --streams.")
    cards_per_stream = cards_total // len(streams)
    if cards_per_stream < 150:
        raise ValueError("Each stream requires at least 150 cards for the safety boundary fixture.")
    known_per_stream = round(cards_per_stream * 0.80)
    changed_per_stream = max(1, round(cards_per_stream * 0.05))
    new_per_stream = cards_per_stream - known_per_stream
    promoted_duplicates = max(1, round(cards_per_stream * 0.05))
    unchanged_per_stream = known_per_stream - changed_per_stream
    fresh_unique_per_stream = new_per_stream - promoted_duplicates
    if fresh_unique_per_stream < 1:
        raise ValueError("Synthetic ratio leaves no unique new cards.")

    known_pool_size = max(
        500
        + len(streams) * max(unchanged_per_stream // 2, 1)
        + unchanged_per_stream,
        1_000,
    )
    new_pool_size = max(fresh_unique_per_stream * max(len(streams) // 2, 1), 250)
    known_pool = [100_000 + index for index in range(known_pool_size)]
    new_pool = [500_000 + index for index in range(new_pool_size)]
    dataset: dict[str, list[SyntheticCard]] = {}
    expected_new: set[int] = set()
    expected_changed: set[int] = set()
    all_known: set[int] = set(known_pool)
    selections: list[tuple[str, list[int], list[int], list[int]]] = []
    for stream_index, stream_key in enumerate(streams):
        changed_ids = [
            known_pool[(stream_index * 11 + offset) % min(500, len(known_pool))]
            for offset in range(changed_per_stream)
        ]
        fresh_ids = [
            new_pool[(stream_index * max(fresh_unique_per_stream // 2, 1) + offset) % len(new_pool)]
            for offset in range(fresh_unique_per_stream)
        ]
        unchanged_ids = [
            known_pool[
                (500 + stream_index * max(unchanged_per_stream // 2, 1) + offset)
                % len(known_pool)
            ]
            for offset in range(unchanged_per_stream)
        ]
        selections.append((stream_key, changed_ids, fresh_ids, unchanged_ids))
        expected_new.update(fresh_ids)
        expected_changed.update(changed_ids)

    for stream_key, changed_ids, fresh_ids, unchanged_ids in selections:
        head: list[SyntheticCard] = [
            SyntheticCard(
                vacancy_id=value,
                title=f"Synthetic New Role {value}",
                company=f"Synthetic Organization {value % 97:02d}",
                publication=RUN_DATE,
            )
            for value in fresh_ids
        ]
        head.extend(
            SyntheticCard(
                vacancy_id=value,
                title=f"Revised Synthetic Role {value}",
                company=f"Synthetic Organization {value % 97:02d}",
                publication=RUN_DATE,
            )
            for value in changed_ids
        )
        duplicate_source = head[:promoted_duplicates]
        head.extend(
            SyntheticCard(
                vacancy_id=item.vacancy_id,
                title=item.title,
                company=item.company,
                publication=item.publication,
                promoted=True,
            )
            for item in duplicate_source
        )
        tail = [
            SyntheticCard(
                vacancy_id=value,
                title=(
                    f"Revised Synthetic Role {value}"
                    if value in expected_changed
                    else f"Synthetic Role {value}"
                ),
                company=f"Synthetic Organization {value % 97:02d}",
                publication=RUN_DATE if value in expected_changed else PREVIOUS_DATE,
            )
            for value in unchanged_ids
        ]
        cards = [*head, *tail]
        if len(cards) != cards_per_stream:
            raise RuntimeError("Synthetic stream cardinality is inconsistent.")
        dataset[stream_key] = cards
    return dataset, all_known, expected_new, expected_changed


def seed_known_vacancies(database: Path, vacancy_ids: Iterable[int]) -> None:
    timestamp = "2026-08-01T09:00:00"
    with contextlib.closing(connect(database)) as conn:
        conn.executemany(
            """
            INSERT INTO vacancies (
                channel, source, external_id, url, title, company,
                first_seen_date, last_seen_date, latest_status,
                latest_stage, updated_at
            ) VALUES ('hh', 'synthetic_benchmark', ?, ?, ?, ?,
                      '2026-08-01', '2026-08-01', 'NEEDS_REVIEW', 'seen', ?)
            """,
            (
                (
                    f"hh:{value}",
                    f"https://example.test/vacancy/{value}",
                    f"Synthetic Role {value}",
                    f"Synthetic Organization {value % 97:02d}",
                    timestamp,
                )
                for value in sorted(set(vacancy_ids))
            ),
        )
        conn.commit()


def seed_eligible_checkpoints(
    database: Path,
    settings: Settings,
    dataset: Mapping[str, Sequence[SyntheticCard]],
    *,
    page_size: int,
) -> None:
    timestamp = "2026-08-18T10:00:00+00:00"
    with contextlib.closing(connect(database)) as conn:
        for stream_key, cards in dataset.items():
            query = query_fingerprint(stream_key)
            config = hh.acquisition_configuration_fingerprint(
                settings,
                source_kind="ordinary_search",
                stream_key=stream_key,
                query_fingerprint=query,
            )
            boundary_index = min(page_size * 4, len(cards) - 1)
            boundary = [f"hh:{cards[boundary_index].vacancy_id}"]
            conn.execute(
                """
                INSERT INTO hh_stream_checkpoints (
                    source, stream_key, source_kind, checkpoint_version,
                    last_successful_run_id, last_successful_date, acquisition_mode,
                    query_fingerprint, configuration_fingerprint, adapter_version,
                    newest_publication, oldest_publication, covered_range_json,
                    boundary_id_hash, boundary_sample_json, last_full_scan_at,
                    last_audit_scan_at, shadow_clean_runs, shadow_runs_required,
                    last_shadow_result_json, last_audit_result_json,
                    session_id_state, session_fingerprint, anomaly_state,
                    eligibility_state, cursor_json, created_at, updated_at
                ) VALUES ('hh', ?, 'ordinary_search', 1, 'synthetic-prior', ?,
                          'shadow', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', '{}',
                          'not_exposed', ?, '', 'eligible', ?, ?, ?)
                """,
                (
                    stream_key,
                    PREVIOUS_DATE,
                    query,
                    config,
                    hh.ADAPTER_VERSION,
                    RUN_DATE,
                    PREVIOUS_DATE,
                    hh.canonical_json(
                        {
                            "newest_publication": RUN_DATE,
                            "oldest_publication": PREVIOUS_DATE,
                        }
                    ),
                    hh.payload_hash(boundary),
                    hh.canonical_json(boundary),
                    timestamp,
                    timestamp,
                    settings.search.hh_acquisition.shadow_runs_required,
                    settings.search.hh_acquisition.shadow_runs_required,
                    hh.sha256_text(f"synthetic-session:{stream_key}"),
                    hh.canonical_json(
                        {
                            "valid_completion_boundary": True,
                            "ordering_compatible": True,
                            "boundary_proven_page": 5,
                        }
                    ),
                    timestamp,
                    timestamp,
                ),
            )
        conn.commit()


def page_capture(
    *,
    settings: Settings,
    stream_key: str,
    cards: Sequence[SyntheticCard],
    page_index: int,
    page_count: int,
    page_size: int,
    captured_at: str,
    ordering_monotonic: bool = True,
    source_reported_count: int | None = None,
) -> dict[str, Any]:
    adapter_cards = [
        item.adapter_card(page_index * page_size + offset + 1)
        for offset, item in enumerate(cards)
    ]
    ordered_ids = [f"hh:{int(item['vacancy_id'])}" for item in adapter_cards]
    ids = sorted(set(ordered_ids))
    digest = hh.payload_hash(ids)
    samples = [
        {
            "sample_index": index,
            "sampled_at": captured_at,
            "relative_offset_ms": index * settings.search.hh_acquisition.page_stability_delay_ms,
            "canonical_ordered_ids": ordered_ids,
            "canonical_ordered_id_hash": hh.payload_hash(ordered_ids),
            "canonical_id_set_hash": digest,
            "visible_card_count": len(ordered_ids),
            "scroll_height": 1_000 + page_index * 100,
            "scroll_position": 1_000 + page_index * 100,
            "maximum_observed_card_position": len(ordered_ids) or None,
            "loader_active": False,
            "mutation_count": 0,
        }
        for index in range(settings.search.hh_acquisition.page_stability_samples + 1)
    ]
    has_next = page_index + 1 < page_count
    return {
        "capture_contract": hh.PAGE_CAPTURE_KIND,
        "contract_version": 1,
        "adapter_version": hh.ADAPTER_VERSION,
        "source_kind": "ordinary_search",
        "canonical_url": f"https://example.test/search/{stream_key}?page={page_index}",
        "query_fingerprint": query_fingerprint(stream_key),
        "page_index": page_index,
        "captured_at": captured_at,
        "source_reported_result_count": source_reported_count,
        "navigation": {
            "consistent": True,
            "previous": {
                "present": page_index > 0,
                "page_index": page_index - 1 if page_index > 0 else None,
                "url": (
                    f"https://example.test/search/{stream_key}?page={page_index - 1}"
                    if page_index > 0
                    else ""
                ),
            },
            "next": {
                "present": has_next,
                "page_index": page_index + 1 if has_next else None,
                "url": (
                    f"https://example.test/search/{stream_key}?page={page_index + 1}"
                    if has_next
                    else ""
                ),
            },
        },
        "ordering": {
            "kind": "publication_descending",
            "monotonic": ordering_monotonic,
            "newest_publication": cards[0].publication if cards else "",
            "oldest_publication": cards[-1].publication if cards else "",
            "evidence": "synthetic deterministic visible order",
        },
        "cards": adapter_cards,
        "loader": {"active": False, "evidence": []},
        "blocker": {"type": "none", "evidence": []},
        "stability": {
            "stability_method": "mutation_observer_visible_dom",
            "mutation_observer_available": True,
            "adapter_version": hh.ADAPTER_VERSION,
            "results_root_selector": "main",
            "required_stable_sample_count": settings.search.hh_acquisition.page_stability_samples,
            "actual_sample_count": len(samples),
            "sampling_interval_ms": settings.search.hh_acquisition.page_stability_delay_ms,
            "timeout_ms": settings.search.hh_acquisition.page_stability_timeout_ms,
            "samples": samples,
            "stable_window_sample_indexes": list(
                range(settings.search.hh_acquisition.page_stability_samples)
            ),
            "final_verification": {
                "performed": True,
                "matched": True,
                "sample_index": len(samples) - 1,
                "observer_mutation_count": 0,
            },
            "bottom_scroll_attempted": True,
            "observer_mutation_evidence_available": True,
            "no_relevant_dom_mutation_after_bottom": True,
            "end_of_list_evidence": not has_next,
        },
        "canonical_id_set_hash": digest,
        "session": {
            "session_id_state": "not_exposed",
            "search_session_id": "",
            "alternative_capture_session_fingerprint": hh.sha256_text(
                f"synthetic-session:{stream_key}"
            ),
            "evidence": ["synthetic page does not expose a session ID"],
        },
        "warnings": [],
    }


def work_ids(conn: sqlite3.Connection, run_id: str) -> set[int]:
    return {
        int(str(row[0]).split(":", 1)[1])
        for row in conn.execute(
            """
            SELECT DISTINCT external_id FROM hh_page_items
            WHERE run_id = ? AND base_classification IN ('new', 'known_changed')
            """,
            (run_id,),
        ).fetchall()
    }


def scan_workspace(
    *,
    workspace: Path,
    settings: Settings,
    dataset: Mapping[str, Sequence[SyntheticCard]],
    page_size: int,
    run_id: str,
    delta: bool,
    interrupt_stream: str | None = None,
) -> dict[str, Any]:
    database = workspace / "data" / "job_search.sqlite"
    begin_run(workspace, run_id)
    metrics: dict[str, Any] = {
        "pages_captured": 0,
        "page_reloads": 0,
        "capture_calls": 0,
        "cards_returned_by_adapter": 0,
        "browser_capture_bytes": 0,
        "bounded_command_response_bytes": 0,
        "sqlite_reconciliation_seconds": 0.0,
        "resumed_stream": "",
    }
    conn = connect(database)
    try:
        for stream_key in dataset:
            plan = hh.build_acquisition_plan(
                conn,
                settings,
                run_id=run_id,
                stream_key=stream_key,
                source_kind="ordinary_search",
                query_fingerprint=query_fingerprint(stream_key),
            )
            expected_mode = "delta" if delta else "full"
            if plan["acquisition_mode"] != expected_mode:
                raise RuntimeError(
                    f"Unexpected plan for {stream_key}: {plan['acquisition_mode']} != {expected_mode}"
                )
        conn.commit()

        for stream_index, (stream_key, cards) in enumerate(dataset.items()):
            pages = [cards[offset : offset + page_size] for offset in range(0, len(cards), page_size)]
            page_index = 0
            while page_index < len(pages):
                capture = page_capture(
                    settings=settings,
                    stream_key=stream_key,
                    cards=pages[page_index],
                    page_index=page_index,
                    page_count=len(pages),
                    page_size=page_size,
                    captured_at=f"2026-08-19T12:{stream_index:02d}:{page_index:02d}Z",
                    source_reported_count=(
                        len({item.vacancy_id for item in pages[page_index]}) - 1
                        if stream_index == 0 and page_index == 0
                        else None
                    ),
                )
                encoded = hh.canonical_json(capture).encode("utf-8")
                started = time.perf_counter()
                result = hh.record_page_capture(
                    conn,
                    settings,
                    run_id=run_id,
                    stream_key=stream_key,
                    payload=capture,
                )
                conn.commit()
                metrics["sqlite_reconciliation_seconds"] += time.perf_counter() - started
                metrics["capture_calls"] += 1
                metrics["cards_returned_by_adapter"] += len(capture["cards"])
                metrics["browser_capture_bytes"] += len(encoded)
                metrics["bounded_command_response_bytes"] += len(
                    hh.canonical_json(result).encode("utf-8")
                )
                if not result["verified"] and result["next_safe_action"]["action"] == "verify_count_drift":
                    recapture = page_capture(
                        settings=settings,
                        stream_key=stream_key,
                        cards=pages[page_index],
                        page_index=page_index,
                        page_count=len(pages),
                        page_size=page_size,
                        captured_at=f"2026-08-19T13:{stream_index:02d}:{page_index:02d}Z",
                        source_reported_count=len(
                            {item.vacancy_id for item in pages[page_index]}
                        )
                        - 1,
                    )
                    started = time.perf_counter()
                    result = hh.record_page_capture(
                        conn,
                        settings,
                        run_id=run_id,
                        stream_key=stream_key,
                        payload=recapture,
                    )
                    conn.commit()
                    metrics["sqlite_reconciliation_seconds"] += time.perf_counter() - started
                    metrics["capture_calls"] += 1
                    metrics["page_reloads"] += 1
                    metrics["cards_returned_by_adapter"] += len(recapture["cards"])
                    metrics["browser_capture_bytes"] += len(
                        hh.canonical_json(recapture).encode("utf-8")
                    )
                    metrics["bounded_command_response_bytes"] += len(
                        hh.canonical_json(result).encode("utf-8")
                    )
                if not result["verified"]:
                    raise RuntimeError(
                        f"Synthetic ordered capture unexpectedly remained unverified: {stream_key}/{page_index}"
                    )
                metrics["pages_captured"] += 1
                if delta:
                    stream = conn.execute(
                        """
                        SELECT boundary_proven_page FROM hh_stream_runs
                        WHERE run_id = ? AND source='hh' AND stream_key = ?
                        """,
                        (run_id, stream_key),
                    ).fetchone()
                    if stream["boundary_proven_page"] is not None:
                        break
                if interrupt_stream == stream_key and page_index == 2:
                    conn.commit()
                    conn.close()
                    conn = connect(database)
                    resumed = hh.build_acquisition_plan(
                        conn,
                        settings,
                        run_id=run_id,
                        stream_key=stream_key,
                        source_kind="ordinary_search",
                        query_fingerprint=query_fingerprint(stream_key),
                    )
                    if resumed["acquisition_mode"] != "resume" or resumed["next_page"] != 3:
                        raise RuntimeError("Interrupted stream did not resume from exact page 3.")
                    metrics["resumed_stream"] = stream_key
                page_index += 1

        started = time.perf_counter()
        status = hh.next_acquisition_work(conn, settings, run_id=run_id)
        status_latency = time.perf_counter() - started
        started = time.perf_counter()
        checkpoint = hh.build_p1_checkpoint_manifest(
            conn,
            settings,
            run_id=run_id,
            stream_key=next(iter(dataset)),
        )
        checkpoint_latency = time.perf_counter() - started
        totals = hh.run_reconciliation_totals(conn, run_id=run_id)
        found = work_ids(conn, run_id)
    finally:
        conn.close()
    metrics.update(
        {
            "sqlite_reconciliation_seconds": round(
                float(metrics["sqlite_reconciliation_seconds"]), 6
            ),
            "checkpoint_manifest_seconds": round(checkpoint_latency, 6),
            "compact_status_seconds": round(status_latency, 6),
            "compact_status_bytes": len(hh.canonical_json(status).encode("utf-8")),
            "checkpoint_manifest_bytes": len(
                hh.canonical_json(checkpoint).encode("utf-8")
            ),
            "known_unchanged": totals["known_unchanged"],
            "known_changed": totals["known_changed"],
            "new": totals["new"],
            "canonical_unique": totals["unique"],
            "duplicates_across_streams": totals["duplicate_across_streams"],
            "found_work_ids": sorted(found),
        }
    )
    return metrics


def adversarial_benchmark(parent: Path, page_size: int) -> dict[str, Any]:
    workspace = parent / "adversarial"
    streams = ["synthetic_reorder", "synthetic_drift"]
    settings = configure_workspace(workspace, streams=streams, page_size=page_size)
    database = workspace / "data" / "job_search.sqlite"
    known_ids = set(range(900_000, 900_100))
    seed_known_vacancies(database, known_ids)
    dataset = {
        stream: [
            SyntheticCard(
                value,
                f"Synthetic Role {value}",
                f"Synthetic Organization {value % 97:02d}",
                PREVIOUS_DATE,
            )
            for value in sorted(known_ids)
        ]
        for stream in streams
    }
    seed_eligible_checkpoints(database, settings, dataset, page_size=page_size)
    run_id = "synthetic-adversarial"
    begin_run(workspace, run_id)
    with contextlib.closing(connect(database)) as conn:
        for stream in streams:
            plan = hh.build_acquisition_plan(
                conn,
                settings,
                run_id=run_id,
                stream_key=stream,
                source_kind="ordinary_search",
                query_fingerprint=query_fingerprint(stream),
            )
            if plan["acquisition_mode"] != "delta":
                raise RuntimeError("Adversarial fixture did not start in delta mode.")
        reorder = page_capture(
            settings=settings,
            stream_key=streams[0],
            cards=dataset[streams[0]][:page_size],
            page_index=0,
            page_count=4,
            page_size=page_size,
            captured_at="2026-08-19T14:00:00Z",
            ordering_monotonic=False,
        )
        reorder_result = hh.record_page_capture(
            conn,
            settings,
            run_id=run_id,
            stream_key=streams[0],
            payload=reorder,
        )
        first_cards = dataset[streams[1]][: max(page_size - 1, 1)]
        drift_first = page_capture(
            settings=settings,
            stream_key=streams[1],
            cards=first_cards,
            page_index=0,
            page_count=4,
            page_size=page_size,
            captured_at="2026-08-19T14:01:00Z",
            source_reported_count=page_size,
        )
        hh.record_page_capture(
            conn,
            settings,
            run_id=run_id,
            stream_key=streams[1],
            payload=drift_first,
        )
        conflicting_cards = [*first_cards[:-1], dataset[streams[1]][page_size + 1]]
        drift_second = page_capture(
            settings=settings,
            stream_key=streams[1],
            cards=conflicting_cards,
            page_index=0,
            page_count=4,
            page_size=page_size,
            captured_at="2026-08-19T14:02:00Z",
            source_reported_count=page_size,
        )
        drift_result = hh.record_page_capture(
            conn,
            settings,
            run_id=run_id,
            stream_key=streams[1],
            payload=drift_second,
        )
        drift_third = page_capture(
            settings=settings,
            stream_key=streams[1],
            cards=conflicting_cards,
            page_index=0,
            page_count=4,
            page_size=page_size,
            captured_at="2026-08-19T14:03:00Z",
            source_reported_count=page_size,
        )
        converged_result = hh.record_page_capture(
            conn,
            settings,
            run_id=run_id,
            stream_key=streams[1],
            payload=drift_third,
        )
        conn.commit()
        if reorder_result["effective_mode"] != "full":
            raise RuntimeError("Ordering anomaly failed to force full fallback.")
        if not (
            drift_result["stream_state"] == "checkpointed"
            and drift_result["next_safe_action"].get("action")
            == "verify_count_drift"
        ):
            raise RuntimeError(
                "Conflicting count-drift recapture failed to remain checkpointed."
            )
        if not (
            converged_result["verified"]
            and converged_result["stream_state"] == "checkpointed"
            and converged_result["next_safe_action"].get("action")
            == "continue_from_page"
        ):
            raise RuntimeError(
                "Independent matching count-drift recapture failed to converge: "
                f"verified={converged_result['verified']}, "
                f"state={converged_result['stream_state']}, "
                f"next={converged_result['next_safe_action'].get('action', '')}."
            )
        return {
            "reordered_source": {
                "safe_result": "full_fallback",
                "effective_mode": reorder_result["effective_mode"],
            },
            "changing_recapture": {
                "safe_result": "checkpointed_until_independent_match",
                "intermediate_action": drift_result["next_safe_action"].get(
                    "action", ""
                ),
                "final_state": converged_result["stream_state"],
                "final_action": converged_result["next_safe_action"].get(
                    "action", ""
                ),
            },
        }


def benchmark(
    *,
    parent: Path,
    streams_count: int,
    cards_total: int,
    page_size: int,
    include_p0p1: bool,
    p0p1_rows: int,
) -> dict[str, Any]:
    streams = [f"synthetic_stream_{index + 1:02d}" for index in range(streams_count)]
    dataset, known_ids, expected_new, expected_changed = synthetic_dataset(
        streams=streams,
        cards_total=cards_total,
    )
    expected_work = expected_new | expected_changed

    full_workspace = parent / "full"
    full_settings = configure_workspace(
        full_workspace, streams=streams, page_size=page_size
    )
    seed_known_vacancies(full_workspace / "data" / "job_search.sqlite", known_ids)
    full = scan_workspace(
        workspace=full_workspace,
        settings=full_settings,
        dataset=dataset,
        page_size=page_size,
        run_id="synthetic-full",
        delta=False,
    )

    delta_workspace = parent / "delta"
    delta_settings = configure_workspace(
        delta_workspace, streams=streams, page_size=page_size
    )
    delta_database = delta_workspace / "data" / "job_search.sqlite"
    seed_known_vacancies(delta_database, known_ids)
    seed_eligible_checkpoints(
        delta_database, delta_settings, dataset, page_size=page_size
    )
    interrupted_stream = streams[len(streams) // 2]
    delta = scan_workspace(
        workspace=delta_workspace,
        settings=delta_settings,
        dataset=dataset,
        page_size=page_size,
        run_id="synthetic-delta",
        delta=True,
        interrupt_stream=interrupted_stream,
    )

    full_found = set(full.pop("found_work_ids"))
    delta_found = set(delta.pop("found_work_ids"))
    full_missed = sorted(expected_work - full_found)
    delta_missed = sorted(expected_work - delta_found)
    if full_missed or delta_missed:
        raise RuntimeError(
            f"Ordered benchmark missed work: full={full_missed[:10]}, delta={delta_missed[:10]}"
        )
    if delta["pages_captured"] >= full["pages_captured"]:
        raise RuntimeError("Delta fixture did not reduce captured pages.")
    adversarial = adversarial_benchmark(parent, page_size)

    p0p1: dict[str, Any] | None = None
    if include_p0p1:
        reference = benchmark_daily_run.benchmark(
            engine_root=ENGINE_ROOT,
            workspace=parent / "p0p1-reference",
            rows=p0p1_rows,
            workflow="deferred",
        )
        p0p1 = {
            "rows": p0p1_rows,
            "full_rebuild_seconds": reference["full_rebuild_seconds"],
            "one_deferred_write_seconds": reference["one_write_seconds"],
            "ten_deferred_writes_and_render_seconds": reference[
                "ten_writes_workflow_seconds"
            ],
            "daily_run_status_seconds": reference["orchestration_status_seconds"],
            "resume_status_seconds": reference["resumed_status_seconds"],
            "dashboard_bytes": reference["dashboard_bytes"],
        }

    page_reduction = 1 - delta["pages_captured"] / full["pages_captured"]
    payload_reduction = 1 - delta["browser_capture_bytes"] / full["browser_capture_bytes"]
    return {
        "benchmark": "synthetic_hh_incremental_v1",
        "adapter_version": hh.ADAPTER_VERSION,
        "schema_version": hh.SCHEMA_VERSION,
        "source_fixture": {
            "configured_streams": streams_count,
            "result_cards": cards_total,
            "cards_per_stream": cards_total // streams_count,
            "configured_page_size": page_size,
            "known_card_ratio": 0.80,
            "new_cards_concentrated_at_head": True,
            "duplicates_across_streams": True,
            "promoted_cards": True,
            "count_drift_recapture": True,
            "interrupted_stream": interrupted_stream,
        },
        "expected": {
            "new_ids": len(expected_new),
            "materially_changed_ids": len(expected_changed),
            "all_expected_work_ids": len(expected_work),
        },
        "full": {**full, "missed_expected_ids": full_missed},
        "delta": {**delta, "missed_expected_ids": delta_missed},
        "reduction": {
            "pages_percent": round(page_reduction * 100, 2),
            "browser_payload_percent": round(payload_reduction * 100, 2),
            "pages_saved": full["pages_captured"] - delta["pages_captured"],
            "browser_bytes_saved": full["browser_capture_bytes"]
            - delta["browser_capture_bytes"],
        },
        "safety": {
            "ordered_fixture_all_expected_found": True,
            "adversarial": adversarial,
        },
        "p0_p1_reference": p0p1,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Синтетический benchmark безопасного HH full/shadow/delta acquisition; "
            "кандидатские данные и внешняя сеть не используются."
        )
    )
    parser.add_argument("--workspace", type=Path, default=None)
    parser.add_argument("--streams", type=int, default=10)
    parser.add_argument("--cards", type=int, default=3_000)
    parser.add_argument("--page-size", type=int, default=25)
    parser.add_argument("--p0p1-rows", type=int, default=25_000)
    parser.add_argument("--skip-p0p1", action="store_true")
    args = parser.parse_args()
    if args.streams < 1 or args.page_size < 1 or args.p0p1_rows < 1:
        parser.error("Числовые параметры должны быть положительными.")
    if args.cards % args.streams:
        parser.error("--cards должен делиться на --streams без остатка.")

    def execute(parent: Path) -> dict[str, Any]:
        parent.mkdir(parents=True, exist_ok=True)
        if any(parent.iterdir()):
            raise RuntimeError(f"Benchmark workspace должен быть пустым: {parent}")
        return benchmark(
            parent=parent,
            streams_count=args.streams,
            cards_total=args.cards,
            page_size=args.page_size,
            include_p0p1=not args.skip_p0p1,
            p0p1_rows=args.p0p1_rows,
        )

    if args.workspace is not None:
        result = execute(args.workspace.expanduser().absolute())
    else:
        with tempfile.TemporaryDirectory(prefix="find-dream-job-hh-p2-benchmark-") as temp:
            result = execute(Path(temp))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
