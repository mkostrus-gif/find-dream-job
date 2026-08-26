from __future__ import annotations

import hashlib
import gzip
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JOBCTL = ROOT / "scripts" / "jobctl.py"
sys.path.insert(0, str(ROOT / "scripts"))

import daily_run_orchestration as orchestration  # noqa: E402
import hh_acquisition as hh  # noqa: E402
import jobctl  # noqa: E402
from jobsearch_config import load_settings  # noqa: E402


QUERY_A = "a" * 64
QUERY_B = "b" * 64
SESSION_A = "c" * 64
SESSION_B = "d" * 64


def card(
    vacancy_id: int,
    *,
    title: str = "Synthetic Product Role",
    company: str = "Example Organization",
    publication: str = "2026-08-18",
    promoted: bool = False,
) -> dict[str, object]:
    return {
        "vacancy_id": str(vacancy_id),
        "canonical_url": f"https://example.test/vacancy/{vacancy_id}",
        "title": title,
        "company": company,
        "position": 1,
        "publication_evidence": publication,
        "promoted": promoted,
        "pinned": False,
    }


def page_capture(
    cards: list[dict[str, object]],
    *,
    page_index: int = 0,
    query_fingerprint: str = QUERY_A,
    source_kind: str = "ordinary_search",
    captured_at: str = "2026-08-18T12:00:00Z",
    source_count: int | None = None,
    has_next: bool = False,
    stable: bool = True,
    blocker: str = "none",
    ordering: bool = True,
    session_state: str = "not_exposed",
    session_value: str = SESSION_A,
) -> dict[str, object]:
    ordered_ids = [f"hh:{int(str(item['vacancy_id']))}" for item in cards]
    external_ids = sorted({f"hh:{int(str(item['vacancy_id']))}" for item in cards})
    id_hash = hh.payload_hash(external_ids)
    sample_ordered_ids = [ordered_ids, ordered_ids, ordered_ids, ordered_ids]
    if not stable:
        sample_ordered_ids[-1] = [*ordered_ids, "hh:999999999"]
    capture: dict[str, object] = {
        "capture_contract": hh.PAGE_CAPTURE_KIND,
        "contract_version": 1,
        "adapter_version": hh.ADAPTER_VERSION,
        "source_kind": source_kind,
        "canonical_url": f"https://example.test/search?page={page_index}",
        "query_fingerprint": query_fingerprint,
        "page_index": page_index,
        "captured_at": captured_at,
        "source_reported_result_count": source_count,
        "navigation": {
            "consistent": True,
            "previous": {
                "present": page_index > 0,
                "page_index": page_index - 1 if page_index > 0 else None,
                "url": f"https://example.test/search?page={page_index - 1}"
                if page_index > 0
                else "",
            },
            "next": {
                "present": has_next,
                "page_index": page_index + 1 if has_next else None,
                "url": f"https://example.test/search?page={page_index + 1}"
                if has_next
                else "",
            },
        },
        "ordering": {
            "kind": "source_position_with_publication",
            "monotonic": ordering,
            "newest_publication": cards[0].get("publication_evidence", "") if cards else "",
            "oldest_publication": cards[-1].get("publication_evidence", "") if cards else "",
            "evidence": "synthetic deterministic order",
        },
        "cards": cards,
        "loader": {"active": blocker == "loading_timeout", "evidence": []},
        "blocker": {
            "type": blocker,
            "evidence": [] if blocker == "none" else [f"synthetic:{blocker}"],
        },
        "stability": {
            "stability_method": "mutation_observer_visible_dom",
            "mutation_observer_available": True,
            "adapter_version": hh.ADAPTER_VERSION,
            "results_root_selector": "main",
            "required_stable_sample_count": 3,
            "actual_sample_count": 4,
            "sampling_interval_ms": 750,
            "timeout_ms": 30000,
            "samples": [
                {
                    "sample_index": index,
                    "sampled_at": f"2026-08-18T12:00:0{index}Z",
                    "relative_offset_ms": index * 750,
                    "canonical_ordered_ids": values,
                    "canonical_ordered_id_hash": hh.payload_hash(values),
                    "canonical_id_set_hash": hh.payload_hash(sorted(set(values))),
                    "visible_card_count": len(values),
                    "scroll_height": 1000,
                    "scroll_position": 1000,
                    "maximum_observed_card_position": len(values) or None,
                    "loader_active": blocker == "loading_timeout",
                    "mutation_count": 0,
                }
                for index, values in enumerate(sample_ordered_ids)
            ],
            "stable_window_sample_indexes": [0, 1, 2],
            "final_verification": {
                "performed": True,
                "matched": stable,
                "sample_index": 3,
                "observer_mutation_count": 0,
            },
            "bottom_scroll_attempted": True,
            "observer_mutation_evidence_available": True,
            "no_relevant_dom_mutation_after_bottom": stable,
            "end_of_list_evidence": not has_next,
        },
        "canonical_id_set_hash": id_hash,
        "session": {
            "session_id_state": session_state,
            "search_session_id": session_value if session_state == "exposed" else "",
            "alternative_capture_session_fingerprint": session_value
            if session_state == "not_exposed"
            else "",
            "evidence": [f"synthetic:{session_state}"],
        },
        "warnings": [],
    }
    return capture


def detail_capture(vacancy_id: int) -> dict[str, object]:
    return {
        "capture_contract": hh.DETAIL_CAPTURE_KIND,
        "contract_version": 1,
        "adapter_version": hh.ADAPTER_VERSION,
        "captured_at": "2026-08-18T12:10:00Z",
        "vacancy_id": str(vacancy_id),
        "canonical_url": f"https://example.test/vacancy/{vacancy_id}",
        "loader": {"active": False},
        "blocker": {"type": "none", "evidence": []},
        "fields": {
            "title": "Synthetic Product Role",
            "company": "Example Organization",
            "description": "Synthetic source-grounded description.",
            "salary": "",
            "location": "Synthetic location",
            "schedule": "Remote",
            "employment_format": "Full time",
            "requirements": "Synthetic requirements.",
            "experience": "Synthetic level",
            "skills": ["Synthetic skill"],
            "publication_evidence": "2026-08-18",
        },
        "source_evidence": ["synthetic visible DOM fixture"],
    }


def unavailable_detail_capture(vacancy_id: int) -> dict[str, object]:
    observed_url = (
        "https://spb.hh.ru/article/32027?utm_source=hh_lead_gen"
        f"&utm_redirect_vacancy_id={vacancy_id}"
    )
    return {
        "capture_contract": hh.DETAIL_CAPTURE_KIND,
        "contract_version": 1,
        "adapter_version": hh.ADAPTER_VERSION,
        "captured_at": "2026-08-18T12:10:00Z",
        "vacancy_id": str(vacancy_id),
        "canonical_url": f"https://spb.hh.ru/vacancy/{vacancy_id}",
        "loader": {"active": False},
        "blocker": {"type": "none", "evidence": []},
        "availability": {
            "state": "unavailable",
            "reason": "same_origin_lead_gen_redirect",
            "observed_url": observed_url,
        },
        "source_evidence": ["visible_url:same_origin_lead_gen_redirect"],
    }


class SafeIncrementalHHTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-p2-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.previous_home = os.environ.get("JOB_SEARCH_HOME")
        os.environ["JOB_SEARCH_HOME"] = str(self.workspace)
        self.run_cli("init", "--json")
        self.config = self.workspace / "config" / "settings.toml"
        self.database = self.workspace / "data" / "job_search.sqlite"
        self.set_streams(["stream_alpha"])
        self.reload_settings()

    def tearDown(self) -> None:
        if self.previous_home is None:
            os.environ.pop("JOB_SEARCH_HOME", None)
        else:
            os.environ["JOB_SEARCH_HOME"] = self.previous_home
        self.temp_dir.cleanup()

    def run_cli(
        self, *args: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(JOBCTL), *args],
            cwd=ROOT,
            env=self.env,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if check and result.returncode != 0:
            self.fail(
                f"jobctl {' '.join(args)} failed ({result.returncode})\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        return result

    def reload_settings(self) -> None:
        self.settings = load_settings(ROOT, self.config)

    def set_streams(self, streams: list[str]) -> None:
        text = self.config.read_text(encoding="utf-8")
        text = __import__("re").sub(
            r"^required_streams\s*=.*$",
            "required_streams = " + json.dumps(streams),
            text,
            flags=__import__("re").MULTILINE,
        )
        self.config.write_text(text, encoding="utf-8")

    def set_acquisition(self, **values: object) -> None:
        text = self.config.read_text(encoding="utf-8")
        for key, value in values.items():
            rendered = (
                str(value).lower()
                if isinstance(value, bool)
                else f'"{value}"'
                if isinstance(value, str)
                else str(value)
            )
            text = __import__("re").sub(
                rf"^{key}\s*=.*$", f"{key} = {rendered}", text, flags=__import__("re").MULTILINE
            )
        self.config.write_text(text, encoding="utf-8")
        self.reload_settings()

    def enable_personal(self, stream: str = "synthetic_personal") -> None:
        text = self.config.read_text(encoding="utf-8")
        text = text.replace(
            "personal_recommendations_enabled = false",
            "personal_recommendations_enabled = true",
        ).replace(
            'personal_recommendation_stream = "personal_recommendations"',
            f'personal_recommendation_stream = "{stream}"',
        )
        self.config.write_text(text, encoding="utf-8")
        self.reload_settings()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def begin(self, run_id: str, run_date: str = "2026-08-18") -> tuple[str, dict[str, object]]:
        payload = json.loads(
            self.run_cli(
                "begin-daily-run",
                "--run-id",
                run_id,
                "--run-date",
                run_date,
                "--timezone",
                "UTC",
                "--owner",
                "synthetic-p2-test",
                "--json",
            ).stdout
        )
        return str(payload["run_lease"]), payload

    def complete_inbound(
        self,
        run_id: str,
        lease: str,
        *,
        observed_at: str = "2026-08-18T11:00:00Z",
    ) -> subprocess.CompletedProcess[str]:
        path = self.workspace / "tmp" / f"{run_id}-inbound.json"
        path.write_text(
            json.dumps(
                {
                    "manifest_version": 1,
                    "kind": "inbound_reconciliation",
                    "run_id": run_id,
                    "step_key": "inbound_reconciliation",
                    "item_key": "",
                    "observed_at": observed_at,
                    "captured_scope": {"configured_sources": []},
                    "counts": {"raw": 0, "processed": 0, "reconciled": 0, "blocked": 0},
                    "completion_boundary": "all configured inbound sources checked",
                    "remote_boundary_verified": True,
                    "blockers": [],
                }
            ),
            encoding="utf-8",
        )
        return self.run_cli(
            "complete-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            "inbound_reconciliation",
            "--manifest",
            str(path),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )

    def enable_telegram(self, handles: list[str]) -> None:
        text = self.config.read_text(encoding="utf-8")
        text = text.replace(
            "enabled = false\ninitial_lookback_days = 30",
            "enabled = true\ninitial_lookback_days = 30",
            1,
        )
        text = text.replace(
            "channels = []",
            "channels = "
            + json.dumps([f"https://t.me/{handle}" for handle in handles]),
            1,
        )
        self.config.write_text(text, encoding="utf-8")
        self.reload_settings()

    def plan(
        self,
        run_id: str,
        lease: str,
        *,
        stream: str = "stream_alpha",
        source_kind: str = "ordinary_search",
        query: str = QUERY_A,
    ) -> dict[str, object]:
        return json.loads(
            self.run_cli(
                "plan-hh-acquisition",
                "--run-id",
                run_id,
                "--stream-key",
                stream,
                "--source-kind",
                source_kind,
                "--query-fingerprint",
                query,
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )

    def freeze_plan_on_v100(
        self,
        run_id: str,
        *,
        stream: str = "stream_alpha",
    ) -> str:
        return self.freeze_plan_on_version(run_id, "hh-dom-v1.0.0", stream=stream)

    def freeze_plan_on_version(
        self,
        run_id: str,
        version: str,
        *,
        stream: str = "stream_alpha",
    ) -> str:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT source_kind, query_fingerprint
                FROM hh_stream_runs
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (run_id, stream),
            ).fetchone()
            self.assertIsNotNone(row)
            config_payload = hh.acquisition_config_payload(
                self.settings,
                source_kind=str(row["source_kind"]),
                stream_key=stream,
                query_fingerprint=str(row["query_fingerprint"]),
            )
            config_payload["adapter_version"] = version
            old_fingerprint = hh.payload_hash(config_payload)
            conn.execute(
                """
                UPDATE hh_stream_runs SET adapter_version = ?,
                    configuration_fingerprint = ?
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (version, old_fingerprint, run_id, stream),
            )
        return old_fingerprint

    def invalidate_zero_evidence_plan(
        self,
        run_id: str,
        lease: str,
        *,
        stream: str = "stream_alpha",
        reason: str = "synthetic adapter/configuration upgrade recovery",
        check: bool = True,
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, object] | None]:
        result = self.run_cli(
            "invalidate-hh-zero-evidence-plan",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--reason",
            reason,
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=check,
        )
        return result, json.loads(result.stdout) if result.stdout.strip() else None

    def write_json(self, name: str, payload: object) -> Path:
        path = self.workspace / "tmp" / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def record_page(
        self,
        run_id: str,
        lease: str,
        payload: dict[str, object],
        *,
        stream: str = "stream_alpha",
        check: bool = True,
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, object] | None]:
        path = self.write_json(
            f"{run_id}-{stream}-{payload['page_index']}-{payload['captured_at'].replace(':', '')}.json",
            payload,
        )
        result = self.run_cli(
            "record-hh-page",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--capture",
            str(path),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=check,
        )
        return result, json.loads(result.stdout) if result.stdout.strip() else None

    def record_detail(
        self, run_id: str, lease: str, vacancy_id: int, *, stream: str = "stream_alpha"
    ) -> dict[str, object]:
        path = self.write_json(f"{run_id}-detail-{vacancy_id}.json", detail_capture(vacancy_id))
        return json.loads(
            self.run_cli(
                "record-hh-detail",
                "--run-id",
                run_id,
                "--stream-key",
                stream,
                "--capture",
                str(path),
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )

    def finalize_stream(
        self,
        run_id: str,
        lease: str,
        *,
        stream: str = "stream_alpha",
        personal: bool = False,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        command = (
            "finalize-hh-personal-recommendations" if personal else "finalize-hh-stream"
        )
        return self.run_cli(
            command,
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=check,
        )

    def record_transient_attempt(
        self,
        run_id: str,
        lease: str,
        *,
        stream: str,
        page_index: int,
        attempt: int,
        error_class: str = "hh_http_502",
        status_code: int = 502,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return self.run_cli(
            "record-hh-transient-error",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--page-index",
            str(page_index),
            "--error-class",
            error_class,
            "--visible-status-code",
            str(status_code),
            "--visible-message",
            "Page temporarily unavailable",
            "--observed-at",
            f"2026-08-18T14:{attempt:02d}:00Z",
            "--remote-evidence-reference",
            f"synthetic-visible-502-{run_id}-{attempt}",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=check,
        )

    def record_rollover_page(
        self,
        run_id: str,
        lease: str,
        payload: dict[str, object],
        *,
        stream: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        path = self.write_json(
            f"{run_id}-{stream}-rollover-{payload['page_index']}-{payload['captured_at'].replace(':', '')}.json",
            payload,
        )
        return self.run_cli(
            "record-hh-rollover-page",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--capture",
            str(path),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=check,
        )

    def prepare_transient_tail(
        self,
        run_id: str,
        *,
        total_pages: int = 3,
        include_mismatched_tail: bool = True,
    ) -> tuple[str, str, list[int]]:
        stream = f"synthetic_personal_{run_id[-8:]}"
        self.enable_personal(stream)
        self.set_acquisition(
            personal_initial_depth_pages=total_pages,
            personal_max_pages=total_pages,
            personal_max_is_completion_boundary=True,
            transient_error_tail_enabled=True,
        )
        vacancy_ids = list(range(820000, 820000 + total_pages))
        self.seed_known(vacancy_ids)
        lease, _ = self.begin(run_id)
        self.complete_inbound(run_id, lease)
        self.plan(
            run_id,
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        for page_index, vacancy_id in enumerate(vacancy_ids[:-1]):
            self.record_page(
                run_id,
                lease,
                page_capture(
                    [card(vacancy_id)],
                    page_index=page_index,
                    source_kind="personal_recommendations",
                    captured_at=f"2026-08-18T12:{page_index:02d}:00Z",
                    has_next=True,
                    session_state="exposed",
                    session_value=SESSION_A,
                ),
                stream=stream,
            )
        if include_mismatched_tail:
            self.record_page(
                run_id,
                lease,
                page_capture(
                    [card(vacancy_ids[-1])],
                    page_index=total_pages - 1,
                    source_kind="personal_recommendations",
                    captured_at="2026-08-18T13:00:00Z",
                    has_next=False,
                    session_state="exposed",
                    session_value=SESSION_B,
                ),
                stream=stream,
            )
        return lease, stream, vacancy_ids

    def seed_known(self, vacancy_ids: list[int], *, title: str = "Synthetic Product Role") -> None:
        with self.connect() as conn:
            timestamp = "2026-08-01T09:00:00"
            for vacancy_id in vacancy_ids:
                cursor = conn.execute(
                    """
                    INSERT INTO vacancies (
                        channel, source, external_id, url, title, company,
                        first_seen_date, last_seen_date, latest_status,
                        latest_stage, updated_at
                    ) VALUES ('hh', 'synthetic_fixture', ?, ?, ?,
                              'Example Organization', '2026-08-01', '2026-08-01',
                              'NEEDS_REVIEW', 'seen', ?)
                    """,
                    (
                        f"hh:{vacancy_id}",
                        f"https://example.test/vacancy/{vacancy_id}",
                        title,
                        timestamp,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO vacancy_external_aliases (
                        vacancy_id, channel, external_id, url,
                        first_seen_date, last_seen_date, created_at, updated_at
                    ) VALUES (?, 'hh', ?, ?, '2026-08-01', '2026-08-01', ?, ?)
                    """,
                    (
                        int(cursor.lastrowid),
                        f"hh:{vacancy_id}",
                        f"https://example.test/vacancy/{vacancy_id}",
                        timestamp,
                        timestamp,
                    ),
                )

    def seed_checkpoint(
        self,
        *,
        stream: str = "stream_alpha",
        source_kind: str = "ordinary_search",
        query: str = QUERY_A,
        run_date: str = "2026-08-17",
        clean_runs: int = 3,
        eligible: bool = True,
        anomaly: str = "",
        boundary_ids: list[int] | None = None,
        audit_at: str = "2026-08-17T10:00:00+00:00",
        config_fingerprint: str | None = None,
    ) -> None:
        boundary = [f"hh:{item}" for item in (boundary_ids or [700001, 700002])]
        config_fp = config_fingerprint or hh.acquisition_configuration_fingerprint(
            self.settings,
            source_kind=source_kind,
            stream_key=stream,
            query_fingerprint=query,
        )
        timestamp = "2026-08-17T10:00:00+00:00"
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO hh_stream_checkpoints (
                    source, stream_key, source_kind, checkpoint_version,
                    last_successful_run_id, last_successful_date, acquisition_mode,
                    query_fingerprint, configuration_fingerprint, adapter_version,
                    newest_publication, oldest_publication, covered_range_json,
                    boundary_id_hash, boundary_sample_json, last_full_scan_at,
                    last_audit_scan_at, shadow_clean_runs, shadow_runs_required,
                    last_shadow_result_json, last_audit_result_json,
                    session_id_state, session_fingerprint, anomaly_state,
                    eligibility_state, cursor_json, created_at, updated_at
                ) VALUES ('hh', ?, ?, 1, 'synthetic-previous', ?,
                          'shadow', ?, ?, ?, '2026-08-17', '2026-08-01', ?, ?, ?,
                          ?, ?, ?, ?, '{}', '{}', 'not_exposed', ?, ?, ?, ?, ?, ?)
                """,
                (
                    stream,
                    source_kind,
                    run_date,
                    query,
                    config_fp,
                    hh.ADAPTER_VERSION,
                    json.dumps(
                        {
                            "newest_publication": "2026-08-17",
                            "oldest_publication": "2026-08-01",
                        }
                    ),
                    hh.payload_hash(boundary),
                    json.dumps(boundary),
                    timestamp,
                    audit_at,
                    clean_runs,
                    self.settings.search.hh_acquisition.shadow_runs_required,
                    SESSION_A,
                    anomaly,
                    "eligible" if eligible else "shadow_required",
                    json.dumps(
                        {
                            "valid_completion_boundary": True,
                            "ordering_compatible": True,
                            "boundary_proven_page": 3,
                        }
                    ),
                    timestamp,
                    timestamp,
                ),
            )

    def complete_new_details(self, run_id: str, lease: str, *, stream: str = "stream_alpha") -> None:
        with self.connect() as conn:
            ids = [
                int(str(row[0]).split(":", 1)[1])
                for row in conn.execute(
                    """
                    SELECT external_id FROM hh_detail_queue
                    WHERE run_id = ? AND stream_key = ? AND state = 'pending'
                    ORDER BY id
                    """,
                    (run_id, stream),
                ).fetchall()
            ]
        for vacancy_id in ids:
            self.record_detail(run_id, lease, vacancy_id, stream=stream)

    def status(self, run_id: str, *, verbose: bool = False) -> dict[str, object]:
        args = ["daily-run-status", "--run-id", run_id]
        if verbose:
            args.append("--verbose")
        args.append("--json")
        return json.loads(self.run_cli(*args).stdout)

    def test_01_v9_to_v10_backup_migration_preserves_evidence(self) -> None:
        lease, _ = self.begin("migration-evidence")
        self.run_cli(
            "pause-daily-run",
            "--run-id",
            "migration-evidence",
            "--reason",
            "synthetic migration handoff",
            "--run-lease",
            lease,
            "--json",
        )
        with self.connect() as conn:
            p1_counts = {
                table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("daily_runs", "daily_run_steps", "daily_run_work_items", "daily_run_transitions")
            }
            conn.executescript(
                """
                DROP TABLE hh_incremental_events;
                DROP TABLE hh_detail_queue;
                DROP TABLE hh_vacancy_snapshots;
                DROP TABLE hh_page_items;
                DROP TABLE hh_page_captures;
                DROP TABLE hh_stream_runs;
                DROP TABLE hh_stream_checkpoint_history;
                DROP TABLE hh_stream_checkpoints;
                PRAGMA user_version = 9;
                """
            )
        migrated = json.loads(self.run_cli("migrate-schema", "--defer-render", "--json").stdout)
        self.assertEqual((migrated["from_version"], migrated["to_version"]), (9, 11))
        self.assertTrue(migrated["backup"])
        backup = self.workspace / migrated["backup"]
        self.assertTrue(backup.is_file())
        with sqlite3.connect(backup) as old:
            self.assertEqual(old.execute("PRAGMA user_version").fetchone()[0], 9)
        with self.connect() as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 11)
            self.assertEqual(
                {
                    table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                    for table in p1_counts
                },
                p1_counts,
            )

    def test_02_no_checkpoint_uses_deterministic_full_plan(self) -> None:
        lease, _ = self.begin("full-plan")
        plan = self.plan("full-plan", lease)
        self.assertEqual(plan["acquisition_mode"], "full")
        self.assertEqual(plan["reason"], "initial_scan_without_checkpoint")
        repeated = self.plan("full-plan", lease)
        self.assertEqual(repeated["acquisition_mode"], "full")
        plain = self.run_cli("next-hh-work", "--run-id", "full-plan").stdout
        self.assertIn("снять следующую устойчивую страницу", plain)
        self.assertNotIn("capture_stable_page", plain)

    def test_03_query_or_configuration_drift_forces_shadow(self) -> None:
        self.set_acquisition(incremental_mode="enabled")
        old_config = hh.acquisition_configuration_fingerprint(
            self.settings,
            source_kind="ordinary_search",
            stream_key="stream_alpha",
            query_fingerprint=QUERY_A,
        )
        self.seed_checkpoint(query=QUERY_A, config_fingerprint=old_config)
        self.set_acquisition(minimum_overlap_pages=3)
        lease, _ = self.begin("config-drift")
        plan = self.plan("config-drift", lease)
        self.assertEqual(plan["acquisition_mode"], "shadow")
        self.assertFalse(plan["eligibility"]["configuration_unchanged"])

    def test_03b_due_audit_never_overrides_query_drift(self) -> None:
        self.set_acquisition(incremental_mode="enabled", full_audit_interval_days=1)
        self.seed_checkpoint(
            query=QUERY_A,
            audit_at="2026-08-01T10:00:00+00:00",
        )
        lease, _ = self.begin("query-drift-before-audit")
        plan = self.plan("query-drift-before-audit", lease, query=QUERY_B)
        self.assertEqual(plan["acquisition_mode"], "shadow")
        self.assertFalse(plan["eligibility"]["query_unchanged"])

    def test_04_clean_shadow_evidence_controls_delta_eligibility(self) -> None:
        self.set_acquisition(incremental_mode="enabled", shadow_runs_required=3)
        self.seed_checkpoint(clean_runs=2, eligible=False, boundary_ids=[700001, 700002])
        self.seed_known([700001, 700002, 700003, 700004])
        lease, _ = self.begin("clean-shadow")
        self.complete_inbound("clean-shadow", lease)
        plan = self.plan("clean-shadow", lease)
        self.assertEqual(plan["acquisition_mode"], "shadow")
        pages = [
            page_capture([card(700001)], page_index=0, has_next=True),
            page_capture([card(700002)], page_index=1, has_next=True, captured_at="2026-08-18T12:01:00Z"),
            page_capture([card(700003)], page_index=2, has_next=True, captured_at="2026-08-18T12:02:00Z"),
            page_capture([card(700004)], page_index=3, has_next=False, captured_at="2026-08-18T12:03:00Z"),
        ]
        for capture in pages:
            self.record_page("clean-shadow", lease, capture)
        completed = json.loads(self.finalize_stream("clean-shadow", lease).stdout)
        self.assertEqual(completed["shadow_clean_runs"], 3)
        self.assertTrue(completed["delta_eligible"])

    def test_04b_shadow_evidence_survives_explicit_policy_activation(self) -> None:
        self.set_acquisition(incremental_mode="shadow", shadow_runs_required=3)
        shadow_fingerprint = hh.acquisition_configuration_fingerprint(
            self.settings,
            source_kind="ordinary_search",
            stream_key="stream_alpha",
            query_fingerprint=QUERY_A,
        )
        self.seed_checkpoint(
            clean_runs=3,
            eligible=True,
            boundary_ids=[700101],
            config_fingerprint=shadow_fingerprint,
        )
        self.set_acquisition(incremental_mode="enabled")
        enabled_fingerprint = hh.acquisition_configuration_fingerprint(
            self.settings,
            source_kind="ordinary_search",
            stream_key="stream_alpha",
            query_fingerprint=QUERY_A,
        )
        self.assertEqual(enabled_fingerprint, shadow_fingerprint)
        lease, _ = self.begin("shadow-activation")
        plan = self.plan("shadow-activation", lease)
        self.assertEqual(plan["acquisition_mode"], "delta")

    def test_05_delta_finds_new_items_before_proven_boundary(self) -> None:
        self.set_acquisition(incremental_mode="enabled")
        self.seed_checkpoint(boundary_ids=[710001, 710002])
        self.seed_known([710001, 710002, 710003, 710004])
        lease, _ = self.begin("delta-new")
        self.complete_inbound("delta-new", lease)
        self.assertEqual(self.plan("delta-new", lease)["acquisition_mode"], "delta")
        captures = [
            page_capture([card(719999), card(710001)], page_index=0, has_next=True),
            page_capture([card(710001)], page_index=1, has_next=True, captured_at="2026-08-18T12:01:00Z"),
            page_capture([card(710002)], page_index=2, has_next=True, captured_at="2026-08-18T12:02:00Z"),
            page_capture([card(710003)], page_index=3, has_next=True, captured_at="2026-08-18T12:03:00Z"),
        ]
        for capture in captures:
            self.record_page("delta-new", lease, capture)
        self.complete_new_details("delta-new", lease)
        payload = json.loads(self.finalize_stream("delta-new", lease).stdout)
        self.assertTrue(payload["completed"])
        with self.connect() as conn:
            stream = conn.execute(
                "SELECT * FROM hh_stream_runs WHERE run_id='delta-new'"
            ).fetchone()
            self.assertEqual(stream["boundary_proven_page"], 3)
            self.assertEqual(stream["new_count"], 1)

    def test_06_minimum_overlap_two_known_pages_and_guard_are_enforced(self) -> None:
        self.set_acquisition(incremental_mode="enabled")
        self.seed_checkpoint(boundary_ids=[720001])
        self.seed_known([720001, 720002, 720003])
        lease, _ = self.begin("boundary-guards")
        self.complete_inbound("boundary-guards", lease)
        self.plan("boundary-guards", lease)
        for index, vacancy_id in enumerate([720001, 720001, 720002]):
            _, result = self.record_page(
                "boundary-guards",
                lease,
                page_capture(
                    [card(vacancy_id)],
                    page_index=index,
                    has_next=True,
                    captured_at=f"2026-08-18T12:0{index}:00Z",
                ),
            )
            if index < 2:
                self.assertNotEqual(result["next_safe_action"]["action"], "finalize_stream")
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM hh_stream_runs WHERE run_id='boundary-guards'").fetchone()
            self.assertEqual(row["boundary_candidate_page"], 1)
            self.assertEqual(row["boundary_proven_page"], 2)
            self.assertEqual(row["guard_pages_verified"], 1)

    def test_07_unproven_delta_boundary_falls_back_to_full(self) -> None:
        self.set_acquisition(incremental_mode="enabled")
        self.seed_checkpoint(boundary_ids=[730001])
        self.seed_known([730001])
        lease, _ = self.begin("delta-fallback")
        self.complete_inbound("delta-fallback", lease)
        self.plan("delta-fallback", lease)
        for index, vacancy_id in enumerate([739991, 739992, 739993]):
            self.record_page(
                "delta-fallback",
                lease,
                page_capture(
                    [card(vacancy_id)],
                    page_index=index,
                    has_next=index < 2,
                    captured_at=f"2026-08-18T12:0{index}:00Z",
                ),
            )
        self.complete_new_details("delta-fallback", lease)
        self.finalize_stream("delta-fallback", lease)
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM hh_stream_runs WHERE run_id='delta-fallback'").fetchone()
            self.assertEqual(row["effective_mode"], "full")
            self.assertIn("not_proven", row["fallback_reason"])
            checkpoint = conn.execute(
                "SELECT * FROM hh_stream_checkpoints WHERE stream_key='stream_alpha'"
            ).fetchone()
            self.assertEqual(checkpoint["shadow_clean_runs"], 0)
            self.assertNotEqual(checkpoint["anomaly_state"], "")

    def test_08_stale_checkpoint_forces_shadow(self) -> None:
        self.set_acquisition(incremental_mode="enabled", checkpoint_staleness_days=3)
        self.seed_checkpoint(run_date="2026-08-01")
        lease, _ = self.begin("stale-checkpoint", "2026-08-18")
        plan = self.plan("stale-checkpoint", lease)
        self.assertEqual(plan["acquisition_mode"], "shadow")
        self.assertFalse(plan["eligibility"]["checkpoint_fresh"])

    def test_09_periodic_audit_is_scheduled(self) -> None:
        self.set_acquisition(incremental_mode="enabled", full_audit_interval_days=7)
        self.seed_checkpoint(audit_at="2026-08-01T10:00:00+00:00")
        lease, _ = self.begin("audit-due", "2026-08-18")
        plan = self.plan("audit-due", lease)
        self.assertEqual(plan["acquisition_mode"], "audit")

    def test_10_audit_discrepancy_invalidates_delta(self) -> None:
        self.set_acquisition(incremental_mode="enabled", full_audit_interval_days=1)
        self.seed_checkpoint(boundary_ids=[740001], audit_at="2026-08-01T10:00:00+00:00")
        self.seed_known([740001, 740002, 740003, 740004])
        lease, _ = self.begin("audit-miss")
        self.complete_inbound("audit-miss", lease)
        self.assertEqual(self.plan("audit-miss", lease)["acquisition_mode"], "audit")
        captures = [
            page_capture([card(740001)], page_index=0, has_next=True),
            page_capture([card(740001)], page_index=1, has_next=True, captured_at="2026-08-18T12:01:00Z"),
            page_capture([card(740002)], page_index=2, has_next=True, captured_at="2026-08-18T12:02:00Z"),
            page_capture([card(749999)], page_index=3, has_next=True, captured_at="2026-08-18T12:03:00Z"),
            page_capture([card(740003)], page_index=4, has_next=False, captured_at="2026-08-18T12:04:00Z"),
        ]
        for capture in captures:
            self.record_page("audit-miss", lease, capture)
        self.complete_new_details("audit-miss", lease)
        failed = self.finalize_stream("audit-miss", lease, check=False)
        self.assertNotEqual(failed.returncode, 0)
        with self.connect() as conn:
            checkpoint = conn.execute(
                "SELECT eligibility_state, anomaly_state FROM hh_stream_checkpoints WHERE stream_key='stream_alpha'"
            ).fetchone()
            self.assertEqual(tuple(checkpoint), ("invalidated", "audit_discrepancy"))
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_incremental_events WHERE event_type='incremental_safety_failure'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_stream_checkpoint_history WHERE event_type='audit_invalidated'"
                ).fetchone()[0],
                1,
            )

    def test_11_stable_99_of_100_requires_two_matching_captures(self) -> None:
        vacancy_ids = list(range(750000, 750099))
        self.seed_known(vacancy_ids)
        lease, _ = self.begin("count-drift-99")
        self.complete_inbound("count-drift-99", lease)
        self.plan("count-drift-99", lease)
        first_capture = page_capture(
            [card(value) for value in vacancy_ids],
            source_count=100,
            has_next=False,
        )
        _, first = self.record_page("count-drift-99", lease, first_capture)
        self.assertFalse(first["verified"])
        self.assertEqual(first["next_safe_action"]["action"], "verify_count_drift")
        second_capture = page_capture(
            [card(value) for value in vacancy_ids],
            source_count=100,
            has_next=False,
            captured_at="2026-08-18T12:01:00Z",
        )
        _, second = self.record_page("count-drift-99", lease, second_capture)
        self.assertTrue(second["verified"])
        finalized = json.loads(self.finalize_stream("count-drift-99", lease).stdout)
        self.assertTrue(finalized["completed"])
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT recapture_no, verified, count_drift_state
                FROM hh_page_captures WHERE run_id='count-drift-99'
                ORDER BY recapture_no
                """
            ).fetchall()
            self.assertEqual([tuple(row) for row in rows], [(1, 0, "awaiting_recapture"), (2, 1, "verified")])
            warning = conn.execute(
                """
                SELECT COUNT(*) FROM hh_incremental_events
                WHERE run_id='count-drift-99'
                  AND event_type='source_reported_count_drift_verified'
                """
            ).fetchone()[0]
            self.assertEqual(warning, 1)

    def test_11b_timed_sampling_count_drift_accepts_two_matching_captures(
        self,
    ) -> None:
        vacancy_ids = list(range(750100, 750199))
        self.seed_known(vacancy_ids)
        lease, _ = self.begin("count-drift-timed-sampling")
        self.complete_inbound("count-drift-timed-sampling", lease)
        self.plan("count-drift-timed-sampling", lease)

        captures = [
            page_capture(
                [card(value) for value in vacancy_ids],
                source_count=100,
                has_next=False,
                captured_at=captured_at,
            )
            for captured_at in (
                "2026-08-18T12:00:00Z",
                "2026-08-18T12:01:00Z",
            )
        ]
        for capture in captures:
            capture["stability"].update(
                {
                    "stability_method": "timed_visible_dom_sampling",
                    "mutation_observer_available": False,
                    "observer_mutation_evidence_available": False,
                    "no_relevant_dom_mutation_after_bottom": None,
                }
            )
            capture["stability"]["final_verification"][
                "observer_mutation_count"
            ] = None

        _, first = self.record_page(
            "count-drift-timed-sampling", lease, captures[0]
        )
        self.assertFalse(first["verified"])
        _, second = self.record_page(
            "count-drift-timed-sampling", lease, captures[1]
        )
        self.assertTrue(second["verified"])
        self.assertEqual(second["stream_state"], "ready_to_finalize")

    def test_11c_timed_sampling_recovers_from_legacy_unclassified_drift_tail(
        self,
    ) -> None:
        first_ids = list(range(750300, 750399))
        settled_ids = [*first_ids[:-1], 750499]
        self.seed_known([*first_ids, 750499])
        lease, _ = self.begin("count-drift-legacy-tail")
        self.complete_inbound("count-drift-legacy-tail", lease)
        self.plan("count-drift-legacy-tail", lease)

        def timed_capture(ids: list[int], captured_at: str) -> dict[str, object]:
            capture = page_capture(
                [card(value) for value in ids],
                source_count=100,
                has_next=False,
                captured_at=captured_at,
            )
            capture["stability"].update(
                {
                    "stability_method": "timed_visible_dom_sampling",
                    "mutation_observer_available": False,
                    "observer_mutation_evidence_available": False,
                    "no_relevant_dom_mutation_after_bottom": None,
                }
            )
            capture["stability"]["final_verification"][
                "observer_mutation_count"
            ] = None
            return capture

        _, first = self.record_page(
            "count-drift-legacy-tail",
            lease,
            timed_capture(first_ids, "2026-08-18T12:00:00Z"),
        )
        self.assertFalse(first["verified"])
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id, stability_json FROM hh_page_captures
                WHERE run_id = 'count-drift-legacy-tail' AND recapture_no = 1
                """
            ).fetchone()
            stability = json.loads(str(row["stability_json"]))
            stability["stability_method"] = "legacy_unclassified_timed_sampling"
            conn.execute(
                "UPDATE hh_page_captures SET stability_json = ? WHERE id = ?",
                (json.dumps(stability, sort_keys=True), int(row["id"])),
            )

        _, second = self.record_page(
            "count-drift-legacy-tail",
            lease,
            timed_capture(settled_ids, "2026-08-18T12:01:00Z"),
        )
        self.assertFalse(second["verified"])
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id, stability_json FROM hh_page_captures
                WHERE run_id = 'count-drift-legacy-tail' AND recapture_no = 1
                """
            ).fetchone()
            stability = json.loads(str(row["stability_json"]))
            stability["stability_method"] = "timed_visible_dom_sampling"
            conn.execute(
                "UPDATE hh_page_captures SET stability_json = ? WHERE id = ?",
                (json.dumps(stability, sort_keys=True), int(row["id"])),
            )

        _, third = self.record_page(
            "count-drift-legacy-tail",
            lease,
            timed_capture(settled_ids, "2026-08-18T12:02:00Z"),
        )
        self.assertTrue(third["verified"])
        self.assertEqual(third["stream_state"], "ready_to_finalize")

    def test_12_unstable_99_of_100_remains_checkpointed(self) -> None:
        lease, _ = self.begin("unstable-99")
        self.complete_inbound("unstable-99", lease)
        self.plan("unstable-99", lease)
        capture = page_capture(
            [card(value) for value in range(751000, 751099)],
            source_count=100,
            stable=False,
        )
        _, result = self.record_page("unstable-99", lease, capture)
        self.assertFalse(result["verified"])
        self.assertEqual(result["stream_state"], "checkpointed")
        self.assertEqual(result["next_safe_action"]["action"], "repeat_unstable_capture")
        next_work = self.status("unstable-99")["next_safe_work"][0]
        self.assertEqual(next_work["action"], "repeat_unstable_capture")
        blocked_finalize = self.finalize_stream("unstable-99", lease, check=False)
        self.assertNotEqual(blocked_finalize.returncode, 0)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_stream_checkpoints WHERE stream_key='stream_alpha'"
                ).fetchone()[0],
                0,
            )

    def test_12b_unstable_capture_does_not_count_as_drift_recapture(self) -> None:
        lease, _ = self.begin("unstable-not-independent")
        self.complete_inbound("unstable-not-independent", lease)
        self.plan("unstable-not-independent", lease)
        cards = [card(751200 + index) for index in range(99)]
        _, unstable = self.record_page(
            "unstable-not-independent",
            lease,
            page_capture(cards, source_count=100, stable=False),
        )
        self.assertFalse(unstable["verified"])
        _, first_stable = self.record_page(
            "unstable-not-independent",
            lease,
            page_capture(
                cards,
                source_count=100,
                captured_at="2026-08-18T12:01:00Z",
            ),
        )
        self.assertFalse(first_stable["verified"])
        self.assertEqual(first_stable["next_safe_action"]["action"], "verify_count_drift")
        _, second_stable = self.record_page(
            "unstable-not-independent",
            lease,
            page_capture(
                cards,
                source_count=100,
                captured_at="2026-08-18T12:02:00Z",
            ),
        )
        self.assertTrue(second_stable["verified"])

    def test_13_stable_overcount_preserves_promoted_and_duplicate_cards(self) -> None:
        vacancy_ids = list(range(752000, 752101))
        self.seed_known(vacancy_ids)
        lease, _ = self.begin("over-count")
        self.complete_inbound("over-count", lease)
        self.plan("over-count", lease)
        cards = [card(value, promoted=(value == vacancy_ids[0])) for value in vacancy_ids]
        cards.append(dict(cards[0]))
        first = page_capture(cards, source_count=100, has_next=False)
        _, first_result = self.record_page("over-count", lease, first)
        self.assertFalse(first_result["verified"])
        second = page_capture(
            cards,
            source_count=100,
            has_next=False,
            captured_at="2026-08-18T12:01:00Z",
        )
        _, second_result = self.record_page("over-count", lease, second)
        self.assertTrue(second_result["verified"])
        self.finalize_stream("over-count", lease)
        with self.connect() as conn:
            stream = conn.execute(
                "SELECT * FROM hh_stream_runs WHERE run_id='over-count'"
            ).fetchone()
            self.assertEqual(stream["raw_count"], 102)
            self.assertEqual(stream["unique_count"], 101)
            self.assertEqual(stream["duplicate_on_page_count"], 1)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(DISTINCT external_id) FROM hh_page_items WHERE run_id='over-count'"
                ).fetchone()[0],
                101,
            )

    def test_14_changing_ids_require_a_converged_recapture_tail(self) -> None:
        self.seed_known([*range(753000, 753099), 753999])
        lease, _ = self.begin("drift-conflict")
        self.complete_inbound("drift-conflict", lease)
        self.plan("drift-conflict", lease)
        first_ids = list(range(753000, 753099))
        second_ids = [*first_ids[:-1], 753999]
        self.record_page(
            "drift-conflict",
            lease,
            page_capture([card(value) for value in first_ids], source_count=100),
        )
        _, second = self.record_page(
            "drift-conflict",
            lease,
            page_capture(
                [card(value) for value in second_ids],
                source_count=100,
                captured_at="2026-08-18T12:01:00Z",
            ),
        )
        self.assertFalse(second["verified"])
        self.assertEqual(second["stream_state"], "checkpointed")
        self.assertEqual(second["next_safe_action"]["action"], "verify_count_drift")
        _, third = self.record_page(
            "drift-conflict",
            lease,
            page_capture(
                [card(value) for value in second_ids],
                source_count=100,
                captured_at="2026-08-18T12:02:00Z",
            ),
        )
        self.assertTrue(third["verified"])
        self.assertEqual(third["stream_state"], "ready_to_finalize")
        self.finalize_stream("drift-conflict", lease)

    def test_15_source_blockers_and_malformed_identity_fail_closed(self) -> None:
        for blocker in ("login", "captcha", "access_denied", "loading_timeout"):
            with self.subTest(blocker=blocker):
                normalized = hh.validate_page_capture(
                    page_capture([card(754000)], blocker=blocker),
                    self.settings,
                )
                self.assertFalse(normalized["stable"])
                self.assertIn(
                    f"source_{blocker}",
                    {item["code"] for item in normalized["blockers"]},
                )
        malformed = page_capture([card(754001)])
        malformed["cards"][0]["vacancy_id"] = "not-a-source-id"
        with self.assertRaisesRegex(ValueError, "vacancy_id"):
            hh.validate_page_capture(malformed, self.settings)
        forbidden = page_capture([card(754002)])
        forbidden["cookies"] = "synthetic forbidden value"
        with self.assertRaisesRegex(ValueError, "запрещены"):
            hh.validate_page_capture(forbidden, self.settings)

    def test_16_order_anomaly_prevents_early_stop_and_forces_full(self) -> None:
        self.set_acquisition(incremental_mode="enabled")
        self.seed_checkpoint(boundary_ids=[755000])
        self.seed_known([755000])
        lease, _ = self.begin("order-fallback")
        self.complete_inbound("order-fallback", lease)
        self.assertEqual(self.plan("order-fallback", lease)["acquisition_mode"], "delta")
        _, result = self.record_page(
            "order-fallback",
            lease,
            page_capture([card(755000)], ordering=False, has_next=False),
        )
        self.assertEqual(result["effective_mode"], "full")
        self.assertEqual(result["next_safe_action"]["action"], "finalize_stream")
        self.finalize_stream("order-fallback", lease)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT effective_mode, boundary_proven_page, fallback_reason FROM hh_stream_runs WHERE run_id='order-fallback'"
            ).fetchone()
            self.assertEqual(tuple(row), ("full", None, "source_order_anomaly"))
            checkpoint = conn.execute(
                "SELECT anomaly_state, eligibility_state, shadow_clean_runs FROM hh_stream_checkpoints WHERE stream_key='stream_alpha'"
            ).fetchone()
            self.assertEqual(
                tuple(checkpoint),
                ("source_order_anomaly", "shadow_required", 0),
            )

    def test_17_batch_reconciliation_resolves_canonical_and_alias_ids(self) -> None:
        self.seed_known([756000])
        with self.connect() as conn:
            vacancy_id = conn.execute(
                "SELECT id FROM vacancies WHERE external_id='hh:756000'"
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO vacancy_external_aliases (
                    vacancy_id, channel, external_id, url,
                    first_seen_date, last_seen_date, created_at, updated_at
                ) VALUES (?, 'hh', 'hh:756001', 'https://example.test/vacancy/756001',
                          '2026-08-01', '2026-08-01', '2026-08-01T09:00:00', '2026-08-01T09:00:00')
                """,
                (vacancy_id,),
            )
        lease, _ = self.begin("alias-batch")
        self.complete_inbound("alias-batch", lease)
        self.plan("alias-batch", lease)
        _, result = self.record_page(
            "alias-batch",
            lease,
            page_capture([card(756000), card(756001)], has_next=False),
        )
        self.assertEqual(result["reconciliation"]["counts"]["known_unchanged"], 1)
        self.assertEqual(result["reconciliation"]["counts"]["duplicate_on_page"], 1)
        with self.connect() as conn:
            vacancy_ids = {
                row[0]
                for row in conn.execute(
                    "SELECT vacancy_id FROM hh_page_items WHERE run_id='alias-batch'"
                )
            }
            self.assertEqual(vacancy_ids, {vacancy_id})

    def test_18_duplicate_ids_across_pages_and_streams_are_explicit(self) -> None:
        self.set_streams(["stream_alpha", "stream_beta"])
        self.reload_settings()
        self.seed_known([757000])
        lease, _ = self.begin("duplicate-streams")
        self.complete_inbound("duplicate-streams", lease)
        self.plan("duplicate-streams", lease, stream="stream_alpha")
        self.plan("duplicate-streams", lease, stream="stream_beta")
        _, alpha = self.record_page(
            "duplicate-streams",
            lease,
            page_capture([card(757000), card(757000)], has_next=False),
            stream="stream_alpha",
        )
        self.assertEqual(alpha["reconciliation"]["counts"]["duplicate_on_page"], 1)
        _, beta = self.record_page(
            "duplicate-streams",
            lease,
            page_capture([card(757000)], has_next=False),
            stream="stream_beta",
        )
        self.assertEqual(beta["reconciliation"]["counts"]["duplicate_across_streams"], 1)
        self.finalize_stream("duplicate-streams", lease, stream="stream_alpha")
        self.finalize_stream("duplicate-streams", lease, stream="stream_beta")

    def test_18b_cross_stream_duplicate_is_not_resent_for_detail(self) -> None:
        self.set_streams(["stream_alpha", "stream_beta"])
        self.reload_settings()
        lease, _ = self.begin("duplicate-no-resend")
        self.complete_inbound("duplicate-no-resend", lease)
        self.plan("duplicate-no-resend", lease, stream="stream_alpha")
        self.plan("duplicate-no-resend", lease, stream="stream_beta")
        _, alpha = self.record_page(
            "duplicate-no-resend",
            lease,
            page_capture([card(757100)], has_next=False),
            stream="stream_alpha",
        )
        self.assertEqual(len(alpha["reconciliation"]["new_or_changed"]), 1)
        _, beta = self.record_page(
            "duplicate-no-resend",
            lease,
            page_capture(
                [card(757100)],
                has_next=False,
                captured_at="2026-08-18T12:01:00Z",
            ),
            stream="stream_beta",
        )
        self.assertEqual(beta["reconciliation"]["new_or_changed"], [])
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_detail_queue WHERE run_id='duplicate-no-resend'"
                ).fetchone()[0],
                1,
            )

    def test_19_material_change_enters_bounded_detail_queue_without_list_overwrite(self) -> None:
        base = card(758099)
        reranked = {**base, "position": 99}
        self.assertEqual(
            hh.list_material_fingerprint(base),
            hh.list_material_fingerprint(reranked),
        )
        self.set_acquisition(max_returned_ids=1)
        self.seed_known([758000], title="Earlier Synthetic Role")
        lease, _ = self.begin("material-change")
        self.complete_inbound("material-change", lease)
        self.plan("material-change", lease)
        changed_capture = page_capture(
            [card(758000, title="Revised Synthetic Role")], has_next=False
        )
        _, result = self.record_page(
            "material-change",
            lease,
            changed_capture,
        )
        self.assertEqual(result["reconciliation"]["counts"]["known_changed"], 1)
        self.assertEqual(
            result["reconciliation"]["new_or_changed"][0]["base_classification"],
            "known_changed",
        )
        with self.connect() as conn:
            queue = conn.execute(
                "SELECT reason, state FROM hh_detail_queue WHERE run_id='material-change'"
            ).fetchone()
            self.assertEqual(tuple(queue), ("materially_changed", "pending"))
            self.assertEqual(
                conn.execute(
                    "SELECT title FROM vacancies WHERE external_id='hh:758000'"
                ).fetchone()[0],
                "Earlier Synthetic Role",
            )
        self.record_detail("material-change", lease, 758000)
        _, repeated = self.record_page(
            "material-change",
            lease,
            changed_capture,
        )
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(repeated["reconciliation"]["new_or_changed"], [])
        self.finalize_stream("material-change", lease)

    def test_20_p1_pause_new_process_resume_continues_exact_page(self) -> None:
        self.seed_known([759000, 759001])
        lease, _ = self.begin("p2-resume")
        self.complete_inbound("p2-resume", lease)
        self.plan("p2-resume", lease)
        self.record_page(
            "p2-resume", lease, page_capture([card(759000)], page_index=0, has_next=True)
        )
        self.run_cli(
            "pause-daily-run",
            "--run-id",
            "p2-resume",
            "--reason",
            "synthetic process interruption",
            "--run-lease",
            lease,
            "--json",
        )
        resumed = json.loads(
            self.run_cli("resume-daily-run", "--run-id", "p2-resume", "--json").stdout
        )
        new_lease = resumed["run_lease"]
        self.assertNotEqual(new_lease, lease)
        plan = self.plan("p2-resume", new_lease)
        self.assertEqual(plan["acquisition_mode"], "resume")
        self.assertEqual(plan["next_page"], 1)
        self.assertEqual(self.status("p2-resume")["next_safe_work"][0]["action"], "continue_from_page")
        self.record_page(
            "p2-resume",
            new_lease,
            page_capture(
                [card(759001)],
                page_index=1,
                has_next=False,
                captured_at="2026-08-18T12:01:00Z",
            ),
        )
        self.finalize_stream("p2-resume", new_lease)

    def test_21_failed_stream_does_not_advance_successful_cursor(self) -> None:
        self.seed_checkpoint(boundary_ids=[760000])
        with self.connect() as conn:
            before = tuple(
                conn.execute(
                    "SELECT checkpoint_version, last_successful_run_id FROM hh_stream_checkpoints WHERE stream_key='stream_alpha'"
                ).fetchone()
            )
        lease, _ = self.begin("failed-cursor")
        self.complete_inbound("failed-cursor", lease)
        self.plan("failed-cursor", lease)
        _, result = self.record_page(
            "failed-cursor",
            lease,
            page_capture([card(760000)], blocker="login"),
        )
        self.assertEqual(result["stream_state"], "blocked")
        with self.connect() as conn:
            after = tuple(
                conn.execute(
                    "SELECT checkpoint_version, last_successful_run_id FROM hh_stream_checkpoints WHERE stream_key='stream_alpha'"
                ).fetchone()
            )
            self.assertEqual(after, before)

    def test_22_completed_stream_checkpoint_survives_another_blocked_stream(self) -> None:
        self.set_streams(["stream_alpha", "stream_beta"])
        self.reload_settings()
        self.seed_known([761000, 761001])
        lease, _ = self.begin("partial-p2")
        self.complete_inbound("partial-p2", lease)
        self.plan("partial-p2", lease, stream="stream_alpha")
        self.plan("partial-p2", lease, stream="stream_beta")
        self.record_page(
            "partial-p2",
            lease,
            page_capture([card(761000)], has_next=False),
            stream="stream_alpha",
        )
        self.finalize_stream("partial-p2", lease, stream="stream_alpha")
        self.record_page(
            "partial-p2",
            lease,
            page_capture([card(761001)], blocker="captcha"),
            stream="stream_beta",
        )
        rejected = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(rejected.returncode, 0)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT last_successful_run_id FROM hh_stream_checkpoints WHERE stream_key='stream_alpha'"
                ).fetchone()[0],
                "partial-p2",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_stream_checkpoints WHERE stream_key='stream_beta'"
                ).fetchone()[0],
                0,
            )

    def test_22d_completed_stream_can_revalidate_invalidated_p1_item(self) -> None:
        lease, _ = self.begin("completed-stream-revalidation")
        self.complete_inbound("completed-stream-revalidation", lease)
        self.plan("completed-stream-revalidation", lease)
        self.record_page(
            "completed-stream-revalidation",
            lease,
            page_capture([card(761900)], has_next=False),
        )
        self.complete_new_details("completed-stream-revalidation", lease)
        first = json.loads(
            self.finalize_stream("completed-stream-revalidation", lease).stdout
        )
        self.assertTrue(first["completed"])

        with self.connect() as conn:
            old_manifest_hash = conn.execute(
                """
                SELECT manifest_hash FROM daily_run_work_items
                WHERE run_id = ? AND step_key = 'hh_coverage'
                """,
                ("completed-stream-revalidation",),
            ).fetchone()[0]

        self.set_acquisition(transient_error_tail_enabled=True)
        self.run_cli(
            "refresh-daily-run-plan",
            "--run-id",
            "completed-stream-revalidation",
            "--reason",
            "synthetic recovery configuration refresh",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        with self.connect() as conn:
            invalidated = conn.execute(
                """
                SELECT state FROM daily_run_work_items
                WHERE run_id = ? AND step_key = 'hh_coverage'
                """,
                ("completed-stream-revalidation",),
            ).fetchone()[0]
            self.assertEqual(invalidated, "invalidated")

        second = json.loads(
            self.finalize_stream("completed-stream-revalidation", lease).stdout
        )
        self.assertTrue(second["idempotent"])
        self.assertTrue(second["p1_revalidated"])
        self.assertTrue(second["p1_integration"]["changed"])
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT state, manifest_hash FROM daily_run_work_items
                WHERE run_id = ? AND step_key = 'hh_coverage'
                """,
                ("completed-stream-revalidation",),
            ).fetchone()
            self.assertEqual(row["state"], "completed")
            self.assertNotEqual(row["manifest_hash"], old_manifest_hash)
            manifest = conn.execute(
                """
                SELECT payload_json FROM daily_run_manifests
                WHERE payload_hash = ?
                """,
                (row["manifest_hash"],),
            ).fetchone()
            self.assertEqual(
                json.loads(str(manifest["payload_json"]))["revalidation"]["reason"],
                "p1_plan_refresh",
            )

    def test_23_personal_recommendations_are_a_separate_p1_source(self) -> None:
        self.enable_personal("synthetic_personal")
        self.seed_known([762000])
        lease, _ = self.begin("personal-separate")
        self.complete_inbound("personal-separate", lease)
        plan = self.plan(
            "personal-separate",
            lease,
            stream="synthetic_personal",
            source_kind="personal_recommendations",
        )
        self.assertEqual(plan["p1"]["step_key"], "personal_recommendations")
        self.record_page(
            "personal-separate",
            lease,
            page_capture(
                [card(762000)],
                source_kind="personal_recommendations",
                has_next=False,
            ),
            stream="synthetic_personal",
        )
        self.finalize_stream(
            "personal-separate", lease, stream="synthetic_personal", personal=True
        )
        with self.connect() as conn:
            personal = conn.execute(
                "SELECT state FROM daily_run_steps WHERE run_id='personal-separate' AND step_key='personal_recommendations'"
            ).fetchone()[0]
            ordinary = conn.execute(
                "SELECT state FROM daily_run_steps WHERE run_id='personal-separate' AND step_key='hh_coverage'"
            ).fetchone()[0]
            self.assertEqual(personal, "completed")
            self.assertNotEqual(ordinary, "completed")

    def test_23a_personal_full_continuation_page_ignores_moving_total(self) -> None:
        self.enable_personal("synthetic_personal")
        first_ids = list(range(762200, 762300))
        second_ids = list(range(762300, 762400))
        self.seed_known(first_ids)
        lease, _ = self.begin("personal-moving-total")
        self.complete_inbound("personal-moving-total", lease)
        self.plan(
            "personal-moving-total",
            lease,
            stream="synthetic_personal",
            source_kind="personal_recommendations",
        )
        _, first = self.record_page(
            "personal-moving-total",
            lease,
            page_capture(
                [card(value) for value in first_ids],
                source_kind="personal_recommendations",
                source_count=1_000_000,
                has_next=True,
            ),
            stream="synthetic_personal",
        )
        self.assertTrue(first["verified"])
        _, second = self.record_page(
            "personal-moving-total",
            lease,
            page_capture(
                [card(value) for value in second_ids],
                source_kind="personal_recommendations",
                page_index=1,
                source_count=1_000_007,
                has_next=True,
                captured_at="2026-08-18T12:01:00Z",
            ),
            stream="synthetic_personal",
        )
        self.assertTrue(second["verified"])
        self.assertEqual(second["next_safe_action"]["action"], "fetch_details")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT recapture_no, count_drift_state
                FROM hh_page_captures
                WHERE run_id = 'personal-moving-total' AND page_index = 1
                """
            ).fetchone()
            self.assertEqual(tuple(row), (1, "none"))

    def test_23aa_refreshed_plan_can_lower_personal_completion_boundary(self) -> None:
        stream = "synthetic_personal"
        first_ids = list(range(762400, 762500))
        second_ids = list(range(762500, 762600))
        self.enable_personal(stream)
        self.set_acquisition(
            personal_initial_depth_pages=3,
            personal_max_pages=3,
            personal_max_is_completion_boundary=False,
        )
        self.seed_known([762399] + first_ids)
        self.seed_checkpoint(
            stream=stream,
            source_kind="personal_recommendations",
        )
        lease, _ = self.begin("personal-lowered-boundary")
        self.complete_inbound("personal-lowered-boundary", lease)
        self.plan("personal-lowered-boundary", lease)
        self.record_page(
            "personal-lowered-boundary",
            lease,
            page_capture([card(762399)], has_next=False),
        )
        self.finalize_stream("personal-lowered-boundary", lease)
        self.plan(
            "personal-lowered-boundary",
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        self.record_page(
            "personal-lowered-boundary",
            lease,
            page_capture(
                [card(value) for value in first_ids],
                source_kind="personal_recommendations",
                source_count=1_000_000,
                has_next=True,
            ),
            stream=stream,
        )
        _, second = self.record_page(
            "personal-lowered-boundary",
            lease,
            page_capture(
                [card(value) for value in second_ids],
                page_index=1,
                source_kind="personal_recommendations",
                source_count=1_000_000,
                has_next=True,
                captured_at="2026-08-18T12:01:00Z",
            ),
            stream=stream,
        )
        self.assertEqual(second["next_safe_action"]["action"], "fetch_details")
        self.complete_new_details(
            "personal-lowered-boundary", lease, stream=stream
        )
        with self.connect() as conn:
            stream_state = conn.execute(
                """
                SELECT state, boundary_proven_page, next_page
                FROM hh_stream_runs
                WHERE run_id = ? AND stream_key = ?
                """,
                ("personal-lowered-boundary", stream),
            ).fetchone()
            self.assertEqual(tuple(stream_state), ("checkpointed", None, 2))

        self.set_acquisition(
            personal_initial_depth_pages=2,
            personal_max_pages=2,
            personal_max_is_completion_boundary=True,
        )
        self.run_cli(
            "refresh-daily-run-plan",
            "--run-id",
            "personal-lowered-boundary",
            "--reason",
            "synthetic user-approved lower personal boundary",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        with self.connect() as conn:
            ordinary_state = conn.execute(
                """
                SELECT state FROM daily_run_work_items
                WHERE run_id = ? AND step_key = 'hh_coverage'
                """,
                ("personal-lowered-boundary",),
            ).fetchone()[0]
            self.assertEqual(ordinary_state, "completed")
        finalized = json.loads(
            self.finalize_stream(
                "personal-lowered-boundary",
                lease,
                stream=stream,
                personal=True,
            ).stdout
        )
        self.assertTrue(finalized["completed"])
        with self.connect() as conn:
            completed = conn.execute(
                """
                SELECT boundary_proven_page, completion_manifest_json
                FROM hh_stream_runs
                WHERE run_id = ? AND stream_key = ?
                """,
                ("personal-lowered-boundary", stream),
            ).fetchone()
            self.assertEqual(int(completed["boundary_proven_page"]), 1)
            completion_manifest = json.loads(str(completed["completion_manifest_json"]))
            self.assertEqual(
                completion_manifest["stop_reason"],
                "personal_novelty_or_configured_boundary",
            )
            event = conn.execute(
                """
                SELECT details_json FROM hh_incremental_events
                WHERE run_id = ? AND stream_key = ?
                  AND event_type = 'personal_configured_boundary_reconciled'
                ORDER BY id DESC LIMIT 1
                """,
                ("personal-lowered-boundary", stream),
            ).fetchone()
            self.assertIsNotNone(event)
            details = json.loads(str(event[0]))
            self.assertEqual(details["previous_personal_max_pages"], 3)
            self.assertEqual(details["current_personal_max_pages"], 2)

    def test_23ab_lowered_personal_boundary_rejects_unrelated_config_drift(self) -> None:
        stream = "synthetic_personal"
        first_ids = list(range(762600, 762700))
        second_ids = list(range(762700, 762800))
        self.enable_personal(stream)
        self.set_acquisition(
            personal_initial_depth_pages=3,
            personal_max_pages=3,
            personal_max_is_completion_boundary=False,
        )
        self.seed_known(first_ids)
        self.seed_checkpoint(
            stream=stream,
            source_kind="personal_recommendations",
        )
        lease, _ = self.begin("personal-unrelated-drift")
        self.complete_inbound("personal-unrelated-drift", lease)
        self.plan(
            "personal-unrelated-drift",
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        for page_index, values in enumerate((first_ids, second_ids)):
            self.record_page(
                "personal-unrelated-drift",
                lease,
                page_capture(
                    [card(value) for value in values],
                    page_index=page_index,
                    source_kind="personal_recommendations",
                    source_count=1_000_000,
                    has_next=True,
                    captured_at=f"2026-08-18T12:0{page_index}:00Z",
                ),
                stream=stream,
            )
        self.complete_new_details("personal-unrelated-drift", lease, stream=stream)

        self.set_acquisition(
            personal_initial_depth_pages=2,
            personal_max_pages=2,
            personal_max_is_completion_boundary=True,
            minimum_overlap_pages=3,
        )
        self.run_cli(
            "refresh-daily-run-plan",
            "--run-id",
            "personal-unrelated-drift",
            "--reason",
            "synthetic unrelated drift must remain fail closed",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        rejected = self.finalize_stream(
            "personal-unrelated-drift",
            lease,
            stream=stream,
            personal=True,
            check=False,
        )
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("не достигли настроенной границы", rejected.stderr)

    def test_23b_personal_p2_manifest_is_required_by_run_aware_doctor(self) -> None:
        self.enable_personal("synthetic_personal")
        self.seed_known([762100, 762101])
        lease, _ = self.begin("personal-doctor")
        self.complete_inbound("personal-doctor", lease)

        self.plan("personal-doctor", lease)
        self.record_page(
            "personal-doctor",
            lease,
            page_capture([card(762100)], has_next=False),
        )
        self.finalize_stream("personal-doctor", lease)
        premature = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(premature.returncode, 0)
        self.assertIn("personal_recommendations", premature.stderr)

        self.plan(
            "personal-doctor",
            lease,
            stream="synthetic_personal",
            source_kind="personal_recommendations",
        )
        self.record_page(
            "personal-doctor",
            lease,
            page_capture(
                [card(762101)],
                source_kind="personal_recommendations",
                has_next=False,
            ),
            stream="synthetic_personal",
        )
        self.finalize_stream(
            "personal-doctor", lease, stream="synthetic_personal", personal=True
        )
        self.run_cli("finalize-daily-run", "--run-lease", lease, "--json")
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                "personal-doctor",
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])

    def test_24_personal_exposed_session_is_hashed_in_durable_artifacts(self) -> None:
        self.enable_personal("synthetic_personal")
        self.seed_known([763000])
        lease, _ = self.begin("personal-exposed")
        self.complete_inbound("personal-exposed", lease)
        self.plan(
            "personal-exposed",
            lease,
            stream="synthetic_personal",
            source_kind="personal_recommendations",
        )
        self.record_page(
            "personal-exposed",
            lease,
            page_capture(
                [card(763000)],
                source_kind="personal_recommendations",
                session_state="exposed",
                session_value=SESSION_A,
                has_next=False,
            ),
            stream="synthetic_personal",
        )
        self.finalize_stream(
            "personal-exposed", lease, stream="synthetic_personal", personal=True
        )
        with self.connect() as conn:
            row = conn.execute(
                "SELECT session_id_state, session_fingerprint, artifact_path FROM hh_page_captures WHERE run_id='personal-exposed'"
            ).fetchone()
            self.assertEqual(row["session_id_state"], "exposed")
            self.assertNotEqual(row["session_fingerprint"], SESSION_A)
            artifact = self.workspace / row["artifact_path"]
            self.assertEqual(artifact.stat().st_mode & 0o777, 0o600)
            artifact_text = gzip.decompress(artifact.read_bytes()).decode("utf-8")
            self.assertNotIn(SESSION_A, artifact_text)

    def test_25_personal_not_exposed_session_uses_alternative_fingerprint(self) -> None:
        self.enable_personal("synthetic_personal")
        self.seed_known([764000])
        lease, _ = self.begin("personal-not-exposed")
        self.complete_inbound("personal-not-exposed", lease)
        self.plan(
            "personal-not-exposed",
            lease,
            stream="synthetic_personal",
            source_kind="personal_recommendations",
        )
        self.record_page(
            "personal-not-exposed",
            lease,
            page_capture(
                [card(764000)],
                source_kind="personal_recommendations",
                session_state="not_exposed",
                session_value=SESSION_A,
                has_next=False,
            ),
            stream="synthetic_personal",
        )
        self.finalize_stream(
            "personal-not-exposed", lease, stream="synthetic_personal", personal=True
        )
        with self.connect() as conn:
            row = conn.execute(
                "SELECT session_id_state, session_fingerprint FROM hh_stream_checkpoints WHERE stream_key='synthetic_personal'"
            ).fetchone()
            self.assertEqual(tuple(row), ("not_exposed", SESSION_A))

    def test_26_missing_or_changing_personal_session_identity_blocks(self) -> None:
        missing = page_capture(
            [card(765000)], source_kind="personal_recommendations", session_state="not_exposed"
        )
        missing["session"]["alternative_capture_session_fingerprint"] = ""
        with self.assertRaisesRegex(ValueError, "alternative"):
            hh.validate_page_capture(missing, self.settings)
        self.enable_personal("synthetic_personal")
        self.seed_known([765000, 765001])
        lease, _ = self.begin("personal-session-change")
        self.complete_inbound("personal-session-change", lease)
        self.plan(
            "personal-session-change",
            lease,
            stream="synthetic_personal",
            source_kind="personal_recommendations",
        )
        self.record_page(
            "personal-session-change",
            lease,
            page_capture(
                [card(765000)],
                source_kind="personal_recommendations",
                has_next=True,
                session_value=SESSION_A,
            ),
            stream="synthetic_personal",
        )
        _, changed = self.record_page(
            "personal-session-change",
            lease,
            page_capture(
                [card(765001)],
                source_kind="personal_recommendations",
                page_index=1,
                has_next=False,
                captured_at="2026-08-18T12:01:00Z",
                session_state="exposed",
                session_value=SESSION_B,
            ),
            stream="synthetic_personal",
        )
        self.assertEqual(changed["stream_state"], "blocked")
        self.assertEqual(changed["next_safe_action"]["code"], "session_identity_changed")

    def test_26b_visible_next_url_can_expose_session_identity_after_page_zero(self) -> None:
        self.seed_known([765100, 765101])
        lease, _ = self.begin("session-url-enrichment")
        self.complete_inbound("session-url-enrichment", lease)
        self.plan("session-url-enrichment", lease)
        first = page_capture([card(765100)], has_next=True, session_value=SESSION_A)
        next_url = (
            "https://example.test/search?page=1&search_session_id=" + SESSION_B
        )
        first["navigation"]["next"]["url"] = next_url
        self.record_page("session-url-enrichment", lease, first)

        second = page_capture(
            [card(765101)],
            page_index=1,
            captured_at="2026-08-18T12:01:00Z",
            session_state="exposed",
            session_value=SESSION_B,
        )
        second["canonical_url"] = next_url
        _, result = self.record_page("session-url-enrichment", lease, second)

        self.assertTrue(result["verified"])
        self.assertNotEqual(result["stream_state"], "blocked")
        with self.connect() as conn:
            row = conn.execute(
                "SELECT session_id_state, session_fingerprint FROM hh_stream_runs "
                "WHERE run_id='session-url-enrichment' AND stream_key='stream_alpha'"
            ).fetchone()
        self.assertEqual(row["session_id_state"], "exposed")
        self.assertNotEqual(row["session_fingerprint"], SESSION_B)

    def test_27_legacy_v1_hh_manifest_remains_valid(self) -> None:
        lease, _ = self.begin("legacy-v1")
        del lease
        with self.connect() as conn:
            item = conn.execute(
                "SELECT item_key FROM daily_run_work_items WHERE run_id='legacy-v1' AND step_key='hh_coverage'"
            ).fetchone()[0]
            manifest = {
                "manifest_version": 1,
                "kind": "hh_stream",
                "run_id": "legacy-v1",
                "step_key": "hh_coverage",
                "item_key": item,
                "observed_at": "2026-08-18T12:00:00Z",
                "captured_scope": {"stream_key": "stream_alpha"},
                "counts": {
                    "raw": 1,
                    "processed": 1,
                    "unique": 1,
                    "known": 1,
                    "new": 0,
                    "blocked": 0,
                },
                "completion_boundary": {"source_exhausted": True},
                "remote_boundary_verified": True,
                "blockers": [],
            }
            validated = orchestration.validate_manifest(
                conn,
                manifest,
                expected_run_id="legacy-v1",
                expected_step_key="hh_coverage",
                expected_item_key=item,
                expected_kind="hh_stream",
                completion=True,
                workspace_root=self.workspace,
            )
            self.assertEqual(validated["manifest_version"], 1)

    def test_28_v2_manifest_modes_validate_with_mode_specific_boundaries(self) -> None:
        counts = {
            "raw": 1,
            "unique": 1,
            "known_unchanged": 1,
            "known_changed": 0,
            "new": 0,
            "duplicate_on_page": 0,
            "duplicate_across_pages": 0,
            "duplicate_across_streams": 0,
            "processed": 1,
            "reconciled": 1,
            "blocked": 0,
        }
        for mode in ("full", "shadow", "delta", "resume", "audit"):
            with self.subTest(mode=mode):
                delta = mode == "delta"
                manifest = {
                    "manifest_version": 2,
                    "kind": "hh_stream",
                    "run_id": "mode-run",
                    "step_key": "hh_coverage",
                    "item_key": "hh:synthetic",
                    "observed_at": "2026-08-18T12:00:00Z",
                    "captured_scope": {
                        "stream_key": "stream_alpha",
                        "source_kind": "ordinary_search",
                        "query_fingerprint": QUERY_A,
                        "configuration_fingerprint": QUERY_B,
                    },
                    "acquisition_mode": mode,
                    "adapter_version": hh.ADAPTER_VERSION,
                    "session_capability": {
                        "session_id_state": "not_exposed",
                        "session_fingerprint": SESSION_A,
                    },
                    "counts": counts,
                    "pages": [
                        {
                            "page_index": 0,
                            "recapture_no": 1,
                            "capture_hash": QUERY_A,
                            "canonical_id_set_hash": QUERY_B,
                            "count_drift_state": "none",
                            "verified": True,
                        }
                    ],
                    "completion_boundary": {
                        "source_exhausted": not delta,
                        "boundary_proven_page": 0 if delta else None,
                    },
                    "boundary_proof": {
                        "consecutive_known_pages_required": 2,
                        "known_page_streak": 2,
                        "guard_page_required": True,
                        "guard_pages_verified": 1,
                    },
                    "stop_reason": "proven_known_boundary" if delta else "source_exhausted",
                    "remote_boundary_verified": True,
                    "blockers": [],
                }
                validated = hh.validate_manifest_v2(
                    manifest,
                    expected_run_id="mode-run",
                    expected_step_key="hh_coverage",
                    expected_item_key="hh:synthetic",
                    expected_kind="hh_stream",
                    completion=True,
                )
                self.assertEqual(validated["acquisition_mode"], mode)

    def test_29_default_output_is_bounded_with_25000_row_fixture(self) -> None:
        self.set_acquisition(max_returned_ids=5)
        timestamp = "2026-08-01T09:00:00"
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO vacancies (
                    channel, source, external_id, url, title, company,
                    first_seen_date, last_seen_date, latest_status,
                    latest_stage, updated_at
                ) VALUES ('hh', 'synthetic_scale', ?, ?, 'Known Synthetic Role',
                          'Example Organization', '2026-08-01', '2026-08-01',
                          'NEEDS_REVIEW', 'seen', ?)
                """,
                (
                    (
                        f"hh:{800000 + index}",
                        f"https://example.test/vacancy/{800000 + index}",
                        timestamp,
                    )
                    for index in range(25_000)
                ),
            )
        lease, _ = self.begin("bounded-25k")
        self.complete_inbound("bounded-25k", lease)
        self.plan("bounded-25k", lease)
        capture = page_capture([card(900000 + index) for index in range(100)], has_next=True)
        path = self.write_json("bounded-25k-page.json", capture)
        started = time.perf_counter()
        result = self.run_cli(
            "record-hh-page",
            "--run-id",
            "bounded-25k",
            "--stream-key",
            "stream_alpha",
            "--capture",
            str(path),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        elapsed = time.perf_counter() - started
        payload = json.loads(result.stdout)
        self.assertEqual(len(payload["reconciliation"]["new_or_changed"]), 5)
        self.assertEqual(payload["reconciliation"]["truncated"], 95)
        self.assertLess(len(result.stdout.encode("utf-8")), 20_000)
        self.assertLess(elapsed, 5.0)
        status_text = self.run_cli(
            "daily-run-status", "--run-id", "bounded-25k", "--json"
        ).stdout
        self.assertLess(len(status_text.encode("utf-8")), 20_000)

    def test_30_p2_preserves_exactly_one_final_render(self) -> None:
        self.seed_known([766000])
        lease, _ = self.begin("one-render-p2")
        self.complete_inbound("one-render-p2", lease)
        self.plan("one-render-p2", lease)
        self.record_page(
            "one-render-p2", lease, page_capture([card(766000)], has_next=False)
        )
        self.finalize_stream("one-render-p2", lease)
        generations = self.workspace / ".jobctl" / "projections" / "generations"
        before = {path.name for path in generations.iterdir() if path.is_dir()}
        finalized = json.loads(
            self.run_cli("finalize-daily-run", "--run-lease", lease, "--json").stdout
        )
        self.assertTrue(finalized["render_performed"])
        after = {path.name for path in generations.iterdir() if path.is_dir()}
        self.assertEqual(len(after - before), 1)
        self.assertEqual(self.status("one-render-p2")["status"], "completed")
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                "one-render-p2",
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])

    def test_31_checked_in_adapter_and_html_fixtures_are_read_only_and_synthetic(self) -> None:
        adapter = (ROOT / "scripts" / "hh_browser_adapter.js").read_text(encoding="utf-8")
        list_fixture = (ROOT / "tests" / "fixtures" / "hh_search_synthetic.html").read_text(
            encoding="utf-8"
        )
        detail_fixture = (
            ROOT / "tests" / "fixtures" / "vacancy" / "910001" / "index.html"
        ).read_text(encoding="utf-8")
        self.assertIn(hh.ADAPTER_VERSION, adapter)
        self.assertIn("captureListPage", adapter)
        self.assertIn("capturePersonalRecommendations", adapter)
        self.assertIn("captureVacancyDetail", adapter)
        self.assertIn("timed_visible_dom_sampling", adapter)
        self.assertNotIn("globalThis", adapter)
        self.assertNotIn("requestAnimationFrame", adapter)
        self.assertNotIn("ResizeObserver", adapter)
        self.assertNotIn(".click(", adapter)
        self.assertNotIn("fetch(", adapter)
        self.assertIn('href="/vacancy/910001"', list_fixture)
        self.assertIn('data-qa="vacancy-description"', detail_fixture)
        self.assertNotIn("hh.ru", list_fixture + detail_fixture)

    def test_31b_timed_sampling_capture_round_trips_through_python_validator(self) -> None:
        capture = page_capture([card(910001)], has_next=False)
        stability = capture["stability"]
        assert isinstance(stability, dict)
        stability["stability_method"] = "timed_visible_dom_sampling"
        stability["mutation_observer_available"] = False
        stability["observer_mutation_evidence_available"] = False
        stability["no_relevant_dom_mutation_after_bottom"] = None
        final_verification = stability["final_verification"]
        assert isinstance(final_verification, dict)
        final_verification["observer_mutation_count"] = None
        normalized = hh.validate_page_capture(capture, self.settings)
        self.assertTrue(normalized["stable"])
        self.assertEqual(
            normalized["stability"]["stability_method"],
            "timed_visible_dom_sampling",
        )
        self.assertFalse(
            normalized["stability"]["mutation_observer_available"]
        )

    def test_32_full_blocker_regression_daily_run_reaches_closeout(self) -> None:
        run_id = "full-blocker-regression"
        hh_streams = [f"synthetic_hh_{index:02d}" for index in range(1, 11)]
        telegram_handles = [f"synthetic_tg_{index:02d}" for index in range(1, 10)]
        personal_stream = "synthetic_personal"
        self.set_streams(hh_streams)
        self.enable_personal(personal_stream)
        self.enable_telegram(telegram_handles)
        self.seed_known([910001], title="Synthetic Product General Manager")

        with self.connect() as conn:
            vacancy_id = int(
                conn.execute(
                    "SELECT id FROM vacancies WHERE external_id='hh:910001'"
                ).fetchone()[0]
            )
            timestamp = "2026-08-18T09:00:00+00:00"
            conn.executemany(
                """
                INSERT INTO source_checkpoints (
                    source, stream_key, cursor_value, cursor_date, initialized_at,
                    last_completed_run_date, last_manifest_file, created_at, updated_at
                ) VALUES ('telegram', ?, '100', '2026-08-18', ?,
                          '2026-08-18', 'synthetic_previous_manifest.json', ?, ?)
                """,
                (
                    (f"telegram:{handle}", timestamp, timestamp, timestamp)
                    for handle in telegram_handles
                ),
            )

        for index in range(5):
            self.run_cli(
                "record-external-action",
                "--id",
                str(vacancy_id),
                "--action-key",
                f"synthetic-historical-authorization-{index}",
                "--action-type",
                "message",
                "--state",
                "authorized",
                "--at",
                f"2026-08-18T09:0{index}:00Z",
                "--source",
                "synthetic_e2e",
                "--authorization-note",
                "synthetic historical authorization only",
                "--defer-render",
                "--json",
            )

        self.run_cli("rebuild", "--json")
        lease, _ = self.begin(run_id, run_date="2026-08-19")
        initial = self.status(run_id, verbose=True)
        self.assertEqual(initial["external_action_scope"]["legacy_backlog"]["total"], 5)
        self.assertEqual(
            [
                item
                for item in initial["work_items"]
                if item["step_key"] == "external_action_reconciliation"
                and item["required"]
            ],
            [],
        )
        self.complete_inbound(
            run_id, lease, observed_at="2026-08-19T12:00:00Z"
        )

        harness = ROOT / "tests" / "hh_browser_adapter_harness.mjs"
        ordinary_result = subprocess.run(
            ["node", str(harness), "success_links", "ordinary_search"],
            cwd=ROOT,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if ordinary_result.returncode != 0:
            self.fail(ordinary_result.stderr)
        ordinary_capture = json.loads(ordinary_result.stdout)
        self.assertEqual(
            [card_item["vacancy_id"] for card_item in ordinary_capture["cards"]],
            ["910001", "910001"],
        )
        self.assertNotIn(
            "/search/vacancy/map",
            {card_item["canonical_url"] for card_item in ordinary_capture["cards"]},
        )
        for stream in hh_streams:
            self.plan(run_id, lease, stream=stream)
            stream_capture = json.loads(json.dumps(ordinary_capture))
            stream_capture["canonical_url"] = (
                f"https://example.test/search/vacancy?stream={stream}&page=0"
            )
            _, recorded = self.record_page(
                run_id,
                lease,
                stream_capture,
                stream=stream,
            )
            self.assertIsNotNone(recorded)
            self.assertEqual(
                recorded["reconciliation"]["counts"]["duplicate_on_page"], 1
            )
            self.finalize_stream(run_id, lease, stream=stream)

        personal_result = subprocess.run(
            ["node", str(harness), "success_links", "personal_recommendations"],
            cwd=ROOT,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if personal_result.returncode != 0:
            self.fail(personal_result.stderr)
        personal_capture = json.loads(personal_result.stdout)
        personal_capture["canonical_url"] = (
            "https://example.test/search/vacancy?stream=synthetic_personal&page=0"
        )
        self.plan(
            run_id,
            lease,
            stream=personal_stream,
            source_kind="personal_recommendations",
        )
        self.record_page(
            run_id,
            lease,
            personal_capture,
            stream=personal_stream,
        )
        self.finalize_stream(
            run_id, lease, stream=personal_stream, personal=True
        )

        telegram_path = self.workspace / "tmp" / "full-telegram-coverage.json"
        self.run_cli(
            "build-telegram-plan",
            "--run-date",
            "2026-08-19",
            "--output",
            str(telegram_path),
            "--json",
        )
        telegram_manifest = json.loads(telegram_path.read_text(encoding="utf-8"))
        for stream in telegram_manifest["streams"]:
            handle = stream["query"]["handle"]
            self.assertEqual(stream["query"]["mode"], "delta")
            self.assertEqual(stream["query"]["after_post_id"], 100)
            stream.update(
                {
                    "status": "completed",
                    "pages": [
                        {
                            "url": stream["query"]["url"],
                            "post_ids": [101, 100],
                        }
                    ],
                    "posts": [
                        {
                            "post_id": 101,
                            "posted_at": "2026-08-19T09:00:00Z",
                            "url": f"https://t.me/{handle}/101",
                            "classification": "non_vacancy",
                            "vacancy_external_ids": [],
                        },
                        {
                            "post_id": 100,
                            "posted_at": "2026-08-18T09:00:00Z",
                            "url": f"https://t.me/{handle}/100",
                            "classification": "out_of_scope",
                            "vacancy_external_ids": [],
                        },
                    ],
                    "boundary": {
                        "reached": True,
                        "kind": "post_id",
                        "value": 100,
                    },
                    "found": 1,
                    "unique": 0,
                    "known": 0,
                    "new": 0,
                }
            )
        telegram_manifest["totals"] = {"unique": 0, "known": 0, "new": 0}
        telegram_path.write_text(json.dumps(telegram_manifest), encoding="utf-8")
        telegram_checked = json.loads(
            self.run_cli(
                "check-telegram-coverage",
                str(telegram_path),
                "--defer-render",
                "--run-lease",
                lease,
            ).stdout
        )
        self.assertTrue(telegram_checked["ok"], telegram_checked["issues"])
        self.assertEqual(
            telegram_checked["daily_run_integration"]["completed"], 9
        )
        self.assertEqual(
            {
                (
                    stream["raw"],
                    stream["processed"],
                    stream["reconciled"],
                    stream["count_contract"],
                )
                for stream in telegram_checked["streams"]
            },
            {(2, 2, 0, "telegram_source_units_v1")},
        )

        action_base = (
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            "synthetic-current-run-follow-up",
            "--action-type",
            "message",
            "--source",
            "synthetic_e2e",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            *action_base,
            "--state",
            "authorized",
            "--at",
            "2026-08-19T13:00:00Z",
            "--authorization-note",
            "synthetic current-run authorization",
        )
        self.run_cli(
            *action_base,
            "--state",
            "attempted",
            "--at",
            "2026-08-19T13:01:00Z",
            "--evidence-note",
            "synthetic attempt",
        )
        self.run_cli(
            *action_base,
            "--state",
            "visibly_confirmed",
            "--at",
            "2026-08-19T13:02:00Z",
            "--evidence-note",
            "synthetic visible delivery confirmation",
            "--external-reference",
            "synthetic-visible-follow-up-1",
        )
        invalidated = self.status(run_id)
        self.assertEqual(
            invalidated["next_safe_work"][0]["action"],
            "reconcile_inbound_after_outbound",
        )
        self.complete_inbound(
            run_id, lease, observed_at="2026-08-19T14:00:00Z"
        )

        generations = self.workspace / ".jobctl" / "projections" / "generations"
        before_generations = {
            path.name for path in generations.iterdir() if path.is_dir()
        }
        finalized = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(finalized["render_performed"])
        after_generations = {
            path.name for path in generations.iterdir() if path.is_dir()
        }
        self.assertEqual(len(after_generations - before_generations), 1)

        final_status = self.status(run_id, verbose=True)
        self.assertEqual(final_status["status"], "completed")
        self.assertEqual(
            next(
                step["state"]
                for step in final_status["steps"]
                if step["step_key"] == "sqlite_reconciliation"
            ),
            "completed",
        )
        self.assertEqual(
            len(
                [
                    item
                    for item in final_status["work_items"]
                    if item["step_key"] == "hh_coverage"
                    and item["state"] == "completed"
                ]
            ),
            10,
        )
        self.assertEqual(
            len(
                [
                    item
                    for item in final_status["work_items"]
                    if item["step_key"] == "telegram_coverage"
                    and item["state"] == "completed"
                ]
            ),
            9,
        )
        self.assertEqual(final_status["external_action_scope"]["legacy_backlog"]["total"], 5)
        structural = json.loads(
            self.run_cli("doctor", "--strict", "--json").stdout
        )
        self.assertTrue(structural["ok"])
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                run_id,
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])

        repeated = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(repeated["already_finalized"])
        self.assertEqual(
            {path.name for path in generations.iterdir() if path.is_dir()},
            after_generations,
        )
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_stream_runs "
                    "WHERE run_id=? AND state='completed'",
                    (run_id,),
                ).fetchone()[0],
                11,
            )
            telegram_manifests = conn.execute(
                "SELECT payload_json FROM daily_run_manifests "
                "WHERE run_id=? AND manifest_kind='telegram_channel' "
                "AND record_type='completion'",
                (run_id,),
            ).fetchall()
            self.assertEqual(len(telegram_manifests), 9)
            for row in telegram_manifests:
                payload = json.loads(row[0])
                self.assertEqual(
                    (
                        payload["counts"]["raw"],
                        payload["counts"]["processed"],
                        payload["counts"]["reconciled"],
                    ),
                    (2, 2, 0),
                )
                self.assertEqual(
                    payload["captured_scope"]["count_contract"],
                    "telegram_source_units_v1",
                )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM external_actions "
                    "WHERE action_key LIKE 'synthetic-historical-authorization-%' "
                    "AND state='authorized'"
                ).fetchone()[0],
                5,
            )
            self.assertEqual(
                [
                    row[0]
                    for row in conn.execute(
                        "SELECT state FROM external_actions "
                        "WHERE action_key='synthetic-current-run-follow-up' "
                        "ORDER BY id"
                    ).fetchall()
                ],
                ["authorized", "attempted", "visibly_confirmed"],
            )

    def test_33_zero_evidence_ordinary_recovery_replans_and_closes_daily_run(
        self,
    ) -> None:
        run_id = "zero-evidence-ordinary"
        reason = "synthetic v1.0.0 to v1.0.2 recovery"
        self.seed_known([920001])
        lease, _ = self.begin(run_id)
        self.complete_inbound(run_id, lease)
        self.plan(run_id, lease)
        old_fingerprint = self.freeze_plan_on_v100(run_id)
        with self.connect() as conn:
            target = conn.execute(
                """
                SELECT p1_step_key, p1_item_key FROM hh_stream_runs
                WHERE run_id = ? AND source = 'hh' AND stream_key = 'stream_alpha'
                """,
                (run_id,),
            ).fetchone()
        self.run_cli(
            "block-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--code",
            "hh_dom_adapter_map_link_identity",
            "--reason",
            "synthetic audited map-link identity blocker",
            "--retryable",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--reason",
            "synthetic audited reopen after adapter review",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        with self.connect() as conn:
            blocker_manifest_hashes = [
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT payload_hash FROM daily_run_manifests
                    WHERE run_id = ? AND step_key = ? AND item_key = ?
                      AND record_type = 'block'
                    ORDER BY id
                    """,
                    (run_id, target["p1_step_key"], target["p1_item_key"]),
                ).fetchall()
            ]
            audit_transition_ids = [
                int(row[0])
                for row in conn.execute(
                    """
                    SELECT id FROM daily_run_transitions
                    WHERE run_id = ? AND entity_type = 'work_item'
                      AND entity_key = ?
                      AND event_type IN ('blocked','reopened','invalidated')
                    ORDER BY id
                    """,
                    (
                        run_id,
                        f"{target['p1_step_key']}/{target['p1_item_key']}",
                    ),
                ).fetchall()
            ]
            before_p1_manifests = conn.execute(
                "SELECT COUNT(*) FROM daily_run_manifests WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]
            before_p1_transitions = conn.execute(
                "SELECT COUNT(*) FROM daily_run_transitions WHERE run_id = ?",
                (run_id,),
            ).fetchone()[0]

        rejected_replan = self.run_cli(
            "plan-hh-acquisition",
            "--run-id",
            run_id,
            "--stream-key",
            "stream_alpha",
            "--source-kind",
            "ordinary_search",
            "--query-fingerprint",
            QUERY_A,
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(rejected_replan.returncode, 0)
        self.assertIn("явно инвалидируйте", rejected_replan.stderr)

        _, invalidated = self.invalidate_zero_evidence_plan(
            run_id, lease, reason=reason
        )
        self.assertIsNotNone(invalidated)
        self.assertFalse(invalidated["idempotent"])
        self.assertTrue(invalidated["replan_required"])
        self.assertEqual(invalidated["previous_adapter_version"], "hh-dom-v1.0.0")
        self.assertEqual(
            invalidated["previous_configuration_fingerprint"], old_fingerprint
        )
        self.assertTrue(invalidated["no_source_evidence_discarded"])
        self.assertEqual(
            invalidated["superseded_p1_audit"][
                "superseded_blocker_manifest_hashes"
            ],
            blocker_manifest_hashes,
        )
        self.assertEqual(
            invalidated["superseded_p1_audit"]["superseded_transition_ids"],
            audit_transition_ids,
        )
        audit_event_id = invalidated["audit_event"]["id"]

        _, repeated = self.invalidate_zero_evidence_plan(
            run_id, lease, reason=reason
        )
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(repeated["audit_event"]["id"], audit_event_id)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_stream_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_incremental_events "
                    "WHERE run_id = ? AND event_type = ?",
                    (run_id, hh.ZERO_EVIDENCE_INVALIDATION_EVENT),
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_manifests WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0],
                before_p1_manifests,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0],
                before_p1_transitions,
            )

        wrong_query = self.run_cli(
            "plan-hh-acquisition",
            "--run-id",
            run_id,
            "--stream-key",
            "stream_alpha",
            "--source-kind",
            "ordinary_search",
            "--query-fingerprint",
            QUERY_B,
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(wrong_query.returncode, 0)
        self.assertIn("прежние source_kind", wrong_query.stderr)
        wrong_source_kind = self.run_cli(
            "plan-hh-acquisition",
            "--run-id",
            run_id,
            "--stream-key",
            "stream_alpha",
            "--source-kind",
            "personal_recommendations",
            "--query-fingerprint",
            QUERY_A,
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(wrong_source_kind.returncode, 0)
        replanned = self.plan(run_id, lease)
        self.assertEqual(replanned["adapter_version"], "hh-dom-v1.0.2")
        self.assertEqual(
            replanned["recovery"]["invalidation_event_id"], audit_event_id
        )
        _, after_replan = self.invalidate_zero_evidence_plan(
            run_id, lease, reason=reason
        )
        self.assertTrue(after_replan["idempotent"])
        self.assertFalse(after_replan["replan_required"])

        self.record_page(
            run_id,
            lease,
            page_capture([card(920001)], has_next=False),
        )
        after_progress, _ = self.invalidate_zero_evidence_plan(
            run_id, lease, reason=reason, check=False
        )
        self.assertNotEqual(after_progress.returncode, 0)
        self.assertIn("hh_page_captures=1", after_progress.stderr)
        self.finalize_stream(run_id, lease)
        finalized = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(finalized["render_performed"])
        self.assertEqual(self.status(run_id)["status"], "completed")
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                run_id,
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])
        with self.connect() as conn:
            audit = conn.execute(
                """
                SELECT details_json, created_at FROM hh_incremental_events
                WHERE id = ? AND event_type = ?
                """,
                (audit_event_id, hh.ZERO_EVIDENCE_INVALIDATION_EVENT),
            ).fetchone()
            self.assertIsNotNone(audit)
            details = json.loads(audit["details_json"])
            self.assertEqual(details["run_id"], run_id)
            self.assertEqual(details["stream_key"], "stream_alpha")
            self.assertEqual(details["operator_reason"], reason)
            self.assertEqual(details["invalidated_at"], audit["created_at"])
            self.assertTrue(details["no_source_evidence_discarded"])
            self.assertFalse(details["source_evidence_discarded"])
            self.assertEqual(
                details["superseded_p1_audit"][
                    "superseded_blocker_manifest_hashes"
                ],
                blocker_manifest_hashes,
            )
            self.assertEqual(
                details["superseded_p1_audit"]["superseded_transition_ids"],
                audit_transition_ids,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_incremental_events "
                    "WHERE run_id = ? AND event_type = ?",
                    (run_id, hh.ZERO_EVIDENCE_REPLAN_EVENT),
                ).fetchone()[0],
                1,
            )

    def test_34_zero_evidence_personal_recommendations_recovery(self) -> None:
        run_id = "zero-evidence-personal"
        stream = "synthetic_personal"
        self.enable_personal(stream)
        self.seed_known([920101])
        lease, _ = self.begin(run_id)
        self.plan(
            run_id,
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        old_fingerprint = self.freeze_plan_on_v100(run_id, stream=stream)
        self.run_cli(
            "block-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            "personal_recommendations",
            "--code",
            "hh_dom_adapter_map_link_identity",
            "--reason",
            "synthetic personal-recommendations audit blocker",
            "--retryable",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            "personal_recommendations",
            "--reason",
            "synthetic personal audit reopen",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        _, invalidated = self.invalidate_zero_evidence_plan(
            run_id, lease, stream=stream
        )
        self.assertEqual(invalidated["source_kind"], "personal_recommendations")
        self.assertEqual(
            invalidated["previous_configuration_fingerprint"], old_fingerprint
        )
        self.assertEqual(
            len(
                invalidated["superseded_p1_audit"][
                    "superseded_blocker_manifest_hashes"
                ]
            ),
            1,
        )
        self.assertGreaterEqual(
            len(invalidated["superseded_p1_audit"]["superseded_transition_ids"]),
            2,
        )
        replanned = self.plan(
            run_id,
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        self.assertEqual(replanned["adapter_version"], hh.ADAPTER_VERSION)
        self.assertEqual(replanned["source_kind"], "personal_recommendations")
        _, recorded = self.record_page(
            run_id,
            lease,
            page_capture(
                [card(920101)],
                source_kind="personal_recommendations",
                has_next=False,
            ),
            stream=stream,
        )
        self.assertTrue(recorded["verified"])

    def test_35_zero_evidence_recovery_rejects_capture_items_and_detail_work(
        self,
    ) -> None:
        run_id = "recovery-rejects-source-evidence"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.record_page(
            run_id,
            lease,
            page_capture([card(920201)], has_next=False),
        )
        self.freeze_plan_on_v100(run_id)
        result, payload = self.invalidate_zero_evidence_plan(
            run_id, lease, check=False
        )
        self.assertIsNone(payload)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("hh_page_captures=1", result.stderr)
        self.assertIn("hh_page_items=1", result.stderr)
        self.assertIn("hh_detail_queue=1", result.stderr)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_stream_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_incremental_events "
                    "WHERE run_id = ? AND event_type = ?",
                    (run_id, hh.ZERO_EVIDENCE_INVALIDATION_EVENT),
                ).fetchone()[0],
                0,
            )

    def test_36_zero_evidence_recovery_rejects_capture_without_items(self) -> None:
        run_id = "recovery-rejects-empty-capture"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.record_page(run_id, lease, page_capture([], has_next=False))
        self.freeze_plan_on_v100(run_id)
        result, _ = self.invalidate_zero_evidence_plan(
            run_id, lease, check=False
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("hh_page_captures=1", result.stderr)
        self.assertNotIn("hh_page_items=", result.stderr)

    def test_37_zero_evidence_recovery_rejects_completed_stream(self) -> None:
        run_id = "recovery-rejects-completed"
        self.seed_known([920301])
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.record_page(
            run_id,
            lease,
            page_capture([card(920301)], has_next=False),
        )
        self.finalize_stream(run_id, lease)
        self.freeze_plan_on_v100(run_id)
        result, _ = self.invalidate_zero_evidence_plan(
            run_id, lease, check=False
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("state=completed", result.stderr)
        self.assertIn("completion_manifest_json", result.stderr)

    def test_38_zero_evidence_recovery_preserves_historical_checkpoint(self) -> None:
        run_id = "recovery-preserves-checkpoint"
        self.seed_checkpoint()
        with self.connect() as conn:
            before = dict(
                conn.execute(
                    "SELECT * FROM hh_stream_checkpoints "
                    "WHERE source = 'hh' AND stream_key = 'stream_alpha'"
                ).fetchone()
            )
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.freeze_plan_on_v100(run_id)
        _, invalidated = self.invalidate_zero_evidence_plan(run_id, lease)
        self.assertTrue(
            invalidated["historical_checkpoint_preserved"]["present"]
        )
        self.plan(run_id, lease)
        with self.connect() as conn:
            after = dict(
                conn.execute(
                    "SELECT * FROM hh_stream_checkpoints "
                    "WHERE source = 'hh' AND stream_key = 'stream_alpha'"
                ).fetchone()
            )
        self.assertEqual(after, before)

    def test_39_zero_evidence_recovery_cli_help_documents_exact_next_step(
        self,
    ) -> None:
        help_text = self.run_cli(
            "invalidate-hh-zero-evidence-plan", "--help"
        ).stdout
        self.assertIn("plan-hh-acquisition", help_text)
        self.assertIn("прежнего query fingerprint", help_text)
        self.assertIn("--defer-render --run-lease <token>", help_text)

    def test_40_zero_evidence_recovery_rejects_detail_work_without_page(
        self,
    ) -> None:
        run_id = "recovery-rejects-detail-only"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.freeze_plan_on_v100(run_id)
        timestamp = "2026-08-18T12:00:00+00:00"
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO hh_detail_queue (
                    run_id, source, stream_key, external_id, vacancy_id,
                    canonical_url, reason, first_page, last_page,
                    material_fingerprint, state, created_at, updated_at
                ) VALUES (?, 'hh', 'stream_alpha', 'hh:920401', NULL,
                          'https://example.test/vacancy/920401', 'new', 0, 0,
                          'synthetic-detail-only', 'pending', ?, ?)
                """,
                (run_id, timestamp, timestamp),
            )
        result, _ = self.invalidate_zero_evidence_plan(
            run_id, lease, check=False
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("hh_detail_queue=1", result.stderr)
        self.assertNotIn("hh_page_captures=", result.stderr)

    def test_41_zero_evidence_recovery_rejects_p1_source_manifest(self) -> None:
        run_id = "recovery-rejects-p1-source-manifest"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.freeze_plan_on_v100(run_id)
        with self.connect() as conn:
            target = conn.execute(
                """
                SELECT p1_step_key, p1_item_key FROM hh_stream_runs
                WHERE run_id = ? AND source = 'hh' AND stream_key = 'stream_alpha'
                """,
                (run_id,),
            ).fetchone()
        manifest = self.write_json(
            "p1-source-checkpoint.json",
            {
                "manifest_version": 1,
                "kind": "hh_stream",
                "run_id": run_id,
                "step_key": target["p1_step_key"],
                "item_key": target["p1_item_key"],
                "observed_at": "2026-08-18T12:00:00Z",
                "captured_scope": {
                    "stream_key": "stream_alpha",
                    "last_verified_page": 0,
                },
                "counts": {
                    "raw": 1,
                    "unique": 1,
                    "known": 0,
                    "new": 1,
                    "processed": 1,
                },
                "completion_boundary": {"last_verified_page": 0},
                "remote_boundary_verified": False,
                "blockers": [],
            },
        )
        self.run_cli(
            "checkpoint-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--manifest",
            str(manifest),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--reason",
            "synthetic source checkpoint reopen",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        result, _ = self.invalidate_zero_evidence_plan(
            run_id, lease, check=False
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("p1_source_manifests=1", result.stderr)
        self.assertIn("p1_source_progress_transitions=1", result.stderr)
        self.assertIn("p1.last_checkpoint_json", result.stderr)

    def test_42_zero_evidence_recovery_rejects_session_boundary_counter_and_event(
        self,
    ) -> None:
        run_id = "recovery-rejects-row-progress"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.freeze_plan_on_v100(run_id)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE hh_stream_runs
                SET session_id_state = 'not_exposed',
                    session_fingerprint = ?, boundary_candidate_page = 0,
                    raw_count = 1
                WHERE run_id = ? AND source = 'hh' AND stream_key = 'stream_alpha'
                """,
                (SESSION_A, run_id),
            )
            hh._append_event(
                conn,
                run_id=run_id,
                stream_key="stream_alpha",
                event_type="capture_blocked",
                severity="failure",
                details={
                    "code": "synthetic_source_blocker",
                    "reason": "synthetic source-bearing event",
                },
            )
        result, _ = self.invalidate_zero_evidence_plan(
            run_id, lease, check=False
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("source_progress_events=1", result.stderr)
        self.assertIn("boundary_candidate_page", result.stderr)
        self.assertIn("raw_count=1", result.stderr)
        self.assertIn("session_id_state", result.stderr)

    def test_43_v101_missing_mutation_observer_zero_evidence_replans_to_v102(
        self,
    ) -> None:
        run_id = "v101-mutation-observer-recovery"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.freeze_plan_on_version(run_id, "hh-dom-v1.0.1")
        with self.connect() as conn:
            target = conn.execute(
                """
                SELECT p1_step_key, p1_item_key FROM hh_stream_runs
                WHERE run_id = ? AND source = 'hh' AND stream_key = 'stream_alpha'
                """,
                (run_id,),
            ).fetchone()
        self.run_cli(
            "block-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--code",
            "hh_dom_runtime_missing_mutation_observer",
            "--reason",
            "Synthetic TypeError: MutationObserver is not a constructor",
            "--retryable",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--reason",
            "synthetic v1.0.1 runtime recovery",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        _, invalidated = self.invalidate_zero_evidence_plan(run_id, lease)
        assert invalidated is not None
        self.assertEqual(invalidated["previous_adapter_version"], "hh-dom-v1.0.1")
        self.assertEqual(invalidated["target_adapter_version"], "hh-dom-v1.0.2")
        self.assertTrue(invalidated["no_source_evidence_discarded"])
        replanned = self.plan(run_id, lease)
        self.assertEqual(replanned["adapter_version"], "hh-dom-v1.0.2")
        self.assertEqual(
            replanned["recovery"]["invalidation_event_id"],
            invalidated["audit_event"]["id"],
        )

    def test_44_v101_superseded_map_link_audit_allows_mutation_recovery(
        self,
    ) -> None:
        run_id = "v101-superseded-map-link-recovery"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.freeze_plan_on_version(run_id, "hh-dom-v1.0.1")
        with self.connect() as conn:
            target = conn.execute(
                """
                SELECT p1_step_key, p1_item_key FROM hh_stream_runs
                WHERE run_id = ? AND source = 'hh' AND stream_key = 'stream_alpha'
                """,
                (run_id,),
            ).fetchone()

        for code, blocker_reason, invalidation_reason in (
            (
                "hh_dom_adapter_map_link_identity",
                "Synthetic superseded map-link identity blocker",
                "synthetic map-link blocker superseded after adapter review",
            ),
            (
                "hh_dom_runtime_missing_mutation_observer",
                "Synthetic TypeError: MutationObserver is not a constructor",
                "synthetic v1.0.1 runtime recovery",
            ),
            (
                "hh_v102_recovery_rejects_superseded_map_link_audit",
                "Synthetic recovery bookkeeping blocker after the runtime failure",
                "synthetic recovery bookkeeping blocker superseded",
            ),
        ):
            self.run_cli(
                "block-daily-run-work",
                "--run-id",
                run_id,
                "--step-key",
                str(target["p1_step_key"]),
                "--item-key",
                str(target["p1_item_key"]),
                "--code",
                code,
                "--reason",
                blocker_reason,
                "--retryable",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            )
            self.run_cli(
                "invalidate-daily-run-work",
                "--run-id",
                run_id,
                "--step-key",
                str(target["p1_step_key"]),
                "--item-key",
                str(target["p1_item_key"]),
                "--reason",
                invalidation_reason,
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            )

        _, invalidated = self.invalidate_zero_evidence_plan(run_id, lease)
        assert invalidated is not None
        self.assertEqual(invalidated["previous_adapter_version"], "hh-dom-v1.0.1")
        self.assertEqual(invalidated["target_adapter_version"], "hh-dom-v1.0.2")
        self.assertEqual(
            len(
                invalidated["superseded_p1_audit"][
                    "superseded_blocker_manifest_hashes"
                ]
            ),
            3,
        )
        replanned = self.plan(run_id, lease)
        self.assertEqual(replanned["adapter_version"], "hh-dom-v1.0.2")

    def test_45_personal_scope_addition_preserves_zero_evidence_audit_history(
        self,
    ) -> None:
        run_id = "v101-personal-additive-scope-recovery"
        stream = "synthetic_personal"
        self.enable_personal(stream)
        lease, _ = self.begin(run_id)
        self.plan(
            run_id,
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        self.freeze_plan_on_version(run_id, "hh-dom-v1.0.1", stream=stream)

        for code, blocker_reason, invalidation_reason in (
            (
                "hh_dom_adapter_map_link_identity",
                "Synthetic superseded personal map-link blocker",
                "synthetic personal map-link blocker superseded",
            ),
            (
                "hh_dom_runtime_missing_mutation_observer",
                "Synthetic TypeError: MutationObserver is not a constructor",
                None,
            ),
        ):
            self.run_cli(
                "block-daily-run-work",
                "--run-id",
                run_id,
                "--step-key",
                "personal_recommendations",
                "--code",
                code,
                "--reason",
                blocker_reason,
                "--retryable",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            )
            if invalidation_reason is not None:
                self.run_cli(
                    "invalidate-daily-run-work",
                    "--run-id",
                    run_id,
                    "--step-key",
                    "personal_recommendations",
                    "--reason",
                    invalidation_reason,
                    "--defer-render",
                    "--run-lease",
                    lease,
                    "--json",
                )

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT scope_json FROM daily_run_steps
                WHERE run_id = ? AND step_key = 'personal_recommendations'
                """,
                (run_id,),
            ).fetchone()
            scope = json.loads(str(row["scope_json"]))
            scope["hh_acquisition"]["synthetic_additive_scope_option"] = True
            conn.execute(
                """
                UPDATE daily_run_steps SET scope_json = ?
                WHERE run_id = ? AND step_key = 'personal_recommendations'
                """,
                (json.dumps(scope, ensure_ascii=False, sort_keys=True), run_id),
            )

        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            "personal_recommendations",
            "--reason",
            "synthetic v1.0.1 runtime recovery after additive scope refresh",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        _, invalidated = self.invalidate_zero_evidence_plan(
            run_id, lease, stream=stream
        )
        assert invalidated is not None
        self.assertEqual(invalidated["previous_adapter_version"], "hh-dom-v1.0.1")
        self.assertEqual(invalidated["target_adapter_version"], "hh-dom-v1.0.2")
        self.assertTrue(invalidated["no_source_evidence_discarded"])
        replanned = self.plan(
            run_id,
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        self.assertEqual(replanned["adapter_version"], "hh-dom-v1.0.2")

    def test_44_v101_zero_evidence_rejects_unrelated_runtime_blocker(self) -> None:
        run_id = "v101-unrelated-blocker-rejected"
        lease, _ = self.begin(run_id)
        self.plan(run_id, lease)
        self.freeze_plan_on_version(run_id, "hh-dom-v1.0.1")
        with self.connect() as conn:
            target = conn.execute(
                """
                SELECT p1_step_key, p1_item_key FROM hh_stream_runs
                WHERE run_id = ? AND source = 'hh' AND stream_key = 'stream_alpha'
                """,
                (run_id,),
            ).fetchone()
        self.run_cli(
            "block-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--code",
            "synthetic_unrelated_blocker",
            "--reason",
            "Synthetic unrelated source access failure",
            "--retryable",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            str(target["p1_step_key"]),
            "--item-key",
            str(target["p1_item_key"]),
            "--reason",
            "synthetic unrelated blocker review",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        result, _ = self.invalidate_zero_evidence_plan(run_id, lease, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "v1.0.1_without_exclusive_missing_mutation_observer_blocker",
            result.stderr,
        )

    def test_45_same_origin_lead_gen_redirect_resolves_detail_without_fake_fields(self) -> None:
        run_id = "lead-gen-detail-unavailable"
        vacancy_id = 135672541
        lease, _ = self.begin(run_id)
        self.complete_inbound(run_id, lease)
        self.plan(run_id, lease)
        _, page = self.record_page(
            run_id,
            lease,
            page_capture([card(vacancy_id)], has_next=False),
        )
        self.assertEqual(page["next_safe_action"]["action"], "fetch_details")
        path = self.write_json(
            f"{run_id}-detail-{vacancy_id}-unavailable.json",
            unavailable_detail_capture(vacancy_id),
        )
        result = json.loads(
            self.run_cli(
                "record-hh-detail",
                "--run-id",
                run_id,
                "--stream-key",
                "stream_alpha",
                "--capture",
                str(path),
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertEqual(result["availability"]["state"], "unavailable")
        self.assertEqual(result["next_safe_action"]["action"], "finalize_stream")
        mismatched = unavailable_detail_capture(vacancy_id)
        mismatched["availability"]["observed_url"] = (
            "https://spb.hh.ru/article/32027?utm_redirect_vacancy_id=999999999"
        )
        with self.assertRaisesRegex(ValueError, "same-origin HH lead-gen redirect"):
            hh.validate_detail_capture(
                mismatched, expected_external_id=f"hh:{vacancy_id}"
            )
        vrsurvey = unavailable_detail_capture(vacancy_id)
        vrsurvey["availability"]["observed_url"] = (
            "https://spb.hh.ru/vrsurvey/synthetic_role"
            f"?utm_redirect_vacancy_id={vacancy_id}"
        )
        normalized_vrsurvey = hh.validate_detail_capture(
            vrsurvey, expected_external_id=f"hh:{vacancy_id}"
        )
        self.assertEqual(normalized_vrsurvey["availability"]["state"], "unavailable")
        with self.connect() as conn:
            queue = conn.execute(
                "SELECT state, detail_payload_json FROM hh_detail_queue "
                "WHERE run_id = ? AND external_id = ?",
                (run_id, f"hh:{vacancy_id}"),
            ).fetchone()
            self.assertEqual(queue["state"], "captured")
            payload = json.loads(queue["detail_payload_json"])
            self.assertNotIn("fields", payload)
            snapshot = conn.execute(
                "SELECT evidence_level, detail_material_fingerprint "
                "FROM hh_vacancy_snapshots WHERE external_id = ?",
                (f"hh:{vacancy_id}",),
            ).fetchone()
            self.assertEqual(tuple(snapshot), ("list", None))
        self.finalize_stream(run_id, lease)

    def test_46_repeated_visible_502_tail_can_recover_without_mixing_sessions(self) -> None:
        run_id = "personal-transient-tail-recovery"
        stream = "synthetic_personal"
        self.enable_personal(stream)
        self.set_acquisition(
            personal_initial_depth_pages=19,
            personal_max_pages=19,
            personal_max_is_completion_boundary=True,
            transient_error_tail_enabled=True,
        )
        vacancy_ids = list(range(810000, 810019))
        self.seed_known(vacancy_ids)
        lease, _ = self.begin(run_id)
        self.complete_inbound(run_id, lease)
        self.plan(
            run_id,
            lease,
            stream=stream,
            source_kind="personal_recommendations",
        )
        for page_index, vacancy_id in enumerate(vacancy_ids[:18]):
            self.record_page(
                run_id,
                lease,
                page_capture(
                    [card(vacancy_id)],
                    page_index=page_index,
                    source_kind="personal_recommendations",
                    captured_at=f"2026-08-18T12:{page_index:02d}:00Z",
                    has_next=True,
                    session_state="exposed",
                    session_value=SESSION_A,
                ),
                stream=stream,
            )
        _, changed = self.record_page(
            run_id,
            lease,
            page_capture(
                [card(vacancy_ids[18])],
                page_index=18,
                source_kind="personal_recommendations",
                captured_at="2026-08-18T13:00:00Z",
                has_next=False,
                session_state="exposed",
                session_value=SESSION_B,
            ),
            stream=stream,
        )
        self.assertEqual(changed["next_safe_action"]["code"], "session_identity_changed")
        for attempt in range(5):
            self.run_cli(
                "record-hh-transient-error",
                "--run-id",
                run_id,
                "--stream-key",
                stream,
                "--page-index",
                "18",
                "--error-class",
                "hh_http_502",
                "--visible-status-code",
                "502",
                "--visible-message",
                "Page temporarily unavailable",
                "--observed-at",
                f"2026-08-18T13:0{attempt}:00Z",
                "--remote-evidence-reference",
                f"synthetic-visible-502-{attempt}",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            )
        resolved = json.loads(
            self.run_cli(
                "resolve-hh-recovery",
                "--run-id",
                run_id,
                "--stream-key",
                stream,
                "--strategy",
                "accepted_unavailable_tail",
                "--reason",
                "five independently observed synthetic visible 502 pages",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertEqual(resolved["resolution"]["strategy"], "accepted_unavailable_tail")
        self.assertEqual(resolved["resolution"]["retry_count"], 5)
        finalized = json.loads(
            self.finalize_stream(run_id, lease, stream=stream, personal=True).stdout
        )
        self.assertTrue(finalized["completed"])
        with self.connect() as conn:
            capture = conn.execute(
                """
                SELECT verified FROM hh_page_captures
                WHERE run_id = ? AND stream_key = ? AND page_index = 18
                """,
                (run_id, stream),
            ).fetchone()
            self.assertEqual(int(capture["verified"]), 0)
            audit_count = conn.execute(
                """
                SELECT COUNT(*) FROM hh_capture_audit_decisions
                WHERE run_id = ? AND stream_key = ? AND decision = 'audit_only'
                """,
                (run_id, stream),
            ).fetchone()[0]
            self.assertEqual(audit_count, 1)
            manifest = json.loads(
                conn.execute(
                    """
                    SELECT completion_manifest_json FROM hh_stream_runs
                    WHERE run_id = ? AND stream_key = ?
                    """,
                    (run_id, stream),
                ).fetchone()[0]
            )
            self.assertEqual(manifest["stop_reason"], "transient_error_tail_exception")
            self.assertEqual(
                manifest["degraded_completion"]["missing_tail_pages"], [18]
            )

    def test_47_four_transient_attempts_remain_blocked(self) -> None:
        run_id = "transient-four-attempts"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        latest: dict[str, object] = {}
        for attempt in range(4):
            latest = json.loads(
                self.record_transient_attempt(
                    run_id,
                    lease,
                    stream=stream,
                    page_index=2,
                    attempt=attempt,
                ).stdout
            )
        self.assertEqual(latest["retry_count"], 4)
        self.assertFalse(latest["resolution_available"])
        failed = self.run_cli(
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
            "--reason",
            "only four attempts",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("Недостаточно", failed.stderr)

    def test_48_fifth_attempt_makes_exact_resolution_available(self) -> None:
        run_id = "transient-fifth-attempt"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        latest: dict[str, object] = {}
        for attempt in range(5):
            latest = json.loads(
                self.record_transient_attempt(
                    run_id,
                    lease,
                    stream=stream,
                    page_index=2,
                    attempt=attempt,
                ).stdout
            )
        self.assertTrue(latest["resolution_available"])
        inspected = json.loads(
            self.run_cli(
                "inspect-hh-checkpoint",
                "--run-id",
                run_id,
                "--stream-key",
                stream,
                "--json",
            ).stdout
        )
        self.assertEqual(len(inspected["transient_error_attempts"]), 5)

    def test_49_middle_page_transient_error_is_rejected(self) -> None:
        run_id = "transient-middle-page"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        failed = self.record_transient_attempt(
            run_id,
            lease,
            stream=stream,
            page_index=1,
            attempt=0,
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("ровно к следующей странице 2", failed.stderr)

    def test_50_missing_earlier_page_blocks_tail_resolution(self) -> None:
        run_id = "transient-missing-earlier"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE hh_page_captures SET verified = 0
                WHERE run_id = ? AND stream_key = ? AND page_index = 0
                """,
                (run_id, stream),
            )
        for attempt in range(5):
            self.record_transient_attempt(
                run_id,
                lease,
                stream=stream,
                page_index=2,
                attempt=attempt,
            )
        failed = self.run_cli(
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
            "--reason",
            "synthetic missing earlier page",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("Проверенные страницы должны образовывать непрерывный диапазон", failed.stderr)

    def test_51_login_captcha_access_denied_and_malformed_errors_are_rejected(self) -> None:
        run_id = "transient-unsupported-errors"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        for attempt, error_class in enumerate(
            ("login", "captcha", "access_denied", "malformed_content")
        ):
            failed = self.record_transient_attempt(
                run_id,
                lease,
                stream=stream,
                page_index=2,
                attempt=attempt,
                error_class=error_class,
                check=False,
            )
            self.assertNotEqual(failed.returncode, 0)
        wrong_status = self.record_transient_attempt(
            run_id,
            lease,
            stream=stream,
            page_index=2,
            attempt=9,
            status_code=503,
            check=False,
        )
        self.assertNotEqual(wrong_status.returncode, 0)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM hh_transient_error_attempts WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0],
                0,
            )

    def test_52_mismatched_query_configuration_source_and_stream_are_rejected(self) -> None:
        run_id = "transient-identity-mismatch"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        missing_stream = self.record_transient_attempt(
            run_id,
            lease,
            stream="missing_stream",
            page_index=2,
            attempt=0,
            check=False,
        )
        self.assertNotEqual(missing_stream.returncode, 0)
        self.plan(run_id, lease, stream="stream_alpha", source_kind="ordinary_search")
        wrong_source = self.record_transient_attempt(
            run_id,
            lease,
            stream="stream_alpha",
            page_index=0,
            attempt=1,
            check=False,
        )
        self.assertNotEqual(wrong_source.returncode, 0)
        with self.connect() as conn:
            for attempt in range(5):
                identity = f"tampered-{attempt}"
                conn.execute(
                    """
                    INSERT INTO hh_transient_error_attempts (
                        run_id, source, stream_key, source_kind, page_index,
                        query_fingerprint, configuration_fingerprint, error_class,
                        visible_status_code, visible_message_hash, observed_at,
                        remote_evidence_reference, attempt_hash, created_at
                    ) VALUES (?, 'hh', ?, 'personal_recommendations', 2, ?, ?,
                              'hh_http_502', 502, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        stream,
                        "0" * 64,
                        "1" * 64,
                        "2" * 64,
                        f"2026-08-18T15:0{attempt}:00Z",
                        identity,
                        hh.sha256_text(identity),
                        "2026-08-18T15:10:00+00:00",
                    ),
                )
        failed = self.run_cli(
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
            "--reason",
            "tampered attempt identities",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("Недостаточно", failed.stderr)

    def test_53_unresolved_detail_queue_blocks_tail_resolution(self) -> None:
        run_id = "transient-detail-blocker"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO hh_detail_queue (
                    run_id, source, stream_key, external_id, vacancy_id,
                    canonical_url, reason, first_page, last_page,
                    material_fingerprint, state, created_at, updated_at
                ) VALUES (?, 'hh', ?, 'hh:999999', NULL,
                          'https://example.test/vacancy/999999', 'new', 0, 0,
                          ?, 'pending', ?, ?)
                """,
                (run_id, stream, "a" * 64, hh.now_iso(), hh.now_iso()),
            )
        for attempt in range(5):
            self.record_transient_attempt(
                run_id,
                lease,
                stream=stream,
                page_index=2,
                attempt=attempt,
            )
        failed = self.run_cli(
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
            "--reason",
            "pending detail must block",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("detail", failed.stderr)

    def test_54_full_session_rollover_from_page_zero_succeeds_without_mixing(self) -> None:
        run_id = "transient-full-rollover"
        lease, stream, vacancy_ids = self.prepare_transient_tail(run_id)
        for page_index, vacancy_id in enumerate(vacancy_ids):
            self.record_rollover_page(
                run_id,
                lease,
                page_capture(
                    [card(vacancy_id)],
                    page_index=page_index,
                    source_kind="personal_recommendations",
                    captured_at=f"2026-08-18T16:{page_index:02d}:00Z",
                    has_next=page_index < len(vacancy_ids) - 1,
                    session_state="exposed",
                    session_value=SESSION_B,
                ),
                stream=stream,
            )
        resolved = json.loads(
            self.run_cli(
                "resolve-hh-recovery",
                "--run-id",
                run_id,
                "--stream-key",
                stream,
                "--strategy",
                "full_session_rollover",
                "--reason",
                "complete synthetic Session B recapture from page zero",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertEqual(resolved["resolution"]["strategy"], "full_session_rollover")
        self.finalize_stream(run_id, lease, stream=stream, personal=True)
        with self.connect() as conn:
            verified = conn.execute(
                """
                SELECT page_index, session_fingerprint FROM hh_page_captures
                WHERE run_id = ? AND stream_key = ? AND verified = 1
                ORDER BY page_index
                """,
                (run_id, stream),
            ).fetchall()
            self.assertEqual([int(row["page_index"]) for row in verified], [0, 1, 2])
            self.assertEqual(len({str(row["session_fingerprint"]) for row in verified}), 1)

    def test_55_partial_session_rollover_fails_closed(self) -> None:
        run_id = "transient-partial-rollover"
        lease, stream, vacancy_ids = self.prepare_transient_tail(run_id)
        for page_index, vacancy_id in enumerate(vacancy_ids[:2]):
            self.record_rollover_page(
                run_id,
                lease,
                page_capture(
                    [card(vacancy_id)],
                    page_index=page_index,
                    source_kind="personal_recommendations",
                    captured_at=f"2026-08-18T17:{page_index:02d}:00Z",
                    has_next=True,
                    session_state="exposed",
                    session_value=SESSION_B,
                ),
                stream=stream,
            )
        failed = self.run_cli(
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "full_session_rollover",
            "--reason",
            "partial recapture",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("не достигла", failed.stderr)

    def test_56_repeated_exact_resolution_is_idempotent(self) -> None:
        run_id = "transient-idempotent-resolution"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        for attempt in range(5):
            self.record_transient_attempt(
                run_id, lease, stream=stream, page_index=2, attempt=attempt
            )
        command = (
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
            "--reason",
            "exact idempotent decision",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        first = json.loads(self.run_cli(*command).stdout)
        second = json.loads(self.run_cli(*command).stdout)
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(
            first["resolution"]["resolution_hash"],
            second["resolution"]["resolution_hash"],
        )

    def test_57_conflicting_resolution_is_rejected(self) -> None:
        run_id = "transient-conflicting-resolution"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        for attempt in range(5):
            self.record_transient_attempt(
                run_id, lease, stream=stream, page_index=2, attempt=attempt
            )
        base = [
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
        ]
        self.run_cli(
            *base,
            "--reason",
            "first exact decision",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        failed = self.run_cli(
            *base,
            "--reason",
            "materially different decision",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("конфликтующее решение запрещено", failed.stderr)

    def test_58_resolution_survives_process_boundary_before_finalize(self) -> None:
        run_id = "transient-interrupted-finalize"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        for attempt in range(5):
            self.record_transient_attempt(
                run_id, lease, stream=stream, page_index=2, attempt=attempt
            )
        self.run_cli(
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
            "--reason",
            "persist across process boundary",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        inspected = json.loads(
            self.run_cli(
                "inspect-hh-checkpoint",
                "--run-id",
                run_id,
                "--stream-key",
                stream,
                "--json",
            ).stdout
        )
        self.assertEqual(
            inspected["recovery_resolutions"][0]["strategy"],
            "accepted_unavailable_tail",
        )
        completed = json.loads(
            self.finalize_stream(run_id, lease, stream=stream, personal=True).stdout
        )
        self.assertTrue(completed["completed"])

    def test_59_recovery_warning_propagates_to_status_doctor_manifest_and_dashboard(self) -> None:
        run_id = "transient-warning-propagation"
        lease, stream, _ = self.prepare_transient_tail(run_id)
        for attempt in range(5):
            self.record_transient_attempt(
                run_id, lease, stream=stream, page_index=2, attempt=attempt
            )
        self.run_cli(
            "resolve-hh-recovery",
            "--run-id",
            run_id,
            "--stream-key",
            stream,
            "--strategy",
            "accepted_unavailable_tail",
            "--reason",
            "warning propagation fixture",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.finalize_stream(run_id, lease, stream=stream, personal=True)
        status = json.loads(
            self.run_cli("daily-run-status", "--run-id", run_id, "--json").stdout
        )
        self.assertEqual(status["hh_recovery_warnings"][0]["retry_count"], 5)
        doctor = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                run_id,
                "--as-of",
                "2026-08-18",
                "--json",
            ).stdout
        )
        recovery_check = next(
            item for item in doctor["checks"] if item["name"] == "hh_transient_tail_recovery"
        )
        self.assertEqual(recovery_check["status"], "warn")
        with self.connect() as conn:
            snapshot = jobctl.build_snapshot(conn, self.database)
            compact = jobctl.compact_dashboard_snapshot(snapshot)
            self.assertEqual(compact["hh_recovery_warnings"][0]["retry_count"], 5)
            manifest = json.loads(
                conn.execute(
                    "SELECT completion_manifest_json FROM hh_stream_runs WHERE run_id = ? AND stream_key = ?",
                    (run_id, stream),
                ).fetchone()[0]
            )
            self.assertEqual(manifest["degraded_completion"]["error_class"], "hh_http_502")

    def test_60_v10_migration_and_replay_preserve_existing_evidence(self) -> None:
        with self.connect() as conn:
            before = conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0]
            conn.executescript(
                """
                DROP TABLE hh_session_rollover_pages;
                DROP TABLE hh_stream_recovery_resolutions;
                DROP TABLE hh_transient_error_attempts;
                DROP TABLE hh_capture_audit_decisions;
                PRAGMA user_version = 10;
                """
            )
        migrated = json.loads(
            self.run_cli("migrate-schema", "--defer-render", "--json").stdout
        )
        self.assertEqual((migrated["from_version"], migrated["to_version"]), (10, 11))
        with self.connect() as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 11)
            self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], before)
            for table in (
                "hh_capture_audit_decisions",
                "hh_transient_error_attempts",
                "hh_stream_recovery_resolutions",
                "hh_session_rollover_pages",
            ):
                self.assertIsNotNone(
                    conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                        (table,),
                    ).fetchone()
                )
        replay = json.loads(
            self.run_cli("migrate-schema", "--defer-render", "--json").stdout
        )
        self.assertTrue(replay["already_current"])

    def test_61_transient_tail_recovery_is_disabled_by_default(self) -> None:
        run_id = "transient-default-disabled"
        stream = "synthetic_personal_default_disabled"
        self.enable_personal(stream)
        self.set_acquisition(
            personal_initial_depth_pages=2,
            personal_max_pages=2,
            personal_max_is_completion_boundary=True,
        )
        self.seed_known([830000, 830001])
        lease, _ = self.begin(run_id)
        self.complete_inbound(run_id, lease)
        self.plan(run_id, lease, stream=stream, source_kind="personal_recommendations")
        self.record_page(
            run_id,
            lease,
            page_capture(
                [card(830000)],
                page_index=0,
                source_kind="personal_recommendations",
                captured_at="2026-08-18T18:00:00Z",
                has_next=True,
                session_state="exposed",
                session_value=SESSION_A,
            ),
            stream=stream,
        )
        failed = self.record_transient_attempt(
            run_id,
            lease,
            stream=stream,
            page_index=1,
            attempt=0,
            check=False,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("выключено конфигурацией", failed.stderr)
