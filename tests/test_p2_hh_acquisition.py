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
    external_ids = sorted({f"hh:{int(str(item['vacancy_id']))}" for item in cards})
    id_hash = hh.payload_hash(external_ids)
    sample_hashes = [id_hash, id_hash, id_hash]
    if not stable:
        sample_hashes[-1] = hh.payload_hash([*external_ids, "hh:999999999"])
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
            "samples": [
                {
                    "canonical_id_set_hash": value,
                    "scroll_height": 1000,
                    "loader_active": blocker == "loading_timeout",
                    "mutation_count": 0,
                }
                for value in sample_hashes
            ],
            "bottom_scroll_attempted": True,
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

    def complete_inbound(self, run_id: str, lease: str) -> None:
        path = self.workspace / "tmp" / f"{run_id}-inbound.json"
        path.write_text(
            json.dumps(
                {
                    "manifest_version": 1,
                    "kind": "inbound_reconciliation",
                    "run_id": run_id,
                    "step_key": "inbound_reconciliation",
                    "item_key": "",
                    "observed_at": "2026-08-18T11:00:00Z",
                    "captured_scope": {"configured_sources": []},
                    "counts": {"raw": 0, "processed": 0, "reconciled": 0, "blocked": 0},
                    "completion_boundary": "all configured inbound sources checked",
                    "remote_boundary_verified": True,
                    "blockers": [],
                }
            ),
            encoding="utf-8",
        )
        self.run_cli(
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
            source_kind="ordinary_search",
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
                ) VALUES ('hh', ?, 'ordinary_search', 1, 'synthetic-previous', ?,
                          'shadow', ?, ?, ?, '2026-08-17', '2026-08-01', ?, ?, ?,
                          ?, ?, ?, ?, '{}', '{}', 'not_exposed', ?, ?, ?, ?, ?, ?)
                """,
                (
                    stream,
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
        self.assertEqual((migrated["from_version"], migrated["to_version"]), (9, 10))
        self.assertTrue(migrated["backup"])
        backup = self.workspace / migrated["backup"]
        self.assertTrue(backup.is_file())
        with sqlite3.connect(backup) as old:
            self.assertEqual(old.execute("PRAGMA user_version").fetchone()[0], 9)
        with self.connect() as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 10)
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

    def test_14_changing_ids_across_drift_recaptures_blocks(self) -> None:
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
        self.assertEqual(second["stream_state"], "blocked")
        self.assertEqual(second["next_safe_action"]["code"], "count_drift_capture_conflict")

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
        self.assertIn("url.origin !== global.location.origin", adapter)
        self.assertNotIn(".click(", adapter)
        self.assertNotIn("fetch(", adapter)
        self.assertIn('href="/vacancy/910001"', list_fixture)
        self.assertIn('data-qa="vacancy-description"', detail_fixture)
        self.assertNotIn("hh.ru", list_fixture + detail_fixture)
