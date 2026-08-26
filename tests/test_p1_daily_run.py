from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JOBCTL = ROOT / "scripts" / "jobctl.py"
sys.path.insert(0, str(ROOT / "scripts"))

import daily_run_orchestration as orchestration  # noqa: E402


class DurableDailyRunP1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-p1-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.run_cli("init", "--json")
        self.database = self.workspace / "data" / "job_search.sqlite"
        self.config = self.workspace / "config" / "settings.toml"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def run_cli(
        self,
        *args: str,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(JOBCTL), *args],
            cwd=ROOT,
            env=env or self.env,
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

    def begin(self, run_id: str = "synthetic-p1") -> dict[str, object]:
        return json.loads(
            self.run_cli(
                "begin-daily-run",
                "--run-id",
                run_id,
                "--run-date",
                "2026-08-18",
                "--timezone",
                "UTC",
                "--owner",
                "synthetic-test",
                "--json",
            ).stdout
        )

    def status(self, run_id: str = "synthetic-p1", *, verbose: bool = False) -> dict[str, object]:
        args = ["daily-run-status", "--run-id", run_id, "--json"]
        if verbose:
            args.append("--verbose")
        return json.loads(self.run_cli(*args).stdout)

    def complete_inbound(
        self,
        run_id: str,
        lease: str,
        *,
        observed_at: str = "2026-08-18T12:00:00Z",
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        path = self.workspace / f"{run_id}-inbound.json"
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
                    "counts": {
                        "raw": 0,
                        "processed": 0,
                        "reconciled": 0,
                        "blocked": 0,
                    },
                    "completion_boundary": "all configured inbound sources checked",
                    "remote_boundary_verified": True,
                    "blockers": [],
                    "metrics": {"elapsed_seconds": 1.25, "tool_count": 1},
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
            check=check,
        )

    def required_streams(self) -> list[str]:
        with self.config.open("rb") as handle:
            return list(tomllib.load(handle)["search"]["required_streams"])

    def write_hh_manifest(
        self,
        run_id: str,
        *,
        partial_stream: str = "",
        blocked_stream: str = "",
    ) -> Path:
        streams = self.required_streams()
        plan_input = self.workspace / f"{run_id}-coverage-input.json"
        manifest_path = self.workspace / f"{run_id}-coverage.json"
        plan_input.write_text(
            json.dumps(
                {
                    "run_date": "2026-08-18",
                    "source": "hh",
                    "required_streams": streams,
                    "streams": [
                        {"key": key, "query": {"any_terms": [f"synthetic {index}"]}}
                        for index, key in enumerate(streams, start=1)
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.run_cli(
            "build-coverage-plan",
            str(plan_input),
            "--output",
            str(manifest_path),
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for stream in manifest["streams"]:
            if stream["key"] == blocked_stream:
                stream.update({"status": "blocked", "error": "synthetic source blocker"})
            elif stream["key"] == partial_stream:
                stream.update(
                    {
                        "status": "completed",
                        "found": 101,
                        "pages": [{"page": 0, "extracted": 100}],
                        "unique": 100,
                        "known": 100,
                        "new": 0,
                    }
                )
            else:
                stream.update(
                    {
                        "status": "completed",
                        "found": 0,
                        "pages": [{"page": 0, "extracted": 0}],
                        "unique": 0,
                        "known": 0,
                        "new": 0,
                    }
                )
        manifest["totals"] = {"unique": 0, "known": 0, "new": 0}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return manifest_path

    def check_hh(
        self,
        run_id: str,
        lease: str,
        *,
        partial_stream: str = "",
        blocked_stream: str = "",
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        manifest = self.write_hh_manifest(
            run_id,
            partial_stream=partial_stream,
            blocked_stream=blocked_stream,
        )
        return self.run_cli(
            "check-coverage",
            str(manifest),
            "--defer-render",
            "--run-lease",
            lease,
            check=check,
        )

    def close_required_gates(self, run_id: str, lease: str) -> None:
        self.complete_inbound(run_id, lease)
        self.check_hh(run_id, lease)

    def ingest_vacancy(self, external_id: str) -> int:
        path = self.workspace / f"{external_id}.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "date": "2026-08-18",
                        "channel": "company_site",
                        "source": "synthetic_p1",
                        "source_stream": "synthetic_p1",
                        "external_id": f"company_site:{external_id}",
                        "title": "Synthetic Product Leader",
                        "company": "Synthetic Employer",
                        "url": f"https://example.test/{external_id}",
                        "status": "NEEDS_REVIEW",
                        "stage": "seen",
                        "score": 80,
                    }
                ]
            ),
            encoding="utf-8",
        )
        self.run_cli("ingest-json", str(path), "--json")
        with sqlite3.connect(self.database) as conn:
            return int(
                conn.execute(
                    "SELECT id FROM vacancies WHERE external_id = ?",
                    (f"company_site:{external_id}",),
                ).fetchone()[0]
            )

    def seed_due_followup(self, external_id: str) -> tuple[int, int]:
        vacancy_id = self.ingest_vacancy(external_id)
        timestamp = "2026-08-01T09:00:00Z"
        with sqlite3.connect(self.database) as conn:
            lifecycle = conn.execute(
                """
                INSERT INTO lifecycle_events (
                    vacancy_id, event_type, event_at, evidence_at, evidence_note,
                    evidence_source, origin, history_complete,
                    authorization_status, dedupe_key, created_at
                ) VALUES (?, 'application_confirmed', ?, ?,
                          'synthetic visible confirmation', 'synthetic_test',
                          'synthetic_test', 1, 'explicit', ?, ?)
                """,
                (
                    vacancy_id,
                    timestamp,
                    timestamp,
                    f"due-cancellation-lifecycle-{external_id}",
                    timestamp,
                ),
            )
            application = conn.execute(
                """
                INSERT INTO applications (
                    vacancy_id, applied_date, status, stage, follow_up_date,
                    origin_file, line_no, lifecycle_event_id
                ) VALUES (?, '2026-08-01', 'APPLIED_VISIBLY_CONFIRMED',
                          'follow_up', '2026-08-18', 'synthetic', 1, ?)
                """,
                (vacancy_id, int(lifecycle.lastrowid)),
            )
            conn.execute(
                """
                UPDATE vacancies
                SET latest_status = 'WAITING_EMPLOYER', latest_stage = 'follow_up',
                    follow_up_date = '2026-08-18'
                WHERE id = ?
                """,
                (vacancy_id,),
            )
            conn.commit()
        return vacancy_id, int(application.lastrowid)

    def seed_historical_interaction(
        self,
        vacancy_id: int,
        *,
        event_type: str = "human_reply",
        is_human: int = 1,
        direction: str = "inbound",
        channel: str = "email",
        suffix: str = "original",
        event_at: str = "2026-08-10T10:00:00+00:00",
    ) -> int:
        with sqlite3.connect(self.database) as conn:
            cursor = conn.execute(
                """
                INSERT INTO employer_interactions (
                    vacancy_id, event_at, direction, event_type, channel,
                    actor_type, is_human, evidence_note, evidence_url,
                    external_reference, dedupe_key, created_at, external_action_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    vacancy_id,
                    event_at,
                    direction,
                    event_type,
                    channel,
                    "recruiter" if is_human else "system",
                    is_human,
                    f"synthetic visible {suffix} evidence",
                    f"https://example.test/evidence/{vacancy_id}/{suffix}",
                    f"synthetic-thread:{vacancy_id}:{suffix}",
                    f"synthetic-interaction-{vacancy_id}-{suffix}",
                    event_at,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def clear_due_dates(self, vacancy_id: int, application_id: int) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE vacancies SET follow_up_date = '' WHERE id = ?", (vacancy_id,)
            )
            conn.execute(
                "UPDATE applications SET follow_up_date = '' WHERE id = ?",
                (application_id,),
            )
            conn.commit()

    def reverified_manifest(
        self,
        run_id: str,
        item_key: str,
        interaction_id: int,
        *,
        observed_at: str,
        channel: str = "email",
        conversation_target: str = "synthetic-thread-target",
        remote_reference: str = "synthetic-remote-proof",
    ) -> tuple[dict[str, object], Path]:
        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row
            item = conn.execute(
                """
                SELECT scope_json FROM daily_run_work_items
                WHERE run_id = ? AND step_key = 'due_followups' AND item_key = ?
                """,
                (run_id, item_key),
            ).fetchone()
            interaction = conn.execute(
                "SELECT * FROM employer_interactions WHERE id = ?",
                (interaction_id,),
            ).fetchone()
        scope = json.loads(str(item["scope_json"]))
        snapshot = orchestration._interaction_evidence_snapshot(interaction)
        scope_fingerprint = orchestration.payload_hash(
            {"run_id": run_id, "item_key": item_key, "frozen_scope": scope}
        )
        boundary = {
            "observed_at": observed_at,
            "channel": channel,
            "conversation_target": conversation_target,
            "remote_evidence_reference": remote_reference,
            "latest_message_interaction_id": interaction_id,
        }
        manifest: dict[str, object] = {
            "contract": "reverified_historical_inbound_v1",
            "run_id": run_id,
            "item_key": item_key,
            "original_interaction_id": interaction_id,
            "original_dedupe_key": snapshot["dedupe_key"],
            "original_event_at": snapshot["event_at"],
            "original_evidence_hash": orchestration.payload_hash(snapshot),
            "vacancy_id": int(scope["vacancy_id"]),
            "application_id": int(scope["application_id"]),
            "follow_up_date": str(scope["follow_up_date"]),
            "due_reason": str(scope.get("reason") or "scheduled_follow_up_date_due"),
            "frozen_scope_hash": orchestration.payload_hash(scope),
            "scope_fingerprint": scope_fingerprint,
            "observed_at": observed_at,
            "channel": channel,
            "conversation_target": conversation_target,
            "remote_evidence_reference": remote_reference,
            "remote_boundary_verified": True,
            "latest_message_matches_interaction": True,
            "no_new_outbound_after_inbound": True,
            "original_interaction_timestamp_preserved": True,
            "completion_boundary": boundary,
        }
        path = self.workspace / f"{run_id}-{item_key.replace(':', '-')}-reverified.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        return manifest, path

    def resolve_reverified_cli(
        self,
        run_id: str,
        lease: str,
        item_key: str,
        interaction_id: int,
        manifest_path: Path,
        *,
        observed_at: str,
        channel: str = "email",
        conversation_target: str = "synthetic-thread-target",
        remote_reference: str = "synthetic-remote-proof",
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return self.run_cli(
            "resolve-due-followup-from-reverified-inbound",
            "--run-id",
            run_id,
            "--item-key",
            item_key,
            "--interaction-id",
            str(interaction_id),
            "--observed-at",
            observed_at,
            "--channel",
            channel,
            "--conversation-target",
            conversation_target,
            "--remote-evidence-reference",
            remote_reference,
            "--manifest",
            str(manifest_path),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
            check=check,
        )

    def record_attempted_action(self, vacancy_id: int, action_key: str) -> None:
        self.run_cli(
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            action_key,
            "--action-type",
            "message",
            "--state",
            "authorized",
            "--source",
            "synthetic_test",
            "--authorization-note",
            "synthetic exact authorization",
            "--json",
        )
        self.run_cli(
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            action_key,
            "--action-type",
            "message",
            "--state",
            "attempted",
            "--source",
            "synthetic_test",
            "--evidence-note",
            "synthetic attempt without visible confirmation",
            "--json",
        )

    def test_schema_v8_to_v9_backup_preserves_p0_evidence_and_projection_generation(self) -> None:
        vacancy_id = self.ingest_vacancy("migration")
        self.record_attempted_action(vacancy_id, "migration-action")
        coverage = self.write_hh_manifest("migration-source")
        self.run_cli("check-coverage", str(coverage))
        with sqlite3.connect(self.database) as conn:
            generation = conn.execute(
                "SELECT published_generation FROM projection_state"
            ).fetchone()[0]
            conn.executescript(
                """
                PRAGMA foreign_keys = OFF;
                DROP TABLE daily_run_transitions;
                DROP TABLE daily_run_manifests;
                DROP TABLE daily_run_work_items;
                DROP TABLE daily_run_step_dependencies;
                DROP TABLE daily_run_steps;
                DROP TABLE daily_run_plan_revisions;
                DROP TABLE daily_runs;
                DROP INDEX idx_daily_run_leases_one_active;
                DROP INDEX idx_daily_run_leases_status;
                ALTER TABLE daily_run_leases RENAME TO daily_run_leases_v9;
                CREATE TABLE daily_run_leases (
                    token TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    status TEXT NOT NULL,
                    lease_seconds INTEGER NOT NULL,
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    released_at TEXT,
                    release_reason TEXT,
                    CHECK(status IN ('active', 'finalized', 'expired')),
                    CHECK(lease_seconds >= 60 AND lease_seconds <= 86400)
                );
                INSERT INTO daily_run_leases SELECT * FROM daily_run_leases_v9;
                DROP TABLE daily_run_leases_v9;
                CREATE UNIQUE INDEX idx_daily_run_leases_one_active
                    ON daily_run_leases(status) WHERE status = 'active';
                CREATE INDEX idx_daily_run_leases_status
                    ON daily_run_leases(status, expires_at, acquired_at);
                INSERT INTO daily_run_leases (
                    token, run_id, owner, status, lease_seconds, acquired_at,
                    heartbeat_at, expires_at, released_at, release_reason
                ) VALUES (
                    'synthetic-v8-expired', 'synthetic-v8-run', 'synthetic-test',
                    'expired', 3600, '2026-08-18T08:00:00', '2026-08-18T08:00:00',
                    '2026-08-18T09:00:00', '2026-08-18T09:00:00', 'lease_expired'
                );
                PRAGMA user_version = 8;
                """
            )
            conn.commit()
            evidence_counts = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "vacancies",
                    "applications",
                    "employer_interactions",
                    "lifecycle_events",
                    "followup_rounds",
                    "search_runs",
                    "search_coverage",
                    "external_actions",
                    "daily_run_leases",
                    "projection_state",
                )
            }

        migrated = json.loads(
            self.run_cli("migrate-schema", "--defer-render", "--json").stdout
        )
        self.assertEqual(migrated["from_version"], 8)
        self.assertEqual(migrated["to_version"], 11)
        self.assertTrue(migrated["backup"])
        self.assertTrue(list(self.database.parent.glob("job_search.sqlite.bak-schema-v8-*")))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 11)
            self.assertEqual(
                conn.execute("SELECT published_generation FROM projection_state").fetchone()[0],
                generation,
            )
            self.assertEqual(
                {
                    table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in evidence_counts
                },
                evidence_counts,
            )
            self.assertEqual(migrated["row_counts_before"], migrated["row_counts_after"])
            for table in (
                "daily_runs",
                "daily_run_steps",
                "daily_run_work_items",
                "daily_run_manifests",
                "daily_run_transitions",
            ):
                self.assertIsNotNone(
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (table,),
                    ).fetchone()
                )

    def test_deterministic_plan_captures_all_configured_and_dynamic_scope(self) -> None:
        config_text = self.config.read_text(encoding="utf-8")
        config_text = config_text.replace("scan_linkedin_inbox = false", "scan_linkedin_inbox = true")
        config_text = config_text.replace("enabled = false\ninitial_lookback_days = 30", "enabled = true\ninitial_lookback_days = 30", 1)
        config_text = config_text.replace(
            "channels = []", 'channels = ["https://t.me/example_exec_jobs"]', 1
        )
        config_text += """

[[daily_run.required_gates]]
key = "additional_source"
kind = "workspace_gate"
order = 25
depends_on = ["hh_coverage"]
required = true
enabled = true
require_remote_boundary = true
"""
        self.config.write_text(config_text, encoding="utf-8")
        due_vacancy = self.ingest_vacancy("due-scope")
        attempted_vacancy = self.ingest_vacancy("attempted-scope")
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                """
                INSERT INTO lifecycle_events (
                    vacancy_id, event_type, event_at, evidence_at, evidence_note,
                    evidence_source, origin, history_complete,
                    authorization_status, dedupe_key, created_at
                ) VALUES (?, 'application_confirmed', ?, ?, ?, ?, ?, 1, 'explicit', ?, ?)
                """,
                (
                    due_vacancy,
                    "2026-08-01T09:00:00",
                    "2026-08-01T09:00:00",
                    "synthetic visible confirmation",
                    "synthetic_test",
                    "synthetic_test",
                    "due-lifecycle",
                    "2026-08-01T09:00:00",
                ),
            )
            lifecycle_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                """
                INSERT INTO applications (
                    id, vacancy_id, applied_date, status, stage, follow_up_date,
                    origin_file, line_no, lifecycle_event_id
                ) VALUES (1, ?, '2026-08-01', 'APPLIED_VISIBLY_CONFIRMED',
                          'follow_up', '2026-08-18', 'synthetic', 1, ?)
                """,
                (due_vacancy, lifecycle_id),
            )
            conn.commit()
        self.record_attempted_action(attempted_vacancy, "attempted-scope-action")

        begun = self.begin("scope-run")
        lease = str(begun["run_lease"])
        verbose = self.status("scope-run", verbose=True)
        step_keys = {step["step_key"] for step in verbose["steps"]}
        self.assertIn("mail_sources", step_keys)
        self.assertIn("telegram_coverage", step_keys)
        self.assertIn("gate:additional_source", step_keys)
        work = verbose["work_items"]
        self.assertEqual(
            {json.loads(item["scope_json"])["stream_key"] for item in work if item["step_key"] == "hh_coverage"},
            {"recommendations", "target_roles"},
        )
        self.assertEqual(
            {item["item_key"] for item in work if item["step_key"] == "telegram_coverage"},
            {"telegram:example_exec_jobs"},
        )
        self.assertEqual(sum(item["step_key"] == "due_followups" for item in work), 1)
        uncertain = [item for item in work if item["step_key"] == "external_action_reconciliation"]
        self.assertEqual(len(uncertain), 1)
        self.assertEqual(uncertain[0]["state"], "needs_verification")
        fingerprint = verbose["plan_fingerprint"]
        repeated = json.loads(
            self.run_cli(
                "begin-daily-run",
                "--run-id",
                "scope-run",
                "--run-date",
                "2026-08-18",
                "--timezone",
                "UTC",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertFalse(repeated["created"])
        self.assertEqual(self.status("scope-run")["plan_fingerprint"], fingerprint)
        competing = self.run_cli(
            "begin-daily-run", "--run-id", "competing-run", "--json", check=False
        )
        self.assertNotEqual(competing.returncode, 0)

    def test_checkpoint_pause_resume_and_expired_lease_preserve_exact_progress(self) -> None:
        begun = self.begin("resume-run")
        lease = str(begun["run_lease"])
        self.complete_inbound("resume-run", lease)
        repeated_completion = json.loads(
            self.run_cli(
                "complete-daily-run-work",
                "--run-id",
                "resume-run",
                "--step-key",
                "inbound_reconciliation",
                "--manifest",
                str(self.workspace / "resume-run-inbound.json"),
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertFalse(repeated_completion["changed"])
        verbose = self.status("resume-run", verbose=True)
        checkpoint_item = next(
            item
            for item in verbose["work_items"]
            if item["step_key"] == "hh_coverage"
        )
        item_key = checkpoint_item["item_key"]
        stream_key = json.loads(checkpoint_item["scope_json"])["stream_key"]
        checkpoint = self.workspace / "partial-hh.json"
        checkpoint.write_text(
            json.dumps(
                {
                    "manifest_version": 1,
                    "kind": "hh_stream",
                    "run_id": "resume-run",
                    "step_key": "hh_coverage",
                    "item_key": item_key,
                    "observed_at": "2026-08-18T12:05:00Z",
                    "captured_scope": {
                        "stream_key": stream_key,
                        "last_verified_page": 3,
                    },
                    "counts": {
                        "raw": 301,
                        "unique": 250,
                        "known": 200,
                        "new": 50,
                        "processed": 300,
                        "blocked": 0,
                    },
                    "completion_boundary": {"last_verified_page": 3},
                    "remote_boundary_verified": False,
                    "blockers": [],
                }
            ),
            encoding="utf-8",
        )
        command = (
            "checkpoint-daily-run-work",
            "--run-id",
            "resume-run",
            "--step-key",
            "hh_coverage",
            "--item-key",
            item_key,
            "--manifest",
            str(checkpoint),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        first = json.loads(self.run_cli(*command).stdout)
        second = json.loads(self.run_cli(*command).stdout)
        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        self.run_cli(
            "pause-daily-run",
            "--run-id",
            "resume-run",
            "--reason",
            "synthetic user blocker",
            "--run-lease",
            lease,
            "--json",
        )
        paused = self.status("resume-run")
        self.assertEqual(paused["status"], "paused")
        self.assertFalse(paused["lease"]["active"])
        self.assertEqual(paused["last_checkpoints"][0]["checkpoint"]["completion_boundary"]["last_verified_page"], 3)
        resumed = json.loads(
            self.run_cli("resume-daily-run", "--run-id", "resume-run", "--json").stdout
        )
        new_lease = str(resumed["run_lease"])
        self.assertNotEqual(new_lease, lease)
        self.assertEqual(
            self.status("resume-run")["next_safe_work"][0]["action"],
            "continue_from_checkpoint",
        )
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE daily_run_leases SET expires_at = '2000-01-01T00:00:00' WHERE token = ?",
                (new_lease,),
            )
            conn.commit()
        expired_resume = json.loads(
            self.run_cli("resume-daily-run", "--run-id", "resume-run", "--json").stdout
        )
        self.assertNotEqual(expired_resume["run_lease"], new_lease)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT state FROM daily_run_work_items WHERE run_id='resume-run' AND item_key=?",
                    (item_key,),
                ).fetchone()[0],
                "checkpointed",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_manifests WHERE run_id='resume-run' AND record_type='checkpoint'"
                ).fetchone()[0],
                1,
            )

    def test_partial_hh_manifest_keeps_checkpoint_and_blocked_stream_prevents_closeout(self) -> None:
        begun = self.begin("partial-run")
        lease = str(begun["run_lease"])
        self.complete_inbound("partial-run", lease)
        result = self.check_hh(
            "partial-run",
            lease,
            partial_stream="recommendations",
            blocked_stream="target_roles",
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        status = self.status("partial-run", verbose=True)
        states = {
            json.loads(item["scope_json"])["stream_key"]: item["state"]
            for item in status["work_items"]
            if item["step_key"] == "hh_coverage"
        }
        self.assertEqual(states["recommendations"], "checkpointed")
        self.assertEqual(states["target_roles"], "blocked")
        blocked_finalize = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(blocked_finalize.returncode, 0)
        self.assertIn("не готов к закрытию", blocked_finalize.stderr)

    def test_per_channel_telegram_completion_and_incomplete_channel_blocking(self) -> None:
        text = self.config.read_text(encoding="utf-8")
        text = text.replace("enabled = false\ninitial_lookback_days = 30", "enabled = true\ninitial_lookback_days = 30", 1)
        text = text.replace(
            "channels = []",
            'channels = ["https://t.me/example_exec_jobs", "https://t.me/example_ops_jobs"]',
            1,
        )
        self.config.write_text(text, encoding="utf-8")
        begun = self.begin("telegram-run")
        lease = str(begun["run_lease"])
        self.complete_inbound("telegram-run", lease)
        manifest_path = self.workspace / "telegram.json"
        self.run_cli(
            "build-telegram-plan",
            "--run-date",
            "2026-08-18",
            "--output",
            str(manifest_path),
            "--json",
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        complete, blocked = manifest["streams"]
        complete.update(
            {
                "status": "completed",
                "pages": [{"url": complete["query"]["url"], "post_ids": []}],
                "posts": [],
                "boundary": {"reached": True, "kind": "channel_start", "value": ""},
                "found": 0,
                "unique": 0,
                "known": 0,
                "new": 0,
            }
        )
        blocked.update({"status": "blocked", "error": "synthetic channel login blocker"})
        manifest["totals"] = {"unique": 0, "known": 0, "new": 0}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        checked = self.run_cli(
            "check-telegram-coverage",
            str(manifest_path),
            "--defer-render",
            "--run-lease",
            lease,
            check=False,
        )
        self.assertNotEqual(checked.returncode, 0)
        status = self.status("telegram-run", verbose=True)
        states = {
            item["item_key"]: item["state"]
            for item in status["work_items"]
            if item["step_key"] == "telegram_coverage"
        }
        self.assertEqual(states["telegram:example_exec_jobs"], "completed")
        self.assertEqual(states["telegram:example_ops_jobs"], "blocked")
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM source_checkpoints WHERE source='telegram'"
                ).fetchone()[0],
                0,
            )

    def test_attempted_external_action_is_needs_verification_never_resend(self) -> None:
        vacancy_id = self.ingest_vacancy("uncertain")
        self.record_attempted_action(vacancy_id, "uncertain-message")
        begun = self.begin("uncertain-run")
        lease = str(begun["run_lease"])
        status = self.status("uncertain-run")
        self.assertEqual(status["status"], "needs_verification")
        self.assertEqual(status["next_safe_work"][0]["action"], "reconcile_without_resend")
        self.assertNotEqual(status["next_safe_work"][0]["action"], "resend")
        self.assertEqual(status["blockers"][0]["code"], "uncertain_external_state")
        self.assertTrue(status["blockers"][0]["reason"])
        failed = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("needs_verification", failed.stderr)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM external_actions WHERE action_key='uncertain-message'"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_manifests "
                    "WHERE run_id='uncertain-run' AND record_type='uncertain'"
                ).fetchone()[0],
                1,
            )

    def test_plan_drift_fails_closed_and_audited_refresh_adds_without_silent_shrink(self) -> None:
        begun = self.begin("drift-run")
        lease = str(begun["run_lease"])
        original = self.status("drift-run")
        text = self.config.read_text(encoding="utf-8").replace(
            'required_streams = ["recommendations", "target_roles"]',
            'required_streams = ["recommendations", "target_roles", "new_stream"]',
        )
        self.config.write_text(text, encoding="utf-8")
        drifted = self.status("drift-run")
        self.assertTrue(drifted["configuration_drift"])
        failed = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("Конфигурация изменилась", failed.stderr)
        refreshed = json.loads(
            self.run_cli(
                "refresh-daily-run-plan",
                "--run-id",
                "drift-run",
                "--reason",
                "synthetic configuration expansion",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertEqual(refreshed["plan_revision"], 2)
        self.assertNotEqual(refreshed["plan_fingerprint"], original["plan_fingerprint"])
        verbose = self.status("drift-run", verbose=True)
        captured_streams = {
            json.loads(item["scope_json"])["stream_key"]
            for item in verbose["work_items"]
            if item["step_key"] == "hh_coverage"
        }
        self.assertEqual(captured_streams, {"recommendations", "target_roles", "new_stream"})
        self.assertFalse(verbose["configuration_drift"])
        self.assertTrue(
            any(event["event_type"] == "refreshed" for event in verbose["history"])
        )
        self.config.write_text(
            self.config.read_text(encoding="utf-8").replace(
                'required_streams = ["recommendations", "target_roles", "new_stream"]',
                'required_streams = ["target_roles", "new_stream"]',
            ),
            encoding="utf-8",
        )
        shrunk = json.loads(
            self.run_cli(
                "refresh-daily-run-plan",
                "--run-id",
                "drift-run",
                "--reason",
                "synthetic explicit configuration shrink",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertEqual(shrunk["plan_revision"], 3)
        self.assertEqual(shrunk["retained_prior_requirements"], 1)
        retained = self.status("drift-run", verbose=True)
        self.assertEqual(
            {
                json.loads(item["scope_json"])["stream_key"]
                for item in retained["work_items"]
                if item["step_key"] == "hh_coverage" and item["required"]
            },
            {"recommendations", "target_roles", "new_stream"},
        )
        repeated_refresh = json.loads(
            self.run_cli(
                "refresh-daily-run-plan",
                "--run-id",
                "drift-run",
                "--reason",
                "synthetic explicit configuration shrink",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertFalse(repeated_refresh["changed"])
        self.assertEqual(repeated_refresh["plan_revision"], 3)

    def test_parent_scope_drift_invalidates_completed_source_items(self) -> None:
        begun = self.begin("source-scope-drift-run")
        lease = str(begun["run_lease"])
        self.close_required_gates("source-scope-drift-run", lease)
        self.config.write_text(
            self.config.read_text(encoding="utf-8").replace(
                "default_period_days = 3", "default_period_days = 7"
            ),
            encoding="utf-8",
        )
        refreshed = json.loads(
            self.run_cli(
                "refresh-daily-run-plan",
                "--run-id",
                "source-scope-drift-run",
                "--reason",
                "synthetic search period change",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertIn("hh_coverage", refreshed["changed_roots"])
        status = self.status("source-scope-drift-run", verbose=True)
        self.assertEqual(
            {
                item["state"]
                for item in status["work_items"]
                if item["step_key"] == "hh_coverage"
            },
            {"invalidated"},
        )
        self.assertFalse(status["configuration_drift"])
        failed = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(failed.returncode, 0)

    def test_explicit_parent_invalidation_reopens_completed_source_items(self) -> None:
        begun = self.begin("explicit-parent-invalidation-run")
        lease = str(begun["run_lease"])
        self.close_required_gates("explicit-parent-invalidation-run", lease)
        result = json.loads(
            self.run_cli(
                "invalidate-daily-run-work",
                "--run-id",
                "explicit-parent-invalidation-run",
                "--step-key",
                "hh_coverage",
                "--reason",
                "synthetic parent evidence correction",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertEqual(
            len(
                [
                    key
                    for key in result["downstream_invalidated"]
                    if key.startswith("hh_coverage/")
                ]
            ),
            len(self.required_streams()),
        )
        status = self.status("explicit-parent-invalidation-run", verbose=True)
        self.assertEqual(
            {
                item["state"]
                for item in status["work_items"]
                if item["step_key"] == "hh_coverage"
            },
            {"pending"},
        )
        failed = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(failed.returncode, 0)

    def test_complete_due_followup_enumeration_ignores_wip_cap_and_message_quota(self) -> None:
        with sqlite3.connect(self.database) as conn:
            timestamp = "2026-08-01T09:00:00"
            for index in range(75):
                cursor = conn.execute(
                    """
                    INSERT INTO vacancies (
                        channel, external_id, title, company, latest_stage, updated_at
                    ) VALUES ('company_site', ?, 'Synthetic role', 'Synthetic employer',
                              'follow_up', ?)
                    """,
                    (f"company_site:due-{index}", timestamp),
                )
                vacancy_id = int(cursor.lastrowid)
                lifecycle = conn.execute(
                    """
                    INSERT INTO lifecycle_events (
                        vacancy_id, event_type, event_at, evidence_at, evidence_note,
                        evidence_source, origin, history_complete,
                        authorization_status, dedupe_key, created_at
                    ) VALUES (?, 'application_confirmed', ?, ?, 'synthetic',
                              'synthetic', 'synthetic', 1, 'explicit', ?, ?)
                    """,
                    (vacancy_id, timestamp, timestamp, f"due-lifecycle-{index}", timestamp),
                )
                conn.execute(
                    """
                    INSERT INTO applications (
                        id, vacancy_id, applied_date, status, stage, follow_up_date,
                        origin_file, line_no, lifecycle_event_id
                    ) VALUES (?, ?, '2026-08-01', 'APPLIED_VISIBLY_CONFIRMED',
                              'follow_up', '2026-08-18', 'synthetic', ?, ?)
                    """,
                    (index + 1, vacancy_id, index + 1, int(lifecycle.lastrowid)),
                )
            conn.commit()
        begun = self.begin("due-run")
        status = self.status("due-run", verbose=True)
        due_items = [
            item for item in status["work_items"] if item["step_key"] == "due_followups"
        ]
        self.assertEqual(len(due_items), 75)
        self.assertEqual(status["counts"]["total_required"], 80)
        self.assertLess(len(json.dumps(self.status("due-run"))), 30_000)
        self.run_cli(
            "pause-daily-run",
            "--run-id",
            "due-run",
            "--reason",
            "synthetic enumeration complete",
            "--run-lease",
            str(begun["run_lease"]),
            "--json",
        )

    def test_newly_due_item_extends_plan_and_run_aware_doctor_fails_closed(self) -> None:
        vacancy_id = self.ingest_vacancy("newly-due")
        begun = self.begin("newly-due-run")
        lease = str(begun["run_lease"])
        self.close_required_gates("newly-due-run", lease)
        with sqlite3.connect(self.database) as conn:
            timestamp = "2026-08-18T13:00:00"
            lifecycle = conn.execute(
                """
                INSERT INTO lifecycle_events (
                    vacancy_id, event_type, event_at, evidence_at, evidence_note,
                    evidence_source, origin, history_complete,
                    authorization_status, dedupe_key, created_at
                ) VALUES (?, 'application_confirmed', ?, ?, 'synthetic visible confirmation',
                          'synthetic_test', 'synthetic_test', 1, 'explicit', ?, ?)
                """,
                (vacancy_id, timestamp, timestamp, "newly-due-lifecycle", timestamp),
            )
            conn.execute(
                """
                INSERT INTO applications (
                    vacancy_id, applied_date, status, stage, follow_up_date,
                    origin_file, line_no, lifecycle_event_id
                ) VALUES (?, '2026-08-18', 'APPLIED_VISIBLY_CONFIRMED',
                          'follow_up', '2026-08-18', 'synthetic', 1, ?)
                """,
                (vacancy_id, int(lifecycle.lastrowid)),
            )
            conn.commit()
        doctor = self.run_cli(
            "operational-doctor",
            "--run-id",
            "newly-due-run",
            "--strict",
            "--json",
            check=False,
        )
        self.assertNotEqual(doctor.returncode, 0)
        doctor_payload = json.loads(doctor.stdout)
        durable_check = next(
            check
            for check in doctor_payload["checks"]
            if check["name"] == "durable_daily_run_preconditions"
        )
        self.assertIn("очередь повторных обращений", durable_check["detail"])
        failed = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(failed.returncode, 0)
        status = self.status("newly-due-run", verbose=True)
        due_items = [
            item for item in status["work_items"] if item["step_key"] == "due_followups"
        ]
        self.assertEqual(len(due_items), 1)
        self.assertEqual(due_items[0]["state"], "pending")
        self.assertGreater(status["plan_revision"], 1)
        self.assertEqual(status["next_safe_work"][0]["step_key"], "due_followups")

    def test_user_cancels_exact_frozen_due_followup_and_refresh_preserves_resolution(
        self,
    ) -> None:
        run_id = "cancel-due-followup-run"
        reason = "synthetic explicit user cancellation"
        vacancy_id, application_id = self.seed_due_followup("cancel-due")
        with sqlite3.connect(self.database) as conn:
            before_vacancy = conn.execute(
                """
                SELECT latest_status, latest_stage FROM vacancies WHERE id = ?
                """,
                (vacancy_id,),
            ).fetchone()
            before_application = conn.execute(
                "SELECT status, stage FROM applications WHERE id = ?",
                (application_id,),
            ).fetchone()
            before_lifecycle = conn.execute(
                "SELECT id, event_type, dedupe_key FROM lifecycle_events "
                "WHERE vacancy_id = ? ORDER BY id",
                (vacancy_id,),
            ).fetchall()

        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        due_item = next(
            item
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        item_key = str(due_item["item_key"])
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            "due_followups",
            "--item-key",
            item_key,
            "--reason",
            "synthetic frozen due item",
            "--leave-invalidated",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        cancelled = json.loads(
            self.run_cli(
                "cancel-due-followup-obligation",
                "--run-id",
                run_id,
                "--item-key",
                item_key,
                "--reason",
                reason,
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertTrue(cancelled["changed"])
        self.assertFalse(cancelled["idempotent"])
        self.assertTrue(cancelled["dates_cleared"])
        self.assertTrue(cancelled["lifecycle_preserved"])
        self.assertFalse(cancelled["message_delivery_inferred"])
        self.assertFalse(cancelled["fresh_inbound_inferred"])
        self.assertFalse(cancelled["rejection_inferred"])
        self.assertFalse(cancelled["withdrawal_inferred"])
        transition_id = cancelled["audit_transition_id"]

        repeated = json.loads(
            self.run_cli(
                "cancel-due-followup-obligation",
                "--run-id",
                run_id,
                "--item-key",
                item_key,
                "--reason",
                reason,
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertFalse(repeated["changed"])
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(repeated["audit_transition_id"], transition_id)

        refreshed = json.loads(
            self.run_cli(
                "refresh-daily-run-plan",
                "--run-id",
                run_id,
                "--reason",
                "synthetic refresh after exact cancellation",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertTrue(refreshed["changed"])
        after_refresh = self.status(run_id, verbose=True)
        refreshed_item = next(
            item
            for item in after_refresh["work_items"]
            if item["step_key"] == "due_followups" and item["item_key"] == item_key
        )
        self.assertEqual(refreshed_item["state"], "completed")
        self.assertEqual(refreshed_item["required"], 1)

        self.close_required_gates(run_id, lease)
        finalized = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(finalized["render_performed"])
        self.assertEqual(self.status(run_id)["status"], "completed")
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT follow_up_date FROM vacancies WHERE id = ?",
                    (vacancy_id,),
                ).fetchone()[0],
                "",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT follow_up_date FROM applications WHERE id = ?",
                    (application_id,),
                ).fetchone()[0],
                "",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT latest_status, latest_stage FROM vacancies WHERE id = ?",
                    (vacancy_id,),
                ).fetchone(),
                before_vacancy,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT status, stage FROM applications WHERE id = ?",
                    (application_id,),
                ).fetchone(),
                before_application,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT id, event_type, dedupe_key FROM lifecycle_events "
                    "WHERE vacancy_id = ? ORDER BY id",
                    (vacancy_id,),
                ).fetchall(),
                before_lifecycle,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions "
                    "WHERE run_id = ? AND event_type = "
                    "'user_cancelled_followup_obligation'",
                    (run_id,),
                ).fetchone()[0],
                1,
            )
            manifest = conn.execute(
                """
                SELECT payload_json FROM daily_run_manifests
                WHERE run_id = ? AND step_key = 'due_followups'
                  AND item_key = ? AND record_type = 'programmatic'
                ORDER BY id DESC LIMIT 1
                """,
                (run_id, item_key),
            ).fetchone()
            self.assertEqual(
                json.loads(manifest[0])["programmatic_resolution"]["type"],
                "user_cancelled_followup_obligation",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM external_actions WHERE vacancy_id = ?",
                    (vacancy_id,),
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM employer_interactions WHERE vacancy_id = ?",
                    (vacancy_id,),
                ).fetchone()[0],
                0,
            )

    def test_user_cancelled_due_followup_accepts_precleared_invalidated_item(
        self,
    ) -> None:
        run_id = "cancel-precleared-due-run"
        vacancy_id, application_id = self.seed_due_followup("cancel-precleared")
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            item["item_key"]
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            "due_followups",
            "--item-key",
            str(item_key),
            "--reason",
            "synthetic earlier date clearing",
            "--leave-invalidated",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE vacancies SET follow_up_date = '' WHERE id = ?",
                (vacancy_id,),
            )
            conn.execute(
                "UPDATE applications SET follow_up_date = '' WHERE id = ?",
                (application_id,),
            )
            conn.commit()
        result = json.loads(
            self.run_cli(
                "cancel-due-followup-obligation",
                "--run-id",
                run_id,
                "--item-key",
                str(item_key),
                "--reason",
                "synthetic explicit recovery of precleared obligation",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertTrue(result["changed"])
        resolved_item = next(
            item
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["item_key"] == item_key
        )
        self.assertEqual(resolved_item["state"], "completed")

    def test_cancel_due_followup_fails_closed_for_wrong_target_reason_and_scope(
        self,
    ) -> None:
        run_id = "cancel-due-fail-closed-run"
        vacancy_id, application_id = self.seed_due_followup("cancel-fail-closed")
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            item["item_key"]
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        base = (
            "cancel-due-followup-obligation",
            "--run-id",
            run_id,
            "--item-key",
            str(item_key),
            "--reason",
            "synthetic exact cancellation",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        wrong_run = self.run_cli(
            *base[:2], "another-run", *base[3:], check=False
        )
        self.assertNotEqual(wrong_run.returncode, 0)
        wrong_item_args = list(base)
        wrong_item_args[4] = "due:999:999:2026-08-18"
        wrong_item = self.run_cli(*wrong_item_args, check=False)
        self.assertNotEqual(wrong_item.returncode, 0)
        blank_reason_args = list(base)
        blank_reason_args[6] = "   "
        blank_reason = self.run_cli(*blank_reason_args, check=False)
        self.assertNotEqual(blank_reason.returncode, 0)
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE applications SET follow_up_date = '2026-08-19' WHERE id = ?",
                (application_id,),
            )
            conn.commit()
        changed_scope = self.run_cli(*base, check=False)
        self.assertNotEqual(changed_scope.returncode, 0)
        self.assertIn("Scope изменился", changed_scope.stderr)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT follow_up_date FROM applications WHERE id = ?",
                    (application_id,),
                ).fetchone()[0],
                "2026-08-19",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT follow_up_date FROM vacancies WHERE id = ?",
                    (vacancy_id,),
                ).fetchone()[0],
                "2026-08-18",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions "
                    "WHERE run_id = ? AND event_type = "
                    "'user_cancelled_followup_obligation'",
                    (run_id,),
                ).fetchone()[0],
                0,
            )

    def test_cancelled_due_followup_reactivation_blocks_finalization(self) -> None:
        run_id = "cancel-due-reactivated-run"
        vacancy_id, application_id = self.seed_due_followup("cancel-reactivated")
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            item["item_key"]
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        self.run_cli(
            "cancel-due-followup-obligation",
            "--run-id",
            run_id,
            "--item-key",
            str(item_key),
            "--reason",
            "synthetic cancellation before reactivation",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.close_required_gates(run_id, lease)
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE vacancies SET follow_up_date = '2026-08-18' WHERE id = ?",
                (vacancy_id,),
            )
            conn.execute(
                "UPDATE applications SET follow_up_date = '2026-08-18' WHERE id = ?",
                (application_id,),
            )
            conn.commit()
        failed = self.run_cli(
            "finalize-daily-run", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("user_cancelled_followup_obligation_reactivated", failed.stderr)
        item = next(
            item
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["item_key"] == item_key
        )
        self.assertEqual(item["state"], "invalidated")

    def test_reverified_historical_human_inbound_resolves_without_rewriting_history(
        self,
    ) -> None:
        run_id = "reverified-historical-inbound"
        vacancy_id, application_id = self.seed_due_followup("reverified-positive")
        interaction_id = self.seed_historical_interaction(vacancy_id)
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            str(item["item_key"])
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row
            valid, resolution = orchestration.due_followup_resolution(
                conn, run_id, "due_followups", item_key
            )
            self.assertFalse(valid)
            self.assertNotEqual(resolution.get("type"), "fresh_inbound")
            original = conn.execute(
                "SELECT * FROM employer_interactions WHERE id = ?", (interaction_id,)
            ).fetchone()
            interaction_hash = orchestration.payload_hash(
                orchestration._interaction_evidence_snapshot(original)
            )
            states_before = conn.execute(
                """
                SELECT a.status, a.stage, v.latest_status, v.latest_stage
                FROM applications a JOIN vacancies v ON v.id = a.vacancy_id
                WHERE a.id = ?
                """,
                (application_id,),
            ).fetchone()
            lifecycle_before = conn.execute(
                "SELECT id, event_type, event_at, dedupe_key FROM lifecycle_events "
                "WHERE vacancy_id = ? ORDER BY id",
                (vacancy_id,),
            ).fetchall()
        self.clear_due_dates(vacancy_id, application_id)
        observed_at = orchestration.now_iso()
        _, manifest_path = self.reverified_manifest(
            run_id, item_key, interaction_id, observed_at=observed_at
        )
        resolved = json.loads(
            self.resolve_reverified_cli(
                run_id,
                lease,
                item_key,
                interaction_id,
                manifest_path,
                observed_at=observed_at,
            ).stdout
        )
        self.assertTrue(resolved["changed"])
        self.assertFalse(resolved["idempotent"])
        self.assertTrue(resolved["interaction_timestamp_preserved"])
        self.assertFalse(resolved["duplicate_interaction_created"])
        self.assertTrue(resolved["lifecycle_preserved"])
        self.assertEqual(
            resolved["resolution"]["type"],
            "reverified_historical_inbound_due_resolution",
        )
        repeated = json.loads(
            self.resolve_reverified_cli(
                run_id,
                lease,
                item_key,
                interaction_id,
                manifest_path,
                observed_at=observed_at,
            ).stdout
        )
        self.assertFalse(repeated["changed"])
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(repeated["audit_transition_id"], resolved["audit_transition_id"])
        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row
            interactions = conn.execute(
                "SELECT * FROM employer_interactions WHERE vacancy_id = ? ORDER BY id",
                (vacancy_id,),
            ).fetchall()
            self.assertEqual(len(interactions), 1)
            self.assertEqual(
                orchestration.payload_hash(
                    orchestration._interaction_evidence_snapshot(interactions[0])
                ),
                interaction_hash,
            )
            self.assertEqual(str(interactions[0]["event_at"]), "2026-08-10T10:00:00+00:00")
            self.assertEqual(
                conn.execute(
                    """
                    SELECT a.status, a.stage, v.latest_status, v.latest_stage
                    FROM applications a JOIN vacancies v ON v.id = a.vacancy_id
                    WHERE a.id = ?
                    """,
                    (application_id,),
                ).fetchone(),
                states_before,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT id, event_type, event_at, dedupe_key FROM lifecycle_events "
                    "WHERE vacancy_id = ? ORDER BY id",
                    (vacancy_id,),
                ).fetchall(),
                lifecycle_before,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions WHERE run_id = ? "
                    "AND event_type = 'reverified_historical_inbound_due_resolution'",
                    (run_id,),
                ).fetchone()[0],
                1,
            )
            item = conn.execute(
                "SELECT state FROM daily_run_work_items WHERE run_id = ? "
                "AND step_key = 'due_followups' AND item_key = ?",
                (run_id, item_key),
            ).fetchone()
            self.assertEqual(item["state"], "completed")

    def test_reverified_historical_inbound_allows_reconciled_status_drift(
        self,
    ) -> None:
        run_id = "reverified-reconciled-status-drift"
        vacancy_id, application_id = self.seed_due_followup("reverified-status-drift")
        interaction_id = self.seed_historical_interaction(vacancy_id)
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            str(item["item_key"])
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        self.clear_due_dates(vacancy_id, application_id)
        reconciled_status = "SYNTHETIC_HUMAN_REPLY_RECONCILED"
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE applications SET status = ? WHERE id = ?",
                (reconciled_status, application_id),
            )
            conn.execute(
                "UPDATE vacancies SET latest_status = ? WHERE id = ?",
                (reconciled_status, vacancy_id),
            )
            conn.commit()
        observed_at = orchestration.now_iso()
        _, manifest_path = self.reverified_manifest(
            run_id, item_key, interaction_id, observed_at=observed_at
        )

        resolved = json.loads(
            self.resolve_reverified_cli(
                run_id,
                lease,
                item_key,
                interaction_id,
                manifest_path,
                observed_at=observed_at,
            ).stdout
        )

        self.assertTrue(resolved["changed"])
        self.assertEqual(resolved["application_status_preserved"], reconciled_status)
        self.assertEqual(resolved["vacancy_status_preserved"], reconciled_status)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT status FROM applications WHERE id = ?",
                    (application_id,),
                ).fetchone()[0],
                reconciled_status,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT latest_status FROM vacancies WHERE id = ?",
                    (vacancy_id,),
                ).fetchone()[0],
                reconciled_status,
            )

    def test_reverified_inbound_rejects_automated_and_material_manifest_mismatches(
        self,
    ) -> None:
        run_id = "reverified-negative-manifest"
        vacancy_id, application_id = self.seed_due_followup("reverified-negative")
        interaction_id = self.seed_historical_interaction(
            vacancy_id, event_type="automated_ack", is_human=0
        )
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            str(item["item_key"])
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        self.clear_due_dates(vacancy_id, application_id)
        observed_at = orchestration.now_iso()
        manifest, manifest_path = self.reverified_manifest(
            run_id, item_key, interaction_id, observed_at=observed_at
        )
        automated = self.resolve_reverified_cli(
            run_id,
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            check=False,
        )
        self.assertNotEqual(automated.returncode, 0)
        self.assertIn("interaction_is_not_effective_human_inbound_reply", automated.stderr)

        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE employer_interactions SET event_type = 'human_reply', "
                "is_human = 1, actor_type = 'recruiter' WHERE id = ?",
                (interaction_id,),
            )
            conn.commit()
        manifest, manifest_path = self.reverified_manifest(
            run_id, item_key, interaction_id, observed_at=observed_at
        )
        mismatch_cases = {
            "remote_boundary_verified": False,
            "latest_message_matches_interaction": False,
            "no_new_outbound_after_inbound": False,
            "original_interaction_timestamp_preserved": False,
            "application_id": int(manifest["application_id"]) + 1,
            "vacancy_id": int(manifest["vacancy_id"]) + 1,
            "original_evidence_hash": "0" * 64,
            "scope_fingerprint": "1" * 64,
        }
        for field, value in mismatch_cases.items():
            with self.subTest(field=field):
                changed = dict(manifest)
                changed[field] = value
                path = self.workspace / f"{field}-mismatch.json"
                path.write_text(json.dumps(changed), encoding="utf-8")
                result = self.resolve_reverified_cli(
                    run_id,
                    lease,
                    item_key,
                    interaction_id,
                    path,
                    observed_at=observed_at,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
        wrong_channel = self.resolve_reverified_cli(
            run_id,
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            channel="linkedin",
            check=False,
        )
        self.assertNotEqual(wrong_channel.returncode, 0)
        wrong_target = self.resolve_reverified_cli(
            run_id,
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            conversation_target="different-target",
            check=False,
        )
        self.assertNotEqual(wrong_target.returncode, 0)
        wrong_reference = self.resolve_reverified_cli(
            run_id,
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            remote_reference="different-proof",
            check=False,
        )
        self.assertNotEqual(wrong_reference.returncode, 0)
        wrong_run = self.resolve_reverified_cli(
            "different-run",
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            check=False,
        )
        self.assertNotEqual(wrong_run.returncode, 0)
        wrong_item = self.resolve_reverified_cli(
            run_id,
            lease,
            "due:999:999:2026-08-18",
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            check=False,
        )
        self.assertNotEqual(wrong_item.returncode, 0)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions WHERE run_id = ? "
                    "AND event_type = 'reverified_historical_inbound_due_resolution'",
                    (run_id,),
                ).fetchone()[0],
                0,
            )

    def test_reverified_inbound_rejects_later_message_outbound_and_reactivated_due(
        self,
    ) -> None:
        run_id = "reverified-negative-latest"
        vacancy_id, application_id = self.seed_due_followup("reverified-latest")
        interaction_id = self.seed_historical_interaction(vacancy_id)
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            str(item["item_key"])
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        observed_at = orchestration.now_iso()
        _, manifest_path = self.reverified_manifest(
            run_id, item_key, interaction_id, observed_at=observed_at
        )
        reactivated = self.resolve_reverified_cli(
            run_id,
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            check=False,
        )
        self.assertNotEqual(reactivated.returncode, 0)
        self.assertIn("follow_up_date_reactivated", reactivated.stderr)
        self.clear_due_dates(vacancy_id, application_id)
        self.seed_historical_interaction(
            vacancy_id,
            suffix="later-outbound",
            direction="outbound",
            event_at="2026-08-11T10:00:00+00:00",
        )
        later = self.resolve_reverified_cli(
            run_id,
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            check=False,
        )
        self.assertNotEqual(later.returncode, 0)
        self.assertTrue(
            "historical_inbound_is_not_latest_interaction" in later.stderr
            or "later_outbound_interaction_exists" in later.stderr
        )

    def test_reverified_inbound_rejects_changed_scope_and_preserves_user_cancellation_separation(
        self,
    ) -> None:
        run_id = "reverified-negative-scope"
        vacancy_id, application_id = self.seed_due_followup("reverified-scope")
        interaction_id = self.seed_historical_interaction(vacancy_id)
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        item_key = next(
            str(item["item_key"])
            for item in self.status(run_id, verbose=True)["work_items"]
            if item["step_key"] == "due_followups"
        )
        self.clear_due_dates(vacancy_id, application_id)
        observed_at = orchestration.now_iso()
        _, manifest_path = self.reverified_manifest(
            run_id, item_key, interaction_id, observed_at=observed_at
        )
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE applications SET status = 'SYNTHETIC_CHANGED' WHERE id = ?",
                (application_id,),
            )
            conn.commit()
        changed_scope = self.resolve_reverified_cli(
            run_id,
            lease,
            item_key,
            interaction_id,
            manifest_path,
            observed_at=observed_at,
            check=False,
        )
        self.assertNotEqual(changed_scope.returncode, 0)
        self.assertIn("Scope изменился", changed_scope.stderr)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions WHERE run_id = ? "
                    "AND event_type = 'user_cancelled_followup_obligation'",
                    (run_id,),
                ).fetchone()[0],
                0,
            )

    def test_failed_final_render_preserves_generation_and_resume_completes_once(self) -> None:
        begun = self.begin("finalize-run")
        lease = str(begun["run_lease"])
        self.close_required_gates("finalize-run", lease)
        current = self.workspace / ".jobctl" / "projections" / "current"
        before_generation = os.readlink(current)
        failed_env = self.env.copy()
        failed_env["JOBCTL_TEST_FAIL_RENDER_BEFORE_PUBLISH"] = "1"
        failed = self.run_cli(
            "finalize-daily-run",
            "--run-lease",
            lease,
            "--json",
            check=False,
            env=failed_env,
        )
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(os.readlink(current), before_generation)
        interrupted = self.status("finalize-run")
        self.assertEqual(interrupted["status"], "finalizing")
        self.assertTrue(interrupted["projection_state"]["dirty"])
        completed = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(completed["render_performed"])
        self.assertFalse(completed["projection_state"]["dirty"])
        after_generation = os.readlink(current)
        self.assertNotEqual(after_generation, before_generation)
        repeated = json.loads(
            self.run_cli(
                "finalize-daily-run",
                "--run-lease",
                lease,
                "--json",
                env=failed_env,
            ).stdout
        )
        self.assertTrue(repeated["already_finalized"])
        self.assertEqual(os.readlink(current), after_generation)
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                "finalize-run",
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])

    def test_after_publish_interruption_recovers_generation_without_second_render(self) -> None:
        begun = self.begin("recover-published-run")
        lease = str(begun["run_lease"])
        self.close_required_gates("recover-published-run", lease)
        current = self.workspace / ".jobctl" / "projections" / "current"
        before_generation = os.readlink(current)
        failed_env = self.env.copy()
        failed_env["JOBCTL_TEST_FAIL_RENDER_AFTER_PUBLISH"] = "1"
        failed = self.run_cli(
            "finalize-daily-run",
            "--run-lease",
            lease,
            "--json",
            check=False,
            env=failed_env,
        )
        self.assertNotEqual(failed.returncode, 0)
        published_generation = os.readlink(current)
        self.assertNotEqual(published_generation, before_generation)
        self.assertEqual(self.status("recover-published-run")["status"], "finalizing")
        self.run_cli(
            "pause-daily-run",
            "--run-id",
            "recover-published-run",
            "--reason",
            "synthetic interruption handoff",
            "--run-lease",
            lease,
            "--json",
        )
        resumed = json.loads(
            self.run_cli(
                "resume-daily-run", "--run-id", "recover-published-run", "--json"
            ).stdout
        )
        recovered_lease = str(resumed["run_lease"])
        self.assertNotEqual(recovered_lease, lease)
        recovered = json.loads(
            self.run_cli(
                "finalize-daily-run",
                "--run-lease",
                recovered_lease,
                "--json",
                env=failed_env,
            ).stdout
        )
        self.assertFalse(recovered["render_performed"])
        self.assertTrue(recovered["recovered_generation"])
        self.assertEqual(os.readlink(current), published_generation)
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                "recover-published-run",
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])
        repeated_old_lease = json.loads(
            self.run_cli(
                "finalize-daily-run",
                "--run-lease",
                lease,
                "--json",
                env=failed_env,
            ).stdout
        )
        self.assertTrue(repeated_old_lease["already_finalized"])
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions "
                    "WHERE run_id='recover-published-run' "
                    "AND event_type='final_projection_recovered'"
                ).fetchone()[0],
                1,
            )

    def test_identical_coverage_completion_is_idempotent(self) -> None:
        begun = self.begin("coverage-idempotent-run")
        lease = str(begun["run_lease"])
        self.complete_inbound("coverage-idempotent-run", lease)
        manifest = self.write_hh_manifest("coverage-idempotent-run")
        command = (
            "check-coverage",
            str(manifest),
            "--defer-render",
            "--run-lease",
            lease,
        )
        self.run_cli(*command)
        self.run_cli(*command)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_manifests "
                    "WHERE run_id='coverage-idempotent-run' "
                    "AND step_key='hh_coverage' AND record_type='completion'"
                ).fetchone()[0],
                len(self.required_streams()),
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_transitions "
                    "WHERE run_id='coverage-idempotent-run' "
                    "AND entity_type='work_item' AND event_type='completed'"
                ).fetchone()[0],
                len(self.required_streams()),
            )

    def test_external_attempt_after_inbound_checkpoint_reopens_freshness_without_resend(self) -> None:
        vacancy_id = self.ingest_vacancy("live-attempt")
        begun = self.begin("live-attempt-run")
        lease = str(begun["run_lease"])
        self.complete_inbound("live-attempt-run", lease)
        base = (
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            "live-attempt-action",
            "--action-type",
            "message",
            "--source",
            "synthetic_test",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            *base,
            "--state",
            "authorized",
            "--authorization-note",
            "synthetic exact authorization",
        )
        self.run_cli(
            *base,
            "--state",
            "attempted",
            "--evidence-note",
            "synthetic attempt without visible confirmation",
        )
        status = self.status("live-attempt-run", verbose=True)
        inbound = next(
            step for step in status["steps"] if step["step_key"] == "inbound_reconciliation"
        )
        self.assertEqual(inbound["state"], "invalidated")
        self.assertEqual(status["status"], "needs_verification")
        self.assertEqual(status["next_safe_work"][0]["action"], "reconcile_without_resend")
        self.assertIn(
            "reconcile_inbound_after_outbound",
            {item["action"] for item in status["next_safe_work"]},
        )
        self.assertGreaterEqual(status["plan_revision"], 2)

    def test_inbound_revalidation_requires_fresh_evidence_and_preserves_other_gates(self) -> None:
        vacancy_id = self.ingest_vacancy("fresh-inbound")
        begun = self.begin("fresh-inbound-run")
        lease = str(begun["run_lease"])
        self.complete_inbound(
            "fresh-inbound-run", lease, observed_at="2026-08-18T12:00:00Z"
        )
        self.check_hh("fresh-inbound-run", lease)
        base = (
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            "fresh-inbound-follow-up",
            "--action-type",
            "message",
            "--source",
            "synthetic_test",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            *base,
            "--state",
            "authorized",
            "--at",
            "2026-08-18T13:00:00Z",
            "--authorization-note",
            "synthetic exact authorization",
        )
        self.run_cli(
            *base,
            "--state",
            "attempted",
            "--at",
            "2026-08-18T13:01:00Z",
            "--evidence-note",
            "synthetic outbound attempt",
        )
        self.run_cli(
            *base,
            "--state",
            "visibly_confirmed",
            "--at",
            "2026-08-18T13:02:00Z",
            "--evidence-note",
            "synthetic visible delivery confirmation",
            "--external-reference",
            "synthetic-visible-message-1",
        )

        invalidated = self.status("fresh-inbound-run", verbose=True)
        inbound = next(
            step
            for step in invalidated["steps"]
            if step["step_key"] == "inbound_reconciliation"
        )
        self.assertEqual(inbound["state"], "invalidated")
        self.assertEqual(
            invalidated["next_safe_work"][0]["action"],
            "reconcile_inbound_after_outbound",
        )
        stale = self.complete_inbound(
            "fresh-inbound-run",
            lease,
            observed_at="2026-08-18T13:02:00Z",
            check=False,
        )
        self.assertNotEqual(stale.returncode, 0)
        self.assertIn("observed_at позже", stale.stderr)

        fresh = json.loads(
            self.complete_inbound(
                "fresh-inbound-run",
                lease,
                observed_at="2026-08-18T14:00:00Z",
            ).stdout
        )
        self.assertTrue(fresh["changed"])
        refreshed = self.status("fresh-inbound-run", verbose=True)
        self.assertEqual(
            next(
                step["state"]
                for step in refreshed["steps"]
                if step["step_key"] == "inbound_reconciliation"
            ),
            "completed",
        )
        self.assertEqual(
            {
                item["state"]
                for item in refreshed["work_items"]
                if item["step_key"] == "hh_coverage"
            },
            {"completed"},
        )
        replayed = json.loads(
            self.run_cli(
                *base,
                "--state",
                "visibly_confirmed",
                "--at",
                "2026-08-18T13:02:00Z",
                "--evidence-note",
                "synthetic visible delivery confirmation",
                "--external-reference",
                "synthetic-visible-message-1",
            ).stdout
        )
        self.assertFalse(replayed["created"])
        replay_status = self.status("fresh-inbound-run", verbose=True)
        self.assertEqual(
            next(
                step["state"]
                for step in replay_status["steps"]
                if step["step_key"] == "inbound_reconciliation"
            ),
            "completed",
        )
        finalized = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(finalized["render_performed"])
        repeated = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(repeated["already_finalized"])
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                "fresh-inbound-run",
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM external_actions "
                    "WHERE action_key='fresh-inbound-follow-up'"
                ).fetchone()[0],
                3,
            )

    def test_finalize_reaggregates_completed_descendants_after_inbound_revalidation(self) -> None:
        run_id = "reaggregate-descendants-run"
        begun = self.begin(run_id)
        lease = str(begun["run_lease"])
        self.close_required_gates(run_id, lease)
        self.run_cli(
            "invalidate-daily-run-work",
            "--run-id",
            run_id,
            "--step-key",
            "inbound_reconciliation",
            "--reason",
            "synthetic full inbound recheck",
            "--leave-invalidated",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        invalidated = self.status(run_id, verbose=True)
        self.assertEqual(
            next(
                step["state"]
                for step in invalidated["steps"]
                if step["step_key"] == "hh_coverage"
            ),
            "invalidated",
        )
        self.assertEqual(
            {
                item["state"]
                for item in invalidated["work_items"]
                if item["step_key"] == "hh_coverage"
            },
            {"completed"},
        )
        self.complete_inbound(
            run_id,
            lease,
            observed_at="2026-08-18T14:00:00Z",
        )

        finalized = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )

        self.assertTrue(finalized["render_performed"])
        finalized_status = self.status(run_id, verbose=True)
        self.assertEqual(
            next(
                step["state"]
                for step in finalized_status["steps"]
                if step["step_key"] == "hh_coverage"
            ),
            "completed",
        )
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

    def test_external_action_scope_uses_captured_id_floor_and_visible_backlog(self) -> None:
        vacancy_id = self.ingest_vacancy("external-scope")
        for index in range(12):
            self.run_cli(
                "record-external-action",
                "--id",
                str(vacancy_id),
                "--action-key",
                f"legacy-authorized-{index:02d}",
                "--action-type",
                "message",
                "--state",
                "authorized",
                "--at",
                f"2026-08-01T08:{index:02d}:00Z",
                "--source",
                "synthetic_test",
                "--authorization-note",
                "synthetic historical authorization",
                "--defer-render",
                "--json",
            )
        self.record_attempted_action(vacancy_id, "unresolved-attempted-carryover")

        begun = self.begin("external-scope-run")
        lease = str(begun["run_lease"])
        initial = self.status("external-scope-run", verbose=True)
        scope = initial["external_action_scope"]
        self.assertEqual(scope["action_id_floor"], 14)
        self.assertEqual(scope["required_current_run"], 0)
        self.assertEqual(scope["required_unresolved_attempted"], 1)
        self.assertEqual(scope["legacy_backlog"]["total"], 12)
        self.assertEqual(scope["legacy_backlog"]["states"]["authorized"], 12)
        required = [
            item
            for item in initial["work_items"]
            if item["step_key"] == "external_action_reconciliation"
            and item["required"]
        ]
        self.assertEqual(len(required), 1)
        self.assertEqual(
            json.loads(required[0]["scope_json"])["reconciliation_scope"],
            "unresolved_attempted_carryover",
        )

        self.run_cli(
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            "current-run-authorization",
            "--action-type",
            "message",
            "--state",
            "authorized",
            "--at",
            "2026-08-18T13:00:00Z",
            "--source",
            "synthetic_test",
            "--authorization-note",
            "synthetic current-run authorization",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        current = self.status("external-scope-run", verbose=True)
        self.assertEqual(current["external_action_scope"]["required_current_run"], 1)
        self.assertEqual(
            len(
                [
                    item
                    for item in current["work_items"]
                    if item["step_key"] == "external_action_reconciliation"
                    and item["required"]
                ]
            ),
            2,
        )
        with sqlite3.connect(self.database) as conn:
            metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM external_actions "
                    "WHERE action_key='current-run-authorization'"
                ).fetchone()[0]
            )
            self.assertEqual(metadata["daily_run_id"], "external-scope-run")
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM external_actions WHERE state='authorized'"
                ).fetchone()[0],
                14,
            )

    def test_frozen_legacy_requirement_needs_explicit_audited_reclassification(self) -> None:
        vacancy_id = self.ingest_vacancy("legacy-reclassification")
        begun = self.begin("legacy-reclassification-run")
        lease = str(begun["run_lease"])
        base = (
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            "frozen-legacy-authorization",
            "--action-type",
            "message",
            "--state",
            "authorized",
            "--source",
            "synthetic_test",
            "--authorization-note",
            "synthetic frozen authorization",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(*base, "--at", "2026-08-18T13:00:00Z")

        # Emulate a plan frozen by the pre-contract behavior: the action is on
        # the captured side of the ID floor, but its required work item remains.
        with sqlite3.connect(self.database) as conn:
            action_id = int(
                conn.execute(
                    "SELECT id FROM external_actions "
                    "WHERE action_key='frozen-legacy-authorization'"
                ).fetchone()[0]
            )
            run_scope = json.loads(
                conn.execute(
                    "SELECT scope_json FROM daily_runs "
                    "WHERE run_id='legacy-reclassification-run'"
                ).fetchone()[0]
            )
            run_scope["external_action_id_floor"] = action_id
            conn.execute(
                "UPDATE daily_runs SET scope_json=? "
                "WHERE run_id='legacy-reclassification-run'",
                (json.dumps(run_scope, sort_keys=True, separators=(",", ":")),),
            )
            conn.execute(
                "UPDATE external_actions SET metadata_json='{}' "
                "WHERE action_key='frozen-legacy-authorization'"
            )
            conn.commit()
            before_rows = conn.execute(
                "SELECT id, state, dedupe_key FROM external_actions "
                "WHERE action_key='frozen-legacy-authorization' ORDER BY id"
            ).fetchall()

        frozen = self.status("legacy-reclassification-run", verbose=True)
        self.assertEqual(frozen["external_action_scope"]["legacy_backlog"]["total"], 1)
        self.assertEqual(
            frozen["external_action_scope"]["legacy_backlog"]["frozen_required_items"],
            1,
        )
        ordinary_refresh = json.loads(
            self.run_cli(
                "refresh-daily-run-plan",
                "--run-id",
                "legacy-reclassification-run",
                "--reason",
                "synthetic ordinary refresh",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        self.assertEqual(ordinary_refresh["retained_prior_requirements"], 1)
        still_frozen = self.status("legacy-reclassification-run", verbose=True)
        frozen_item = next(
            item
            for item in still_frozen["work_items"]
            if item["step_key"] == "external_action_reconciliation"
        )
        self.assertEqual(frozen_item["required"], 1)

        explicit = json.loads(
            self.run_cli(
                "refresh-daily-run-plan",
                "--run-id",
                "legacy-reclassification-run",
                "--reason",
                "synthetic explicit legacy backlog migration",
                "--reclassify-legacy-external-actions",
                "--defer-render",
                "--run-lease",
                lease,
                "--json",
            ).stdout
        )
        reclassified = explicit["legacy_external_action_reclassification"]
        self.assertTrue(reclassified["changed"])
        self.assertEqual(reclassified["reclassified"], 1)
        self.assertEqual(reclassified["external_action_rows_changed"], 0)
        self.assertFalse(reclassified["delivery_inferred"])
        migrated = self.status("legacy-reclassification-run", verbose=True)
        migrated_item = next(
            item
            for item in migrated["work_items"]
            if item["step_key"] == "external_action_reconciliation"
        )
        self.assertEqual(migrated_item["required"], 0)
        self.assertEqual(migrated_item["state"], "not_applicable")
        with sqlite3.connect(self.database) as conn:
            after_rows = conn.execute(
                "SELECT id, state, dedupe_key FROM external_actions "
                "WHERE action_key='frozen-legacy-authorization' ORDER BY id"
            ).fetchall()
            self.assertEqual(after_rows, before_rows)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM daily_run_manifests "
                    "WHERE run_id='legacy-reclassification-run' "
                    "AND record_type='programmatic' "
                    "AND manifest_kind='legacy_external_action_backlog'"
                ).fetchone()[0],
                1,
            )

        self.run_cli(*base, "--at", "2026-08-18T14:00:00Z")
        restored = self.status("legacy-reclassification-run", verbose=True)
        restored_item = next(
            item
            for item in restored["work_items"]
            if item["step_key"] == "external_action_reconciliation"
        )
        self.assertEqual(restored_item["required"], 1)
        self.assertEqual(restored_item["state"], "pending")
        self.assertEqual(restored["external_action_scope"]["required_current_run"], 1)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT GROUP_CONCAT(state, ',') FROM external_actions "
                    "WHERE action_key='frozen-legacy-authorization' ORDER BY id"
                ).fetchone()[0],
                "authorized,authorized",
            )

    def test_interrupted_run_resume_end_to_end(self) -> None:
        begun = self.begin("synthetic-e2e-run")
        lease = str(begun["run_lease"])
        self.complete_inbound("synthetic-e2e-run", lease)
        verbose = self.status("synthetic-e2e-run", verbose=True)
        checkpoint_item = next(
            item
            for item in verbose["work_items"]
            if item["step_key"] == "hh_coverage"
        )
        item_key = checkpoint_item["item_key"]
        stream_key = json.loads(checkpoint_item["scope_json"])["stream_key"]
        checkpoint = self.workspace / "e2e-partial.json"
        checkpoint.write_text(
            json.dumps(
                {
                    "manifest_version": 1,
                    "kind": "hh_stream",
                    "run_id": "synthetic-e2e-run",
                    "step_key": "hh_coverage",
                    "item_key": item_key,
                    "observed_at": "2026-08-18T12:10:00Z",
                    "captured_scope": {
                        "stream_key": stream_key,
                        "last_verified_page": 2,
                    },
                    "counts": {
                        "raw": 200,
                        "unique": 180,
                        "known": 150,
                        "new": 30,
                        "processed": 200,
                        "blocked": 0,
                    },
                    "completion_boundary": {"last_verified_page": 2},
                    "remote_boundary_verified": False,
                    "blockers": [],
                }
            ),
            encoding="utf-8",
        )
        self.run_cli(
            "checkpoint-daily-run-work",
            "--run-id",
            "synthetic-e2e-run",
            "--step-key",
            "hh_coverage",
            "--item-key",
            item_key,
            "--manifest",
            str(checkpoint),
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            "pause-daily-run",
            "--run-id",
            "synthetic-e2e-run",
            "--reason",
            "synthetic process interruption",
            "--run-lease",
            lease,
            "--json",
        )
        blocked_rebuild = self.run_cli("rebuild", "--json", check=False)
        self.assertNotEqual(blocked_rebuild.returncode, 0)
        interrupted = self.status("synthetic-e2e-run")
        self.assertEqual(interrupted["status"], "paused")
        self.assertEqual(interrupted["next_safe_work"][0]["action"], "continue_from_checkpoint")
        resumed = json.loads(
            self.run_cli(
                "resume-daily-run", "--run-id", "synthetic-e2e-run", "--json"
            ).stdout
        )
        new_lease = str(resumed["run_lease"])
        self.assertNotEqual(new_lease, lease)
        self.check_hh("synthetic-e2e-run", new_lease)
        generations = self.workspace / ".jobctl" / "projections" / "generations"
        before_generations = {path.name for path in generations.iterdir() if path.is_dir()}
        finalized = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", new_lease, "--json"
            ).stdout
        )
        self.assertTrue(finalized["render_performed"])
        self.assertEqual(self.status("synthetic-e2e-run")["status"], "completed")
        after_generations = {path.name for path in generations.iterdir() if path.is_dir()}
        self.assertEqual(len(after_generations - before_generations), 1)
        strict_doctor = json.loads(
            self.run_cli("doctor", "--strict", "--json").stdout
        )
        self.assertTrue(strict_doctor["ok"])
        operational = json.loads(
            self.run_cli(
                "operational-doctor",
                "--run-id",
                "synthetic-e2e-run",
                "--strict",
                "--json",
            ).stdout
        )
        self.assertTrue(operational["ready_for_daily_closeout"])

    def test_compact_status_is_bounded_on_25000_synthetic_rows(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.executemany(
                """
                INSERT INTO vacancies (
                    channel, external_id, title, company, latest_status,
                    latest_stage, score, updated_at
                ) VALUES ('company_site', ?, 'Synthetic role', 'Synthetic employer',
                          'NEEDS_REVIEW', 'seen', 80, '2026-08-18T09:00:00')
                """,
                ((f"company_site:scale-{index}",) for index in range(25_000)),
            )
            conn.commit()
        self.run_cli("rebuild", "--json")
        begun = self.begin("scale-status-run")
        started = time.perf_counter()
        result = self.run_cli("daily-run-status", "--json")
        elapsed = time.perf_counter() - started
        self.assertLess(len(result.stdout.encode("utf-8")), 30_000)
        self.assertLess(elapsed, 3.0)
        status = json.loads(result.stdout)
        self.assertNotIn("vacancies", status)
        self.run_cli(
            "pause-daily-run",
            "--run-id",
            "scale-status-run",
            "--reason",
            "synthetic bounded-status check",
            "--run-lease",
            str(begun["run_lease"]),
            "--json",
        )
        resumed = json.loads(
            self.run_cli("resume-daily-run", "--run-id", "scale-status-run", "--json").stdout
        )
        self.assertTrue(resumed["write_flags"])


if __name__ == "__main__":
    unittest.main()
