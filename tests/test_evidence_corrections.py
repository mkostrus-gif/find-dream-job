from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JOBCTL = ROOT / "scripts" / "jobctl.py"


class EvidenceCorrectionIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(
            prefix="find-dream-job-corrections-"
        )
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.config = self.workspace / "synthetic-settings.toml"
        self.config.write_text(
            """
[project]
title = "Synthetic Evidence Correction Test"
locale = "en"

[profile]
files = ["private/profile.md"]
preferences_file = "private/preferences.md"
scoring_file = "private/scoring.md"
answers_file = "private/questions_and_answers.md"

[automation]
auto_apply = false
apply_threshold = 85
require_visible_confirmation = true

[follow_up]
limit = 3
interval_business_days = 5
primary_channel = "email"
direct_channels = ["linkedin"]
max_direct_messages_per_round = 1

[search]
required_streams = ["synthetic_stream"]
default_period_days = 3
items_per_page = 100
""".strip()
            + "\n",
            encoding="utf-8",
        )
        self.config_args = ("--config", str(self.config))
        self.run_cli("init", *self.config_args, "--json")
        self.database = self.workspace / "data" / "job_search.sqlite"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def run_cli(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
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

    def ingest(self, filename: str, row: dict[str, object]) -> None:
        path = self.workspace / "tmp" / filename
        path.write_text(json.dumps([row]), encoding="utf-8")
        self.run_cli("ingest-json", str(path), *self.config_args, "--json")

    def dashboard_data(self) -> dict[str, object]:
        dashboard = (self.workspace / "dashboard" / "index.html").read_text(
            encoding="utf-8"
        )
        match = re.search(r"\n    const DATA = (.*);\n    const STAGE_LABELS", dashboard)
        self.assertIsNotNone(match)
        return json.loads(match.group(1))

    @staticmethod
    def application_row(
        *, status: str, follow_up_date: str, questions: str
    ) -> dict[str, object]:
        return {
            "date": "2026-01-01",
            "channel": "company_site",
            "source": "synthetic_board",
            "source_stream": "synthetic_stream",
            "external_id": "company_site:synthetic-correction",
            "title": "Synthetic Product Lead",
            "company": "Example Labs",
            "description": "Generic synthetic role used only for regression testing.",
            "url": "https://example.com/jobs/synthetic-correction",
            "kind": "application",
            "status": status,
            "stage": "follow_up",
            "score": 80,
            "open_questions": questions,
            "follow_up_date": follow_up_date,
            "why_applied": "Generic synthetic fit",
        }

    def confirm_application(self) -> None:
        common = (
            "--external-id",
            "company_site:synthetic-correction",
            "--action-key",
            "synthetic-application-v1",
            "--action-type",
            "application",
            "--source",
            "synthetic_test",
            *self.config_args,
        )
        self.run_cli(
            "record-external-action",
            *common,
            "--state",
            "authorized",
            "--at",
            "2026-01-01T10:00:00",
            "--authorization-note",
            "Synthetic authorization",
            "--json",
        )
        self.run_cli(
            "record-external-action",
            *common,
            "--state",
            "visibly_confirmed",
            "--at",
            "2026-01-01T10:05:00",
            "--evidence-at",
            "2026-01-01T10:05:00",
            "--evidence-note",
            "Synthetic visible application confirmation",
            "--external-reference",
            "synthetic-confirmation-reference",
            "--json",
        )

    def test_invalidation_removes_false_reply_and_deduplicates_followups(self) -> None:
        vacancy = self.application_row(
            status="NEEDS_REVIEW",
            follow_up_date="",
            questions="",
        )
        vacancy.update({"kind": "screening", "stage": "seen"})
        self.ingest(
            "01-vacancy.json",
            vacancy,
        )
        self.confirm_application()
        self.run_cli(
            "update-vacancy",
            "--external-id",
            "company_site:synthetic-correction",
            "--date",
            "2026-01-02",
            "--status",
            "NEEDS_INPUT_OBSOLETE",
            "--stage",
            "follow_up",
            "--open-questions",
            "Synthetic question one; synthetic question two",
            "--follow-up-date",
            "2026-01-10",
            "--sync-application",
            *self.config_args,
        )
        self.ingest(
            "02-current-application.json",
            self.application_row(
                status="NEEDS_INPUT_CURRENT",
                follow_up_date="2026-01-12",
                questions="Synthetic question one; synthetic question two",
            ),
        )
        interaction = json.loads(
            self.run_cli(
                "record-employer-interaction",
                "--external-id",
                "company_site:synthetic-correction",
                "--at",
                "2026-01-05T12:00:00",
                "--direction",
                "inbound",
                "--event-type",
                "screening_request",
                "--channel",
                "email",
                "--actor-type",
                "recruiter",
                "--humanity",
                "human",
                "--evidence-note",
                "Synthetic claim containing two employer questions",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.run_cli(
            "set-current-action",
            "--external-id",
            "company_site:synthetic-correction",
            "--action-state",
            "needs_input",
            "--bucket",
            "urgent",
            "--at",
            "2026-01-05T12:05:00",
            "--due-date",
            "2026-01-06",
            "--priority-reason",
            "Answer two synthetic questions",
            "--source",
            "synthetic_test",
            *self.config_args,
            "--json",
        )

        before = self.dashboard_data()
        self.assertEqual(before["kpis"]["human_replies"], 1)
        self.assertEqual(before["kpis"]["screening_requests"], 1)
        self.assertEqual(len(before["followups"]), 1)
        self.assertEqual(before["followups"][0]["status"], "NEEDS_INPUT_CURRENT")
        self.assertEqual(before["kpis"]["needs_user"], 1)

        correction_args = (
            "invalidate-employer-interaction",
            "--interaction-id",
            str(interaction["interaction_id"]),
            "--vacancy-external-id",
            "company_site:synthetic-correction",
            "--corrected-at",
            "2026-01-06T09:00:00",
            "--reason",
            "The interaction was recorded in error",
            "--evidence-note",
            "Synthetic operator verification found no employer message",
            "--source",
            "synthetic_operator_review",
            "--operator-context",
            "regression_test",
            *self.config_args,
            "--json",
        )
        first_correction = json.loads(self.run_cli(*correction_args).stdout)
        repeated_correction = json.loads(self.run_cli(*correction_args).stdout)
        self.assertTrue(first_correction["created"])
        self.assertFalse(repeated_correction["created"])
        self.assertEqual(
            first_correction["invalidation_id"],
            repeated_correction["invalidation_id"],
        )
        self.assertEqual(first_correction["dedupe_key"], repeated_correction["dedupe_key"])

        self.run_cli(
            "update-vacancy",
            "--external-id",
            "company_site:synthetic-correction",
            "--date",
            "2026-01-06",
            "--status",
            "WAITING_FOLLOW_UP",
            "--stage",
            "follow_up",
            "--open-questions",
            "",
            "--next-action",
            "Wait for the scheduled follow-up",
            "--follow-up-date",
            "2026-02-20",
            "--sync-application",
            *self.config_args,
        )
        self.run_cli("rebuild", *self.config_args, "--json")

        scorecard = json.loads(
            self.run_cli(
                "outcome-scorecard",
                "--as-of",
                "2026-02-28",
                *self.config_args,
                "--json",
            ).stdout
        )
        conversion = json.loads(
            self.run_cli(
                "conversion-report",
                "--as-of",
                "2026-02-28",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertEqual(scorecard["overall"]["confirmed_applications"], 1)
        self.assertEqual(scorecard["overall"]["recorded_inbound_human_replies"], 0)
        self.assertEqual(scorecard["overall"]["screening_requests"], 0)
        self.assertEqual(scorecard["overall"]["human_reply_rate_14d"]["percent"], 0.0)
        self.assertEqual(conversion["overall"]["applications_unique"], 1)
        self.assertEqual(conversion["overall"]["human_replies"], 0)
        self.assertEqual(conversion["overall"]["screening_requests"], 0)

        after = self.dashboard_data()
        self.assertEqual(after["kpis"]["human_replies"], 0)
        self.assertEqual(after["kpis"]["screening_requests"], 0)
        self.assertEqual(after["kpis"]["applied"], 1)
        self.assertEqual(after["kpis"]["applications_unique"], 1)
        self.assertEqual(after["kpis"]["needs_user"], 0)
        self.assertEqual(len(after["followups"]), 1)
        self.assertEqual(after["followups"][0]["follow_up_date"], "2026-02-20")
        vacancy = next(
            item
            for item in after["vacancies"]
            if item["external_id"] == "company_site:synthetic-correction"
        )
        self.assertEqual(vacancy["latest_status"], "WAITING_FOLLOW_UP")
        self.assertEqual(vacancy["current_action_state"], "follow_up")
        self.assertEqual(vacancy["open_questions"], "")

        followups_md = (self.workspace / "views" / "followups.md").read_text(
            encoding="utf-8"
        )
        self.assertEqual(followups_md.count("Synthetic Product Lead"), 1)
        self.assertIn("2026-02-20", followups_md)
        self.assertNotIn("Synthetic question one", followups_md)
        self.assertNotIn("NEEDS_INPUT", followups_md)

        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM employer_interactions").fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM employer_interaction_invalidations"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM effective_employer_interactions"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM lifecycle_events "
                    "WHERE event_type = 'application_confirmed'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0], 2)
            obsolete_application = conn.execute(
                "SELECT status, follow_up_date FROM applications ORDER BY id LIMIT 1"
            ).fetchone()
            self.assertEqual(
                obsolete_application,
                ("NEEDS_INPUT_OBSOLETE", "2026-01-10"),
            )
            effective_application = conn.execute(
                "SELECT status, stage, follow_up_date FROM effective_applications"
            ).fetchone()
            self.assertEqual(
                effective_application,
                ("WAITING_FOLLOW_UP", "follow_up", "2026-02-20"),
            )
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

        doctor = json.loads(
            self.run_cli("doctor", "--strict", *self.config_args, "--json").stdout
        )
        self.assertTrue(doctor["ok"])

    def test_invalidation_fails_closed_for_missing_mismatched_or_ambiguous_input(self) -> None:
        self.ingest(
            "correction-target.json",
            self.application_row(
                status="APPLIED_CONFIRMED",
                follow_up_date="2026-02-20",
                questions="",
            ),
        )
        other = self.application_row(
            status="APPLIED_CONFIRMED",
            follow_up_date="2026-02-21",
            questions="",
        )
        other.update(
            {
                "external_id": "company_site:other-synthetic-vacancy",
                "title": "Other Synthetic Role",
                "url": "https://example.com/jobs/other-synthetic-vacancy",
            }
        )
        self.ingest("other-vacancy.json", other)
        interaction = json.loads(
            self.run_cli(
                "record-employer-interaction",
                "--external-id",
                "company_site:synthetic-correction",
                "--event-type",
                "screening_request",
                "--channel",
                "email",
                "--actor-type",
                "recruiter",
                "--humanity",
                "human",
                "--evidence-note",
                "Synthetic erroneous interaction",
                *self.config_args,
                "--json",
            ).stdout
        )
        base = (
            "--reason",
            "Synthetic correction reason",
            "--evidence-note",
            "Synthetic correction evidence",
            "--source",
            "synthetic_test",
            *self.config_args,
            "--json",
        )
        missing = self.run_cli(
            "invalidate-employer-interaction",
            "--interaction-id",
            "999999",
            *base,
            check=False,
        )
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("не найдено", missing.stderr)

        mismatch = self.run_cli(
            "invalidate-employer-interaction",
            "--interaction-id",
            str(interaction["interaction_id"]),
            "--vacancy-external-id",
            "company_site:other-synthetic-vacancy",
            *base,
            check=False,
        )
        self.assertNotEqual(mismatch.returncode, 0)
        self.assertIn("другой вакансии", mismatch.stderr)

        empty_reason = self.run_cli(
            "invalidate-employer-interaction",
            "--interaction-id",
            str(interaction["interaction_id"]),
            "--reason",
            "",
            "--evidence-note",
            "Synthetic correction evidence",
            "--source",
            "synthetic_test",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(empty_reason.returncode, 0)
        self.assertIn("--reason", empty_reason.stderr)

        empty_evidence = self.run_cli(
            "invalidate-employer-interaction",
            "--interaction-id",
            str(interaction["interaction_id"]),
            "--reason",
            "Synthetic correction reason",
            "--evidence-note",
            "",
            "--source",
            "synthetic_test",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(empty_evidence.returncode, 0)
        self.assertIn("--evidence-note", empty_evidence.stderr)

        self.run_cli(
            "invalidate-employer-interaction",
            "--interaction-id",
            str(interaction["interaction_id"]),
            *base,
        )
        conflicting = self.run_cli(
            "invalidate-employer-interaction",
            "--interaction-id",
            str(interaction["interaction_id"]),
            "--reason",
            "Different reason",
            "--evidence-note",
            "Synthetic correction evidence",
            "--source",
            "synthetic_test",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(conflicting.returncode, 0)
        self.assertIn("неоднозначное", conflicting.stderr)

    def test_v6_to_v8_migration_is_backed_up_and_idempotent(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.executescript(
                """
                DROP VIEW effective_applications;
                DROP VIEW effective_employer_interactions;
                DROP TABLE employer_interaction_invalidations;
                DROP INDEX idx_employer_interactions_identity;
                PRAGMA user_version = 6;
                """
            )
            conn.commit()

        migrated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertEqual(migrated["from_version"], 6)
        self.assertEqual(migrated["to_version"], 10)
        self.assertTrue(migrated["backup"])
        backups = list(self.database.parent.glob("job_search.sqlite.bak-schema-v6-*"))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as backup:
            self.assertEqual(backup.execute("PRAGMA user_version").fetchone()[0], 6)
            self.assertIsNone(
                backup.execute(
                    "SELECT 1 FROM sqlite_master WHERE name = "
                    "'employer_interaction_invalidations'"
                ).fetchone()
            )
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 10)
            self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

        repeated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertTrue(repeated["already_current"])
        self.assertEqual(len(list(self.database.parent.glob("job_search.sqlite.bak-schema-v6-*"))), 1)


if __name__ == "__main__":
    unittest.main()
