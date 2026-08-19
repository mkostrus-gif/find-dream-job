from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JOBCTL = ROOT / "scripts" / "jobctl.py"
PUBLIC_AUDIT = ROOT / "scripts" / "public_audit.py"


class JobctlIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-test-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)

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

    def test_init_is_non_destructive_and_doctor_passes(self) -> None:
        first = json.loads(self.run_cli("init", "--json").stdout)
        self.assertIn("config/settings.toml", first["created"])
        self.assertTrue((self.workspace / "private" / "profile.md").is_file())
        self.assertTrue((self.workspace / "data" / "job_search.sqlite").is_file())
        self.assertTrue((self.workspace / "dashboard" / "index.html").is_file())

        profile = self.workspace / "private" / "profile.md"
        profile.write_text("private sentinel\n", encoding="utf-8")
        second = json.loads(self.run_cli("init", "--json").stdout)
        self.assertIn("private/profile.md", second["kept"])
        self.assertEqual(profile.read_text(encoding="utf-8"), "private sentinel\n")

        doctor = json.loads(self.run_cli("doctor", "--strict", "--json").stdout)
        self.assertTrue(doctor["ok"])
        self.assertFalse(doctor["auto_apply"])
        self.assertEqual(doctor["apply_threshold"], 85)
        self.assertFalse(doctor["scan_linkedin_inbox"])
        self.assertFalse(doctor["archive_processed_linkedin"])

        with sqlite3.connect(self.workspace / "data" / "job_search.sqlite") as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 10)
            self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")

    def test_ingest_update_and_inline_json_escaping(self) -> None:
        self.run_cli("init", "--json")
        payload_path = self.workspace / "tmp" / "vacancies.json"
        payload_path.write_text(
            json.dumps(
                {
                    "vacancies": [
                        {
                            "date": "2026-01-15",
                            "channel": "company_site",
                            "source": "test",
                            "kind": "screening",
                            "title": "</script><script>alert(1)</script>",
                            "company": "Example Labs",
                            "url": "https://example.com/jobs/1",
                            "status": "NEEDS_REVIEW",
                            "stage": "seen",
                            "score": 77,
                        },
                        {
                            "date": "2026-01-15",
                            "channel": "company_site",
                            "source": "test",
                            "kind": "screening",
                            "title": "Unsafe Link Example",
                            "company": "Example Labs",
                            "url": "javascript:alert(1)",
                            "status": "NEEDS_REVIEW",
                            "stage": "seen",
                            "score": 70,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.run_cli("ingest-json", str(payload_path), "--json")

        with sqlite3.connect(self.workspace / "data" / "job_search.sqlite") as conn:
            vacancy_id = conn.execute("SELECT id FROM vacancies").fetchone()[0]
        self.run_cli(
            "update-vacancy",
            "--id",
            str(vacancy_id),
            "--stage",
            "applied",
            "--status",
            "APPLIED_CONFIRMED",
            "--note",
            "Visible success",
        )

        dashboard = (self.workspace / "dashboard" / "index.html").read_text(encoding="utf-8")
        self.assertNotIn("</script><script>alert(1)</script>", dashboard)
        self.assertIn("\\u003c/script\\u003e", dashboard)
        today_view = (self.workspace / "views" / "today.md").read_text(encoding="utf-8")
        self.assertNotIn("](javascript:alert(1))", today_view)

        stats = json.loads(self.run_cli("stats").stdout)
        self.assertEqual(stats["vacancies"], 1)
        self.assertEqual(stats["applied"], 0)
        self.assertEqual(stats["quarantine_pending"], 1)

    def test_linkedin_gmail_ingest_uses_stable_job_identity_and_supports_screening(self) -> None:
        self.run_cli("init", "--json")
        mail_payload = self.workspace / "tmp" / "linkedin-mail.json"
        mail_payload.write_text(
            json.dumps(
                {
                    "vacancies": [
                        {
                            "date": "2026-08-01",
                            "title": "Head of Product",
                            "company": "Example Labs",
                            "url": "https://www.linkedin.com/jobs/view/head-of-product-at-example-labs-4321098765/?trackingId=mail",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        result = self.run_cli(
            "ingest-gmail-json",
            str(mail_payload),
            "--provider",
            "linkedin",
            "--json",
        )
        self.assertIn('"provider": "linkedin"', result.stdout)

        screening_payload = self.workspace / "tmp" / "linkedin-screening.json"
        screening_payload.write_text(
            json.dumps(
                {
                    "vacancies": [
                        {
                            "date": "2026-08-01",
                            "kind": "screening",
                            "title": "Head of Product",
                            "company": "Example Labs",
                            "url": "https://linkedin.com/jobs/view/4321098765/",
                            "status": "NEEDS_REVIEW",
                            "stage": "seen",
                            "score": 88,
                            "reason": "Strong mandate fit",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.run_cli(
            "ingest-json",
            str(screening_payload),
            "--channel",
            "linkedin",
            "--source",
            "linkedin_gmail_job_alert",
        )

        with sqlite3.connect(self.workspace / "data" / "job_search.sqlite") as conn:
            vacancy = conn.execute(
                """
                SELECT channel, source, external_id, latest_status, score
                FROM vacancies
                """
            ).fetchone()
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 1)
            self.assertEqual(
                vacancy,
                (
                    "linkedin",
                    "linkedin_gmail_job_alert",
                    "linkedin:4321098765",
                    "NEEDS_REVIEW",
                    88,
                ),
            )
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM source_hits").fetchone()[0], 2)

    def test_gmail_ingest_keeps_hh_as_the_backward_compatible_default(self) -> None:
        self.run_cli("init", "--json")
        payload = self.workspace / "tmp" / "hh-mail.json"
        payload.write_text(
            json.dumps(
                {
                    "vacancies": [
                        {
                            "date": "2026-08-01",
                            "title": "Product Lead",
                            "company": "Example Labs",
                            "url": "https://hh.ru/vacancy/123456789",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.run_cli("ingest-gmail-json", str(payload))

        with sqlite3.connect(self.workspace / "data" / "job_search.sqlite") as conn:
            vacancy = conn.execute(
                "SELECT channel, source, external_id, latest_status FROM vacancies"
            ).fetchone()
        self.assertEqual(
            vacancy,
            (
                "gmail_hh",
                "hh_gmail_digest",
                "hh:123456789",
                "DISCOVERED_FROM_GMAIL",
            ),
        )

    def test_explicit_config_after_subcommand_is_supported(self) -> None:
        config = self.workspace / "custom.toml"
        config.write_text(
            """
[project]
title = "Custom Workspace"
locale = "en"

[profile]
files = ["private/profile.md"]
preferences_file = "private/preferences.md"
scoring_file = "private/scoring.md"
answers_file = "private/questions_and_answers.md"

[follow_up]
primary_channel = "email"
direct_channels = ["signal"]
limit = 2
interval_business_days = 4
max_direct_messages_per_round = 1
""".strip()
            + "\n",
            encoding="utf-8",
        )
        self.run_cli("init", "--config", str(config), "--json")
        dashboard = (self.workspace / "dashboard" / "index.html").read_text(encoding="utf-8")
        self.assertIn("<title>Custom Workspace</title>", dashboard)
        doctor = json.loads(
            self.run_cli("doctor", "--config", str(config), "--strict", "--json").stdout
        )
        self.assertTrue(doctor["ok"])

    def test_configured_follow_up_channels_and_limit(self) -> None:
        config = self.workspace / "follow-up.toml"
        config.write_text(
            """
[project]
title = "Follow-up Test"
locale = "en"

[profile]
files = ["private/profile.md"]
preferences_file = "private/preferences.md"
scoring_file = "private/scoring.md"
answers_file = "private/questions_and_answers.md"

[follow_up]
primary_channel = "email"
direct_channels = ["signal"]
limit = 2
interval_business_days = 4
max_direct_messages_per_round = 1
""".strip()
            + "\n",
            encoding="utf-8",
        )
        config_args = ("--config", str(config))
        self.run_cli("init", *config_args, "--json")

        payload_path = self.workspace / "tmp" / "application.json"
        payload_path.write_text(
            json.dumps(
                {
                    "vacancies": [
                        {
                            "date": "2026-01-02",
                            "channel": "company_site",
                            "source": "test",
                            "kind": "application",
                            "title": "Operations Lead",
                            "company": "Example Labs",
                            "url": "https://example.com/jobs/follow-up",
                            "status": "APPLIED_CONFIRMED",
                            "stage": "applied",
                            "score": 90,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.run_cli("ingest-json", str(payload_path), *config_args)
        database = self.workspace / "data" / "job_search.sqlite"
        with sqlite3.connect(database) as conn:
            vacancy_id = conn.execute("SELECT id FROM vacancies").fetchone()[0]

        self.run_cli(
            "upsert-contact",
            "--id",
            str(vacancy_id),
            "--person-name",
            "Example Recruiter",
            "--relationship",
            "recruiter",
            "--confidence",
            "confirmed",
            "--contact-channel",
            "signal",
            "--contact-address",
            "example-handle",
            "--evidence-note",
            "Official vacancy contact",
            *config_args,
        )
        with sqlite3.connect(database) as conn:
            contact_id = conn.execute("SELECT id FROM employer_contacts").fetchone()[0]

        outreach = self.workspace / "tmp" / "outreach.json"
        action_key = "synthetic-follow-up-message"
        self.run_cli(
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            action_key,
            "--action-type",
            "follow_up",
            "--state",
            "authorized",
            "--authorization-note",
            "Synthetic test authorization",
            "--source",
            "synthetic_test",
            *config_args,
        )
        self.run_cli(
            "record-external-action",
            "--id",
            str(vacancy_id),
            "--action-key",
            action_key,
            "--action-type",
            "follow_up",
            "--state",
            "visibly_confirmed",
            "--evidence-note",
            "Synthetic visible sent marker",
            "--external-reference",
            "synthetic-message-1",
            "--source",
            "synthetic_test",
            *config_args,
        )
        outreach.write_text(
            json.dumps(
                {
                    "contact_search": {
                        "status": "reused_verified_contact",
                        "channels_checked": ["signal"],
                        "note": "Verified stored contact",
                    },
                    "touchpoints": [
                        {
                            "channel": "signal",
                            "contact_id": contact_id,
                            "message_text": "Hello from the synthetic test.",
                            "delivery_status": "sent",
                            "evidence_note": "Visible sent marker",
                            "external_action_key": action_key,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.run_cli(
            "record-followup",
            "--id",
            str(vacancy_id),
            "--date",
            "2026-01-05",
            "--outreach-json",
            str(outreach),
            *config_args,
        )
        with sqlite3.connect(database) as conn:
            first = conn.execute(
                "SELECT latest_stage, latest_status, follow_up_date FROM vacancies"
            ).fetchone()
        self.assertEqual(first, ("follow_up", "FOLLOW_UP_1_SENT_WAITING_EMPLOYER", "2026-01-09"))

        self.run_cli(
            "record-followup",
            "--id",
            str(vacancy_id),
            "--date",
            "2026-01-09",
            "--outreach-json",
            str(outreach),
            *config_args,
        )
        with sqlite3.connect(database) as conn:
            second = conn.execute(
                "SELECT latest_stage, latest_status, follow_up_date FROM vacancies"
            ).fetchone()
        self.assertEqual(
            second,
            ("applied", "FOLLOW_UP_2_SENT_LIMIT_REACHED_WAITING_EMPLOYER", ""),
        )


class PublicTreeTests(unittest.TestCase):
    def test_public_tree_audit(self) -> None:
        result = subprocess.run(
            [sys.executable, str(PUBLIC_AUDIT), "--strict", "--json"],
            cwd=ROOT,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            self.fail(
                f"public audit failed ({result.returncode})\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        report = json.loads(result.stdout)
        self.assertTrue(report["ok"])
        self.assertNotIn("data/job_search.sqlite", report["candidates"])
        self.assertFalse(any(path.startswith("private/") for path in report["candidates"]))


if __name__ == "__main__":
    unittest.main()
