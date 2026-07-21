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

        with sqlite3.connect(self.workspace / "data" / "job_search.sqlite") as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 2)
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
        self.assertEqual(stats["vacancies"], 2)
        self.assertEqual(stats["applied"], 1)

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
