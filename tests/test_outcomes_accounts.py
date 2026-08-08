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


class OutcomeAndAccountIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-outcomes-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.config = self.workspace / "synthetic-settings.toml"
        self.config.write_text(
            """
[project]
title = "Synthetic Outcome Test"
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
required_streams = ["recommendations", "target_roles"]
default_period_days = 3
items_per_page = 100

[source_stream_aliases]
"legacy product" = "product_roles"
"other legacy" = "secondary_roles"
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

    def ingest(self, name: str, rows: list[dict[str, object]]) -> None:
        path = self.workspace / "tmp" / name
        path.write_text(json.dumps(rows), encoding="utf-8")
        self.run_cli("ingest-json", str(path), *self.config_args, "--json")

    @staticmethod
    def vacancy_row(
        external_id: str,
        *,
        date: str,
        source_stream: str,
        kind: str = "screening",
        stage: str = "seen",
        status: str = "NEEDS_REVIEW",
        score: int = 80,
        factors: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        row: dict[str, object] = {
            "date": date,
            "channel": "company_site",
            "source": "synthetic_board",
            "source_stream": source_stream,
            "external_id": external_id,
            "title": "Product General Manager",
            "company": "Example Labs",
            "description": "Own a synthetic product portfolio and operating plan. " * 8,
            "url": f"https://example.com/jobs/{external_id.rsplit(':', 1)[-1]}",
            "kind": kind,
            "stage": stage,
            "status": status,
            "score": score,
            "reason": "Synthetic evidence",
        }
        if factors is not None:
            row["factors"] = factors
        return row

    def test_new_workspace_uses_schema_v5_and_safe_defaults(self) -> None:
        expected = {
            "employer_interactions",
            "employer_accounts",
            "employer_account_signals",
            "vacancy_employer_accounts",
            "vacancy_factors",
            "source_checkpoints",
        }
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 5)
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            self.assertTrue(expected.issubset(tables))
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(source_hits)")
            }
            self.assertIn("canonical_source_stream", columns)
        doctor = json.loads(
            self.run_cli("doctor", *self.config_args, "--strict", "--json").stdout
        )
        self.assertTrue(doctor["ok"])
        self.assertFalse(doctor["auto_apply"])

    def test_interactions_conversion_deduplication_and_first_touch(self) -> None:
        external_id = "company_site:conversion-a"
        self.ingest(
            "01-first-touch.json",
            [
                self.vacancy_row(
                    external_id,
                    date="2026-01-01",
                    source_stream="Legacy Product",
                )
            ],
        )
        self.ingest(
            "02-second-touch.json",
            [
                self.vacancy_row(
                    external_id,
                    date="2026-01-01",
                    source_stream="Other Legacy",
                )
            ],
        )
        application = self.vacancy_row(
            external_id,
            date="2026-01-02",
            source_stream="legacy product",
            kind="application",
            stage="applied",
            status="APPLIED_CONFIRMED",
            score=82,
        )
        self.ingest("03-application.json", [application])
        self.ingest("04-duplicate-application-row.json", [application])
        recent_application = self.vacancy_row(
            "company_site:conversion-recent",
            date="2026-02-10",
            source_stream="Unmapped Stream",
            kind="application",
            stage="applied",
            status="APPLIED_CONFIRMED",
        )
        recent_application["title"] = "Operations Director"
        recent_application["description"] = (
            "Own synthetic service operations, capacity, and quality systems. " * 8
        )
        self.ingest("05-recent-application.json", [recent_application])

        automated = json.loads(
            self.run_cli(
                "record-employer-interaction",
                "--external-id",
                external_id,
                "--at",
                "2026-01-03T09:00:00",
                "--event-type",
                "automated_ack",
                "--channel",
                "email",
                "--actor-type",
                "system",
                "--humanity",
                "automated",
                "--evidence-note",
                "Synthetic receipt",
                "--external-reference",
                "message-ack-1",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertTrue(automated["created"])
        repeated = json.loads(
            self.run_cli(
                "record-employer-interaction",
                "--external-id",
                external_id,
                "--at",
                "2026-01-03T09:00:00",
                "--event-type",
                "automated_ack",
                "--channel",
                "email",
                "--actor-type",
                "system",
                "--humanity",
                "automated",
                "--evidence-note",
                "Synthetic receipt",
                "--external-reference",
                "message-ack-1",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertFalse(repeated["created"])
        self.run_cli(
            "record-employer-interaction",
            "--external-id",
            external_id,
            "--at",
            "2026-01-04T00:00:00",
            "--event-type",
            "human_reply",
            "--channel",
            "email",
            "--actor-type",
            "recruiter",
            "--humanity",
            "human",
            "--evidence-note",
            "Synthetic recruiter reply",
            *self.config_args,
            "--json",
        )

        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT latest_stage FROM vacancies WHERE external_id = ?",
                    (external_id,),
                ).fetchone()[0],
                "applied",
            )
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0], 3)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM employer_interactions").fetchone()[0],
                2,
            )
            stream_rows = conn.execute(
                "SELECT source_stream, canonical_source_stream FROM source_hits"
            ).fetchall()
            self.assertIn(("Legacy Product", "product_roles"), stream_rows)
            self.assertIn(("Other Legacy", "secondary_roles"), stream_rows)
            self.assertIn(("Unmapped Stream", "Unmapped Stream"), stream_rows)

        self.run_cli(
            "upsert-contact",
            "--external-id",
            external_id,
            "--person-name",
            "Example Recruiter",
            "--relationship",
            "recruiter",
            "--confidence",
            "confirmed",
            "--contact-channel",
            "linkedin",
            "--contact-address",
            "example-recruiter",
            "--evidence-note",
            "Synthetic official directory",
            "--verified-date",
            "2026-01-05",
            *self.config_args,
        )
        self.run_cli(
            "record-contact-search",
            "--external-id",
            external_id,
            "--date",
            "2026-01-05",
            "--status",
            "found",
            "--channels-checked",
            "linkedin",
            "--note",
            "Synthetic contact search completed",
            *self.config_args,
        )
        self.run_cli(
            "update-vacancy",
            "--external-id",
            external_id,
            "--date",
            "2026-01-10",
            "--stage",
            "interview_1",
            "--status",
            "INTERVIEW_1_CONFIRMED",
            "--note",
            "Synthetic calendar invitation evidence",
            *self.config_args,
        )

        report = json.loads(
            self.run_cli(
                "conversion-report",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )
        overall = report["overall"]
        self.assertEqual(overall["applications_unique"], 2)
        self.assertEqual(overall["matured_applications_14d"], 1)
        self.assertEqual(overall["human_replies"], 1)
        self.assertEqual(overall["human_reply_rate_14d"], 100.0)
        self.assertEqual(overall["matured_applications_30d"], 1)
        self.assertEqual(overall["interview_1_ever"], 1)
        self.assertEqual(overall["interview_1_rate_30d"], 100.0)
        self.assertEqual(overall["median_time_to_first_human_reply_days"], 2.0)
        self.assertEqual(overall["verified_contact_coverage"], 50.0)
        self.assertEqual(overall["contact_search_coverage"], 50.0)
        streams = {
            row["source_stream"]: row
            for row in report["breakdowns"]["source_stream"]
        }
        self.assertEqual(streams["product_roles"]["applications_unique"], 1)
        self.assertEqual(streams["Unmapped Stream"]["applications_unique"], 1)

        source_quality = (self.workspace / "reports" / "source_quality.md").read_text(
            encoding="utf-8"
        )
        diagnostics = (self.workspace / "reports" / "source_streams.md").read_text(
            encoding="utf-8"
        )
        conversion_md = (self.workspace / "reports" / "conversion_cohorts.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("product_roles", source_quality)
        self.assertIn("1/1 (100.0%)", source_quality)
        self.assertIn("Unmapped Stream", diagnostics)
        self.assertIn("unmapped/identity", diagnostics)
        self.assertIn("earliest source hit", conversion_md)

    def test_missing_interaction_history_is_reported_as_na(self) -> None:
        self.ingest(
            "application-without-interactions.json",
            [
                self.vacancy_row(
                    "company_site:no-history",
                    date="2026-01-01",
                    source_stream="Legacy Product",
                    kind="application",
                    stage="applied",
                    status="APPLIED_CONFIRMED",
                )
            ],
        )
        report = json.loads(
            self.run_cli(
                "conversion-report",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertFalse(report["interaction_history_available"])
        self.assertIsNone(report["overall"]["human_replies"])
        self.assertIsNone(report["overall"]["human_reply_rate_14d"])
        dashboard = (self.workspace / "dashboard" / "index.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("Historical employer interactions are absent", dashboard)

    def test_interaction_validation_and_alias_resolution(self) -> None:
        canonical_id = "company_site:interaction-canonical"
        alias_id = "company_site:interaction-alias"
        description = "Same synthetic mandate and operating outcomes. " * 10
        first = self.vacancy_row(
            canonical_id,
            date="2026-01-01",
            source_stream="Legacy Product",
        )
        second = self.vacancy_row(
            alias_id,
            date="2026-01-02",
            source_stream="Legacy Product",
        )
        first["description"] = description
        second["description"] = description
        self.ingest("aliases.json", [first, second])
        missing = self.run_cli(
            "record-employer-interaction",
            "--external-id",
            alias_id,
            "--event-type",
            "human_reply",
            "--channel",
            "email",
            "--actor-type",
            "recruiter",
            "--humanity",
            "human",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("evidence-note", missing.stderr)
        invalid = self.run_cli(
            "record-employer-interaction",
            "--external-id",
            alias_id,
            "--event-type",
            "not-a-type",
            "--channel",
            "email",
            "--humanity",
            "human",
            "--evidence-note",
            "Synthetic evidence",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(invalid.returncode, 0)
        self.assertIn("invalid choice", invalid.stderr)
        unresolved = self.run_cli(
            "record-employer-interaction",
            "--external-id",
            "company_site:missing",
            "--event-type",
            "human_reply",
            "--channel",
            "email",
            "--actor-type",
            "recruiter",
            "--humanity",
            "human",
            "--evidence-note",
            "Synthetic evidence",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(unresolved.returncode, 0)
        self.assertIn("Vacancy not found", unresolved.stderr)

    def test_accounts_signals_and_factors_do_not_change_score_or_fuzzy_link(self) -> None:
        external_id = "company_site:account-factor"
        malicious = "high|</script><script>alert(1)</script>"
        self.ingest(
            "factor-ingest.json",
            [
                self.vacancy_row(
                    external_id,
                    date="2026-01-01",
                    source_stream="Legacy Product",
                    score=88,
                    factors=[
                        {
                            "factor_key": "technology_adoption_maturity",
                            "value": malicious,
                            "observed_date": "2026-01-01",
                            "evidence_note": malicious,
                            "evidence_url": "https://example.com/evidence",
                            "confidence": "high",
                        }
                    ],
                )
            ],
        )
        account = json.loads(
            self.run_cli(
                "upsert-employer-account",
                "--canonical-name",
                "Example Labs",
                "--website",
                "https://example.com",
                "--careers-url",
                "https://example.com/careers",
                "--country-market",
                "Example Market",
                "--priority",
                "high",
                "--status",
                "target",
                "--last-checked-date",
                "2026-01-05",
                "--notes",
                malicious,
                *self.config_args,
                "--json",
            ).stdout
        )
        same_account = json.loads(
            self.run_cli(
                "upsert-employer-account",
                "--canonical-name",
                " example labs ",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertEqual(account["account_id"], same_account["account_id"])
        other = json.loads(
            self.run_cli(
                "upsert-employer-account",
                "--canonical-name",
                "Example Labs Ventures",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.run_cli(
            "link-vacancy-account",
            "--external-id",
            external_id,
            "--account-id",
            str(account["account_id"]),
            "--evidence-note",
            "Explicit synthetic link",
            *self.config_args,
            "--json",
        )
        ambiguous_relink = self.run_cli(
            "link-vacancy-account",
            "--external-id",
            external_id,
            "--account-id",
            str(other["account_id"]),
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(ambiguous_relink.returncode, 0)
        self.assertIn("already linked", ambiguous_relink.stderr)
        self.run_cli(
            "record-employer-signal",
            "--account-id",
            str(account["account_id"]),
            "--signal-type",
            "ai_adoption",
            "--observed-date",
            "2026-01-06",
            "--confidence",
            "confirmed",
            "--evidence-url",
            "https://example.com/news",
            "--evidence-note",
            malicious,
            *self.config_args,
            "--json",
        )
        self.run_cli(
            "record-vacancy-factor",
            "--external-id",
            external_id,
            "--factor-key",
            "human_access",
            "--value",
            "verified recruiter",
            "--observed-date",
            "2026-01-07",
            "--confidence",
            "confirmed",
            "--evidence-note",
            "Synthetic directory evidence",
            *self.config_args,
            "--json",
        )

        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT score FROM vacancies").fetchone()[0], 88)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM employer_accounts").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM employer_account_signals").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancy_factors").fetchone()[0], 2)
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

        accounts_view = (self.workspace / "views" / "employer_accounts.md").read_text(
            encoding="utf-8"
        )
        factors_view = (self.workspace / "views" / "vacancy_factors.md").read_text(
            encoding="utf-8"
        )
        dashboard = (self.workspace / "dashboard" / "index.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("Employer Account Radar", accounts_view)
        self.assertNotIn("</script><script>alert(1)</script>", accounts_view)
        self.assertNotIn("</script><script>alert(1)</script>", factors_view)
        self.assertIn("&lt;/script&gt;", factors_view)
        self.assertNotIn("</script><script>alert(1)</script>", dashboard)
        self.assertIn("\\u003c/script\\u003e", dashboard)

    def test_v3_migration_preserves_rows_creates_backup_and_is_idempotent(self) -> None:
        self.ingest(
            "pre-migration.json",
            [
                self.vacancy_row(
                    "company_site:migration-v3",
                    date="2026-01-01",
                    source_stream="Legacy Product",
                )
            ],
        )
        with sqlite3.connect(self.database) as conn:
            conn.executescript(
                """
                DROP TABLE employer_account_signals;
                DROP TABLE vacancy_employer_accounts;
                DROP TABLE employer_accounts;
                DROP TABLE employer_interactions;
                DROP TABLE vacancy_factors;
                ALTER TABLE source_hits DROP COLUMN canonical_source_stream;
                PRAGMA user_version = 3;
                """
            )
            conn.commit()

        migrated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertEqual(migrated["from_version"], 3)
        self.assertEqual(migrated["to_version"], 5)
        self.assertEqual(migrated["backfilled_source_streams"], 1)
        self.assertTrue(migrated["backup"])
        backups = list(self.database.parent.glob("job_search.sqlite.bak-schema-v3-*"))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as backup:
            self.assertEqual(backup.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertIsNone(
                backup.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'employer_interactions'"
                ).fetchone()
            )
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 5)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM source_hits").fetchone()[0], 1)
            self.assertEqual(
                conn.execute("SELECT canonical_source_stream FROM source_hits").fetchone()[0],
                "product_roles",
            )
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

        repeated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertTrue(repeated["already_current"])
        self.assertEqual(repeated["backfilled_source_streams"], 0)
        self.assertEqual(len(list(self.database.parent.glob("job_search.sqlite.bak-schema-v3-*"))), 1)


if __name__ == "__main__":
    unittest.main()
