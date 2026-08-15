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


def synthetic_row(
    external_id: str,
    *,
    date: str = "2026-01-10",
    title: str = "Director of Platform",
    description: str = "Own platform strategy, portfolio governance, and operating model. " * 7,
    url: str | None = None,
    status: str = "NEEDS_REVIEW",
    stage: str = "seen",
    score: int = 70,
) -> dict[str, object]:
    return {
        "date": date,
        "channel": "hh",
        "source": "synthetic_scan",
        "source_stream": "synthetic_roles",
        "external_id": external_id,
        "title": title,
        "company": "Example Labs",
        "description": description,
        "url": url or f"https://example.com/jobs/{external_id.rsplit(':', 1)[-1]}",
        "status": status,
        "stage": stage,
        "score": score,
    }


class VacancyExternalAliasIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-aliases-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.run_cli("init", "--json")
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
        self.run_cli("ingest-json", str(path), "--json")

    def test_semantic_reposts_persist_aliases_and_reconcile_all_scan_ids(self) -> None:
        first_id = "hh:synthetic-repost-a"
        second_id = "hh:synthetic-repost-b"
        first_url = "https://example.com/jobs/platform-a"
        second_url = "https://example.com/jobs/platform-b"
        self.ingest(
            "semantic-reposts.json",
            [
                synthetic_row(
                    first_id,
                    date="2026-01-10",
                    url=first_url,
                    score=72,
                ),
                synthetic_row(
                    second_id,
                    date="2026-01-11",
                    url=second_url,
                    status="NEEDS_INPUT",
                    stage="needs_input",
                    score=84,
                ),
            ],
        )

        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row
            vacancies = conn.execute("SELECT * FROM vacancies").fetchall()
            aliases = conn.execute(
                """
                SELECT vacancy_id, channel, external_id, url,
                       first_seen_date, last_seen_date
                FROM vacancy_external_aliases
                ORDER BY external_id
                """
            ).fetchall()
            self.assertEqual(len(vacancies), 1)
            self.assertEqual(vacancies[0]["external_id"], first_id)
            self.assertEqual(vacancies[0]["url"], first_url)
            self.assertEqual(vacancies[0]["score"], 84)
            self.assertEqual(vacancies[0]["latest_stage"], "needs_input")
            self.assertEqual({row["external_id"] for row in aliases}, {first_id, second_id})
            self.assertEqual({row["vacancy_id"] for row in aliases}, {vacancies[0]["id"]})
            self.assertEqual(
                {row["external_id"]: row["url"] for row in aliases},
                {first_id: first_url, second_id: second_url},
            )

            resolved = {
                external_id: conn.execute(
                    """
                    SELECT v.id
                    FROM vacancy_external_aliases a
                    JOIN vacancies v ON v.id = a.vacancy_id
                    WHERE a.external_id = ?
                    """,
                    (external_id,),
                ).fetchone()[0]
                for external_id in (first_id, second_id)
            }
            self.assertEqual(len(set(resolved.values())), 1)

            represented_ids = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT external_id FROM vacancies
                    UNION
                    SELECT external_id FROM vacancy_external_aliases
                    """
                ).fetchall()
            }
            self.assertEqual({first_id, second_id} - represented_ids, set())

    def test_repeated_alias_ingest_preserves_merge_and_does_not_duplicate(self) -> None:
        first_id = "hh:synthetic-repeat-a"
        second_id = "hh:synthetic-repeat-b"
        self.ingest(
            "repeat-initial.json",
            [
                synthetic_row(first_id, date="2026-01-10", score=71),
                synthetic_row(
                    second_id,
                    date="2026-01-11",
                    status="NEEDS_INPUT",
                    stage="needs_input",
                    score=86,
                ),
            ],
        )
        self.ingest(
            "repeat-second-alias.json",
            [
                synthetic_row(
                    second_id,
                    date="2026-01-12",
                    status="RECHECKED",
                    stage="seen",
                    score=60,
                )
            ],
        )

        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row
            vacancy = conn.execute("SELECT * FROM vacancies").fetchone()
            aliases = conn.execute(
                "SELECT * FROM vacancy_external_aliases ORDER BY external_id"
            ).fetchall()
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 1)
            self.assertEqual(len(aliases), 2)
            self.assertEqual(vacancy["score"], 86)
            self.assertEqual(vacancy["latest_stage"], "needs_input")
            self.assertEqual(vacancy["latest_status"], "RECHECKED")
            repeated = next(row for row in aliases if row["external_id"] == second_id)
            self.assertEqual(repeated["first_seen_date"], "2026-01-11")
            self.assertEqual(repeated["last_seen_date"], "2026-01-12")
            self.assertEqual(
                conn.execute("PRAGMA foreign_key_check").fetchall(),
                [],
            )

    def test_distinct_mandates_for_one_employer_are_not_merged(self) -> None:
        scan_ids = {"hh:synthetic-platform", "hh:synthetic-growth"}
        self.ingest(
            "distinct-mandates.json",
            [
                synthetic_row(
                    "hh:synthetic-platform",
                    title="Director of Platform",
                    description="Own platform architecture, developer experience, and shared services. " * 7,
                ),
                synthetic_row(
                    "hh:synthetic-growth",
                    title="Director of Growth",
                    description="Own acquisition, activation, lifecycle experiments, and growth economics. " * 7,
                ),
            ],
        )
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 2)
            self.assertEqual(
                {
                    row[0]
                    for row in conn.execute(
                        "SELECT external_id FROM vacancy_external_aliases"
                    ).fetchall()
                },
                scan_ids,
            )

    def test_exact_canonical_external_id_ingest_remains_stable(self) -> None:
        external_id = "hh:synthetic-canonical"
        self.ingest(
            "canonical-first.json",
            [
                synthetic_row(
                    external_id,
                    date="2026-01-10",
                    url="https://example.com/jobs/canonical-first",
                    score=70,
                )
            ],
        )
        self.ingest(
            "canonical-repeat.json",
            [
                synthetic_row(
                    external_id,
                    date="2026-01-12",
                    url="https://example.com/jobs/canonical-current",
                    score=81,
                )
            ],
        )
        with sqlite3.connect(self.database) as conn:
            row = conn.execute(
                "SELECT external_id, url, score FROM vacancies"
            ).fetchone()
            self.assertEqual(
                row,
                (external_id, "https://example.com/jobs/canonical-current", 81),
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM vacancy_external_aliases").fetchone()[0],
                1,
            )

    def test_every_external_id_cli_command_resolves_an_alias(self) -> None:
        canonical_id = "hh:synthetic-cli-canonical"
        alias_id = "hh:synthetic-cli-alias"
        self.ingest(
            "cli-aliases.json",
            [
                synthetic_row(canonical_id, score=80),
                synthetic_row(alias_id, date="2026-01-11", score=82),
            ],
        )
        self.run_cli(
            "update-vacancy",
            "--external-id",
            alias_id,
            "--status",
            "APPLIED_CONFIRMED",
            "--stage",
            "applied",
            "--score",
            "83",
            "--note",
            "Synthetic visible confirmation",
        )
        self.run_cli(
            "upsert-contact",
            "--external-id",
            alias_id,
            "--person-name",
            "Example Recruiter",
            "--relationship",
            "recruiter",
            "--confidence",
            "confirmed",
            "--contact-channel",
            "linkedin",
            "--contact-address",
            "example-profile",
            "--evidence-note",
            "Synthetic official directory evidence",
        )
        self.run_cli(
            "record-contact-search",
            "--external-id",
            alias_id,
            "--status",
            "found",
            "--channels-checked",
            "linkedin",
            "--note",
            "Synthetic verified contact found",
        )
        outreach = self.workspace / "tmp" / "alias-outreach.json"
        action_key = "synthetic-alias-follow-up"
        self.run_cli(
            "record-external-action",
            "--external-id",
            alias_id,
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
        )
        self.run_cli(
            "record-external-action",
            "--external-id",
            alias_id,
            "--action-key",
            action_key,
            "--action-type",
            "follow_up",
            "--state",
            "visibly_confirmed",
            "--evidence-note",
            "Synthetic visible sent marker",
            "--external-reference",
            "synthetic-alias-message-1",
            "--source",
            "synthetic_test",
        )
        outreach.write_text(
            json.dumps(
                {
                    "contact_search": {
                        "status": "found",
                        "channels_checked": ["email"],
                        "note": "Synthetic primary-channel review",
                    },
                    "touchpoints": [
                        {
                            "channel": "email",
                            "message_text": "Synthetic follow-up text.",
                            "delivery_status": "sent",
                            "evidence_note": "Synthetic visible sent marker",
                            "external_action_key": action_key,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.run_cli(
            "record-followup",
            "--external-id",
            alias_id,
            "--date",
            "2026-01-20",
            "--outreach-json",
            str(outreach),
        )
        summary = self.workspace / "private" / "synthetic-interview.md"
        summary.write_text("# Synthetic interview summary\n", encoding="utf-8")
        self.run_cli(
            "attach-interview-summary",
            "--external-id",
            alias_id,
            "--file",
            str(summary),
            "--interview-no",
            "1",
        )

        with sqlite3.connect(self.database) as conn:
            vacancy_id = conn.execute("SELECT id FROM vacancies").fetchone()[0]
            self.assertEqual(
                conn.execute(
                    "SELECT score, latest_stage FROM vacancies WHERE id = ?",
                    (vacancy_id,),
                ).fetchone(),
                (83, "follow_up"),
            )
            for table in (
                "employer_contacts",
                "contact_searches",
                "followup_rounds",
                "interview_summaries",
            ):
                self.assertEqual(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE vacancy_id = ?",
                        (vacancy_id,),
                    ).fetchone()[0],
                    1 if table != "contact_searches" else 2,
                )

    def test_v2_migration_backs_up_backfills_and_is_idempotent(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                """
                INSERT INTO vacancies (
                    channel, source, external_id, url, title, company,
                    first_seen_date, last_seen_date, latest_status, latest_stage,
                    score, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "company_site",
                    "synthetic_board",
                    "company_site:synthetic-canonical",
                    "https://example.com/jobs/synthetic-canonical",
                    "General Manager",
                    "Example Labs",
                    "2026-01-02",
                    "2026-01-08",
                    "NEEDS_REVIEW",
                    "seen",
                    78,
                    "2026-01-08T12:00:00",
                ),
            )
            conn.execute("DROP TABLE vacancy_external_aliases")
            conn.execute("PRAGMA user_version = 2")
            conn.commit()

        blocked = self.run_cli("doctor", "--strict", "--json", check=False)
        self.assertNotEqual(blocked.returncode, 0)
        migrated = json.loads(self.run_cli("migrate-schema", "--json").stdout)
        self.assertEqual(migrated["from_version"], 2)
        self.assertEqual(migrated["to_version"], 7)
        self.assertEqual(migrated["backfilled_aliases"], 1)
        self.assertTrue(migrated["backup"])
        backups = list(self.database.parent.glob("job_search.sqlite.bak-schema-v2-*"))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as backup:
            self.assertEqual(backup.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertIsNone(
                backup.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'vacancy_external_aliases'
                    """
                ).fetchone()
            )

        with sqlite3.connect(self.database) as conn:
            alias = conn.execute(
                """
                SELECT channel, external_id, url, first_seen_date, last_seen_date
                FROM vacancy_external_aliases
                """
            ).fetchone()
            self.assertEqual(
                alias,
                (
                    "company_site",
                    "company_site:synthetic-canonical",
                    "https://example.com/jobs/synthetic-canonical",
                    "2026-01-02",
                    "2026-01-08",
                ),
            )
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])

        repeated = json.loads(self.run_cli("migrate-schema", "--json").stdout)
        self.assertTrue(repeated["already_current"])
        self.assertEqual(repeated["backfilled_aliases"], 0)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM vacancy_external_aliases").fetchone()[0],
                1,
            )

        doctor = json.loads(self.run_cli("doctor", "--strict", "--json").stdout)
        self.assertTrue(doctor["ok"])
        rebuild = json.loads(self.run_cli("rebuild", "--json").stdout)
        self.assertEqual(rebuild["kpis"]["vacancies"], 1)
        stats = json.loads(self.run_cli("stats").stdout)
        self.assertEqual(stats["vacancies"], 1)


if __name__ == "__main__":
    unittest.main()
