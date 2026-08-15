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


class EngineV6AcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-v6-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.config = self.workspace / "synthetic-settings.toml"
        self.config.write_text(
            """
[project]
title = "Синтетический поиск работы"
locale = "ru"

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
required_streams = ["stream_alpha", "stream_beta"]
default_period_days = 3
items_per_page = 100
personal_recommendations_enabled = false
personal_recommendation_stream = "personal_recommendations"

[decision]
campaign_ids = ["Campaign Alpha", "Campaign Beta"]
role_families = ["Role Alpha", "Role Beta"]
resume_ids = ["Resume Alpha", "Resume Beta"]
message_variants = ["Message Alpha", "Message Beta"]

[queue]
page_size = 100

[queue.limits]
urgent = 2
due_follow_up = 2
deep_review = 3
account_research = 2
backlog = 0

[queue.sla_days]
urgent = 1
due_follow_up = 2
deep_review = 5
account_research = 7
backlog = 30

[queue.labels]
urgent = "Срочный ответ или данные"
due_follow_up = "Наступивший срок повторного обращения"
deep_review = "Углублённая проверка"
account_research = "Исследование работодателя"
backlog = "Резерв вне активного лимита"

[policy]
active_version = "synthetic-policy-v1"
effective_date = "2026-01-01"

[account]
active_portfolio_limit = 2

[source_stream_aliases]
"Campaign Alpha + Campaign Beta" = ["stream_alpha", "stream_beta"]

[channel_labels]
email = "Почта"
linkedin = "LinkedIn"
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

    def ingest(self, name: str, rows: list[object]) -> dict[str, object]:
        path = self.workspace / "tmp" / name
        path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        return json.loads(
            self.run_cli(
                "ingest-json", str(path), *self.config_args, "--json"
            ).stdout
        )

    @staticmethod
    def vacancy(
        suffix: str,
        *,
        date: str = "2026-01-01",
        source_stream: str = "stream_alpha",
        score: int = 80,
        title: str = "Synthetic Product Lead",
    ) -> dict[str, object]:
        return {
            "date": date,
            "channel": "company_site",
            "source": "synthetic_board",
            "source_stream": source_stream,
            "external_id": f"company_site:{suffix}",
            "title": title,
            "company": "Example Labs",
            "description": f"Synthetic record {suffix}",
            "url": f"https://example.com/jobs/{suffix}",
            "kind": "screening",
            "status": "NEEDS_REVIEW",
            "stage": "seen",
            "score": score,
            "reason": "Synthetic evidence",
        }

    def confirm_application(
        self,
        external_id: str,
        *,
        key: str,
        at: str,
        metadata: bool = False,
    ) -> dict[str, object]:
        common = (
            "--external-id",
            external_id,
            "--action-key",
            key,
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
            at,
            "--authorization-note",
            "Synthetic authorization for this exact action",
            "--json",
        )
        visible_args = [
            "record-external-action",
            *common,
            "--state",
            "visibly_confirmed",
            "--at",
            at,
            "--evidence-at",
            at,
            "--evidence-note",
            "Synthetic visible success marker",
            "--external-reference",
            f"{key}-visible-reference",
        ]
        if metadata:
            visible_args.extend(
                [
                    "--campaign-id",
                    "Campaign Alpha",
                    "--role-family",
                    "Role Alpha",
                    "--confidence",
                    "confirmed",
                    "--master-resume-id",
                    "Resume Alpha",
                    "--planned-resume-id",
                    "Resume Beta",
                    "--actual-resume-id",
                    "Resume Alpha",
                    "--message-variant",
                    "Message Alpha",
                    "--human-path-status",
                    "verified",
                    "--hard-gates-json",
                    json.dumps(
                        [
                            {
                                "gate": "synthetic_work_authorization",
                                "result": "pass",
                                "evidence_note": "Synthetic evidence",
                            }
                        ]
                    ),
                    "--unresolved-questions-json",
                    json.dumps(
                        [
                            {
                                "question": "Synthetic scope detail",
                                "status": "open",
                            }
                        ]
                    ),
                ]
            )
        visible_args.append("--json")
        return json.loads(self.run_cli(*visible_args).stdout)

    def test_lifecycle_is_durable_and_score_never_authorizes_action(self) -> None:
        external_id = "company_site:lifecycle"
        self.ingest("score-only.json", [self.vacancy("lifecycle", score=95)])
        before = json.loads(
            self.run_cli(
                "outcome-scorecard",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertEqual(before["overall"]["confirmed_applications"], 0)

        unauthorized = self.run_cli(
            "record-external-action",
            "--external-id",
            external_id,
            "--action-key",
            "unauthorized-application",
            "--action-type",
            "application",
            "--state",
            "visibly_confirmed",
            "--evidence-note",
            "Synthetic marker",
            "--external-reference",
            "unauthorized-visible-reference",
            "--source",
            "synthetic_test",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(unauthorized.returncode, 0)

        confirmed = self.confirm_application(
            external_id,
            key="authorized-application",
            at="2026-01-02T00:00:00",
        )
        replayed = json.loads(
            self.run_cli(
                "record-external-action",
                "--external-id",
                external_id,
                "--action-key",
                "authorized-application",
                "--action-type",
                "application",
                "--state",
                "visibly_confirmed",
                "--at",
                "2026-01-02T00:00:00",
                "--evidence-at",
                "2026-01-02T00:00:00",
                "--evidence-note",
                "Synthetic visible success marker",
                "--external-reference",
                "authorized-application-visible-reference",
                "--source",
                "synthetic_test",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertTrue(confirmed["created"])
        self.assertFalse(replayed["created"])
        self.assertFalse(replayed["lifecycle_created"])

        self.run_cli(
            "update-vacancy",
            "--external-id",
            external_id,
            "--date",
            "2026-01-03",
            "--stage",
            "needs_input",
            "--status",
            "NEEDS_INPUT",
            "--note",
            "Synthetic questionnaire requires input",
            *self.config_args,
        )
        after_input = json.loads(
            self.run_cli(
                "outcome-scorecard",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertEqual(after_input["overall"]["confirmed_applications"], 1)

        self.run_cli(
            "record-lifecycle-event",
            "--external-id",
            external_id,
            "--event-type",
            "rejected",
            "--at",
            "2026-01-04T00:00:00",
            "--evidence-at",
            "2026-01-04T00:00:00",
            "--evidence-note",
            "Synthetic visible rejection",
            "--source",
            "synthetic_test",
            *self.config_args,
            "--json",
        )
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "UPDATE vacancies SET latest_stage = 'seen', latest_status = 'LEGACY_CLEANUP'"
            )
            conn.commit()
        self.run_cli("rebuild", *self.config_args, "--json")
        funnel_view = (self.workspace / "views" / "funnel.md").read_text(encoding="utf-8")
        self.assertIn("| Отказ |", funnel_view)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM lifecycle_events WHERE event_type = 'application_confirmed'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM lifecycle_events WHERE event_type = 'rejected'"
                ).fetchone()[0],
                1,
            )
            latest_action = conn.execute(
                "SELECT action_state FROM action_events ORDER BY event_at DESC, id DESC LIMIT 1"
            ).fetchone()[0]
            self.assertEqual(latest_action, "needs_input")

        regression = self.run_cli(
            "record-lifecycle-event",
            "--external-id",
            external_id,
            "--event-type",
            "interview_invited",
            "--at",
            "2026-01-05T00:00:00",
            "--evidence-at",
            "2026-01-05T00:00:00",
            "--evidence-note",
            "Synthetic impossible regression",
            "--source",
            "synthetic_test",
            "--round-no",
            "1",
            *self.config_args,
            check=False,
        )
        self.assertNotEqual(regression.returncode, 0)

    def test_interview_evidence_levels_and_summary_completion_rule(self) -> None:
        external_id = "company_site:interview-evidence"
        self.ingest("interview.json", [self.vacancy("interview-evidence")])
        self.confirm_application(
            external_id,
            key="interview-application",
            at="2026-01-01T00:00:00",
        )
        for event_type, event_at, extra in (
            ("interview_invited", "2026-01-02T00:00:00", []),
            (
                "interview_scheduled",
                "2026-01-03T00:00:00",
                ["--scheduled-at", "2026-01-05T10:00:00"],
            ),
        ):
            self.run_cli(
                "record-lifecycle-event",
                "--external-id",
                external_id,
                "--event-type",
                event_type,
                "--at",
                event_at,
                "--evidence-at",
                event_at,
                "--evidence-note",
                f"Synthetic evidence for {event_type}",
                "--source",
                "synthetic_test",
                "--round-no",
                "1",
                *extra,
                *self.config_args,
                "--json",
            )
        self.run_cli(
            "record-employer-interaction",
            "--external-id",
            external_id,
            "--at",
            "2026-01-03T12:00:00",
            "--event-type",
            "automated_ack",
            "--channel",
            "email",
            "--actor-type",
            "system",
            "--humanity",
            "automated",
            "--evidence-note",
            "Synthetic automated receipt",
            *self.config_args,
            "--json",
        )
        invited = json.loads(
            self.run_cli(
                "outcome-scorecard",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )["overall"]
        self.assertEqual(invited["interview_invitations"], 1)
        self.assertEqual(invited["scheduled_interviews"], 1)
        self.assertEqual(invited["completed_first_interviews"], 0)
        stats_before_completion = json.loads(
            self.run_cli("stats", *self.config_args).stdout
        )
        self.assertEqual(stats_before_completion["interviews"], 0)
        self.assertEqual(stats_before_completion["interview_invitations"], 1)
        self.assertEqual(stats_before_completion["scheduled_interviews"], 1)
        self.assertEqual(invited["recorded_inbound_human_replies"], 0)

        summary = self.workspace / "private" / "synthetic-interview-summary.md"
        summary.write_text(
            "# Синтетическое резюме интервью\n\n"
            "Обсудили вымышленный продуктовый контур и способ принятия решений.\n\n"
            "Зафиксировали вымышленные вопросы, риски и следующие шаги для теста.\n",
            encoding="utf-8",
        )
        self.run_cli(
            "attach-interview-summary",
            "--external-id",
            external_id,
            "--file",
            str(summary),
            "--interview-no",
            "1",
            "--date",
            "2026-01-05",
            "--confirms-completion",
            "--completion-evidence-note",
            "Synthetic verified interview summary",
            *self.config_args,
        )
        self.run_cli(
            "record-lifecycle-event",
            "--external-id",
            external_id,
            "--event-type",
            "interview_invited",
            "--at",
            "2026-01-06T00:00:00",
            "--evidence-at",
            "2026-01-06T00:00:00",
            "--evidence-note",
            "Synthetic second-round invitation",
            "--source",
            "synthetic_test",
            "--round-no",
            "2",
            *self.config_args,
            "--json",
        )
        completed = json.loads(
            self.run_cli(
                "outcome-scorecard",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )["overall"]
        self.assertEqual(completed["completed_first_interviews"], 1)
        self.assertEqual(completed["later_interview_rounds"], 1)
        stats_after_completion = json.loads(
            self.run_cli("stats", *self.config_args).stdout
        )
        self.assertEqual(stats_after_completion["interviews"], 1)
        self.assertEqual(stats_after_completion["completed_first_interviews"], 1)
        with sqlite3.connect(self.database) as conn:
            summary_row = conn.execute(
                "SELECT confirms_completion, completion_lifecycle_event_id FROM interview_summaries"
            ).fetchone()
            self.assertEqual(summary_row[0], 1)
            self.assertIsNotNone(summary_row[1])

        separate_id = "company_site:interview-exceptions"
        self.ingest(
            "interview-exceptions.json",
            [self.vacancy("interview-exceptions", title="Synthetic Interview Exception")],
        )
        for event_type in (
            "interview_cancelled",
            "interview_no_show_candidate",
            "interview_no_show_employer",
        ):
            self.run_cli(
                "record-lifecycle-event",
                "--external-id",
                separate_id,
                "--event-type",
                event_type,
                "--at",
                "2026-01-10T00:00:00",
                "--evidence-at",
                "2026-01-10T00:00:00",
                "--evidence-note",
                f"Synthetic evidence for {event_type}",
                "--source",
                "synthetic_test",
                "--round-no",
                "1",
                *self.config_args,
                "--json",
            )
        with sqlite3.connect(self.database) as conn:
            exception_events = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT le.event_type
                    FROM lifecycle_events le
                    JOIN vacancies v ON v.id = le.vacancy_id
                    WHERE v.external_id = ?
                    """,
                    (separate_id,),
                ).fetchall()
            }
        self.assertEqual(
            exception_events,
            {
                "interview_cancelled",
                "interview_no_show_candidate",
                "interview_no_show_employer",
            },
        )

    def test_configured_metadata_multilabel_sources_and_account_boundaries(self) -> None:
        external_id = "company_site:metadata"
        row = self.vacancy(
            "metadata",
            source_stream="Campaign Alpha + Campaign Beta",
            score=88,
        )
        row.update(
            {
                "campaign_id": "Campaign Alpha",
                "role_family": "Role Alpha",
                "confidence": "high",
                "master_resume_id": "Resume Alpha",
                "planned_resume_id": "Resume Beta",
                "actual_resume_id": "Resume Alpha",
                "message_variant": "Message Alpha",
                "human_path_status": "verified",
                "hard_gates": [
                    {
                        "gate": "synthetic_gate",
                        "result": "pass",
                        "evidence_note": "Synthetic evidence",
                    }
                ],
                "unresolved_questions": ["Synthetic open question"],
            }
        )
        self.ingest("metadata.json", [row])
        self.confirm_application(
            external_id,
            key="metadata-application",
            at="2026-01-02T00:00:00",
            metadata=True,
        )
        with sqlite3.connect(self.database) as conn:
            raw = conn.execute("SELECT source_stream FROM source_hits").fetchone()[0]
            labels = {
                item[0]
                for item in conn.execute(
                    "SELECT label_key FROM source_hit_labels ORDER BY label_key"
                ).fetchall()
            }
            metadata = conn.execute(
                """
                SELECT campaign_id, role_family, master_resume_id,
                       planned_resume_id, actual_resume_id, message_variant
                FROM vacancy_decision_metadata
                """
            ).fetchone()
        self.assertEqual(raw, "Campaign Alpha + Campaign Beta")
        self.assertEqual(labels, {"stream_alpha", "stream_beta"})
        self.assertEqual(
            metadata,
            (
                "Campaign Alpha",
                "Role Alpha",
                "Resume Alpha",
                "Resume Beta",
                "Resume Alpha",
                "Message Alpha",
            ),
        )

        scorecard = json.loads(
            self.run_cli(
                "outcome-scorecard",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertEqual(
            scorecard["breakdowns"]["campaign_id"][0]["campaign_id"],
            "Campaign Alpha",
        )
        self.assertEqual(
            scorecard["breakdowns"]["source_stream"][0]["source_stream"],
            "stream_alpha",
        )
        self.assertEqual(
            scorecard["breakdowns"]["actual_resume_version"][0][
                "actual_resume_version"
            ],
            "Resume Alpha",
        )
        outcome_markdown = (
            self.workspace / "reports" / "outcome_scorecard.md"
        ).read_text(encoding="utf-8")
        self.assertIn("| Сайт работодателя |", outcome_markdown)
        self.assertNotIn("| company_site |", outcome_markdown)

        updated_config = self.config.read_text(encoding="utf-8").replace(
            '"Campaign Alpha + Campaign Beta" = ["stream_alpha", "stream_beta"]',
            '"Campaign Alpha + Campaign Beta" = ["stream_beta"]',
        )
        self.config.write_text(updated_config, encoding="utf-8")
        self.run_cli("rebuild", *self.config_args, "--json")
        with sqlite3.connect(self.database) as conn:
            remapped = conn.execute(
                "SELECT source_stream, canonical_source_stream FROM source_hits"
            ).fetchone()
            remapped_labels = {
                item[0]
                for item in conn.execute(
                    "SELECT label_key FROM source_hit_labels ORDER BY label_key"
                ).fetchall()
            }
        self.assertEqual(remapped[0], "Campaign Alpha + Campaign Beta")
        self.assertEqual(remapped[1], "stream_beta")
        self.assertEqual(remapped_labels, {"stream_beta"})

        account = json.loads(
            self.run_cli(
                "upsert-employer-account",
                "--canonical-name",
                "Example Labs",
                "--website",
                "https://example.com",
                "--careers-url",
                "https://example.com/careers",
                "--priority",
                "high",
                "--status",
                "active",
                "--portfolio-limit",
                "2",
                "--review-cadence-days",
                "14",
                "--next-review-date",
                "2026-02-01",
                "--website-checked-date",
                "2026-01-01",
                "--careers-checked-date",
                "2026-01-01",
                "--target-campaigns",
                "Campaign Alpha,Campaign Beta",
                "--target-role-families",
                "Role Alpha,Role Beta",
                "--owner-evidence",
                "Synthetic owner evidence",
                "--sponsor-evidence",
                "Synthetic sponsor evidence",
                "--governance-evidence",
                "Synthetic governance evidence",
                "--human-path-status",
                "verified",
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
            "Synthetic exact link",
            *self.config_args,
            "--json",
        )
        self.run_cli(
            "record-employer-signal",
            "--account-id",
            str(account["account_id"]),
            "--signal-type",
            "ai_adoption",
            "--observed-date",
            "2026-01-03",
            "--confidence",
            "confirmed",
            "--evidence-note",
            "Synthetic account signal",
            *self.config_args,
            "--json",
        )
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT score FROM vacancies").fetchone()[0], 88)
            account_row = conn.execute(
                """
                SELECT target_campaigns_json, target_role_families_json,
                       human_path_status, portfolio_limit
                FROM employer_accounts
                """
            ).fetchone()
        self.assertEqual(json.loads(account_row[0]), ["Campaign Alpha", "Campaign Beta"])
        self.assertEqual(json.loads(account_row[1]), ["Role Alpha", "Role Beta"])
        self.assertEqual(account_row[2:], ("verified", 2))

        invalid = self.vacancy("invalid-metadata")
        invalid["campaign_id"] = "Campaign Gamma"
        invalid_path = self.workspace / "tmp" / "invalid-metadata.json"
        invalid_path.write_text(json.dumps([invalid]), encoding="utf-8")
        rejected = self.run_cli(
            "ingest-json",
            str(invalid_path),
            *self.config_args,
            "--json",
            check=False,
        )
        self.assertNotEqual(rejected.returncode, 0)

    def test_quarantine_exclusion_audit_and_explicit_reprocessing(self) -> None:
        cases = [
            {
                **self.vacancy("captcha"),
                "title": "CAPTCHA challenge",
                "source_stream": "raw captcha stream",
            },
            {
                **self.vacancy("logged-out"),
                "title": "Sign in to continue — logged out",
                "source_stream": "raw logged-out stream",
            },
            {
                **self.vacancy("malformed"),
                "url": "javascript:alert(1)",
                "source_stream": "raw malformed stream",
            },
            {
                **self.vacancy("missing-title"),
                "title": "",
                "source_stream": "raw missing-field stream",
            },
        ]
        result = self.ingest("quarantine.json", cases)
        self.assertEqual(result["ingested"], 0)
        self.assertEqual(result["quarantined"], 4)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 0)
            classes = {
                row[0]
                for row in conn.execute(
                    "SELECT classification FROM quarantine_records"
                ).fetchall()
            }
            self.assertEqual(
                classes,
                {"captcha", "logged_out", "malformed", "missing_required_fields"},
            )
            missing_id = conn.execute(
                "SELECT id FROM quarantine_records WHERE classification = 'missing_required_fields'"
            ).fetchone()[0]
        scorecard = json.loads(
            self.run_cli(
                "outcome-scorecard",
                "--as-of",
                "2026-02-15",
                *self.config_args,
                "--json",
            ).stdout
        )
        queue = json.loads(
            self.run_cli("wip-queue", *self.config_args, "--json").stdout
        )
        self.assertEqual(scorecard["overall"]["confirmed_applications"], 0)
        self.assertEqual(queue["pagination"]["total_items"], 0)

        replacement = self.workspace / "tmp" / "replacement.json"
        replacement.write_text(
            json.dumps(
                {
                    "vacancies": [
                        self.vacancy(
                            "missing-title-reprocessed",
                            title="Synthetic Reprocessed Role",
                        )
                    ]
                }
            ),
            encoding="utf-8",
        )
        reprocessed = json.loads(
            self.run_cli(
                "reprocess-quarantine",
                "--id",
                str(missing_id),
                "--replacement-json",
                str(replacement),
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertEqual(reprocessed["status"], "reprocessed")
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 1)
            self.assertEqual(
                conn.execute(
                    "SELECT retry_count FROM quarantine_records WHERE id = ?", (missing_id,)
                ).fetchone()[0],
                1,
            )

    def test_large_wip_queue_has_stable_pagination_sla_and_overflow(self) -> None:
        rows: list[dict[str, object]] = []
        for index in range(225):
            row = self.vacancy(f"backlog-{index:03d}", date="2026-01-01")
            row.update(
                {
                    "action_state": "review",
                    "action_bucket": "deep_review",
                    "action_at": "2026-01-01T00:00:00",
                    "action_priority": index % 101,
                    "priority_reason": f"Synthetic priority {index:03d}",
                }
            )
            rows.append(row)
        self.ingest("large-backlog.json", rows)
        pages = [
            json.loads(
                self.run_cli(
                    "wip-queue",
                    "--as-of",
                    "2026-02-01",
                    "--page",
                    str(page),
                    "--page-size",
                    "100",
                    *self.config_args,
                    "--json",
                ).stdout
            )
            for page in (1, 2, 3)
        ]
        self.assertEqual(pages[0]["pagination"]["total_items"], 225)
        self.assertEqual(pages[0]["pagination"]["total_pages"], 3)
        self.assertEqual([len(page["items"]) for page in pages], [100, 100, 25])
        self.assertEqual(pages[0]["active_wip_total"], 3)
        self.assertEqual(pages[0]["overflow_total"], 222)
        self.assertEqual(pages[0]["overdue_total"], 225)
        ids = [item["vacancy_id"] for page in pages for item in page["items"]]
        self.assertEqual(len(ids), len(set(ids)))
        repeated = json.loads(
            self.run_cli(
                "wip-queue",
                "--as-of",
                "2026-02-01",
                "--page",
                "1",
                "--page-size",
                "100",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertEqual(
            [item["vacancy_id"] for item in pages[0]["items"]],
            [item["vacancy_id"] for item in repeated["items"]],
        )
        self.run_cli("rebuild", *self.config_args, "--json")
        self.assertTrue((self.workspace / "views" / "wip_queue_page_0003.md").is_file())

    def persist_complete_coverage(self, run_date: str) -> None:
        plan_input = self.workspace / "tmp" / f"coverage-plan-{run_date}.json"
        plan_input.write_text(
            json.dumps(
                {
                    "run_date": run_date,
                    "source": "hh",
                    "streams": [
                        {
                            "key": key,
                            "query": {
                                "any_terms": ["Synthetic Product Lead"],
                                "fields": ["NAME", "DESCRIPTION"],
                                "search_period_days": 3,
                                "items_per_page": 100,
                            },
                        }
                        for key in ("stream_alpha", "stream_beta")
                    ],
                }
            ),
            encoding="utf-8",
        )
        manifest = self.workspace / "tmp" / f"coverage-{run_date}.json"
        self.run_cli(
            "build-coverage-plan",
            str(plan_input),
            "--output",
            str(manifest),
            *self.config_args,
        )
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        for stream in payload["streams"]:
            stream.update(
                {
                    "status": "completed",
                    "found": 0,
                    "pages": [{"page": 0, "extracted": 0}],
                    "unique": 0,
                    "known": 0,
                    "new": 0,
                    "error": "",
                }
            )
        payload["totals"] = {"unique": 0, "known": 0, "new": 0}
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        checked = json.loads(
            self.run_cli("check-coverage", str(manifest), *self.config_args).stdout
        )
        self.assertTrue(checked["ok"], checked["issues"])

    def test_operational_doctor_blocks_stale_coverage_and_passes_current_coverage(self) -> None:
        self.persist_complete_coverage("2026-08-11")
        stale = json.loads(
            self.run_cli(
                "operational-doctor",
                "--as-of",
                "2026-08-12",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertTrue(stale["technical_health"])
        self.assertFalse(stale["ready_for_daily_closeout"])
        coverage_check = next(
            check for check in stale["checks"] if check["name"] == "required_search_coverage"
        )
        self.assertEqual(coverage_check["status"], "fail")
        self.assertIn("2026-08-12", coverage_check["detail"])

        self.persist_complete_coverage("2026-08-12")
        ready = json.loads(
            self.run_cli(
                "operational-doctor",
                "--as-of",
                "2026-08-12",
                "--strict",
                *self.config_args,
                "--json",
            ).stdout
        )
        self.assertTrue(ready["technical_health"])
        self.assertTrue(ready["ready_for_daily_closeout"])

    def test_false_negative_audit_is_versioned_and_reproducible(self) -> None:
        rows = []
        for index in range(12):
            row = self.vacancy(f"low-priority-{index:02d}", score=40 + index)
            row.update(
                {
                    "screening_decision": "low_priority",
                    "priority": "low",
                    "decision_evidence_note": "Synthetic screening evidence",
                    "rule_results": [
                        {
                            "rule_key": "synthetic_scope_gap",
                            "result": "matched",
                            "note": "Synthetic rule evidence",
                        },
                        {
                            "rule_key": "synthetic_secondary_rule",
                            "result": "matched" if index % 2 == 0 else "not_matched",
                            "note": "Synthetic rule evidence",
                        },
                    ],
                }
            )
            rows.append(row)
        self.ingest("false-negative-population.json", rows)
        command = (
            "false-negative-audit",
            "--as-of",
            "2026-02-01",
            "--sample-size",
            "8",
            "--seed",
            "synthetic-seed",
            *self.config_args,
            "--json",
        )
        first = json.loads(self.run_cli(*command).stdout)
        second = json.loads(self.run_cli(*command).stdout)
        self.assertEqual(first, second)
        self.assertEqual(first["policy_version"], "synthetic-policy-v1")
        self.assertEqual(first["policy_effective_date"], "2026-01-01")
        self.assertEqual(first["sample_size"], 8)
        dominant = first["rule_counts"][0]
        self.assertEqual(dominant["rule_key"], "synthetic_scope_gap")
        self.assertEqual(dominant["count"], 8)
        self.assertTrue(dominant["requires_review"])

    def _downgrade_to_v5_shape(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.execute("DROP VIEW effective_applications")
            conn.execute("DROP VIEW effective_employer_interactions")
            for table in (
                "employer_interaction_invalidations",
                "source_hit_labels",
                "source_labels",
                "vacancy_decision_metadata",
                "action_events",
                "lifecycle_events",
                "external_actions",
                "quarantine_records",
                "screening_decisions",
                "policy_versions",
                "migration_log",
            ):
                conn.execute(f"DROP TABLE {table}")
            conn.execute("PRAGMA user_version = 5")
            conn.commit()

    def test_v5_migration_is_backed_up_row_preserving_and_idempotent(self) -> None:
        application = self.vacancy(
            "legacy-v5",
            source_stream="Campaign Alpha + Campaign Beta",
        )
        application.update(
            {
                "kind": "application",
                "stage": "applied",
                "status": "APPLIED_CONFIRMED",
                "resume_version": "Legacy Synthetic Resume",
            }
        )
        self.ingest("legacy-v5.json", [application])
        with sqlite3.connect(self.database) as conn:
            before = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "vacancies",
                    "source_hits",
                    "applications",
                    "stage_events",
                )
            }
        self._downgrade_to_v5_shape()
        migrated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertEqual(migrated["from_version"], 5)
        self.assertEqual(migrated["to_version"], 7)
        self.assertTrue(migrated["backup"])
        backups = list(self.database.parent.glob("job_search.sqlite.bak-schema-v5-*"))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as backup:
            self.assertEqual(backup.execute("PRAGMA user_version").fetchone()[0], 5)
            self.assertIsNone(
                backup.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lifecycle_events'"
                ).fetchone()
            )
        with sqlite3.connect(self.database) as conn:
            after = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in before
            }
            self.assertEqual(before, after)
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 7)
            self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
            lifecycle = conn.execute(
                """
                SELECT event_type, history_complete, authorization_status
                FROM lifecycle_events
                """
            ).fetchone()
            self.assertEqual(lifecycle, ("application_confirmed", 0, "legacy_unknown"))
            self.assertEqual(
                conn.execute("SELECT source_stream FROM source_hits").fetchone()[0],
                "Campaign Alpha + Campaign Beta",
            )
        self.run_cli("rebuild", *self.config_args, "--json")
        strict = json.loads(
            self.run_cli("doctor", "--strict", *self.config_args, "--json").stdout
        )
        self.assertTrue(strict["ok"])
        repeated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertTrue(repeated["already_current"])
        self.assertEqual(len(list(self.database.parent.glob("job_search.sqlite.bak-schema-v5-*"))), 1)

    def test_empty_v5_migration_and_generated_language_and_escaping(self) -> None:
        self._downgrade_to_v5_shape()
        migrated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertEqual(migrated["to_version"], 7)
        self.ingest(
            "escaped.json",
            [
                self.vacancy(
                    "escaped",
                    title="</script><script>alert(1)</script> [ссылка](javascript:alert(2))",
                )
            ],
        )
        self.run_cli("rebuild", *self.config_args, "--json")
        strict = json.loads(
            self.run_cli("doctor", "--strict", *self.config_args, "--json").stdout
        )
        self.assertTrue(strict["ok"])
        markdown_files = [
            self.workspace / "reports" / "outcome_scorecard.md",
            self.workspace / "reports" / "conversion_cohorts.md",
            self.workspace / "reports" / "quarantine.md",
            self.workspace / "reports" / "false_negative_audit.md",
            self.workspace / "views" / "wip_queue.md",
            self.workspace / "views" / "employer_accounts.md",
        ]
        combined = "\n".join(path.read_text(encoding="utf-8") for path in markdown_files)
        self.assertIn("Карта исходов", combined)
        self.assertIn("Очередь WIP и SLA", combined)
        self.assertIn("Карантин импорта", combined)
        self.assertIn("Портфель целевых работодателей", combined)
        for forbidden in (
            "Application Conversion Cohorts",
            "Outcome Scorecard",
            "Employer Account Radar",
            "WIP Queue",
            "## Methodology",
            "## Overall",
        ):
            self.assertNotIn(forbidden, combined)
        dashboard = (self.workspace / "dashboard" / "index.html").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("</script><script>alert(1)</script>", dashboard)
        self.assertIn("\\u003c/script\\u003e", dashboard)
        today_view = (self.workspace / "views" / "today.md").read_text(encoding="utf-8")
        self.assertNotRegex(today_view, r"(?<!\\)\]\(javascript:")


if __name__ == "__main__":
    unittest.main()
