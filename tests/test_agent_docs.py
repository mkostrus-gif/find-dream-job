from __future__ import annotations

import json
import re
import subprocess
import sys
import unittest
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_AUDIT = ROOT / "scripts" / "public_audit.py"


class AgentDocumentationTests(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text(encoding="utf-8")

    def public_candidates(self) -> list[str]:
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
        return json.loads(result.stdout)["candidates"]

    def test_agent_entrypoints_are_public_and_cross_linked(self) -> None:
        required = {
            "AGENTS.md",
            "docs/agent-runbook.md",
            "prompts/onboarding.md",
        }
        candidates = set(self.public_candidates())
        self.assertTrue(required.issubset(candidates))

        readme = self.read("README.md")
        self.assertLess(readme.index("AGENTS.md"), readme.index("## What the agent deploys"))
        for path in required:
            self.assertIn(path, readme)

    def test_agent_contract_contains_bootstrap_and_safety_gates(self) -> None:
        contract = self.read("AGENTS.md")
        required_text = (
            "prompts/onboarding.md",
            "prompts/daily_run.md",
            "JOB_SEARCH_HOME",
            "python3 scripts/jobctl.py init --json",
            "python3 scripts/jobctl.py doctor --strict --json",
            "automation.auto_apply = false",
            "visible external success",
            "first review-only search",
            "Do not initialize Git",
        )
        for text in required_text:
            with self.subTest(text=text):
                self.assertIn(text, contract)

    def test_operational_documents_point_agents_to_primary_contract(self) -> None:
        operational_docs = (
            "PROJECT_RULES.md",
            "JOB_SYSTEM.md",
            "docs/architecture.md",
            "docs/configuration.md",
            "docs/privacy.md",
            "docs/agent-runbook.md",
            "prompts/agent.md",
            "prompts/onboarding.md",
            "prompts/daily_run.md",
            "prompts/scan_channel.md",
            "prompts/scoring.md",
            "prompts/ats_application_playbook.md",
            "prompts/gmail_hh_digest.md",
        )
        for relative in operational_docs:
            with self.subTest(path=relative):
                self.assertIn("AGENTS.md", self.read(relative))

    def test_daily_run_requires_fail_closed_coverage(self) -> None:
        daily = self.read("prompts/daily_run.md")
        required_text = (
            "search.required_streams",
            "build-coverage-plan",
            "check-coverage",
            "plan-hh-acquisition",
            "record-hh-page",
            "source_reported_count_drift",
            "доказанной границе известных результатов",
            "fail-closed",
            "reports/search_coverage.md",
        )
        for text in required_text:
            with self.subTest(text=text):
                self.assertIn(text, daily)

        settings = self.read("config/settings.example.toml")
        self.assertIn("[search]", settings)
        self.assertIn("[search.hh_acquisition]", settings)
        self.assertIn("required_streams", settings)
        self.assertIn("[mail]", settings)
        self.assertIn("scan_linkedin_inbox = false", settings)
        self.assertIn("archive_processed_linkedin = false", settings)

    def test_daily_run_defers_writes_and_has_one_final_render(self) -> None:
        daily = self.read("prompts/daily_run.md")
        required_text = (
            "begin-daily-run",
            "projection-status",
            "--defer-render",
            "--run-lease",
            "finalize-daily-run",
            "exactly one full render",
        )
        for text in required_text:
            with self.subTest(text=text):
                self.assertIn(text, daily)

    def test_daily_run_documents_durable_v10_resume_contract(self) -> None:
        daily = self.read("prompts/daily_run.md")
        system = self.read("JOB_SYSTEM.md")
        architecture = self.read("docs/architecture.md")
        settings = self.read("config/settings.example.toml")
        for text in (
            "daily-run-status",
            "resume-daily-run",
            "pause-daily-run",
            "checkpoint-daily-run-work",
            "needs_verification",
            "refresh-daily-run-plan",
            "operational-doctor --run-id",
        ):
            with self.subTest(text=text):
                self.assertIn(text, daily)
        for text in (
            "схема v10",
            "схема v9",
            "daily_runs",
            "daily_run_work_items",
            "daily_run_manifests",
            "daily_run_transitions",
            "manifest_version",
        ):
            with self.subTest(text=text):
                self.assertIn(text, system.lower())
        normalized_architecture = " ".join(architecture.split())
        self.assertIn("Срок жизни запуска", normalized_architecture)
        self.assertIn("срока жизни `lease`", normalized_architecture)
        self.assertIn("[daily_run]", settings)
        self.assertIn("[[daily_run.required_gates]]", settings)

    def test_p2_hh_acquisition_contract_is_public_and_fail_closed(self) -> None:
        candidates = set(self.public_candidates())
        self.assertIn("scripts/hh_acquisition.py", candidates)
        self.assertIn("scripts/hh_browser_adapter.js", candidates)
        self.assertIn("scripts/benchmark_hh_incremental.py", candidates)
        self.assertIn("tests/fixtures/hh_links_synthetic.json", candidates)
        self.assertIn("tests/hh_browser_adapter_harness.mjs", candidates)
        self.assertIn("tests/test_hh_browser_adapter.mjs", candidates)

        daily = self.read("prompts/daily_run.md")
        system = self.read("JOB_SYSTEM.md")
        runbook = self.read("docs/agent-runbook.md")
        architecture = self.read("docs/architecture.md")
        settings = self.read("config/settings.example.toml")
        for text in (
            "авторизованный встроенный браузер",
            "Не переключайтесь молча",
            "видимые данные DOM",
            "capturePersonalRecommendations",
            "record-hh-detail",
            "finalize-hh-personal-recommendations",
            "doctor --strict",
        ):
            with self.subTest(text=text):
                self.assertIn(text, daily)
        for text in (
            "hh-dom-v1.0.2",
            "source_reported_count_drift",
            "known_unchanged",
            "duplicate_across_streams",
            "Манифест HH v2",
            "incremental_safety_failure",
        ):
            with self.subTest(text=text):
                self.assertIn(text, system)
        for document_name, document in (
            ("daily", daily),
            ("system", system),
            ("runbook", runbook),
        ):
            for text in (
                "invalidate-hh-zero-evidence-plan",
                "zero_evidence_plan_invalidated",
                "zero_evidence_plan_replanned",
                "source-bearing",
            ):
                with self.subTest(document=document_name, text=text):
                    self.assertIn(text, document)
        for text in (
            "full",
            "shadow",
            "delta",
            "resume",
            "audit",
            "hh_stream_checkpoints",
            "hh_page_captures",
        ):
            with self.subTest(text=text):
                self.assertIn(text, architecture)
        self.assertIn("не переключайтесь молча", runbook.lower())
        self.assertIn("no_source_evidence_discarded", system)
        for key in (
            "incremental_mode",
            "minimum_overlap_pages",
            "consecutive_known_boundary_pages",
            "guard_page_required",
            "checkpoint_staleness_days",
            "shadow_runs_required",
            "full_audit_interval_days",
            "page_stability_samples",
            "page_stability_timeout_ms",
            "count_drift_recaptures",
            "personal_max_is_completion_boundary",
            "max_returned_ids",
        ):
            with self.subTest(key=key):
                self.assertIn(key, settings)

    def test_hh_dom_adapter_node_regressions(self) -> None:
        result = subprocess.run(
            ["node", "--test", str(ROOT / "tests" / "test_hh_browser_adapter.mjs")],
            cwd=ROOT,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            self.fail(
                "HH DOM adapter Node regressions failed "
                f"({result.returncode})\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )

    def test_daily_run_requires_complete_linkedin_mail_processing(self) -> None:
        daily = self.read("prompts/daily_run.md")
        mail_workflow = self.read("prompts/gmail_hh_digest.md")
        required_daily_text = (
            "complete set of LinkedIn messages",
            "currently in Inbox",
            "mail.scan_linkedin_inbox = true",
            "mail.archive_processed_linkedin = true",
            "linkedin_gmail_job_alert",
            "Verify removal of the `INBOX` label",
            "unaccounted LinkedIn message",
            "archive-verified",
        )
        for text in required_daily_text:
            with self.subTest(text=text):
                self.assertIn(text, daily)

        required_mail_text = (
            "Review every matching message",
            "Extract every vacancy link",
            "--provider linkedin",
            "Archive is not delete",
            "unverified",
        )
        for text in required_mail_text:
            with self.subTest(text=text):
                self.assertIn(text, mail_workflow)

    def test_daily_run_requires_success_gated_telegram_backfill_and_delta(self) -> None:
        daily = self.read("prompts/daily_run.md")
        system = self.read("JOB_SYSTEM.md")
        settings = self.read("config/settings.example.toml")
        required_daily_text = (
            "telegram.enabled = true",
            "build-telegram-plan",
            "check-telegram-coverage",
            "30 days by default",
            "telegram:<handle>:<post_id>",
            "advances a channel cursor only when the entire",
            "Telegram manifest passes",
            "reports/source_checkpoints.md",
        )
        for text in required_daily_text:
            with self.subTest(text=text):
                self.assertIn(text, daily)
        for text in (
            "source_checkpoints",
            "initial backfill",
            "delta plan",
            "missing 0–100 score",
        ):
            with self.subTest(text=text):
                self.assertIn(text, system)
        self.assertIn("[telegram]", settings)
        self.assertIn("enabled = false", settings)
        self.assertIn("initial_lookback_days = 30", settings)
        self.assertIn("channels = []", settings)

    def test_daily_run_documents_blocker_regression_contracts(self) -> None:
        daily = self.read("prompts/daily_run.md")
        system = self.read("JOB_SYSTEM.md")
        runbook = self.read("docs/agent-runbook.md")
        architecture = self.read("docs/architecture.md")
        for text in (
            "telegram_source_units_v1",
            "raw >= processed >= reconciled",
            "Граничная публикация `delta`",
            "external_action_id_floor",
            "daily_run_external_action_scope_v1",
            "legacy_backlog",
            "--reclassify-legacy-external-actions",
            "reconcile_inbound_after_outbound",
            "строго более поздним",
        ):
            with self.subTest(text=text):
                self.assertIn(text, system)
        for document in (daily, runbook):
            for text in (
                "telegram_source_units_v1",
                "reconcile_inbound_after_outbound",
                "--reclassify-legacy-external-actions",
            ):
                with self.subTest(text=text):
                    self.assertIn(text, document)
        self.assertIn("external_action_id_floor", architecture)

    def test_daily_run_documents_exact_user_cancelled_followup_resolution(self) -> None:
        readme = self.read("README.md")
        daily = self.read("prompts/daily_run.md")
        system = self.read("JOB_SYSTEM.md")
        runbook = self.read("docs/agent-runbook.md")
        for document_name, document in (
            ("readme", readme),
            ("daily", daily),
            ("system", system),
            ("runbook", runbook),
        ):
            with self.subTest(document=document_name):
                self.assertIn("cancel-due-followup-obligation", document)
                self.assertIn("user_cancelled_followup_obligation", document)
        for text in (
            "--run-id <run_id>",
            "--item-key <due:item:key>",
            "--reason",
            "refresh-daily-run-plan",
            "finalize-daily-run",
        ):
            with self.subTest(text=text):
                self.assertIn(text, system)

    def test_daily_run_documents_reverified_historical_inbound_resolution(self) -> None:
        documents = {
            "readme": self.read("README.md"),
            "daily": self.read("prompts/daily_run.md"),
            "system": self.read("JOB_SYSTEM.md"),
            "runbook": self.read("docs/agent-runbook.md"),
        }
        for document_name, document in documents.items():
            for text in (
                "resolve-due-followup-from-reverified-inbound",
                "reverified_historical_inbound_due_resolution",
                "reverified_historical_inbound_v1",
                "user_cancelled_followup_obligation",
            ):
                with self.subTest(document=document_name, text=text):
                    self.assertIn(text, document)

        for text in (
            "returned_adapter_object_v1",
            "mutation_observer_visible_dom",
            "timed_visible_dom_sampling",
            "page_stability_timeout_ms",
        ):
            with self.subTest(text=text):
                self.assertIn(text, documents["runbook"])

    def test_outcome_and_ai_evidence_contract_is_public(self) -> None:
        system = self.read("JOB_SYSTEM.md")
        daily = self.read("prompts/daily_run.md")
        scoring = self.read("prompts/scoring.md")
        for text in (
            "Screening signal",
            "Automated acknowledgment",
            "Human reply",
            "Verified contact",
            "Employer signal",
            "Candidate evidence",
            "record-employer-interaction",
            "conversion-report",
            "deterministic first touch",
            "[source_stream_aliases]",
        ):
            with self.subTest(text=text):
                self.assertIn(text, system)
        self.assertIn("Do not narrow discovery to AI-titled vacancies", daily)
        self.assertIn("enterprise AI transformation experience", scoring)

    def test_relative_markdown_links_resolve(self) -> None:
        candidates = self.public_candidates()
        markdown_files = [ROOT / path for path in candidates if path.endswith(".md")]
        link_pattern = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")

        failures: list[str] = []
        for document in markdown_files:
            content = document.read_text(encoding="utf-8")
            for raw_target in link_pattern.findall(content):
                target = raw_target.strip().strip("<>")
                parsed = urlsplit(target)
                if parsed.scheme or target.startswith("#"):
                    continue
                relative_path = unquote(parsed.path)
                if not relative_path:
                    continue
                resolved = (document.parent / relative_path).resolve()
                try:
                    resolved.relative_to(ROOT)
                except ValueError:
                    failures.append(
                        f"{document.relative_to(ROOT)} -> {raw_target} escapes repository"
                    )
                    continue
                if not resolved.exists():
                    failures.append(
                        f"{document.relative_to(ROOT)} -> {raw_target} does not exist"
                    )

        self.assertEqual(failures, [])


if __name__ == "__main__":
    unittest.main()
