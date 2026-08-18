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
            "search_period",
            "items_on_page=100",
            "min(page_size, remaining found results)",
            "fail-closed",
            "reports/search_coverage.md",
        )
        for text in required_text:
            with self.subTest(text=text):
                self.assertIn(text, daily)

        settings = self.read("config/settings.example.toml")
        self.assertIn("[search]", settings)
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

    def test_daily_run_documents_durable_v9_resume_contract(self) -> None:
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
