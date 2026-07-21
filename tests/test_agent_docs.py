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
