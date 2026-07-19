from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import public_audit  # noqa: E402


class PublicAuditDetectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-audit-test-")
        self.root = Path(self.temp_dir.name)
        self.original_root = public_audit.ROOT
        public_audit.ROOT = self.root

    def tearDown(self) -> None:
        public_audit.ROOT = self.original_root
        self.temp_dir.cleanup()

    def audit(self, text: str, *, name: str = "sample.txt", deny: list[str] | None = None) -> list[str]:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return public_audit.audit_file(path, deny or [], public_audit.MAX_PUBLIC_FILE_BYTES)

    def test_sensitive_findings_are_detected_without_echoing_values(self) -> None:
        email = "person" + "@company.test"
        phone = "+1 (415) " + "555-0199"
        home = "/" + "Users/private-user/work/"
        marker = "private-person-marker"
        public_ip = "8.8." + "8.8"
        issues = self.audit(
            f"{email}\n{phone}\n{home}\n{public_ip}\n{marker}\n",
            deny=[marker],
        )

        self.assertIn("non-example email address", issues)
        self.assertIn("possible phone number", issues)
        self.assertIn("absolute home path", issues)
        self.assertIn("non-example IPv4 address", issues)
        self.assertIn("local deny literal found", issues)
        report = "\n".join(issues)
        for value in (email, phone, home, marker, public_ip):
            self.assertNotIn(value, report)

    def test_placeholder_identity_and_documentation_addresses_are_allowed(self) -> None:
        issues = self.audit(
            "you@example.com\n/Users/your-account/work/\n"
            "192.0.2.10 198.51.100.20 203.0.113.30 127.0.0.1\n"
            "2026-01-15\n"
        )
        self.assertEqual(issues, [])

    def test_additional_secret_families_are_detected(self) -> None:
        issues = self.audit(
            "glpat-" + "A" * 24 + "\n"
            "hf_" + "B" * 24 + "\n"
            "npm_" + "C" * 24 + "\n"
            "https://" + "user:password" + "@example.org/private\n"
        )
        self.assertIn("possible GitLab token", issues)
        self.assertIn("possible Hugging Face token", issues)
        self.assertIn("possible npm token", issues)
        self.assertIn("possible credential in URL", issues)

    def test_windows_home_and_email_in_filename_are_scanned(self) -> None:
        issues = self.audit(
            "C:\\" + "Users\\private-user\\Documents\\resume.txt\n",
            name="person" + "@company.test.txt",
        )
        self.assertIn("absolute Windows home path", issues)
        self.assertIn("non-example email address", issues)


if __name__ == "__main__":
    unittest.main()
