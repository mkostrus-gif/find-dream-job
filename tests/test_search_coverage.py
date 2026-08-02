from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parents[1]
JOBCTL = ROOT / "scripts" / "jobctl.py"
sys.path.insert(0, str(ROOT / "scripts"))

from search_coverage import (  # noqa: E402
    build_hh_query,
    semantic_vacancy_fingerprint,
    validate_coverage_manifest,
)


def query_spec() -> dict[str, object]:
    return {
        "any_terms": ["Chief Product Officer", "CPO", "Head of Product"],
        "fields": ["NAME", "DESCRIPTION"],
        "search_period_days": 3,
        "items_per_page": 100,
    }


def completed_stream(key: str, *, found: int = 137) -> dict[str, object]:
    query = query_spec()
    generated = build_hh_query(query)
    return {
        "key": key,
        "status": "completed",
        "query": {**query, "url": generated["url"]},
        "found": found,
        "pages": [
            {"page": 0, "extracted": min(found, 100)},
            *([{"page": 1, "extracted": found - 100}] if found > 100 else []),
        ],
        "unique": found,
        "known": 20,
        "new": found - 20,
        "error": "",
    }


class SearchCoverageUnitTests(unittest.TestCase):
    def test_hh_query_builder_uses_or_and_current_period_parameter(self) -> None:
        result = build_hh_query(query_spec())
        params = parse_qs(urlsplit(result["url"]).query)
        self.assertIn(" OR ", result["query_text"])
        self.assertIn("NAME:", result["query_text"])
        self.assertIn("DESCRIPTION:", result["query_text"])
        self.assertEqual(params["search_period"], ["3"])
        self.assertEqual(params["items_on_page"], ["100"])
        self.assertNotIn("period", params)

    def test_complete_manifest_passes(self) -> None:
        manifest = {
            "run_date": "2026-01-15",
            "source": "hh",
            "required_streams": ["A", "B"],
            "streams": [completed_stream("A"), completed_stream("B")],
            "totals": {"unique": 200, "known": 40, "new": 160},
        }
        result = validate_coverage_manifest(manifest, ("A", "B"))
        self.assertTrue(result["ok"], result["issues"])
        self.assertEqual(result["streams"][0]["pages_visited"], 2)

    def test_missing_stream_and_blocked_stream_fail_closed(self) -> None:
        blocked = completed_stream("A")
        blocked.update({"status": "blocked", "error": "login required"})
        manifest = {
            "run_date": "2026-01-15",
            "source": "hh",
            "required_streams": ["A", "B"],
            "streams": [blocked],
            "totals": {"unique": 0, "known": 0, "new": 0},
        }
        result = validate_coverage_manifest(manifest, ("A", "B"))
        self.assertFalse(result["ok"])
        self.assertTrue(any("required stream is blocked" in issue for issue in result["issues"]))
        self.assertIn("missing required stream: B", result["issues"])

    def test_wrong_period_or_non_declarative_url_is_rejected(self) -> None:
        stream = completed_stream("A")
        stream["query"]["url"] = (
            "https://hh.ru/search/vacancy?text=Chief+Product+Officer+CPO"
            "&period=3&items_on_page=100"
        )
        manifest = {
            "run_date": "2026-01-15",
            "source": "hh",
            "required_streams": ["A"],
            "streams": [stream],
            "totals": {"unique": 137, "known": 20, "new": 117},
        }
        result = validate_coverage_manifest(manifest, ("A",))
        self.assertFalse(result["ok"])
        joined = "\n".join(result["issues"])
        self.assertIn("deprecated period", joined)
        self.assertIn("declarative OR/search_period plan", joined)

    def test_partial_lazy_load_is_rejected(self) -> None:
        stream = completed_stream("A")
        stream["pages"][0]["extracted"] = 20
        stream["unique"] = 57
        stream["known"] = 20
        stream["new"] = 37
        manifest = {
            "run_date": "2026-01-15",
            "source": "hh",
            "required_streams": ["A"],
            "streams": [stream],
            "totals": {"unique": 57, "known": 20, "new": 37},
        }
        result = validate_coverage_manifest(manifest, ("A",))
        self.assertFalse(result["ok"])
        self.assertTrue(any("expected 100 after lazy-load" in issue for issue in result["issues"]))

    def test_semantic_fingerprint_normalizes_reposts_conservatively(self) -> None:
        description = "<p>Lead a product and own P&amp;L. </p>" + "Build and scale teams. " * 8
        first = semantic_vacancy_fingerprint("Example Labs", "Head of Product", description)
        second = semantic_vacancy_fingerprint(
            " example   LABS ",
            "HEAD OF PRODUCT",
            "Lead a product and own P&L. " + "Build and scale teams. " * 8,
        )
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("semantic:v1:"))
        self.assertEqual(semantic_vacancy_fingerprint("Example", "Role", "short"), "")


class SearchCoverageIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-coverage-")
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

    def test_plan_check_persistence_report_and_semantic_dedupe(self) -> None:
        self.run_cli("init", "--json")
        plan_input = self.workspace / "tmp" / "plan-input.json"
        plan_input.write_text(
            json.dumps(
                {
                    "run_date": "2026-01-15",
                    "source": "hh",
                    "streams": [
                        {"key": "recommendations", "query": query_spec()},
                        {"key": "target_roles", "query": query_spec()},
                    ],
                }
            ),
            encoding="utf-8",
        )
        manifest_path = self.workspace / "tmp" / "coverage.json"
        self.run_cli(
            "build-coverage-plan",
            "tmp/plan-input.json",
            "--output",
            "tmp/coverage.json",
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for stream in manifest["streams"]:
            stream.update(completed_stream(stream["key"], found=100))
        manifest["totals"] = {"unique": 150, "known": 20, "new": 130}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = json.loads(self.run_cli("check-coverage", "tmp/coverage.json").stdout)
        self.assertTrue(result["ok"])

        description = "Own the product strategy and operating model. " * 8
        batch = self.workspace / "tmp" / "dedupe.json"
        batch.write_text(
            json.dumps(
                [
                    {
                        "date": "2026-01-15",
                        "channel": "hh",
                        "title": "Head of Product",
                        "company": "Example Labs",
                        "description": description,
                        "url": "https://hh.ru/vacancy/10001",
                    },
                    {
                        "date": "2026-01-15",
                        "channel": "hh",
                        "title": "HEAD OF PRODUCT",
                        "company": "example labs",
                        "description": description,
                        "url": "https://hh.ru/vacancy/10002",
                    },
                ]
            ),
            encoding="utf-8",
        )
        self.run_cli("ingest-json", str(batch), "--json")
        short_batch = self.workspace / "tmp" / "short-dedupe.json"
        short_batch.write_text(
            json.dumps(
                [
                    {
                        "date": "2026-01-15",
                        "channel": "hh",
                        "title": "Operations Lead",
                        "company": "Example Labs",
                        "description": "Short preview",
                        "url": "https://hh.ru/vacancy/20001",
                    },
                    {
                        "date": "2026-01-15",
                        "channel": "hh",
                        "title": "Operations Lead",
                        "company": "Example Labs",
                        "description": "Short preview",
                        "url": "https://hh.ru/vacancy/20002",
                    },
                ]
            ),
            encoding="utf-8",
        )
        self.run_cli("ingest-json", str(short_batch), "--json")
        database = self.workspace / "data" / "job_search.sqlite"
        with sqlite3.connect(database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM search_runs").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM search_coverage").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM source_hits").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancy_fingerprints").fetchone()[0], 1)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM vacancy_external_aliases").fetchone()[0],
                4,
            )
        report = (self.workspace / "reports" / "search_coverage.md").read_text(encoding="utf-8")
        self.assertIn("Run 2026-01-15 / hh: completed", report)

    def test_schema_migration_requires_and_creates_backup(self) -> None:
        self.run_cli("init", "--json")
        database = self.workspace / "data" / "job_search.sqlite"
        with sqlite3.connect(database) as conn:
            conn.execute("DROP TABLE search_coverage")
            conn.execute("DROP TABLE search_runs")
            conn.execute("DROP TABLE vacancy_fingerprints")
            conn.execute("DROP TABLE vacancy_external_aliases")
            conn.execute("PRAGMA user_version = 1")
            conn.commit()
        blocked = self.run_cli("doctor", "--strict", "--json", check=False)
        self.assertNotEqual(blocked.returncode, 0)
        migrated = json.loads(self.run_cli("migrate-schema", "--json").stdout)
        self.assertEqual(migrated["from_version"], 1)
        self.assertEqual(migrated["to_version"], 4)
        self.assertTrue(migrated["backup"])
        self.assertTrue(list(database.parent.glob("job_search.sqlite.bak-schema-v1-*")))
        doctor = json.loads(self.run_cli("doctor", "--strict", "--json").stdout)
        self.assertTrue(doctor["ok"])


if __name__ == "__main__":
    unittest.main()
