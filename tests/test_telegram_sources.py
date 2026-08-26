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
sys.path.insert(0, str(ROOT / "scripts"))

from jobsearch_config import TelegramChannelSettings  # noqa: E402
from telegram_source import (  # noqa: E402
    TELEGRAM_COUNT_CONTRACT,
    build_telegram_plan,
    validate_telegram_manifest,
)


HANDLE = "example_exec_jobs"
STREAM = f"telegram:{HANDLE}"


class TelegramCountContractTests(unittest.TestCase):
    channel = TelegramChannelSettings(
        handle=HANDLE,
        url=f"https://t.me/{HANDLE}",
        preview_url=f"https://t.me/s/{HANDLE}",
    )
    checkpoints = {
        STREAM: {
            "cursor_value": "100",
            "cursor_date": "2026-08-08",
            "last_completed_run_date": "2026-08-08",
        }
    }

    def validate(
        self,
        *,
        pages: list[dict[str, object]],
        posts: list[dict[str, object]],
        boundary: dict[str, object],
        found: int,
        evidence: dict[str, dict[str, object]] | None = None,
        unique: int = 0,
        known: int = 0,
        new: int = 0,
    ) -> dict[str, object]:
        plan = build_telegram_plan(
            "2026-08-09",
            [self.channel],
            initial_lookback_days=30,
            checkpoints=self.checkpoints,
        )
        stream = plan["streams"][0]
        stream.update(
            {
                "status": "completed",
                "pages": pages,
                "posts": posts,
                "boundary": boundary,
                "found": found,
                "unique": unique,
                "known": known,
                "new": new,
            }
        )
        plan["totals"] = {"unique": unique, "known": known, "new": new}
        return validate_telegram_manifest(
            plan,
            [self.channel],
            initial_lookback_days=30,
            checkpoints=self.checkpoints,
            vacancy_evidence=evidence or {},
        )

    @staticmethod
    def post(
        post_id: int,
        classification: str,
        *,
        external_ids: list[str] | None = None,
        url: str | None = None,
    ) -> dict[str, object]:
        return {
            "post_id": post_id,
            "posted_at": "2026-08-09T09:00:00Z" if post_id > 100 else "2026-08-08T09:00:00Z",
            "url": url or f"https://t.me/{HANDLE}/{post_id}",
            "classification": classification,
            "vacancy_external_ids": external_ids or [],
        }

    def assert_contract(
        self,
        result: dict[str, object],
        *,
        raw: int,
        processed: int,
        reconciled: int,
    ) -> None:
        stream = result["streams"][0]
        self.assertEqual(stream["count_contract"], TELEGRAM_COUNT_CONTRACT)
        self.assertEqual(
            (stream["raw"], stream["processed"], stream["reconciled"]),
            (raw, processed, reconciled),
        )
        self.assertGreaterEqual(raw, processed)
        self.assertGreaterEqual(processed, reconciled)

    def test_empty_delta_has_zero_source_units(self) -> None:
        result = self.validate(
            pages=[{"url": f"https://t.me/s/{HANDLE}", "post_ids": []}],
            posts=[],
            boundary={"reached": True, "kind": "channel_start", "value": ""},
            found=0,
        )
        self.assertTrue(result["ok"], result["issues"])
        self.assert_contract(result, raw=0, processed=0, reconciled=0)

    def test_delta_boundary_is_raw_and_processed_but_not_found(self) -> None:
        result = self.validate(
            pages=[{"url": f"https://t.me/s/{HANDLE}", "post_ids": [100]}],
            posts=[self.post(100, "out_of_scope")],
            boundary={"reached": True, "kind": "post_id", "value": 100},
            found=0,
        )
        self.assertTrue(result["ok"], result["issues"])
        self.assertEqual(result["streams"][0]["found"], 0)
        self.assert_contract(result, raw=1, processed=1, reconciled=0)

    def test_boundary_plus_new_posts_preserves_actual_units(self) -> None:
        result = self.validate(
            pages=[{"url": f"https://t.me/s/{HANDLE}", "post_ids": [102, 101, 100]}],
            posts=[
                self.post(102, "non_vacancy"),
                self.post(101, "non_vacancy"),
                self.post(100, "out_of_scope"),
            ],
            boundary={"reached": True, "kind": "post_id", "value": 100},
            found=2,
        )
        self.assertTrue(result["ok"], result["issues"])
        self.assert_contract(result, raw=3, processed=3, reconciled=0)

    def test_duplicate_page_observation_increases_raw_without_duplicate_processing(self) -> None:
        result = self.validate(
            pages=[
                {"url": f"https://t.me/s/{HANDLE}", "post_ids": [101, 100]},
                {"url": f"https://t.me/s/{HANDLE}?before=100", "post_ids": [100]},
            ],
            posts=[self.post(101, "non_vacancy"), self.post(100, "out_of_scope")],
            boundary={"reached": True, "kind": "post_id", "value": 100},
            found=1,
        )
        self.assertTrue(result["ok"], result["issues"])
        self.assert_contract(result, raw=3, processed=2, reconciled=0)

    def test_out_of_scope_evidence_remains_counted(self) -> None:
        result = self.validate(
            pages=[{"url": f"https://t.me/s/{HANDLE}", "post_ids": [101, 100, 99]}],
            posts=[
                self.post(101, "non_vacancy"),
                self.post(100, "out_of_scope"),
                self.post(99, "out_of_scope"),
            ],
            boundary={"reached": True, "kind": "post_id", "value": 100},
            found=1,
        )
        self.assertTrue(result["ok"], result["issues"])
        self.assert_contract(result, raw=3, processed=3, reconciled=0)

    def test_malformed_post_is_raw_but_not_processed_or_reconciled(self) -> None:
        external_id = f"telegram:{HANDLE}:101"
        result = self.validate(
            pages=[{"url": f"https://t.me/s/{HANDLE}", "post_ids": [101]}],
            posts=[
                self.post(
                    101,
                    "vacancy",
                    external_ids=[external_id],
                    url=f"https://t.me/{HANDLE}/wrong",
                )
            ],
            boundary={"reached": True, "kind": "channel_start", "value": ""},
            found=1,
            unique=0,
            evidence={
                external_id: {
                    "url": f"https://t.me/{HANDLE}/101",
                    "score": 90,
                    "source_streams": [STREAM],
                    "vacancy_id": 1,
                }
            },
        )
        self.assertFalse(result["ok"])
        self.assert_contract(result, raw=1, processed=0, reconciled=0)

    def test_multiple_new_posts_and_multi_vacancy_post_reconcile_exactly(self) -> None:
        first = f"telegram:{HANDLE}:103:first"
        second = f"telegram:{HANDLE}:103:second"
        third = f"telegram:{HANDLE}:102"
        evidence = {
            first: {
                "url": f"https://t.me/{HANDLE}/103",
                "score": 91,
                "source_streams": [STREAM],
                "vacancy_id": 1,
            },
            second: {
                "url": f"https://t.me/{HANDLE}/103",
                "score": 89,
                "source_streams": [STREAM],
                "vacancy_id": 2,
            },
            third: {
                "url": f"https://t.me/{HANDLE}/102",
                "score": 87,
                "source_streams": [STREAM],
                "vacancy_id": 3,
            },
        }
        result = self.validate(
            pages=[{"url": f"https://t.me/s/{HANDLE}", "post_ids": [103, 102, 100]}],
            posts=[
                self.post(103, "vacancy", external_ids=[first, second]),
                self.post(102, "vacancy", external_ids=[third]),
                self.post(100, "out_of_scope"),
            ],
            boundary={"reached": True, "kind": "post_id", "value": 100},
            found=2,
            unique=3,
            new=3,
            evidence=evidence,
        )
        self.assertTrue(result["ok"], result["issues"])
        self.assert_contract(result, raw=4, processed=4, reconciled=3)
        repeated = self.validate(
            pages=[{"url": f"https://t.me/s/{HANDLE}", "post_ids": [103, 102, 100]}],
            posts=[
                self.post(103, "vacancy", external_ids=[first, second]),
                self.post(102, "vacancy", external_ids=[third]),
                self.post(100, "out_of_scope"),
            ],
            boundary={"reached": True, "kind": "post_id", "value": 100},
            found=2,
            unique=3,
            new=3,
            evidence=evidence,
        )
        self.assertEqual(repeated, result)


class TelegramSourceIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-telegram-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.config = self.workspace / "synthetic-settings.toml"
        self.config.write_text(
            f"""
[project]
title = "Synthetic Telegram Test"
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

[telegram]
enabled = true
initial_lookback_days = 30
channels = ["https://t.me/{HANDLE}/"]

[search]
required_streams = ["recommendations", "target_roles"]
default_period_days = 3
items_per_page = 100
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

    def build_plan(self, run_date: str, name: str) -> tuple[Path, dict[str, object]]:
        path = self.workspace / "tmp" / name
        self.run_cli(
            "build-telegram-plan",
            "--run-date",
            run_date,
            "--output",
            str(path),
            *self.config_args,
            "--json",
        )
        return path, json.loads(path.read_text(encoding="utf-8"))

    def ingest_vacancy(self, post_id: int, date: str, *, score: int = 88) -> None:
        path = self.workspace / "tmp" / f"telegram-{post_id}.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "date": date,
                        "channel": "telegram",
                        "source": "telegram_public_channel",
                        "source_stream": STREAM,
                        "external_id": f"telegram:{HANDLE}:{post_id}",
                        "title": "Product General Manager",
                        "company": "Example Labs",
                        "description": "Own the product portfolio and operating plan. " * 8,
                        "url": f"https://t.me/{HANDLE}/{post_id}",
                        "kind": "screening",
                        "stage": "seen",
                        "status": "NEEDS_REVIEW",
                        "score": score,
                        "reason": "Synthetic Telegram vacancy evidence",
                    }
                ]
            ),
            encoding="utf-8",
        )
        self.run_cli("ingest-json", str(path), *self.config_args, "--json")

    def test_first_scan_backfills_30_days_then_next_plan_uses_delta_cursor(self) -> None:
        manifest_path, manifest = self.build_plan(
            "2026-08-08", "telegram-coverage-first.json"
        )
        stream = manifest["streams"][0]
        self.assertEqual(stream["key"], STREAM)
        self.assertEqual(stream["query"]["mode"], "backfill")
        self.assertEqual(stream["query"]["since_date"], "2026-07-09")
        self.assertIsNone(stream["query"]["after_post_id"])

        self.ingest_vacancy(102, "2026-08-07")
        stream.update(
            {
                "status": "completed",
                "pages": [
                    {
                        "url": f"https://t.me/s/{HANDLE}",
                        "post_ids": [102, 101, 80],
                    }
                ],
                "posts": [
                    {
                        "post_id": 102,
                        "posted_at": "2026-08-07T12:00:00+03:00",
                        "url": f"https://t.me/{HANDLE}/102",
                        "classification": "vacancy",
                        "vacancy_external_ids": [f"telegram:{HANDLE}:102"],
                    },
                    {
                        "post_id": 101,
                        "posted_at": "2026-08-06T12:00:00+03:00",
                        "url": f"https://t.me/{HANDLE}/101",
                        "classification": "non_vacancy",
                        "vacancy_external_ids": [],
                    },
                    {
                        "post_id": 80,
                        "posted_at": "2026-07-08T12:00:00+03:00",
                        "url": f"https://t.me/{HANDLE}/80",
                        "classification": "out_of_scope",
                        "vacancy_external_ids": [],
                    },
                ],
                "boundary": {
                    "reached": True,
                    "kind": "date",
                    "value": "2026-07-08",
                },
                "found": 2,
                "unique": 1,
                "known": 0,
                "new": 1,
            }
        )
        manifest["totals"] = {"unique": 1, "known": 0, "new": 1}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        checked = json.loads(
            self.run_cli(
                "check-telegram-coverage", str(manifest_path), *self.config_args
            ).stdout
        )
        self.assertTrue(checked["ok"], checked["issues"])

        with sqlite3.connect(self.database) as conn:
            checkpoint = conn.execute(
                """
                SELECT cursor_value, cursor_date, last_completed_run_date
                FROM source_checkpoints
                WHERE source = 'telegram' AND stream_key = ?
                """,
                (STREAM,),
            ).fetchone()
            self.assertEqual(checkpoint, ("102", "2026-08-07", "2026-08-08"))

        _, delta = self.build_plan("2026-08-09", "telegram-coverage-delta.json")
        delta_query = delta["streams"][0]["query"]
        self.assertEqual(delta_query["mode"], "delta")
        self.assertEqual(delta_query["after_post_id"], 102)
        self.assertEqual(delta_query["since_date"], "2026-08-08")
        checkpoint_report = (
            self.workspace / "reports" / "source_checkpoints.md"
        ).read_text(encoding="utf-8")
        self.assertIn(STREAM, checkpoint_report)
        self.assertIn("2026-08-08", checkpoint_report)

    def test_failed_delta_does_not_advance_checkpoint(self) -> None:
        first_path, first = self.build_plan("2026-08-08", "first-empty.json")
        first_stream = first["streams"][0]
        first_stream.update(
            {
                "status": "completed",
                "pages": [{"url": f"https://t.me/s/{HANDLE}", "post_ids": []}],
                "posts": [],
                "boundary": {
                    "reached": True,
                    "kind": "channel_start",
                    "value": "",
                },
                "found": 0,
                "unique": 0,
                "known": 0,
                "new": 0,
            }
        )
        first["totals"] = {"unique": 0, "known": 0, "new": 0}
        first_path.write_text(json.dumps(first), encoding="utf-8")
        self.run_cli("check-telegram-coverage", str(first_path), *self.config_args)

        failed_path, failed = self.build_plan("2026-08-09", "failed-delta.json")
        failed_stream = failed["streams"][0]
        failed_stream.update(
            {
                "status": "completed",
                "pages": [
                    {
                        "url": f"https://t.me/s/{HANDLE}",
                        "post_ids": [103],
                    }
                ],
                "posts": [
                    {
                        "post_id": 103,
                        "posted_at": "2026-08-09T09:00:00+03:00",
                        "url": f"https://t.me/{HANDLE}/103",
                        "classification": "vacancy",
                        "vacancy_external_ids": [f"telegram:{HANDLE}:103"],
                    }
                ],
                "boundary": {
                    "reached": True,
                    "kind": "channel_start",
                    "value": "",
                },
                "found": 1,
                "unique": 1,
                "known": 0,
                "new": 1,
            }
        )
        failed["totals"] = {"unique": 1, "known": 0, "new": 1}
        failed_path.write_text(json.dumps(failed), encoding="utf-8")
        result = self.run_cli(
            "check-telegram-coverage", str(failed_path), *self.config_args, check=False
        )
        self.assertNotEqual(result.returncode, 0)
        checked = json.loads(result.stdout)
        self.assertTrue(
            any("не найден в SQLite" in issue for issue in checked["issues"]),
            checked["issues"],
        )
        with sqlite3.connect(self.database) as conn:
            checkpoint = conn.execute(
                """
                SELECT cursor_value, last_completed_run_date
                FROM source_checkpoints
                WHERE source = 'telegram' AND stream_key = ?
                """,
                (STREAM,),
            ).fetchone()
            self.assertEqual(checkpoint, ("", "2026-08-08"))

        _, retry = self.build_plan("2026-08-10", "retry-delta.json")
        self.assertEqual(retry["streams"][0]["query"]["mode"], "delta")
        self.assertIsNone(retry["streams"][0]["query"]["after_post_id"])
        self.assertEqual(retry["streams"][0]["query"]["since_date"], "2026-08-08")

    def test_schema_v4_migration_creates_checkpoint_table_and_backup(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.execute("DROP TABLE source_checkpoints")
            conn.execute("PRAGMA user_version = 4")
            conn.commit()
        migrated = json.loads(
            self.run_cli("migrate-schema", *self.config_args, "--json").stdout
        )
        self.assertEqual(migrated["from_version"], 4)
        self.assertEqual(migrated["to_version"], 11)
        self.assertTrue(migrated["backup"])
        self.assertTrue(list(self.database.parent.glob("job_search.sqlite.bak-schema-v4-*")))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 11)
            self.assertIsNotNone(
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_checkpoints'"
                ).fetchone()
            )
        doctor = json.loads(
            self.run_cli("doctor", *self.config_args, "--strict", "--json").stdout
        )
        self.assertTrue(doctor["ok"])
        self.assertTrue(doctor["telegram_enabled"])
        self.assertEqual(doctor["telegram_channels"], [HANDLE])


if __name__ == "__main__":
    unittest.main()
