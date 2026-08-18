from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JOBCTL = ROOT / "scripts" / "jobctl.py"


class DailyRunP0Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="find-dream-job-p0-")
        self.workspace = Path(self.temp_dir.name)
        self.env = os.environ.copy()
        self.env["JOB_SEARCH_HOME"] = str(self.workspace)
        self.run_cli("init", "--json")
        self.database = self.workspace / "data" / "job_search.sqlite"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def run_cli(
        self,
        *args: str,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(JOBCTL), *args],
            cwd=ROOT,
            env=env or self.env,
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

    def generated_signature(self) -> tuple[str, tuple[tuple[str, int, str], ...]]:
        current = self.workspace / ".jobctl" / "projections" / "current"
        generation = os.readlink(current)
        rows: list[tuple[str, int, str]] = []
        root = current.resolve()
        for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
            payload = path.read_bytes()
            rows.append(
                (
                    path.relative_to(root).as_posix(),
                    len(payload),
                    hashlib.sha256(payload).hexdigest(),
                )
            )
        return generation, tuple(rows)

    def write_payload(self, name: str, rows: list[dict[str, object]]) -> Path:
        path = self.workspace / "tmp" / name
        path.write_text(json.dumps({"vacancies": rows}), encoding="utf-8")
        return path

    @staticmethod
    def vacancy(
        external_id: str,
        *,
        action_state: str = "review",
        bucket: str = "deep_review",
        index: int = 0,
    ) -> dict[str, object]:
        return {
            "date": "2026-08-18",
            "channel": "company_site",
            "source": "synthetic_p0",
            "source_stream": "synthetic_p0",
            "external_id": f"company_site:{external_id}",
            "title": f"Synthetic Role {index:05d}",
            "company": f"Synthetic Employer {index % 100:03d}",
            "url": f"https://example.test/jobs/{external_id}",
            "status": "NEEDS_REVIEW",
            "stage": "seen",
            "score": 70 + index % 20,
            "action_state": action_state,
            "action_bucket": bucket,
            "action_at": "2026-08-18T09:00:00",
            "action_priority": index % 101,
            "priority_reason": "Synthetic P0 fixture",
        }

    def ingest_one(self) -> int:
        payload = self.write_payload("one.json", [self.vacancy("one")])
        self.run_cli("ingest-json", str(payload), "--json")
        with contextlib.closing(sqlite3.connect(self.database)) as conn:
            return int(conn.execute("SELECT id FROM vacancies").fetchone()[0])

    def test_deferred_init_creates_durable_dirty_database_without_outputs(self) -> None:
        with tempfile.TemporaryDirectory(prefix="find-dream-job-p0-deferred-init-") as temp:
            workspace = Path(temp)
            env = os.environ.copy()
            env["JOB_SEARCH_HOME"] = str(workspace)
            initialized = json.loads(
                self.run_cli("init", "--defer-render", "--json", env=env).stdout
            )
            self.assertTrue(initialized["render_deferred"])
            self.assertTrue(initialized["projection_state"]["dirty"])
            self.assertTrue((workspace / "data" / "job_search.sqlite").is_file())
            self.assertFalse((workspace / "dashboard" / "index.html").exists())
            rebuilt = json.loads(self.run_cli("rebuild", "--json", env=env).stdout)
            self.assertFalse(rebuilt["projection_state"]["dirty"])
            self.assertTrue((workspace / "dashboard" / "index.html").is_file())

    def test_deferred_external_action_sequence_is_durable_and_one_finalize_renders(self) -> None:
        vacancy_id = self.ingest_one()
        before = self.generated_signature()
        lease = json.loads(
            self.run_cli(
                "begin-daily-run", "--run-id", "synthetic-p0-run", "--json"
            ).stdout
        )["run_lease"]
        concurrent = self.run_cli(
            "begin-daily-run",
            "--run-id",
            "synthetic-p0-run-2",
            "--json",
            check=False,
        )
        self.assertNotEqual(concurrent.returncode, 0)
        self.assertIn("Нельзя начать второй daily run", concurrent.stderr)
        missing_lease = self.run_cli(
            "set-current-action",
            "--id",
            str(vacancy_id),
            "--action-state",
            "review",
            "--bucket",
            "deep_review",
            "--source",
            "synthetic_test",
            "--defer-render",
            check=False,
        )
        self.assertNotEqual(missing_lease.returncode, 0)
        self.assertIn("активен другой daily run", missing_lease.stderr)
        intermediate_write = self.run_cli(
            "set-current-action",
            "--id",
            str(vacancy_id),
            "--action-state",
            "review",
            "--bucket",
            "deep_review",
            "--source",
            "synthetic_test",
            "--run-lease",
            lease,
            check=False,
        )
        self.assertNotEqual(intermediate_write.returncode, 0)
        self.assertIn("требует --defer-render", intermediate_write.stderr)
        intermediate_rebuild = self.run_cli(
            "rebuild", "--run-lease", lease, "--json", check=False
        )
        self.assertNotEqual(intermediate_rebuild.returncode, 0)
        self.assertIn("Полный render запрещён", intermediate_rebuild.stderr)
        common = (
            "--id",
            str(vacancy_id),
            "--action-key",
            "synthetic-application-v1",
            "--action-type",
            "application",
            "--source",
            "synthetic_test",
            "--defer-render",
            "--run-lease",
            lease,
            "--json",
        )
        self.run_cli(
            "record-external-action",
            *common,
            "--state",
            "authorized",
            "--authorization-note",
            "Synthetic exact authorization",
        )
        self.run_cli(
            "record-external-action",
            *common,
            "--state",
            "attempted",
            "--evidence-note",
            "Synthetic attempt",
        )
        confirmed_args = (
            "record-external-action",
            *common,
            "--state",
            "visibly_confirmed",
            "--evidence-note",
            "Synthetic visible confirmation",
            "--external-reference",
            "synthetic-visible-1",
        )
        self.run_cli(*confirmed_args)
        self.run_cli(*confirmed_args)
        self.assertEqual(self.generated_signature(), before)

        with contextlib.closing(sqlite3.connect(self.database)) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM external_actions").fetchone()[0], 3
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM lifecycle_events WHERE event_type='application_confirmed'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0], 1)
            dirty, rendered = conn.execute(
                "SELECT dirty_revision, rendered_revision FROM projection_state"
            ).fetchone()
            self.assertGreater(dirty, rendered)

        finalized = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertFalse(finalized["projection_state"]["dirty"])
        after = self.generated_signature()
        self.assertNotEqual(after, before)
        repeated = json.loads(
            self.run_cli(
                "finalize-daily-run", "--run-lease", lease, "--json"
            ).stdout
        )
        self.assertTrue(repeated["already_finalized"])
        self.assertEqual(self.generated_signature(), after)

    def test_failed_and_concurrent_render_never_publish_partial_outputs(self) -> None:
        vacancy_id = self.ingest_one()
        baseline = self.generated_signature()
        self.run_cli(
            "set-current-action",
            "--id",
            str(vacancy_id),
            "--action-state",
            "needs_input",
            "--bucket",
            "urgent",
            "--source",
            "synthetic_test",
            "--defer-render",
        )
        interrupted_env = self.env.copy()
        interrupted_env["JOBCTL_TEST_FAIL_RENDER_BEFORE_PUBLISH"] = "1"
        interrupted = self.run_cli(
            "rebuild", "--json", check=False, env=interrupted_env
        )
        self.assertNotEqual(interrupted.returncode, 0)
        self.assertEqual(self.generated_signature(), baseline)

        delayed_env = self.env.copy()
        delayed_env["JOBCTL_TEST_RENDER_DELAY_SECONDS"] = "3"
        first = subprocess.Popen(
            [sys.executable, str(JOBCTL), "rebuild", "--json"],
            cwd=ROOT,
            env=delayed_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        writer_lock = self.database.parent / f".{self.database.name}.writer.lock"
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if first.poll() is not None:
                break
            if writer_lock.exists() and "rebuild" in writer_lock.read_text(
                encoding="utf-8"
            ):
                time.sleep(0.1)
                break
            time.sleep(0.02)
        second = self.run_cli(
            "rebuild", "--lock-timeout", "0.05", "--json", check=False
        )
        self.assertNotEqual(second.returncode, 0)
        self.assertIn("Блокировка 'writer'", second.stderr)
        self.assertEqual(self.generated_signature(), baseline)
        stdout, stderr = first.communicate(timeout=15)
        self.assertEqual(first.returncode, 0, f"stdout={stdout}\nstderr={stderr}")

        published = self.generated_signature()
        self.assertNotEqual(published, baseline)
        manifest_path = (
            self.workspace / ".jobctl" / "projections" / "current" / "manifest.json"
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertIn(manifest["generation"], os.readlink(manifest_path.parent))
        staging = self.workspace / ".jobctl" / "projections" / "staging"
        self.assertEqual(list(staging.iterdir()), [])

        self.run_cli(
            "set-current-action",
            "--id",
            str(vacancy_id),
            "--action-state",
            "review",
            "--bucket",
            "deep_review",
            "--priority",
            "91",
            "--source",
            "synthetic_interrupt_test",
            "--defer-render",
        )
        before_interrupt = self.generated_signature()
        interrupt_env = self.env.copy()
        interrupt_env["JOBCTL_TEST_RENDER_DELAY_SECONDS"] = "10"
        interrupted_process = subprocess.Popen(
            [sys.executable, str(JOBCTL), "rebuild", "--json"],
            cwd=ROOT,
            env=interrupt_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not any(staging.iterdir()):
            if interrupted_process.poll() is not None:
                break
            time.sleep(0.02)
        self.assertIsNone(interrupted_process.poll())
        self.assertTrue(any(staging.iterdir()))
        interrupted_process.terminate()
        interrupted_process.communicate(timeout=5)
        self.assertNotEqual(interrupted_process.returncode, 0)
        self.assertEqual(self.generated_signature(), before_interrupt)

        # SIGTERM leaves metadata and may leave staging, but neither is
        # authoritative.  The kernel lock is released and the exact rebuild is
        # resumable even with a zero bounded wait.
        self.run_cli("rebuild", "--lock-timeout", "0", "--json")
        self.assertNotEqual(self.generated_signature(), before_interrupt)
        self.assertEqual(list(staging.iterdir()), [])

    def test_wip_filters_inactive_and_terminal_rows_and_keeps_pagination(self) -> None:
        rows = [self.vacancy(f"active-{index:03d}", index=index) for index in range(225)]
        rows.append(
            self.vacancy(
                "inactive-none", action_state="none", bucket="backlog", index=225
            )
        )
        rows.append(self.vacancy("terminal-rejected", index=226))
        payload = self.write_payload("wip.json", rows)
        self.run_cli("ingest-json", str(payload), "--defer-render", "--json")
        with contextlib.closing(sqlite3.connect(self.database)) as conn:
            terminal_id = int(
                conn.execute(
                    "SELECT id FROM vacancies WHERE external_id='company_site:terminal-rejected'"
                ).fetchone()[0]
            )
        self.run_cli(
            "record-lifecycle-event",
            "--id",
            str(terminal_id),
            "--event-type",
            "rejected",
            "--evidence-note",
            "Synthetic terminal evidence",
            "--source",
            "synthetic_test",
            "--defer-render",
            "--json",
        )
        pages = [
            json.loads(
                self.run_cli(
                    "wip-queue",
                    "--as-of",
                    "2026-08-19",
                    "--page",
                    str(page),
                    "--page-size",
                    "100",
                    "--json",
                ).stdout
            )
            for page in (1, 2, 3)
        ]
        self.assertEqual(pages[0]["pagination"]["total_items"], 225)
        self.assertEqual([len(page["items"]) for page in pages], [100, 100, 25])
        ids = [item["vacancy_id"] for page in pages for item in page["items"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertNotIn(terminal_id, ids)
        self.assertTrue(all(item["action_state"] != "none" for page in pages for item in page["items"]))
        self.assertTrue(all(item["bucket"] != "backlog" for page in pages for item in page["items"]))

        self.run_cli("rebuild", "--json")
        self.assertTrue((self.workspace / "views" / "wip_queue_page_0003.md").is_file())
        with contextlib.closing(sqlite3.connect(self.database)) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM action_events").fetchone()[0], 227)

    def test_schema_v7_migration_adds_projection_control_without_evidence_changes(self) -> None:
        self.ingest_one()
        with contextlib.closing(sqlite3.connect(self.database)) as conn:
            before = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("vacancies", "action_events", "external_actions", "lifecycle_events")
            }
            conn.execute("DROP TABLE daily_run_leases")
            conn.execute("DROP TABLE projection_state")
            conn.execute("PRAGMA user_version = 7")
            conn.commit()
        migrated = json.loads(
            self.run_cli("migrate-schema", "--defer-render", "--json").stdout
        )
        self.assertEqual(migrated["from_version"], 7)
        self.assertEqual(migrated["to_version"], 8)
        self.assertTrue(migrated["render_deferred"])
        self.assertTrue(migrated["projection_state"]["dirty"])
        backups = list(self.database.parent.glob("job_search.sqlite.bak-schema-v7-*"))
        self.assertEqual(len(backups), 1)
        with contextlib.closing(sqlite3.connect(self.database)) as conn:
            after = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in before
            }
            self.assertEqual(before, after)
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 8)
            self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
        self.run_cli("rebuild", "--json")


class DailyRunP0ScaleTests(unittest.TestCase):
    def test_25000_deferred_rows_render_once_with_compact_dashboard_and_one_wip_file(self) -> None:
        with tempfile.TemporaryDirectory(prefix="find-dream-job-p0-25k-") as temp:
            workspace = Path(temp)
            env = os.environ.copy()
            env["JOB_SEARCH_HOME"] = str(workspace)

            def run(*args: str) -> subprocess.CompletedProcess[str]:
                result = subprocess.run(
                    [sys.executable, str(JOBCTL), *args],
                    cwd=ROOT,
                    env=env,
                    check=False,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    f"jobctl {' '.join(args)}\nstdout={result.stdout}\nstderr={result.stderr}",
                )
                return result

            run("init", "--json")
            current = workspace / ".jobctl" / "projections" / "current"
            before_generation = os.readlink(current)
            rows = [
                DailyRunP0Tests.vacancy(
                    f"scale-{index:05d}",
                    action_state="none",
                    bucket="backlog",
                    index=index,
                )
                for index in range(25_000)
            ]
            payload = workspace / "tmp" / "scale-25000.json"
            payload.write_text(json.dumps({"vacancies": rows}), encoding="utf-8")
            ingested = json.loads(
                run("ingest-json", str(payload), "--defer-render", "--json").stdout
            )
            self.assertEqual(ingested["ingested"], 25_000)
            self.assertEqual(os.readlink(current), before_generation)
            rebuilt = json.loads(run("rebuild", "--json").stdout)
            self.assertEqual(rebuilt["kpis"]["vacancies"], 25_000)
            self.assertFalse(rebuilt["projection_state"]["dirty"])
            self.assertNotEqual(os.readlink(current), before_generation)

            dashboard = workspace / "dashboard" / "index.html"
            self.assertLess(dashboard.stat().st_size, 2_000_000)
            wip = json.loads(run("wip-queue", "--json").stdout)
            self.assertEqual(wip["pagination"]["total_items"], 0)
            self.assertEqual(
                [path.name for path in (workspace / "views").glob("wip_queue*.md")],
                ["wip_queue.md"],
            )
            with contextlib.closing(
                sqlite3.connect(workspace / "data" / "job_search.sqlite")
            ) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM vacancies").fetchone()[0], 25_000)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM action_events").fetchone()[0], 25_000)


if __name__ == "__main__":
    unittest.main()
