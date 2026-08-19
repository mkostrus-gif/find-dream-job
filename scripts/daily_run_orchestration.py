#!/usr/bin/env python3
"""Durable, deterministic daily-run execution plans.

The module deliberately does not perform remote collection.  It stores and
validates evidence supplied by source-specific collectors and keeps the
execution plan resumable independently of a short-lived writer lease.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import shlex
import sqlite3
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from jobsearch_config import Settings


PLAN_VERSION = 1
RUN_STATES = {
    "running",
    "paused",
    "blocked",
    "needs_verification",
    "finalizing",
    "completed",
}
WORK_STATES = {
    "pending",
    "in_progress",
    "checkpointed",
    "completed",
    "blocked",
    "needs_verification",
    "invalidated",
    "not_applicable",
}
FINISHED_WORK_STATES = {"completed", "not_applicable"}
REMOTE_MANIFEST_KINDS = {
    "hh_stream",
    "telegram_channel",
    "mail_source",
    "inbound_reconciliation",
    "source_gate",
    "workspace_gate",
}
TERMINAL_EXTERNAL_ACTION_STATES = {"visibly_confirmed", "blocked", "failed"}
NONTERMINAL_EXTERNAL_ACTION_STATES = {"drafted", "authorized", "attempted"}
COUNT_FIELDS = (
    "raw",
    "unique",
    "known",
    "new",
    "processed",
    "reconciled",
    "blocked",
    "known_unchanged",
    "known_changed",
    "duplicate_on_page",
    "duplicate_across_pages",
    "duplicate_across_streams",
)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def stable_item_key(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}:{digest}"


def ensure_v9_schema(conn: sqlite3.Connection) -> None:
    """Add the append-only daily-run control plane without rewriting history."""

    lease_sql_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'daily_run_leases'"
    ).fetchone()
    lease_sql = str(lease_sql_row[0] or "") if lease_sql_row else ""
    if lease_sql and "'released'" not in lease_sql:
        conn.executescript(
            """
            DROP INDEX IF EXISTS idx_daily_run_leases_one_active;
            DROP INDEX IF EXISTS idx_daily_run_leases_status;
            ALTER TABLE daily_run_leases RENAME TO daily_run_leases_v8;
            CREATE TABLE daily_run_leases (
                token TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                owner TEXT NOT NULL,
                status TEXT NOT NULL,
                lease_seconds INTEGER NOT NULL,
                acquired_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                released_at TEXT,
                release_reason TEXT,
                CHECK(status IN ('active', 'finalized', 'expired', 'released')),
                CHECK(lease_seconds >= 60 AND lease_seconds <= 86400)
            );
            INSERT INTO daily_run_leases (
                token, run_id, owner, status, lease_seconds, acquired_at,
                heartbeat_at, expires_at, released_at, release_reason
            )
            SELECT token, run_id, owner, status, lease_seconds, acquired_at,
                   heartbeat_at, expires_at, released_at, release_reason
            FROM daily_run_leases_v8;
            DROP TABLE daily_run_leases_v8;
            CREATE UNIQUE INDEX idx_daily_run_leases_one_active
                ON daily_run_leases(status) WHERE status = 'active';
            CREATE INDEX idx_daily_run_leases_status
                ON daily_run_leases(status, expires_at, acquired_at);
            """
        )

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS daily_runs (
            run_id TEXT PRIMARY KEY,
            run_date TEXT NOT NULL,
            timezone TEXT NOT NULL,
            status TEXT NOT NULL,
            plan_version INTEGER NOT NULL,
            plan_revision INTEGER NOT NULL,
            plan_fingerprint TEXT NOT NULL,
            configuration_fingerprint TEXT NOT NULL,
            scope_json TEXT NOT NULL,
            projection_revision_start INTEGER NOT NULL,
            projection_revision_finalizing INTEGER,
            projection_revision_completed INTEGER,
            current_lease_token TEXT,
            last_lease_token TEXT,
            aggregate_metrics_json TEXT NOT NULL DEFAULT '{}',
            blocker_summary_json TEXT NOT NULL DEFAULT '[]',
            last_verified_step_key TEXT,
            next_safe_step_key TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            paused_at TEXT,
            resumed_at TEXT,
            finalization_started_at TEXT,
            completed_at TEXT,
            CHECK(status IN ('running','paused','blocked','needs_verification','finalizing','completed')),
            CHECK(plan_version >= 1),
            CHECK(plan_revision >= 1)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_runs_one_open
            ON daily_runs((1)) WHERE status <> 'completed';
        CREATE INDEX IF NOT EXISTS idx_daily_runs_status
            ON daily_runs(status, created_at, run_date);

        CREATE TABLE IF NOT EXISTS daily_run_plan_revisions (
            run_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            plan_version INTEGER NOT NULL,
            plan_fingerprint TEXT NOT NULL,
            configuration_fingerprint TEXT NOT NULL,
            scope_json TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(run_id, revision),
            FOREIGN KEY(run_id) REFERENCES daily_runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS daily_run_steps (
            run_id TEXT NOT NULL,
            step_key TEXT NOT NULL,
            step_kind TEXT NOT NULL,
            order_no INTEGER NOT NULL,
            required INTEGER NOT NULL,
            state TEXT NOT NULL,
            plan_revision_added INTEGER NOT NULL,
            input_fingerprint TEXT NOT NULL,
            output_fingerprint TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            first_started_at TEXT,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            last_checkpoint_json TEXT,
            manifest_hash TEXT,
            evidence_hash TEXT,
            blocker_code TEXT,
            blocker_reason TEXT,
            retryable INTEGER,
            elapsed_seconds REAL NOT NULL DEFAULT 0,
            token_count INTEGER,
            tool_count INTEGER,
            scope_json TEXT NOT NULL,
            PRIMARY KEY(run_id, step_key),
            FOREIGN KEY(run_id) REFERENCES daily_runs(run_id) ON DELETE CASCADE,
            CHECK(required IN (0,1)),
            CHECK(state IN ('pending','in_progress','checkpointed','completed','blocked','needs_verification','invalidated','not_applicable'))
        );
        CREATE INDEX IF NOT EXISTS idx_daily_run_steps_state
            ON daily_run_steps(run_id, state, order_no, step_key);

        CREATE TABLE IF NOT EXISTS daily_run_step_dependencies (
            run_id TEXT NOT NULL,
            step_key TEXT NOT NULL,
            depends_on_step_key TEXT NOT NULL,
            plan_revision_added INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(run_id, step_key, depends_on_step_key),
            FOREIGN KEY(run_id, step_key)
                REFERENCES daily_run_steps(run_id, step_key) ON DELETE CASCADE
                DEFERRABLE INITIALLY DEFERRED,
            FOREIGN KEY(run_id, depends_on_step_key)
                REFERENCES daily_run_steps(run_id, step_key) ON DELETE CASCADE
                DEFERRABLE INITIALLY DEFERRED
        );

        CREATE TABLE IF NOT EXISTS daily_run_work_items (
            run_id TEXT NOT NULL,
            step_key TEXT NOT NULL,
            item_key TEXT NOT NULL,
            item_kind TEXT NOT NULL,
            order_no INTEGER NOT NULL,
            required INTEGER NOT NULL,
            state TEXT NOT NULL,
            plan_revision_added INTEGER NOT NULL,
            input_fingerprint TEXT NOT NULL,
            output_fingerprint TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            first_started_at TEXT,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            last_checkpoint_json TEXT,
            manifest_hash TEXT,
            evidence_hash TEXT,
            blocker_code TEXT,
            blocker_reason TEXT,
            retryable INTEGER,
            elapsed_seconds REAL NOT NULL DEFAULT 0,
            token_count INTEGER,
            tool_count INTEGER,
            scope_json TEXT NOT NULL,
            PRIMARY KEY(run_id, step_key, item_key),
            FOREIGN KEY(run_id, step_key)
                REFERENCES daily_run_steps(run_id, step_key) ON DELETE CASCADE,
            CHECK(required IN (0,1)),
            CHECK(state IN ('pending','in_progress','checkpointed','completed','blocked','needs_verification','invalidated','not_applicable'))
        );
        CREATE INDEX IF NOT EXISTS idx_daily_run_work_items_state
            ON daily_run_work_items(run_id, state, step_key, order_no, item_key);

        CREATE TABLE IF NOT EXISTS daily_run_manifests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            step_key TEXT NOT NULL,
            item_key TEXT NOT NULL DEFAULT '',
            record_type TEXT NOT NULL,
            manifest_version INTEGER NOT NULL,
            manifest_kind TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            artifact_path TEXT,
            artifact_sha256 TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id, step_key)
                REFERENCES daily_run_steps(run_id, step_key) ON DELETE CASCADE,
            UNIQUE(run_id, step_key, item_key, record_type, payload_hash),
            CHECK(record_type IN ('checkpoint','completion','block','uncertain','invalidation','scope_empty','programmatic')),
            CHECK(validation_status IN ('validated','partial','blocked','invalidated'))
        );
        CREATE INDEX IF NOT EXISTS idx_daily_run_manifests_lookup
            ON daily_run_manifests(run_id, step_key, item_key, id);

        CREATE TABLE IF NOT EXISTS daily_run_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT,
            reason TEXT NOT NULL,
            details_json TEXT NOT NULL,
            event_hash TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES daily_runs(run_id) ON DELETE CASCADE,
            UNIQUE(run_id, event_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_run_transitions_lookup
            ON daily_run_transitions(run_id, id);
        """
    )


def schema_v9_issues(conn: sqlite3.Connection) -> list[str]:
    required = {
        "daily_runs": {"run_id", "run_date", "status", "plan_fingerprint"},
        "daily_run_plan_revisions": {"run_id", "revision", "scope_json"},
        "daily_run_steps": {"run_id", "step_key", "state", "manifest_hash"},
        "daily_run_step_dependencies": {"run_id", "step_key", "depends_on_step_key"},
        "daily_run_work_items": {"run_id", "step_key", "item_key", "state"},
        "daily_run_manifests": {"run_id", "payload_hash", "record_type"},
        "daily_run_transitions": {"run_id", "event_hash", "event_type"},
    }
    issues: list[str] = []
    for table, columns in required.items():
        present = {
            str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        missing = sorted(columns - present)
        if missing:
            issues.append(f"{table}: отсутствуют столбцы {', '.join(missing)}")
    return issues


def append_transition(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    entity_type: str,
    entity_key: str,
    event_type: str,
    from_state: str | None,
    to_state: str | None,
    reason: str,
    details: Mapping[str, Any] | None = None,
) -> bool:
    details_dict = dict(details or {})
    previous = conn.execute(
        """
        SELECT id, event_type, from_state, to_state, reason, details_json, event_hash
        FROM daily_run_transitions
        WHERE run_id = ? AND entity_type = ? AND entity_key = ?
        ORDER BY id DESC LIMIT 1
        """,
        (run_id, entity_type, entity_key),
    ).fetchone()
    if previous is not None and (
        str(previous["event_type"]) == event_type
        and str(previous["from_state"] or "") == str(from_state or "")
        and str(previous["to_state"] or "") == str(to_state or "")
        and str(previous["reason"]) == reason
        and str(previous["details_json"]) == canonical_json(details_dict)
    ):
        return False
    identity = {
        "entity_type": entity_type,
        "entity_key": entity_key,
        "event_type": event_type,
        "from_state": from_state or "",
        "to_state": to_state or "",
        "reason": reason,
        "details": details_dict,
        "previous_event_hash": str(previous["event_hash"]) if previous else "",
    }
    digest = payload_hash(identity)
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO daily_run_transitions (
            run_id, entity_type, entity_key, event_type, from_state, to_state,
            reason, details_json, event_hash, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            entity_type,
            entity_key,
            event_type,
            from_state,
            to_state,
            reason,
            canonical_json(details_dict),
            digest,
            now_iso(),
        ),
    )
    return conn.total_changes > before


def _config_snapshot(settings: Settings, timezone: str) -> dict[str, Any]:
    return {
        "timezone": timezone,
        "required_streams": list(settings.search.required_streams),
        "search_period_days": settings.search.default_period_days,
        "search_items_per_page": settings.search.items_per_page,
        "personal_recommendations": {
            "enabled": settings.search.personal_recommendations_enabled,
            "stream": settings.search.personal_recommendation_stream,
        },
        "hh_acquisition": {
            "incremental_mode": settings.search.hh_acquisition.incremental_mode,
            "minimum_overlap_pages": settings.search.hh_acquisition.minimum_overlap_pages,
            "consecutive_known_boundary_pages": settings.search.hh_acquisition.consecutive_known_boundary_pages,
            "guard_page_required": settings.search.hh_acquisition.guard_page_required,
            "checkpoint_staleness_days": settings.search.hh_acquisition.checkpoint_staleness_days,
            "shadow_runs_required": settings.search.hh_acquisition.shadow_runs_required,
            "full_audit_interval_days": settings.search.hh_acquisition.full_audit_interval_days,
            "page_stability_samples": settings.search.hh_acquisition.page_stability_samples,
            "page_stability_delay_ms": settings.search.hh_acquisition.page_stability_delay_ms,
            "count_drift_recaptures": settings.search.hh_acquisition.count_drift_recaptures,
            "max_pages_per_stream": settings.search.hh_acquisition.max_pages_per_stream,
            "max_returned_ids": settings.search.hh_acquisition.max_returned_ids,
            "personal_initial_depth_pages": settings.search.hh_acquisition.personal_initial_depth_pages,
            "personal_minimum_stable_pages": settings.search.hh_acquisition.personal_minimum_stable_pages,
            "personal_consecutive_known_pages": settings.search.hh_acquisition.personal_consecutive_known_pages,
            "personal_max_pages": settings.search.hh_acquisition.personal_max_pages,
            "personal_max_is_completion_boundary": settings.search.hh_acquisition.personal_max_is_completion_boundary,
        },
        "telegram": {
            "enabled": settings.telegram.enabled,
            "initial_lookback_days": settings.telegram.initial_lookback_days,
            "channels": [
                {"stream_key": channel.stream_key, "url": channel.url}
                for channel in settings.telegram.channels
            ],
        },
        "mail": {
            "scan_linkedin_inbox": settings.mail.scan_linkedin_inbox,
            "archive_processed_linkedin": settings.mail.archive_processed_linkedin,
        },
        "required_gates": [
            {
                "key": gate.key,
                "kind": gate.kind,
                "order": gate.order,
                "depends_on": list(gate.depends_on),
                "required": gate.required,
                "enabled": gate.enabled,
                "require_remote_boundary": gate.require_remote_boundary,
            }
            for gate in settings.daily_run.required_gates
        ],
    }


def configuration_fingerprint(settings: Settings, timezone: str) -> str:
    return payload_hash(_config_snapshot(settings, timezone))


def authoritative_due_followups(
    conn: sqlite3.Connection, run_date: str
) -> list[dict[str, Any]]:
    """Enumerate the complete due queue without WIP pagination or quotas."""

    try:
        dt.date.fromisoformat(run_date)
    except ValueError as exc:
        raise ValueError("Дата запуска должна иметь формат ГГГГ-ММ-ДД.") from exc
    rows = conn.execute(
        """
        SELECT a.id AS application_id, a.vacancy_id, a.follow_up_date,
               a.status AS application_status, v.external_id, v.channel,
               v.company, v.title
        FROM effective_applications a
        JOIN vacancies v ON v.id = a.vacancy_id
        WHERE COALESCE(a.follow_up_date, '') NOT IN ('', '-', '—')
          AND date(a.follow_up_date) <= date(?)
          AND LOWER(COALESCE(a.status, '')) NOT LIKE '%reject%'
          AND LOWER(COALESCE(a.status, '')) NOT LIKE '%отказ%'
          AND EXISTS (
              SELECT 1 FROM lifecycle_events le
              WHERE le.vacancy_id = a.vacancy_id
                AND le.event_type = 'application_confirmed'
          )
          AND NOT EXISTS (
              SELECT 1 FROM lifecycle_events terminal
              WHERE terminal.vacancy_id = a.vacancy_id
                AND terminal.event_type IN ('rejected', 'offer_received')
          )
        ORDER BY date(a.follow_up_date), a.vacancy_id, a.id
        """,
        (run_date,),
    ).fetchall()
    return [dict(row) for row in rows]


def latest_nonterminal_external_actions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT action.*
        FROM external_actions action
        WHERE action.id = (
            SELECT candidate.id
            FROM external_actions candidate
            WHERE candidate.action_key = action.action_key
            ORDER BY candidate.event_at DESC, candidate.id DESC
            LIMIT 1
        )
          AND action.state IN ('drafted', 'authorized', 'attempted')
        ORDER BY action.action_key, action.id
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _step(
    key: str,
    kind: str,
    order: int,
    *,
    required: bool = True,
    enabled: bool = True,
    depends_on: Sequence[str] = (),
    scope: Mapping[str, Any] | None = None,
    items: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    data = dict(scope or {})
    data["enabled"] = enabled
    return {
        "key": key,
        "kind": kind,
        "order": order,
        "required": required,
        "enabled": enabled,
        "depends_on": list(depends_on),
        "scope": data,
        "items": list(items),
    }


def _item(
    key: str,
    kind: str,
    order: int,
    scope: Mapping[str, Any],
    *,
    required: bool = True,
    state: str = "pending",
) -> dict[str, Any]:
    return {
        "key": key,
        "kind": kind,
        "order": order,
        "required": required,
        "state": state,
        "scope": dict(scope),
    }


def _step_input_fingerprint(step: Mapping[str, Any]) -> str:
    return payload_hash(
        {
            "kind": step["kind"],
            "required": bool(step["required"]),
            "enabled": bool(step["enabled"]),
            "depends_on": list(step["depends_on"]),
            "scope": step["scope"],
        }
    )


def _item_input_fingerprint(item: Mapping[str, Any]) -> str:
    return payload_hash(
        {
            "kind": item["kind"],
            "required": bool(item["required"]),
            "scope": item["scope"],
        }
    )


def _validate_plan_graph(steps: Sequence[dict[str, Any]]) -> None:
    keys = {step["key"] for step in steps}
    dependencies: dict[str, set[str]] = {}
    for step in steps:
        unknown = set(step["depends_on"]) - keys
        if unknown:
            raise ValueError(
                f"Шаг {step['key']} ссылается на неизвестные зависимости: "
                + ", ".join(sorted(unknown))
            )
        dependencies[step["key"]] = set(step["depends_on"])
    ready = deque(sorted(key for key, deps in dependencies.items() if not deps))
    visited: set[str] = set()
    while ready:
        key = ready.popleft()
        if key in visited:
            continue
        visited.add(key)
        for candidate, deps in dependencies.items():
            if candidate not in visited and deps <= visited:
                ready.append(candidate)
    if visited != keys:
        raise ValueError(
            "Граф ежедневного запуска содержит цикл: " + ", ".join(sorted(keys - visited))
        )


def build_plan_definition(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_date: str,
    timezone: str,
) -> dict[str, Any]:
    """Capture one deterministic configuration and authoritative SQLite scope."""

    ZoneInfo(timezone)
    hh_items = [
        _item(
            stable_item_key("hh", stream.casefold()),
            "hh_stream",
            index,
            {"source": "hh", "stream_key": stream},
        )
        for index, stream in enumerate(settings.search.required_streams, start=1)
    ]
    telegram_items = [
        _item(
            channel.stream_key,
            "telegram_channel",
            index,
            {
                "source": "telegram",
                "stream_key": channel.stream_key,
                "channel_url": channel.url,
            },
        )
        for index, channel in enumerate(settings.telegram.channels, start=1)
    ]
    mail_items = []
    if settings.mail.scan_linkedin_inbox:
        mail_items.append(
            _item(
                "mail:linkedin_inbox",
                "mail_source",
                1,
                {
                    "source": "mail",
                    "feature": "linkedin_inbox",
                    "archive_enabled": settings.mail.archive_processed_linkedin,
                },
            )
        )
    due_items = [
        _item(
            f"due:{row['vacancy_id']}:{row['application_id']}:{row['follow_up_date']}",
            "due_followup",
            index,
            row,
        )
        for index, row in enumerate(authoritative_due_followups(conn, run_date), start=1)
    ]
    action_items: list[dict[str, Any]] = []
    for index, row in enumerate(latest_nonterminal_external_actions(conn), start=1):
        state = "needs_verification" if row["state"] == "attempted" else "pending"
        action_items.append(
            _item(
                stable_item_key("external", str(row["action_key"])),
                "external_action",
                index,
                {
                    "external_action_id": row["id"],
                    "action_key": row["action_key"],
                    "action_type": row["action_type"],
                    "state": row["state"],
                    "vacancy_id": row["vacancy_id"],
                    "external_reference": row["external_reference"] or "",
                },
                state=state,
            )
        )

    steps: list[dict[str, Any]] = [
        _step(
            "inbound_reconciliation",
            "inbound_reconciliation",
            100,
            scope={"required_scope": "configured inbound sources"},
        ),
        _step(
            "mail_sources",
            "mail_source",
            200,
            required=settings.mail.scan_linkedin_inbox,
            enabled=settings.mail.scan_linkedin_inbox,
            depends_on=("inbound_reconciliation",),
            scope={
                "scan_linkedin_inbox": settings.mail.scan_linkedin_inbox,
                "archive_processed_linkedin": settings.mail.archive_processed_linkedin,
            },
            items=mail_items,
        ),
        _step(
            "hh_coverage",
            "source_coverage",
            300,
            depends_on=("inbound_reconciliation",),
            scope={
                "source": "hh",
                "required_streams": list(settings.search.required_streams),
                "period_days": settings.search.default_period_days,
                "items_per_page": settings.search.items_per_page,
                "hh_acquisition": _config_snapshot(settings, timezone)["hh_acquisition"],
            },
            items=hh_items,
        ),
        _step(
            "telegram_coverage",
            "source_coverage",
            320,
            required=settings.telegram.enabled,
            enabled=settings.telegram.enabled,
            depends_on=("inbound_reconciliation",),
            scope={
                "source": "telegram",
                "initial_lookback_days": settings.telegram.initial_lookback_days,
                "channels": [channel.stream_key for channel in settings.telegram.channels],
            },
            items=telegram_items if settings.telegram.enabled else (),
        ),
        _step(
            "personal_recommendations",
            "source_gate",
            340,
            required=settings.search.personal_recommendations_enabled,
            enabled=settings.search.personal_recommendations_enabled,
            depends_on=("inbound_reconciliation",),
            scope={
                "stream_key": settings.search.personal_recommendation_stream,
                "hh_acquisition": _config_snapshot(settings, timezone)["hh_acquisition"],
            },
        ),
    ]

    builtin_keys = {step["key"] for step in steps} | {
        "external_action_reconciliation",
        "due_followups",
        "sqlite_reconciliation",
        "closeout",
    }
    extra_key_map = {gate.key: f"gate:{gate.key}" for gate in settings.daily_run.required_gates}
    for gate in settings.daily_run.required_gates:
        dependencies: list[str] = []
        for dependency in gate.depends_on:
            if dependency in builtin_keys:
                dependencies.append(dependency)
            elif dependency in extra_key_map:
                dependencies.append(extra_key_map[dependency])
            else:
                raise ValueError(
                    f"Дополнительное условие {gate.key} ссылается на неизвестный шаг {dependency}."
                )
        if not dependencies:
            dependencies = ["inbound_reconciliation"]
        steps.append(
            _step(
                extra_key_map[gate.key],
                gate.kind,
                400 + gate.order,
                required=gate.required,
                enabled=gate.enabled,
                depends_on=dependencies,
                scope={
                    "gate_key": gate.key,
                    "require_remote_boundary": gate.require_remote_boundary,
                },
            )
        )

    source_dependencies = [
        step["key"]
        for step in steps
        if step["enabled"] and step["required"] and step["key"] != "inbound_reconciliation"
    ]
    steps.extend(
        [
            _step(
                "external_action_reconciliation",
                "external_action_reconciliation",
                800,
                depends_on=("inbound_reconciliation",),
                scope={"captured_count": len(action_items)},
                items=action_items,
            ),
            _step(
                "due_followups",
                "due_followup_queue",
                820,
                depends_on=("inbound_reconciliation", "external_action_reconciliation"),
                scope={"captured_count": len(due_items), "as_of": run_date},
                items=due_items,
            ),
        ]
    )
    reconciliation_dependencies = sorted(
        set(source_dependencies)
        | {"inbound_reconciliation", "external_action_reconciliation", "due_followups"}
    )
    steps.extend(
        [
            _step(
                "sqlite_reconciliation",
                "sqlite_reconciliation",
                900,
                depends_on=reconciliation_dependencies,
                scope={"foreign_keys": True, "authoritative_queues": True},
            ),
            _step(
                "closeout",
                "internal_closeout",
                1000,
                required=False,
                depends_on=("sqlite_reconciliation",),
                scope={"atomic_projection_publication": True},
            ),
        ]
    )
    steps.sort(key=lambda step: (step["order"], step["key"]))
    _validate_plan_graph(steps)
    config = _config_snapshot(settings, timezone)
    scope = {
        "run_date": run_date,
        "timezone": timezone,
        "configuration": config,
        "steps": steps,
    }
    return {
        "plan_version": PLAN_VERSION,
        "configuration_fingerprint": payload_hash(config),
        "plan_fingerprint": payload_hash(scope),
        "scope": scope,
        "steps": steps,
    }


def _initial_step_state(step: Mapping[str, Any]) -> str:
    if not step["enabled"]:
        return "not_applicable"
    items = list(step["items"])
    if items:
        states = {str(item["state"]) for item in items if item["required"]}
        if "needs_verification" in states:
            return "needs_verification"
        return "pending"
    if step["key"] in {"external_action_reconciliation", "due_followups"}:
        return "completed"
    return "pending"


def create_run(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    run_date: str,
    timezone: str,
    projection_revision_start: int,
    lease_token: str,
) -> dict[str, Any]:
    plan = build_plan_definition(
        conn, settings, run_date=run_date, timezone=timezone
    )
    timestamp = now_iso()
    conn.execute(
        """
        INSERT INTO daily_runs (
            run_id, run_date, timezone, status, plan_version, plan_revision,
            plan_fingerprint, configuration_fingerprint, scope_json,
            projection_revision_start, current_lease_token, last_lease_token,
            aggregate_metrics_json, blocker_summary_json, created_at, updated_at,
            resumed_at
        ) VALUES (?, ?, ?, 'running', ?, 1, ?, ?, ?, ?, ?, ?, '{}', '[]', ?, ?, ?)
        """,
        (
            run_id,
            run_date,
            timezone,
            plan["plan_version"],
            plan["plan_fingerprint"],
            plan["configuration_fingerprint"],
            canonical_json(plan["scope"]),
            projection_revision_start,
            lease_token,
            lease_token,
            timestamp,
            timestamp,
            timestamp,
        ),
    )
    conn.execute(
        """
        INSERT INTO daily_run_plan_revisions (
            run_id, revision, plan_version, plan_fingerprint,
            configuration_fingerprint, scope_json, reason, created_at
        ) VALUES (?, 1, ?, ?, ?, ?, 'initial_plan', ?)
        """,
        (
            run_id,
            plan["plan_version"],
            plan["plan_fingerprint"],
            plan["configuration_fingerprint"],
            canonical_json(plan["scope"]),
            timestamp,
        ),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="run",
        entity_key=run_id,
        event_type="created",
        from_state=None,
        to_state="running",
        reason="initial_plan",
        details={
            "plan_revision": 1,
            "plan_fingerprint": plan["plan_fingerprint"],
            "lease_token_hash": hashlib.sha256(lease_token.encode()).hexdigest(),
        },
    )
    initial_uncertain_items: list[tuple[str, str]] = []
    for step in plan["steps"]:
        state = _initial_step_state(step)
        scope_json = canonical_json(step["scope"])
        conn.execute(
            """
            INSERT INTO daily_run_steps (
                run_id, step_key, step_kind, order_no, required, state,
                plan_revision_added, input_fingerprint, updated_at, completed_at,
                scope_json
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            """,
            (
                run_id,
                step["key"],
                step["kind"],
                step["order"],
                int(step["required"]),
                state,
                _step_input_fingerprint(step),
                timestamp,
                timestamp if state in FINISHED_WORK_STATES else None,
                scope_json,
            ),
        )
        append_transition(
            conn,
            run_id=run_id,
            entity_type="step",
            entity_key=step["key"],
            event_type="planned",
            from_state=None,
            to_state=state,
            reason="captured_initial_plan",
            details={"plan_revision": 1, "required": bool(step["required"])},
        )
        for dependency in step["depends_on"]:
            conn.execute(
                """
                INSERT INTO daily_run_step_dependencies (
                    run_id, step_key, depends_on_step_key,
                    plan_revision_added, created_at
                ) VALUES (?, ?, ?, 1, ?)
                """,
                (run_id, step["key"], dependency, timestamp),
            )
        for item in step["items"]:
            item_scope_json = canonical_json(item["scope"])
            stored_item_state = (
                "pending" if item["state"] == "needs_verification" else item["state"]
            )
            conn.execute(
                """
                INSERT INTO daily_run_work_items (
                    run_id, step_key, item_key, item_kind, order_no, required,
                    state, plan_revision_added, input_fingerprint, updated_at,
                    scope_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (
                    run_id,
                    step["key"],
                    item["key"],
                    item["kind"],
                    item["order"],
                    int(item["required"]),
                    stored_item_state,
                    _item_input_fingerprint(item),
                    timestamp,
                    item_scope_json,
                ),
            )
            append_transition(
                conn,
                run_id=run_id,
                entity_type="work_item",
                entity_key=f"{step['key']}/{item['key']}",
                event_type="planned",
                from_state=None,
                to_state=stored_item_state,
                reason="captured_initial_plan",
                details={"plan_revision": 1, "required": bool(item["required"])},
            )
            if item["state"] == "needs_verification":
                initial_uncertain_items.append((step["key"], item["key"]))
        if state in FINISHED_WORK_STATES:
            reason = "captured_disabled_scope" if state == "not_applicable" else "captured_empty_scope"
            manifest = {
                "manifest_version": 1,
                "kind": "scope_empty",
                "run_id": run_id,
                "step_key": step["key"],
                "item_key": "",
                "observed_at": timestamp,
                "captured_scope": step["scope"],
                "remote_boundary_verified": False,
                "completion_boundary": reason,
            }
            digest, _ = _insert_manifest(
                conn,
                manifest=manifest,
                record_type="scope_empty",
                validation_status="validated",
            )
            conn.execute(
                """
                UPDATE daily_run_steps
                SET manifest_hash = ?, evidence_hash = ?, output_fingerprint = ?
                WHERE run_id = ? AND step_key = ?
                """,
                (digest, digest, digest, run_id, step["key"]),
            )
    for step_key, item_key in initial_uncertain_items:
        mark_uncertain(
            conn,
            run_id=run_id,
            step_key=step_key,
            item_key=item_key,
            reason="Попытка внешнего действия не имеет видимого подтверждения.",
        )
    refresh_run_snapshot(conn, run_id)
    return dict(conn.execute("SELECT * FROM daily_runs WHERE run_id = ?", (run_id,)).fetchone())


def get_run(
    conn: sqlite3.Connection, run_id: str | None = None, *, open_only: bool = False
) -> sqlite3.Row | None:
    if run_id:
        row = conn.execute("SELECT * FROM daily_runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is not None and open_only and row["status"] == "completed":
            return None
        return row
    condition = "WHERE status <> 'completed'" if open_only else ""
    return conn.execute(
        f"SELECT * FROM daily_runs {condition} ORDER BY created_at DESC, run_id DESC LIMIT 1"
    ).fetchone()


def bind_lease(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    lease_token: str,
    event_type: str,
    owner: str,
    expires_at: str,
) -> None:
    row = get_run(conn, run_id)
    if row is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    if row["status"] == "completed":
        raise RuntimeError(f"Ежедневный запуск {run_id} уже завершён.")
    previous = str(row["status"])
    timestamp = now_iso()
    new_status = (
        "finalizing"
        if row["projection_revision_finalizing"] is not None
        else "running"
    )
    conn.execute(
        """
        UPDATE daily_runs
        SET status = ?, current_lease_token = ?, last_lease_token = ?,
            resumed_at = ?, updated_at = ?
        WHERE run_id = ?
        """,
        (new_status, lease_token, lease_token, timestamp, timestamp, run_id),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="lease",
        entity_key=hashlib.sha256(lease_token.encode()).hexdigest(),
        event_type=event_type,
        from_state=previous,
        to_state=new_status,
        reason="lease_acquired",
        details={"owner": owner, "expires_at": expires_at},
    )


def release_lease(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    lease_token: str,
    reason: str,
) -> None:
    row = get_run(conn, run_id)
    if row is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    if row["status"] == "completed":
        return
    previous = str(row["status"])
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE daily_runs
        SET status = 'paused', current_lease_token = NULL, paused_at = ?,
            updated_at = ? WHERE run_id = ?
        """,
        (timestamp, timestamp, run_id),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="run",
        entity_key=run_id,
        event_type="paused",
        from_state=previous,
        to_state="paused",
        reason=reason,
        details={"lease_token_hash": hashlib.sha256(lease_token.encode()).hexdigest()},
    )


def note_expired_lease(conn: sqlite3.Connection, run_id: str, lease_token: str) -> None:
    row = get_run(conn, run_id)
    if row is None or row["status"] == "completed":
        return
    if row["current_lease_token"] == lease_token:
        conn.execute(
            "UPDATE daily_runs SET current_lease_token = NULL, updated_at = ? WHERE run_id = ?",
            (now_iso(), run_id),
        )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="lease",
        entity_key=hashlib.sha256(lease_token.encode()).hexdigest(),
        event_type="expired",
        from_state="active",
        to_state="expired",
        reason="lease_expired",
    )


def _work_row(
    conn: sqlite3.Connection, run_id: str, step_key: str, item_key: str
) -> tuple[str, sqlite3.Row]:
    if item_key:
        row = conn.execute(
            """
            SELECT * FROM daily_run_work_items
            WHERE run_id = ? AND step_key = ? AND item_key = ?
            """,
            (run_id, step_key, item_key),
        ).fetchone()
        entity_type = "work_item"
    else:
        row = conn.execute(
            "SELECT * FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
            (run_id, step_key),
        ).fetchone()
        entity_type = "step"
    if row is None:
        suffix = f"/{item_key}" if item_key else ""
        raise ValueError(f"Работа {step_key}{suffix} не найдена в плане {run_id}.")
    return entity_type, row


def _dependencies_complete(conn: sqlite3.Connection, run_id: str, step_key: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM daily_run_step_dependencies dependency
        JOIN daily_run_steps predecessor
          ON predecessor.run_id = dependency.run_id
         AND predecessor.step_key = dependency.depends_on_step_key
        WHERE dependency.run_id = ? AND dependency.step_key = ?
          AND predecessor.state NOT IN ('completed', 'not_applicable')
        """,
        (run_id, step_key),
    ).fetchone()
    return int(row[0]) == 0


def start_work(
    conn: sqlite3.Connection, *, run_id: str, step_key: str, item_key: str = ""
) -> bool:
    entity_type, row = _work_row(conn, run_id, step_key, item_key)
    state = str(row["state"])
    if state == "in_progress":
        return False
    if state in FINISHED_WORK_STATES:
        return False
    if state in {"blocked", "needs_verification", "invalidated"}:
        raise RuntimeError(
            "Работу нужно явно возобновить через invalidate-daily-run-work с причиной."
        )
    if not _dependencies_complete(conn, run_id, step_key):
        raise RuntimeError(f"Зависимости шага {step_key} ещё не завершены.")
    timestamp = now_iso()
    table = "daily_run_work_items" if item_key else "daily_run_steps"
    where = "run_id = ? AND step_key = ?" + (" AND item_key = ?" if item_key else "")
    params: list[Any] = [timestamp, timestamp, run_id, step_key]
    if item_key:
        params.append(item_key)
    conn.execute(
        f"""
        UPDATE {table}
        SET state = 'in_progress', attempt_count = attempt_count + 1,
            first_started_at = COALESCE(first_started_at, ?), updated_at = ?,
            blocker_code = NULL, blocker_reason = NULL, retryable = NULL
        WHERE {where}
        """,
        params,
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type=entity_type,
        entity_key=f"{step_key}/{item_key}" if item_key else step_key,
        event_type="started",
        from_state=state,
        to_state="in_progress",
        reason="explicit_start",
        details={"attempt_count": int(row["attempt_count"]) + 1},
    )
    refresh_run_snapshot(conn, run_id)
    return True


def _validate_artifact(
    manifest: Mapping[str, Any], workspace_root: Path
) -> tuple[str, str]:
    artifact = manifest.get("artifact")
    if artifact in (None, {}):
        return "", ""
    if not isinstance(artifact, dict):
        raise ValueError("Поле artifact должно быть объектом.")
    raw_path = artifact.get("path")
    expected_sha = artifact.get("sha256")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError("Для artifact требуется непустое поле path.")
    if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        raise ValueError("Для artifact.sha256 требуется SHA-256 из 64 строчных hex-символов.")
    path = Path(raw_path)
    if not path.is_absolute():
        path = workspace_root / path
    resolved = path.resolve()
    try:
        resolved.relative_to(workspace_root.resolve())
    except ValueError as exc:
        raise ValueError("Файл artifact должен находиться внутри рабочей области.") from exc
    if not resolved.is_file():
        raise ValueError(f"Файл artifact не найден: {resolved}.")
    actual_sha = hashlib.sha256(resolved.read_bytes()).hexdigest()
    if actual_sha != expected_sha:
        raise ValueError("SHA-256 файла artifact не совпадает с манифестом.")
    return str(resolved.relative_to(workspace_root.resolve())), actual_sha


def validate_manifest(
    conn: sqlite3.Connection,
    manifest: Mapping[str, Any],
    *,
    expected_run_id: str,
    expected_step_key: str,
    expected_item_key: str,
    expected_kind: str,
    completion: bool,
    workspace_root: Path,
    require_remote_boundary: bool = False,
) -> dict[str, Any]:
    if not isinstance(manifest, Mapping):
        raise ValueError("Манифест должен быть объектом JSON.")
    if len(canonical_json(manifest).encode("utf-8")) > 1_000_000:
        raise ValueError(
            "Манифест превышает 1 МБ; подробное доказательство сохраните в artifact с SHA-256."
        )
    normalized = dict(manifest)
    manifest_version = normalized.get("manifest_version")
    if manifest_version not in {1, 2}:
        raise ValueError("Поддерживаются manifest_version = 1 и HH coverage v2.")
    expected = {
        "run_id": expected_run_id,
        "step_key": expected_step_key,
        "item_key": expected_item_key,
        "kind": expected_kind,
    }
    for field, value in expected.items():
        actual = normalized.get(field, "")
        if actual != value:
            raise ValueError(f"Поле {field} должно быть равно {value!r}.")
    observed_at = normalized.get("observed_at")
    if not isinstance(observed_at, str):
        raise ValueError("Для манифеста требуется observed_at в формате ISO 8601.")
    try:
        dt.datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Поле observed_at должно иметь формат ISO 8601.") from exc
    captured_scope = normalized.get("captured_scope")
    if not isinstance(captured_scope, (dict, list)):
        raise ValueError("Поле captured_scope должно быть объектом или массивом.")
    _, expected_row = _work_row(
        conn, expected_run_id, expected_step_key, expected_item_key
    )
    expected_scope = json.loads(str(expected_row["scope_json"]))
    typed_identity_fields: tuple[str, ...] = ()
    if expected_kind in {"hh_stream", "telegram_channel"}:
        typed_identity_fields = ("stream_key",)
    elif expected_kind == "mail_source":
        typed_identity_fields = ("feature",)
    elif expected_kind == "due_followup":
        typed_identity_fields = ("vacancy_id", "application_id", "follow_up_date")
    elif expected_kind == "external_action":
        typed_identity_fields = ("action_key", "vacancy_id")
    elif "gate_key" in expected_scope:
        typed_identity_fields = ("gate_key",)
    elif expected_kind == "source_gate" and "stream_key" in expected_scope:
        typed_identity_fields = ("stream_key",)
    if typed_identity_fields:
        if not isinstance(captured_scope, dict):
            raise ValueError("Типизированный манифест требует объект captured_scope.")
        for field in typed_identity_fields:
            if captured_scope.get(field) != expected_scope.get(field):
                raise ValueError(
                    f"captured_scope.{field} должен совпадать с зафиксированным планом."
                )
    counts = normalized.get("counts", {})
    if counts is not None:
        if not isinstance(counts, dict):
            raise ValueError("Поле counts должно быть объектом.")
        for key, value in counts.items():
            if key not in COUNT_FIELDS:
                raise ValueError(f"Неподдерживаемый счётчик: {key}.")
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"Счётчик counts.{key} должен быть неотрицательным целым.")
        if manifest_version == 1 and all(key in counts for key in ("known", "new", "unique")):
            if counts["known"] + counts["new"] != counts["unique"]:
                raise ValueError("Сумма counts.known и counts.new должна равняться counts.unique.")
        if manifest_version == 2 and all(
            key in counts for key in ("known_unchanged", "known_changed", "new", "unique")
        ):
            if counts["known_unchanged"] + counts["known_changed"] + counts["new"] != counts["unique"]:
                raise ValueError(
                    "Сумма counts.known_unchanged, counts.known_changed и counts.new "
                    "должна равняться counts.unique."
                )
        if "raw" in counts and "processed" in counts and counts["processed"] > counts["raw"]:
            raise ValueError("counts.processed не может превышать counts.raw.")
        if "processed" in counts and "unique" in counts and counts["unique"] > counts["processed"]:
            raise ValueError("counts.unique не может превышать counts.processed.")
        if "processed" in counts and "reconciled" in counts and counts["reconciled"] > counts["processed"]:
            raise ValueError("counts.reconciled не может превышать counts.processed.")
    blockers = normalized.get("blockers", [])
    if not isinstance(blockers, list) or not all(isinstance(item, dict) for item in blockers):
        raise ValueError("Поле blockers должно быть массивом объектов.")
    artifact_path, artifact_sha = _validate_artifact(normalized, workspace_root)
    metrics = normalized.get("metrics", {})
    if metrics is not None:
        if not isinstance(metrics, dict):
            raise ValueError("Поле metrics должно быть объектом.")
        elapsed = metrics.get("elapsed_seconds")
        if elapsed is not None and (
            not isinstance(elapsed, (int, float))
            or isinstance(elapsed, bool)
            or elapsed < 0
        ):
            raise ValueError("metrics.elapsed_seconds должно быть неотрицательным числом.")
        for key in ("token_count", "tool_count"):
            value = metrics.get(key)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ValueError(f"metrics.{key} должно быть неотрицательным целым.")
    if completion and (
        expected_kind in REMOTE_MANIFEST_KINDS or require_remote_boundary
    ):
        if normalized.get("remote_boundary_verified") is not True:
            raise ValueError(
                "Для завершения удалённого источника требуется remote_boundary_verified = true."
            )
        if not normalized.get("completion_boundary"):
            raise ValueError("Для завершения требуется непустая completion_boundary.")
        if blockers:
            raise ValueError("Завершённый манифест удалённого источника не может содержать blockers.")
    if manifest_version == 2:
        from hh_acquisition import validate_manifest_v2

        normalized = validate_manifest_v2(
            normalized,
            expected_run_id=expected_run_id,
            expected_step_key=expected_step_key,
            expected_item_key=expected_item_key,
            expected_kind=expected_kind,
            completion=completion,
        )
        normalized["artifact_path"] = artifact_path
        normalized["artifact_sha256"] = artifact_sha
    if completion and expected_kind == "due_followup":
        valid, resolution = due_followup_resolution(
            conn, expected_run_id, expected_step_key, expected_item_key
        )
        if not valid:
            raise ValueError(
                "Повторное обращение не имеет подтверждённой доставки, свежего входящего ответа или терминального решения."
            )
        normalized["programmatic_resolution"] = resolution
    if completion and expected_kind == "external_action":
        valid, resolution = external_action_resolution(
            conn, expected_run_id, expected_step_key, expected_item_key
        )
        if not valid:
            raise ValueError(
                "Внешнее действие остаётся нетерминальным или требует проверки; повторная отправка запрещена."
            )
        normalized["programmatic_resolution"] = resolution
    normalized["artifact_path"] = artifact_path
    normalized["artifact_sha256"] = artifact_sha
    return normalized


def _insert_manifest(
    conn: sqlite3.Connection,
    *,
    manifest: Mapping[str, Any],
    record_type: str,
    validation_status: str,
) -> tuple[str, bool]:
    normalized = dict(manifest)
    digest = payload_hash(normalized)
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO daily_run_manifests (
            run_id, step_key, item_key, record_type, manifest_version,
            manifest_kind, observed_at, payload_json, payload_hash,
            validation_status, artifact_path, artifact_sha256, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalized["run_id"],
            normalized["step_key"],
            normalized.get("item_key", ""),
            record_type,
            normalized.get("manifest_version", 1),
            normalized.get("kind", "generic"),
            normalized.get("observed_at", now_iso()),
            canonical_json(normalized),
            digest,
            validation_status,
            normalized.get("artifact_path") or "",
            normalized.get("artifact_sha256") or "",
            now_iso(),
        ),
    )
    return digest, conn.total_changes > before


def record_checkpoint(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    step_key: str,
    item_key: str,
    manifest: Mapping[str, Any],
) -> bool:
    entity_type, row = _work_row(conn, run_id, step_key, item_key)
    if row["state"] in FINISHED_WORK_STATES:
        raise RuntimeError(
            "Завершённую работу нельзя менять без явной инвалидации с причиной."
        )
    normalized = validate_manifest(
        conn,
        manifest,
        expected_run_id=run_id,
        expected_step_key=step_key,
        expected_item_key=item_key,
        expected_kind=str(row["item_kind"] if item_key else row["step_kind"]),
        completion=False,
        workspace_root=settings.workspace_root,
        require_remote_boundary=bool(
            json.loads(str(row["scope_json"])).get("require_remote_boundary", False)
        ),
    )
    digest, created = _insert_manifest(
        conn,
        manifest=normalized,
        record_type="checkpoint",
        validation_status="partial",
    )
    if not created:
        return False
    table = "daily_run_work_items" if item_key else "daily_run_steps"
    where = "run_id = ? AND step_key = ?" + (" AND item_key = ?" if item_key else "")
    metrics = normalized.get("metrics") or {}
    params: list[Any] = [
        normalized["observed_at"],
        canonical_json(normalized),
        float(metrics.get("elapsed_seconds") or 0),
        metrics.get("token_count"),
        metrics.get("tool_count"),
        now_iso(),
        run_id,
        step_key,
    ]
    if item_key:
        params.append(item_key)
    conn.execute(
        f"""
        UPDATE {table} SET state = 'checkpointed', attempt_count = MAX(attempt_count, 1),
            first_started_at = COALESCE(first_started_at, ?), last_checkpoint_json = ?,
            elapsed_seconds = MAX(elapsed_seconds, ?),
            token_count = COALESCE(?, token_count),
            tool_count = COALESCE(?, tool_count), updated_at = ? WHERE {where}
        """,
        params,
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type=entity_type,
        entity_key=f"{step_key}/{item_key}" if item_key else step_key,
        event_type="checkpointed",
        from_state=str(row["state"]),
        to_state="checkpointed",
        reason="verified_partial_progress",
        details={"manifest_hash": digest},
    )
    _aggregate_step(conn, run_id, step_key)
    refresh_run_snapshot(conn, run_id)
    return True


def complete_work(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    step_key: str,
    item_key: str,
    manifest: Mapping[str, Any],
    record_type: str = "completion",
) -> bool:
    entity_type, row = _work_row(conn, run_id, step_key, item_key)
    normalized = validate_manifest(
        conn,
        manifest,
        expected_run_id=run_id,
        expected_step_key=step_key,
        expected_item_key=item_key,
        expected_kind=str(row["item_kind"] if item_key else row["step_kind"]),
        completion=True,
        workspace_root=settings.workspace_root,
        require_remote_boundary=bool(
            json.loads(str(row["scope_json"])).get("require_remote_boundary", False)
        ),
    )
    digest = payload_hash(normalized)
    if row["state"] == "completed":
        if row["manifest_hash"] == digest:
            _insert_manifest(
                conn,
                manifest=normalized,
                record_type=record_type,
                validation_status="validated",
            )
            return False
        raise RuntimeError(
            "Доказательство завершённой работы отличается. Сначала выполните явную инвалидацию с причиной."
        )
    if row["manifest_hash"] == digest and row["evidence_hash"] == digest:
        raise RuntimeError(
            "После инвалидации требуется заново проверенное доказательство с новой observed_at; "
            "прежний завершающий манифест повторно использовать нельзя."
        )
    if row["state"] == "not_applicable":
        raise RuntimeError("Работа была доказанно вне зафиксированного объёма.")
    _, created = _insert_manifest(
        conn,
        manifest=normalized,
        record_type=record_type,
        validation_status="validated",
    )
    timestamp = now_iso()
    metrics = normalized.get("metrics") or {}
    table = "daily_run_work_items" if item_key else "daily_run_steps"
    where = "run_id = ? AND step_key = ?" + (" AND item_key = ?" if item_key else "")
    params: list[Any] = [digest, digest, timestamp, timestamp, run_id, step_key]
    if item_key:
        params.append(item_key)
    conn.execute(
        f"""
        UPDATE {table}
        SET state = 'completed', attempt_count = MAX(attempt_count, 1),
            first_started_at = COALESCE(first_started_at, ?),
            output_fingerprint = ?, manifest_hash = ?,
            evidence_hash = ?, completed_at = ?,
            elapsed_seconds = MAX(elapsed_seconds, ?),
            token_count = COALESCE(?, token_count),
            tool_count = COALESCE(?, tool_count), updated_at = ?,
            blocker_code = NULL, blocker_reason = NULL, retryable = NULL
        WHERE {where}
        """,
        [
            normalized["observed_at"],
            digest,
            digest,
            digest,
            timestamp,
            float(metrics.get("elapsed_seconds") or 0),
            metrics.get("token_count"),
            metrics.get("tool_count"),
            timestamp,
            run_id,
            step_key,
            *([item_key] if item_key else []),
        ],
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type=entity_type,
        entity_key=f"{step_key}/{item_key}" if item_key else step_key,
        event_type="completed",
        from_state=str(row["state"]),
        to_state="completed",
        reason="validated_manifest",
        details={"manifest_hash": digest, "new_manifest": created},
    )
    if item_key:
        _aggregate_step(conn, run_id, step_key)
    refresh_run_snapshot(conn, run_id)
    return True


def block_work(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    step_key: str,
    item_key: str,
    code: str,
    reason: str,
    retryable: bool,
) -> bool:
    code = code.strip()
    reason = reason.strip()
    if not code or not reason:
        raise ValueError("Для блокировки требуются точные поля code и reason.")
    if len(code) > 128 or len(reason) > 1000:
        raise ValueError("Код блокировки должен быть не длиннее 128, а reason — 1000 символов.")
    entity_type, row = _work_row(conn, run_id, step_key, item_key)
    if row["state"] in FINISHED_WORK_STATES:
        raise RuntimeError("Завершённую работу сначала нужно явно инвалидировать.")
    if (
        row["state"] == "blocked"
        and str(row["blocker_code"] or "") == code
        and str(row["blocker_reason"] or "") == reason
        and bool(row["retryable"]) == retryable
    ):
        return False
    manifest = {
        "manifest_version": 1,
        "kind": str(row["item_kind"] if item_key else row["step_kind"]),
        "run_id": run_id,
        "step_key": step_key,
        "item_key": item_key,
        "observed_at": now_iso(),
        "captured_scope": json.loads(str(row["scope_json"])),
        "blockers": [{"code": code, "reason": reason, "retryable": retryable}],
    }
    digest, created = _insert_manifest(
        conn, manifest=manifest, record_type="block", validation_status="blocked"
    )
    if row["state"] == "blocked" and row["manifest_hash"] == digest:
        return False
    table = "daily_run_work_items" if item_key else "daily_run_steps"
    where = "run_id = ? AND step_key = ?" + (" AND item_key = ?" if item_key else "")
    conn.execute(
        f"""
        UPDATE {table} SET state = 'blocked', blocker_code = ?, blocker_reason = ?,
            retryable = ?, manifest_hash = ?, updated_at = ? WHERE {where}
        """,
        [code, reason, int(retryable), digest, now_iso(), run_id, step_key, *([item_key] if item_key else [])],
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type=entity_type,
        entity_key=f"{step_key}/{item_key}" if item_key else step_key,
        event_type="blocked",
        from_state=str(row["state"]),
        to_state="blocked",
        reason=reason,
        details={"code": code, "retryable": retryable, "manifest_hash": digest, "new_manifest": created},
    )
    if item_key:
        _aggregate_step(conn, run_id, step_key)
    refresh_run_snapshot(conn, run_id)
    return True


def mark_uncertain(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    step_key: str,
    item_key: str,
    reason: str,
) -> bool:
    reason = reason.strip()
    if not reason:
        raise ValueError("Для needs_verification требуется точная причина.")
    if len(reason) > 1000:
        raise ValueError("Причина needs_verification не должна превышать 1000 символов.")
    entity_type, row = _work_row(conn, run_id, step_key, item_key)
    if row["state"] in FINISHED_WORK_STATES:
        raise RuntimeError("Завершённую работу сначала нужно явно инвалидировать.")
    if (
        row["state"] == "needs_verification"
        and str(row["blocker_reason"] or "") == reason
    ):
        return False
    manifest = {
        "manifest_version": 1,
        "kind": str(row["item_kind"] if item_key else row["step_kind"]),
        "run_id": run_id,
        "step_key": step_key,
        "item_key": item_key,
        "observed_at": now_iso(),
        "captured_scope": json.loads(str(row["scope_json"])),
        "blockers": [{"code": "uncertain_external_state", "reason": reason, "retryable": True}],
        "next_safe_action": "reconcile_without_resend",
    }
    digest, created = _insert_manifest(
        conn, manifest=manifest, record_type="uncertain", validation_status="partial"
    )
    if row["state"] == "needs_verification" and row["manifest_hash"] == digest:
        return False
    table = "daily_run_work_items" if item_key else "daily_run_steps"
    where = "run_id = ? AND step_key = ?" + (" AND item_key = ?" if item_key else "")
    conn.execute(
        f"""
        UPDATE {table} SET state = 'needs_verification', blocker_code = ?,
            blocker_reason = ?, retryable = 1, manifest_hash = ?, updated_at = ?
        WHERE {where}
        """,
        ["uncertain_external_state", reason, digest, now_iso(), run_id, step_key, *([item_key] if item_key else [])],
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type=entity_type,
        entity_key=f"{step_key}/{item_key}" if item_key else step_key,
        event_type="needs_verification",
        from_state=str(row["state"]),
        to_state="needs_verification",
        reason=reason,
        details={"manifest_hash": digest, "new_manifest": created, "default_action": "reconcile_without_resend"},
    )
    if item_key:
        _aggregate_step(conn, run_id, step_key)
    refresh_run_snapshot(conn, run_id)
    return True


def _descendants(conn: sqlite3.Connection, run_id: str, roots: Iterable[str]) -> list[str]:
    graph: dict[str, list[str]] = defaultdict(list)
    for row in conn.execute(
        "SELECT step_key, depends_on_step_key FROM daily_run_step_dependencies WHERE run_id = ?",
        (run_id,),
    ):
        graph[str(row["depends_on_step_key"])].append(str(row["step_key"]))
    queue = deque(sorted(set(roots)))
    seen: set[str] = set()
    while queue:
        key = queue.popleft()
        for child in sorted(graph.get(key, [])):
            if child not in seen:
                seen.add(child)
                queue.append(child)
    return sorted(seen)


def invalidate_work(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    step_key: str,
    item_key: str,
    reason: str,
    reopen: bool = True,
) -> list[str]:
    reason = reason.strip()
    if not reason:
        raise ValueError("Для invalidation требуется точная причина.")
    if len(reason) > 4000:
        raise ValueError("Причина invalidation не должна превышать 4000 символов.")
    entity_type, row = _work_row(conn, run_id, step_key, item_key)
    target_state = "pending" if reopen else "invalidated"
    manifest = {
        "manifest_version": 1,
        "kind": str(row["item_kind"] if item_key else row["step_kind"]),
        "run_id": run_id,
        "step_key": step_key,
        "item_key": item_key,
        "observed_at": now_iso(),
        "captured_scope": json.loads(str(row["scope_json"])),
        "reason": reason,
        "previous_manifest_hash": row["manifest_hash"] or "",
    }
    digest, _ = _insert_manifest(
        conn, manifest=manifest, record_type="invalidation", validation_status="invalidated"
    )
    table = "daily_run_work_items" if item_key else "daily_run_steps"
    where = "run_id = ? AND step_key = ?" + (" AND item_key = ?" if item_key else "")
    conn.execute(
        f"""
        UPDATE {table} SET state = ?, completed_at = NULL, blocker_code = NULL,
            blocker_reason = NULL, retryable = NULL, updated_at = ? WHERE {where}
        """,
        [target_state, now_iso(), run_id, step_key, *([item_key] if item_key else [])],
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type=entity_type,
        entity_key=f"{step_key}/{item_key}" if item_key else step_key,
        event_type="reopened" if reopen else "invalidated",
        from_state=str(row["state"]),
        to_state=target_state,
        reason=reason,
        details={"invalidation_hash": digest},
    )
    invalidated: list[str] = []
    if not item_key:
        for child in conn.execute(
            """
            SELECT item_key, state FROM daily_run_work_items
            WHERE run_id = ? AND step_key = ? AND required = 1
              AND state = 'completed'
            ORDER BY order_no, item_key
            """,
            (run_id, step_key),
        ).fetchall():
            conn.execute(
                """
                UPDATE daily_run_work_items SET state = ?, completed_at = NULL,
                    blocker_code = NULL, blocker_reason = NULL, retryable = NULL,
                    updated_at = ?
                WHERE run_id = ? AND step_key = ? AND item_key = ?
                """,
                (target_state, now_iso(), run_id, step_key, child["item_key"]),
            )
            append_transition(
                conn,
                run_id=run_id,
                entity_type="work_item",
                entity_key=f"{step_key}/{child['item_key']}",
                event_type="reopened" if reopen else "invalidated",
                from_state="completed",
                to_state=target_state,
                reason=f"parent:{reason}",
                details={"upstream_step": step_key, "invalidation_hash": digest},
            )
            invalidated.append(f"{step_key}/{child['item_key']}")
    if item_key:
        _aggregate_step(conn, run_id, step_key)
    descendants = _descendants(conn, run_id, [step_key])
    for descendant in descendants:
        descendant_row = conn.execute(
            "SELECT state FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
            (run_id, descendant),
        ).fetchone()
        if descendant_row is None or descendant_row["state"] not in FINISHED_WORK_STATES:
            continue
        if descendant_row["state"] == "not_applicable":
            continue
        conn.execute(
            """
            UPDATE daily_run_steps SET state = 'invalidated', completed_at = NULL,
                updated_at = ? WHERE run_id = ? AND step_key = ?
            """,
            (now_iso(), run_id, descendant),
        )
        append_transition(
            conn,
            run_id=run_id,
            entity_type="step",
            entity_key=descendant,
            event_type="invalidated",
            from_state="completed",
            to_state="invalidated",
            reason=f"downstream:{reason}",
            details={"upstream_step": step_key},
        )
        invalidated.append(descendant)
    refresh_run_snapshot(conn, run_id)
    return invalidated


def _aggregate_step(conn: sqlite3.Connection, run_id: str, step_key: str) -> None:
    step = conn.execute(
        "SELECT * FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
        (run_id, step_key),
    ).fetchone()
    if step is None:
        return
    rows = conn.execute(
        """
        SELECT state, manifest_hash FROM daily_run_work_items
        WHERE run_id = ? AND step_key = ? AND required = 1
        ORDER BY order_no, item_key
        """,
        (run_id, step_key),
    ).fetchall()
    if not rows:
        if step_key in {"due_followups", "external_action_reconciliation"} and step["state"] != "completed":
            timestamp = now_iso()
            manifest = {
                "manifest_version": 1,
                "kind": "scope_empty",
                "run_id": run_id,
                "step_key": step_key,
                "item_key": "",
                "observed_at": timestamp,
                "captured_scope": json.loads(str(step["scope_json"])),
                "remote_boundary_verified": False,
                "completion_boundary": "authoritative_queue_empty",
            }
            digest, _ = _insert_manifest(
                conn,
                manifest=manifest,
                record_type="scope_empty",
                validation_status="validated",
            )
            conn.execute(
                """
                UPDATE daily_run_steps SET state = 'completed', manifest_hash = ?,
                    completed_at = ?, updated_at = ?
                WHERE run_id = ? AND step_key = ?
                """,
                (digest, timestamp, timestamp, run_id, step_key),
            )
            append_transition(
                conn,
                run_id=run_id,
                entity_type="step",
                entity_key=step_key,
                event_type="aggregate_changed",
                from_state=str(step["state"]),
                to_state="completed",
                reason="authoritative_queue_empty",
            )
        return
    states = [str(row["state"]) for row in rows]
    if all(state in FINISHED_WORK_STATES for state in states):
        state = "completed"
    elif "needs_verification" in states:
        state = "needs_verification"
    elif "blocked" in states:
        state = "blocked"
    elif "invalidated" in states:
        state = "invalidated"
    elif "in_progress" in states:
        state = "in_progress"
    elif "checkpointed" in states:
        state = "checkpointed"
    else:
        state = "pending"
    old_state = str(step["state"])
    if state == old_state:
        return
    manifest_hash = payload_hash([row["manifest_hash"] or "" for row in rows]) if state == "completed" else None
    conn.execute(
        """
        UPDATE daily_run_steps SET state = ?, manifest_hash = COALESCE(?, manifest_hash),
            output_fingerprint = COALESCE(?, output_fingerprint), updated_at = ?,
            completed_at = CASE WHEN ? = 'completed' THEN ? ELSE NULL END
        WHERE run_id = ? AND step_key = ?
        """,
        (state, manifest_hash, manifest_hash, now_iso(), state, now_iso(), run_id, step_key),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="step",
        entity_key=step_key,
        event_type="aggregate_changed",
        from_state=old_state,
        to_state=state,
        reason="child_work_state",
        details={"states": states},
    )


def _blocking_fields(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    entity_type: str,
    entity_key: str,
    row: sqlite3.Row,
) -> tuple[str | None, str | None, int | None]:
    code = row["blocker_code"]
    reason = row["blocker_reason"]
    retryable = row["retryable"]
    if row["state"] == "invalidated" and not reason:
        transition = conn.execute(
            """
            SELECT reason FROM daily_run_transitions
            WHERE run_id = ? AND entity_type = ? AND entity_key = ?
              AND event_type = 'invalidated'
            ORDER BY id DESC LIMIT 1
            """,
            (run_id, entity_type, entity_key),
        ).fetchone()
        code = code or "invalidated"
        reason = str(transition["reason"]) if transition is not None else "Требуется повторная проверка."
        retryable = 1
    return code, reason, retryable


def _leaf_rows(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    steps = conn.execute(
        "SELECT * FROM daily_run_steps WHERE run_id = ? ORDER BY order_no, step_key",
        (run_id,),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for step in steps:
        items = conn.execute(
            """
            SELECT * FROM daily_run_work_items
            WHERE run_id = ? AND step_key = ? ORDER BY order_no, item_key
            """,
            (run_id, step["step_key"]),
        ).fetchall()
        if items:
            for item in items:
                blocker_code, blocker_reason, retryable = _blocking_fields(
                    conn,
                    run_id=run_id,
                    entity_type="work_item",
                    entity_key=f"{step['step_key']}/{item['item_key']}",
                    row=item,
                )
                result.append(
                    {
                        "step_key": step["step_key"],
                        "step_order": step["order_no"],
                        "item_key": item["item_key"],
                        "kind": item["item_kind"],
                        "required": bool(item["required"]),
                        "state": item["state"],
                        "checkpoint": item["last_checkpoint_json"],
                        "blocker_code": blocker_code,
                        "blocker_reason": blocker_reason,
                        "retryable": retryable,
                        "scope": json.loads(str(item["scope_json"])),
                    }
                )
        else:
            blocker_code, blocker_reason, retryable = _blocking_fields(
                conn,
                run_id=run_id,
                entity_type="step",
                entity_key=str(step["step_key"]),
                row=step,
            )
            result.append(
                {
                    "step_key": step["step_key"],
                    "step_order": step["order_no"],
                    "item_key": "",
                    "kind": step["step_kind"],
                    "required": bool(step["required"]),
                    "state": step["state"],
                    "checkpoint": step["last_checkpoint_json"],
                    "blocker_code": blocker_code,
                    "blocker_reason": blocker_reason,
                    "retryable": retryable,
                    "scope": json.loads(str(step["scope_json"])),
                }
            )
    return result


def _p2_next_safe_action(
    conn: sqlite3.Connection, run_id: str, item: Mapping[str, Any]
) -> dict[str, Any] | None:
    if item.get("kind") not in {"hh_stream", "source_gate"}:
        return None
    scope = item.get("scope", {})
    stream_key = str(scope.get("stream_key", ""))
    if not stream_key:
        return None
    row = conn.execute(
        """
        SELECT * FROM hh_stream_runs
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        if item.get("state") == "checkpointed":
            return None
        return {
            "action": "build_hh_acquisition_plan",
            "acquisition_mode": "unplanned",
        }
    if row["state"] == "blocked":
        return {
            "action": "resolve_hh_capture_blocker",
            "code": row["blocker_code"] or "",
            "reason": row["blocker_reason"] or "",
        }
    if row["unresolved_drift_page"] is not None:
        return {
            "action": "verify_count_drift",
            "page_index": int(row["unresolved_drift_page"]),
        }
    latest_unverified = conn.execute(
        """
        SELECT page_index, blockers_json FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND verified = 0
        ORDER BY id DESC LIMIT 1
        """,
        (run_id, stream_key),
    ).fetchone()
    if latest_unverified is not None and int(latest_unverified["page_index"]) == int(
        row["next_page"]
    ):
        blockers = json.loads(str(latest_unverified["blockers_json"] or "[]"))
        if any(item.get("code") == "unstable_dom" for item in blockers):
            return {
                "action": "repeat_unstable_capture",
                "page_index": int(row["next_page"]),
            }
    pending = conn.execute(
        """
        SELECT COUNT(*) FROM hh_detail_queue
        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND state = 'pending'
        """,
        (run_id, stream_key),
    ).fetchone()
    if pending is not None and int(pending[0]) > 0:
        return {"action": "fetch_bounded_new_changed_details", "count": int(pending[0])}
    if row["state"] == "ready_to_finalize":
        return {"action": "finalize_hh_stream"}
    if row["fallback_reason"]:
        return {
            "action": "continue_full_scan_after_fallback",
            "page_index": int(row["next_page"]),
            "reason": row["fallback_reason"],
        }
    return {
        "action": (
            "continue_from_page" if int(row["last_verified_page"]) >= 0 else "capture_stable_page"
        ),
        "page_index": int(row["next_page"]),
        "acquisition_mode": row["effective_mode"],
    }


def _next_safe_items(conn: sqlite3.Connection, run_id: str, limit: int = 10) -> list[dict[str, Any]]:
    run = get_run(conn, run_id)
    if run is not None and run["status"] == "finalizing":
        return [{"step_key": "closeout", "item_key": "", "state": "finalizing", "action": "finalize"}]
    priority = {
        "needs_verification": 0,
        "invalidated": 1,
        "checkpointed": 2,
        "pending": 3,
        "blocked": 4,
        "in_progress": 5,
    }
    candidates = []
    dependency_ready: dict[str, bool] = {}
    for item in _leaf_rows(conn, run_id):
        if not item["required"] or item["state"] in FINISHED_WORK_STATES:
            continue
        step_key = str(item["step_key"])
        if step_key not in dependency_ready:
            dependency_ready[step_key] = _dependencies_complete(
                conn, run_id, step_key
            )
        if (
            item["state"] != "needs_verification"
            and not dependency_ready[step_key]
        ):
            continue
        action = "start"
        if item["state"] == "needs_verification":
            action = "reconcile_without_resend"
        elif item["state"] == "checkpointed":
            action = "continue_from_checkpoint"
        elif item["state"] == "blocked":
            action = "retry_after_blocker" if item["retryable"] else "resolve_blocker"
        elif item["state"] == "invalidated":
            action = "reverify"
        elif item["state"] == "pending" and item["kind"] == "external_action":
            action = (
                "continue_authorized_work"
                if item["scope"].get("state") == "authorized"
                else "review_draft"
            )
        p2 = _p2_next_safe_action(conn, run_id, item)
        if p2 is not None:
            action = str(p2["action"])
        candidates.append(
            {
                "step_key": item["step_key"],
                "item_key": item["item_key"],
                "kind": item["kind"],
                "state": item["state"],
                "action": action,
                "scope": item["scope"],
                **({"p2": p2} if p2 is not None else {}),
                "_step_order": int(item["step_order"]),
            }
        )
    candidates.sort(
        key=lambda item: (
            priority.get(str(item["state"]), 99),
            int(item["_step_order"]),
            str(item["item_key"]),
        )
    )
    if not candidates:
        required = [item for item in _leaf_rows(conn, run_id) if item["required"]]
        if required and all(item["state"] in FINISHED_WORK_STATES for item in required):
            return [
                {
                    "step_key": "closeout",
                    "item_key": "",
                    "kind": "internal_closeout",
                    "state": "pending",
                    "action": "finalize",
                    "scope": {"atomic_projection_publication": True},
                }
            ]
    return [
        {key: value for key, value in item.items() if key != "_step_order"}
        for item in candidates[:limit]
    ]


def refresh_run_snapshot(conn: sqlite3.Connection, run_id: str) -> None:
    leaves = [item for item in _leaf_rows(conn, run_id) if item["required"]]
    counts: dict[str, int] = {state: 0 for state in WORK_STATES}
    for item in leaves:
        counts[str(item["state"])] += 1
    blockers = [
        {
            "step_key": item["step_key"],
            "item_key": item["item_key"],
            "state": item["state"],
            "code": item["blocker_code"] or "",
            "reason": item["blocker_reason"] or "",
            "retryable": bool(item["retryable"]) if item["retryable"] is not None else None,
        }
        for item in leaves
        if item["state"] in {"blocked", "needs_verification", "invalidated"}
    ]
    last = conn.execute(
        """
        SELECT step_key FROM daily_run_steps
        WHERE run_id = ? AND state = 'completed'
        ORDER BY completed_at DESC, order_no DESC LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    next_items = _next_safe_items(conn, run_id, 1)
    run = get_run(conn, run_id)
    metric_rows = conn.execute(
        """
        SELECT elapsed_seconds, token_count, tool_count
        FROM daily_run_steps step
        WHERE step.run_id = ? AND step.required = 1
          AND NOT EXISTS (
              SELECT 1 FROM daily_run_work_items item
              WHERE item.run_id = step.run_id AND item.step_key = step.step_key
          )
        UNION ALL
        SELECT elapsed_seconds, token_count, tool_count
        FROM daily_run_work_items
        WHERE run_id = ? AND required = 1
        """,
        (run_id, run_id),
    ).fetchall()
    metrics = {
        "work_items": len(leaves),
        "states": counts,
        "elapsed_seconds": round(
            sum(float(row["elapsed_seconds"] or 0) for row in metric_rows), 6
        ),
        "token_count": sum(int(row["token_count"] or 0) for row in metric_rows),
        "tool_count": sum(int(row["tool_count"] or 0) for row in metric_rows),
    }
    derived_status: str | None = None
    if run is not None and run["status"] not in {"paused", "finalizing", "completed"}:
        if counts["needs_verification"]:
            derived_status = "needs_verification"
        elif counts["blocked"]:
            derived_status = "blocked"
        else:
            derived_status = "running"
    if run is not None and derived_status and derived_status != str(run["status"]):
        append_transition(
            conn,
            run_id=run_id,
            entity_type="run",
            entity_key=run_id,
            event_type="status_changed",
            from_state=str(run["status"]),
            to_state=derived_status,
            reason="derived_required_work_state",
            details={"states": counts},
        )
    if run is None:
        return
    metrics_json = canonical_json(metrics)
    blockers_json = canonical_json(blockers[:50])
    last_step = str(last["step_key"]) if last else None
    next_step = str(next_items[0]["step_key"]) if next_items else None
    next_status = derived_status or str(run["status"])
    if (
        next_status == str(run["status"])
        and metrics_json == str(run["aggregate_metrics_json"])
        and blockers_json == str(run["blocker_summary_json"])
        and last_step == run["last_verified_step_key"]
        and next_step == run["next_safe_step_key"]
    ):
        return
    conn.execute(
        """
        UPDATE daily_runs SET status = COALESCE(?, status),
            aggregate_metrics_json = ?, blocker_summary_json = ?,
            last_verified_step_key = ?, next_safe_step_key = ?, updated_at = ?
        WHERE run_id = ?
        """,
        (
            derived_status,
            metrics_json,
            blockers_json,
            last_step,
            next_step,
            now_iso(),
            run_id,
        ),
    )


def _bounded_status_value(value: Any, *, depth: int = 0) -> Any:
    """Keep the default status response bounded; verbose retains full evidence."""

    if depth >= 5:
        return "<глубокое значение скрыто; используйте --verbose>"
    if isinstance(value, str):
        return value if len(value) <= 512 else value[:512] + "…"
    if isinstance(value, list):
        result = [
            _bounded_status_value(item, depth=depth + 1) for item in value[:20]
        ]
        if len(value) > 20:
            result.append({"truncated_items": len(value) - 20})
        return result
    if isinstance(value, dict):
        result = {
            str(key): _bounded_status_value(value[key], depth=depth + 1)
            for key in sorted(value)[:30]
        }
        if len(value) > 30:
            result["truncated_fields"] = len(value) - 30
        return result
    return value


def _compact_checkpoint(raw: str) -> dict[str, Any]:
    payload = json.loads(raw)
    fields = (
        "observed_at",
        "counts",
        "completion_boundary",
        "captured_scope",
        "remote_boundary_verified",
        "artifact_path",
        "artifact_sha256",
        "blockers",
    )
    return {
        field: _bounded_status_value(payload[field])
        for field in fields
        if field in payload and payload[field] not in (None, "", [], {})
    }


def run_status(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str | None,
    projection_state: Mapping[str, Any],
    verbose: bool = False,
    history_limit: int = 100,
) -> dict[str, Any] | None:
    run = get_run(conn, run_id, open_only=run_id is None)
    if run is None:
        return None
    run_id_value = str(run["run_id"])
    active_lease = conn.execute(
        """
        SELECT token, owner, acquired_at, heartbeat_at, expires_at
        FROM daily_run_leases
        WHERE run_id = ? AND status = 'active' AND expires_at > ?
        ORDER BY acquired_at DESC LIMIT 1
        """,
        (run_id_value, dt.datetime.now().replace(microsecond=0).isoformat()),
    ).fetchone()
    last_lease = conn.execute(
        """
        SELECT owner, status, acquired_at, heartbeat_at, expires_at,
               released_at, release_reason
        FROM daily_run_leases
        WHERE run_id = ? ORDER BY acquired_at DESC, token DESC LIMIT 1
        """,
        (run_id_value,),
    ).fetchone()
    leaves = [item for item in _leaf_rows(conn, run_id_value) if item["required"]]
    counts = {state: 0 for state in WORK_STATES}
    for item in leaves:
        counts[str(item["state"])] += 1
    all_blockers = [
        {
            "step_key": item["step_key"],
            "item_key": item["item_key"],
            "state": item["state"],
            "code": item["blocker_code"] or "",
            "reason": item["blocker_reason"] or "",
            "retryable": bool(item["retryable"]) if item["retryable"] is not None else None,
        }
        for item in leaves
        if item["state"] in {"blocked", "needs_verification", "invalidated"}
    ]
    blockers = all_blockers[:20]
    all_checkpoints = [
        {
            "step_key": item["step_key"],
            "item_key": item["item_key"],
            "state": item["state"],
            "checkpoint": _compact_checkpoint(str(item["checkpoint"])),
        }
        for item in leaves
        if item["checkpoint"]
    ]
    checkpoints = all_checkpoints[:20]
    current_config_fingerprint = configuration_fingerprint(settings, str(run["timezone"]))
    drift = current_config_fingerprint != str(run["configuration_fingerprint"])
    result: dict[str, Any] = {
        "run_id": run_id_value,
        "run_date": run["run_date"],
        "timezone": run["timezone"],
        "status": run["status"],
        "plan_version": run["plan_version"],
        "plan_revision": run["plan_revision"],
        "plan_fingerprint": run["plan_fingerprint"],
        "configuration_fingerprint": run["configuration_fingerprint"],
        "configuration_drift": drift,
        "aggregate_metrics": json.loads(str(run["aggregate_metrics_json"])),
        "projection_state": dict(projection_state),
        "counts": {
            "completed": counts["completed"] + counts["not_applicable"],
            "pending": counts["pending"] + counts["checkpointed"] + counts["invalidated"],
            "in_progress": counts["in_progress"],
            "blocked": counts["blocked"],
            "needs_verification": counts["needs_verification"],
            "total_required": len(leaves),
        },
        "blockers": blockers,
        "blockers_truncated": max(len(all_blockers) - len(blockers), 0),
        "last_checkpoints": checkpoints,
        "last_checkpoints_truncated": max(
            len(all_checkpoints) - len(checkpoints), 0
        ),
        "last_verified_step": run["last_verified_step_key"] or "",
        "next_safe_work": _next_safe_items(conn, run_id_value, 10),
        "lease": (
            {
                "active": True,
                "owner": active_lease["owner"],
                "acquired_at": active_lease["acquired_at"],
                "heartbeat_at": active_lease["heartbeat_at"],
                "expires_at": active_lease["expires_at"],
            }
            if active_lease
            else {
                "active": False,
                "last": dict(last_lease) if last_lease is not None else {},
            }
        ),
        "resume_command": (
            "python3 scripts/jobctl.py resume-daily-run --run-id "
            f"{shlex.quote(run_id_value)} --json"
            if run["status"] != "completed" and active_lease is None
            else ""
        ),
        "write_flags": (
            ["--defer-render", f"--run-lease={active_lease['token']}"]
            if run["status"] != "completed" and active_lease is not None
            else ["--defer-render", "--run-lease=<token-from-resume>"]
            if run["status"] != "completed"
            else []
        ),
        "plan_refresh_command": (
            "python3 scripts/jobctl.py refresh-daily-run-plan --run-id "
            f"{shlex.quote(run_id_value)} "
            "--reason '<причина>' --defer-render --run-lease=<token>"
            if drift
            else ""
        ),
    }
    result["next_safe_work"] = [
        {**item, "scope": _bounded_status_value(item.get("scope", {}))}
        for item in result["next_safe_work"]
    ]
    if drift and run["status"] != "completed":
        result["next_safe_work"] = [
            {
                "step_key": "plan",
                "item_key": "",
                "kind": "plan_refresh",
                "state": "invalidated",
                "action": "refresh_plan",
                "scope": {"configuration_drift": True},
            },
            *result["next_safe_work"][:9],
        ]
    if verbose:
        result["scope"] = json.loads(str(run["scope_json"]))
        result["steps"] = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM daily_run_steps WHERE run_id = ? ORDER BY order_no, step_key",
                (run_id_value,),
            )
        ]
        result["work_items"] = [
            dict(row)
            for row in conn.execute(
                """
                SELECT * FROM daily_run_work_items WHERE run_id = ?
                ORDER BY step_key, order_no, item_key
                """,
                (run_id_value,),
            )
        ]
        result["manifests"] = [
            dict(row)
            for row in conn.execute(
                """
                SELECT * FROM daily_run_manifests WHERE run_id = ?
                ORDER BY id DESC LIMIT ?
                """,
                (run_id_value, max(1, min(history_limit, 1000))),
            )
        ]
        result["history"] = [
            dict(row)
            for row in conn.execute(
                """
                SELECT * FROM daily_run_transitions WHERE run_id = ?
                ORDER BY id DESC LIMIT ?
                """,
                (run_id_value, max(1, min(history_limit, 1000))),
            )
        ]
    return result


def _coverage_manifest(
    run_id: str,
    step_key: str,
    item_key: str,
    kind: str,
    stream: Mapping[str, Any],
    manifest_file: str,
    manifest_sha256: str,
    observed_at: str,
    source_run_id: int,
) -> dict[str, Any]:
    return {
        "manifest_version": 1,
        "kind": kind,
        "run_id": run_id,
        "step_key": step_key,
        "item_key": item_key,
        "observed_at": observed_at,
        "captured_scope": {
            "stream_key": stream.get("key"),
            "query_url": stream.get("query_url", ""),
            "manifest_file": manifest_file,
            "manifest_sha256": manifest_sha256,
            "source_run_id": source_run_id,
        },
        "counts": {
            "raw": int(stream.get("found") or 0),
            "unique": int(stream.get("unique") or 0),
            "known": int(stream.get("known") or 0),
            "new": int(stream.get("new") or 0),
            "processed": int(
                stream.get("extracted")
                if stream.get("extracted") is not None
                else stream.get("found") or 0
            ),
            "blocked": 1 if stream.get("status") == "blocked" else 0,
        },
        "completion_boundary": {
            "pages_expected": int(stream.get("pages_expected") or 0),
            "pages_visited": int(stream.get("pages_visited") or 0),
            "checkpoint": stream.get("checkpoint") or {},
        },
        "remote_boundary_verified": stream.get("status") == "completed" and not stream.get("issues"),
        "blockers": [
            {"code": "coverage_incomplete", "reason": issue, "retryable": True}
            for issue in stream.get("issues", [])
        ],
        "artifact": {"path": manifest_file, "sha256": manifest_sha256},
    }


def integrate_coverage_result(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    source: str,
    result: Mapping[str, Any],
    manifest_file: str,
    manifest_sha256: str,
    observed_at: str,
    source_run_id: int,
) -> dict[str, int]:
    run = get_run(conn, run_id)
    if run is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    if str(result.get("run_date")) != str(run["run_date"]):
        raise ValueError("Дата манифеста покрытия не совпадает с зафиксированным run_date.")
    step_key = "hh_coverage" if source == "hh" else "telegram_coverage"
    kind = "hh_stream" if source == "hh" else "telegram_channel"
    items = conn.execute(
        """
        SELECT item_key, scope_json FROM daily_run_work_items
        WHERE run_id = ? AND step_key = ?
        """,
        (run_id, step_key),
    ).fetchall()
    by_stream = {
        str(json.loads(str(row["scope_json"])).get("stream_key", "")).casefold(): str(row["item_key"])
        for row in items
    }
    stats = {"completed": 0, "checkpointed": 0, "blocked": 0}
    for stream in result.get("streams", []):
        item_key = by_stream.get(str(stream.get("key", "")).casefold())
        if not item_key:
            continue
        manifest = _coverage_manifest(
            run_id,
            step_key,
            item_key,
            kind,
            stream,
            manifest_file,
            manifest_sha256,
            observed_at,
            source_run_id,
        )
        if stream.get("status") == "completed" and not stream.get("issues"):
            complete_work(
                conn,
                settings,
                run_id=run_id,
                step_key=step_key,
                item_key=item_key,
                manifest=manifest,
                record_type="completion",
            )
            stats["completed"] += 1
        elif stream.get("status") == "blocked":
            if (
                int(stream.get("pages_visited") or 0) > 0
                or int(stream.get("extracted") or 0) > 0
                or bool(stream.get("checkpoint"))
            ):
                record_checkpoint(
                    conn,
                    settings,
                    run_id=run_id,
                    step_key=step_key,
                    item_key=item_key,
                    manifest=manifest,
                )
                stats["checkpointed"] += 1
            reason = str(stream.get("error") or "; ".join(stream.get("issues", [])) or "Источник заблокирован.")
            block_work(
                conn,
                run_id=run_id,
                step_key=step_key,
                item_key=item_key,
                code="coverage_blocked",
                reason=reason,
                retryable=True,
            )
            stats["blocked"] += 1
        else:
            record_checkpoint(
                conn,
                settings,
                run_id=run_id,
                step_key=step_key,
                item_key=item_key,
                manifest=manifest,
            )
            stats["checkpointed"] += 1
    return stats


def due_followup_resolution(
    conn: sqlite3.Connection, run_id: str, step_key: str, item_key: str
) -> tuple[bool, dict[str, Any]]:
    _, item = _work_row(conn, run_id, step_key, item_key)
    scope = json.loads(str(item["scope_json"]))
    run = get_run(conn, run_id)
    if run is None:
        return False, {}
    vacancy_id = int(scope["vacancy_id"])
    external = conn.execute(
        """
        SELECT * FROM external_actions
        WHERE vacancy_id = ? AND action_type = 'follow_up'
          AND date(event_at) >= date(?)
        ORDER BY event_at DESC, id DESC LIMIT 1
        """,
        (vacancy_id, str(scope["follow_up_date"])),
    ).fetchone()
    if external is not None and external["state"] == "visibly_confirmed":
        return True, {"type": "visibly_confirmed_delivery", "external_action_id": external["id"]}
    inbound = conn.execute(
        """
        SELECT id, event_at FROM effective_employer_interactions
        WHERE vacancy_id = ? AND direction = 'inbound'
          AND datetime(event_at) >= datetime(?)
        ORDER BY event_at DESC, id DESC LIMIT 1
        """,
        (vacancy_id, run["created_at"]),
    ).fetchone()
    if inbound is not None:
        return True, {"type": "fresh_inbound", "interaction_id": inbound["id"]}
    terminal = conn.execute(
        """
        SELECT id, event_type FROM lifecycle_events
        WHERE vacancy_id = ? AND event_type IN ('rejected', 'offer_received')
        ORDER BY event_at DESC, id DESC LIMIT 1
        """,
        (vacancy_id,),
    ).fetchone()
    if terminal is not None:
        return True, {"type": "terminal_resolution", "lifecycle_event_id": terminal["id"], "event_type": terminal["event_type"]}
    if external is not None and external["state"] == "attempted":
        return False, {"type": "needs_verification", "external_action_id": external["id"], "default_action": "reconcile_without_resend"}
    return False, {}


def external_action_resolution(
    conn: sqlite3.Connection, run_id: str, step_key: str, item_key: str
) -> tuple[bool, dict[str, Any]]:
    _, item = _work_row(conn, run_id, step_key, item_key)
    scope = json.loads(str(item["scope_json"]))
    action = conn.execute(
        """
        SELECT * FROM external_actions WHERE action_key = ?
        ORDER BY event_at DESC, id DESC LIMIT 1
        """,
        (scope["action_key"],),
    ).fetchone()
    if action is None:
        return False, {}
    if action["state"] in TERMINAL_EXTERNAL_ACTION_STATES:
        return True, {"state": action["state"], "external_action_id": action["id"]}
    return False, {
        "state": action["state"],
        "external_action_id": action["id"],
        "default_action": "reconcile_without_resend" if action["state"] == "attempted" else "continue_authorized_work",
    }


def _programmatic_manifest(
    run_id: str,
    step_key: str,
    item_key: str,
    kind: str,
    scope: Mapping[str, Any],
    resolution: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "manifest_version": 1,
        "kind": kind,
        "run_id": run_id,
        "step_key": step_key,
        "item_key": item_key,
        "observed_at": now_iso(),
        "captured_scope": dict(scope),
        "completion_boundary": "authoritative_sqlite_resolution",
        "remote_boundary_verified": False,
        "programmatic_resolution": dict(resolution),
        "blockers": [],
    }


def _record_dynamic_plan_revision(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    added_items: Sequence[tuple[str, str]],
    changed_items: Sequence[tuple[str, str]],
) -> int:
    run = get_run(conn, run_id)
    if run is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    new_revision = int(run["plan_revision"]) + 1
    timestamp = now_iso()
    dynamic_requirements = [
        {
            "step_key": str(row["step_key"]),
            "item_key": str(row["item_key"]),
            "input_fingerprint": str(row["input_fingerprint"]),
        }
        for row in conn.execute(
            """
            SELECT step_key, item_key, input_fingerprint
            FROM daily_run_work_items
            WHERE run_id = ? AND required = 1
              AND step_key IN ('due_followups','external_action_reconciliation')
            ORDER BY step_key, order_no, item_key
            """,
            (run_id,),
        ).fetchall()
    ]
    scope = json.loads(str(run["scope_json"]))
    scope["dynamic_requirements"] = dynamic_requirements
    fingerprint = payload_hash(scope)
    conn.execute(
        """
        INSERT INTO daily_run_plan_revisions (
            run_id, revision, plan_version, plan_fingerprint,
            configuration_fingerprint, scope_json, reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'authoritative_queue_changed', ?)
        """,
        (
            run_id,
            new_revision,
            run["plan_version"],
            fingerprint,
            run["configuration_fingerprint"],
            canonical_json(scope),
            timestamp,
        ),
    )
    for step_key, item_key in added_items:
        conn.execute(
            """
            UPDATE daily_run_work_items SET plan_revision_added = ?
            WHERE run_id = ? AND step_key = ? AND item_key = ?
            """,
            (new_revision, run_id, step_key, item_key),
        )
    conn.execute(
        """
        UPDATE daily_runs SET plan_revision = ?, plan_fingerprint = ?,
            scope_json = ?, updated_at = ? WHERE run_id = ?
        """,
        (new_revision, fingerprint, canonical_json(scope), timestamp, run_id),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="plan",
        entity_key=str(new_revision),
        event_type="extended",
        from_state=str(run["plan_revision"]),
        to_state=str(new_revision),
        reason="authoritative_queue_changed",
        details={
            "added_items": [f"{step}/{item}" for step, item in sorted(added_items)],
            "changed_items": [f"{step}/{item}" for step, item in sorted(changed_items)],
        },
    )
    return new_revision


def refresh_dynamic_work(
    conn: sqlite3.Connection, settings: Settings, *, run_id: str
) -> dict[str, int]:
    run = get_run(conn, run_id)
    if run is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    added = {"due_followups": 0, "external_actions": 0, "resolved": 0, "uncertain": 0}
    added_item_keys: list[tuple[str, str]] = []
    changed_item_keys: list[tuple[str, str]] = []
    timestamp = now_iso()
    due_rows = authoritative_due_followups(conn, str(run["run_date"]))
    for index, scope in enumerate(due_rows, start=1):
        item_key = f"due:{scope['vacancy_id']}:{scope['application_id']}:{scope['follow_up_date']}"
        existing = conn.execute(
            """
            SELECT * FROM daily_run_work_items
            WHERE run_id = ? AND step_key = 'due_followups' AND item_key = ?
            """,
            (run_id, item_key),
        ).fetchone()
        item_definition = _item(item_key, "due_followup", index, scope)
        item_fingerprint = _item_input_fingerprint(item_definition)
        if existing:
            if str(existing["input_fingerprint"]) != item_fingerprint:
                state = (
                    "invalidated"
                    if existing["state"] in FINISHED_WORK_STATES
                    else str(existing["state"])
                )
                conn.execute(
                    """
                    UPDATE daily_run_work_items SET input_fingerprint = ?, scope_json = ?,
                        state = ?, completed_at = CASE WHEN ? = 'invalidated' THEN NULL ELSE completed_at END,
                        updated_at = ?
                    WHERE run_id = ? AND step_key = 'due_followups' AND item_key = ?
                    """,
                    (
                        item_fingerprint,
                        canonical_json(scope),
                        state,
                        state,
                        timestamp,
                        run_id,
                        item_key,
                    ),
                )
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="work_item",
                    entity_key=f"due_followups/{item_key}",
                    event_type="invalidated" if state == "invalidated" else "scope_refreshed",
                    from_state=str(existing["state"]),
                    to_state=state,
                    reason="authoritative_queue_scope_changed",
                    details={
                        "old_input_fingerprint": existing["input_fingerprint"],
                        "new_input_fingerprint": item_fingerprint,
                    },
                )
                changed_item_keys.append(("due_followups", item_key))
            continue
        conn.execute(
            """
            INSERT INTO daily_run_work_items (
                run_id, step_key, item_key, item_kind, order_no, required, state,
                plan_revision_added, input_fingerprint, updated_at, scope_json
            ) VALUES (?, 'due_followups', ?, 'due_followup', ?, 1, 'pending', ?, ?, ?, ?)
            """,
            (
                run_id,
                item_key,
                index,
                run["plan_revision"],
                item_fingerprint,
                timestamp,
                canonical_json(scope),
            ),
        )
        added["due_followups"] += 1
        added_item_keys.append(("due_followups", item_key))
        append_transition(
            conn,
            run_id=run_id,
            entity_type="work_item",
            entity_key=f"due_followups/{item_key}",
            event_type="added_to_plan",
            from_state=None,
            to_state="pending",
            reason="authoritative_queue_extended",
            details={"plan_revision": int(run["plan_revision"]) + 1},
        )
    actions = latest_nonterminal_external_actions(conn)
    for index, scope in enumerate(actions, start=1):
        item_key = stable_item_key("external", str(scope["action_key"]))
        existing = conn.execute(
            """
            SELECT * FROM daily_run_work_items
            WHERE run_id = ? AND step_key = 'external_action_reconciliation' AND item_key = ?
            """,
            (run_id, item_key),
        ).fetchone()
        state = "needs_verification" if scope["state"] == "attempted" else "pending"
        compact_scope = {
            "external_action_id": scope["id"],
            "action_key": scope["action_key"],
            "action_type": scope["action_type"],
            "state": scope["state"],
            "vacancy_id": scope["vacancy_id"],
            "external_reference": scope["external_reference"] or "",
        }
        item_definition = _item(
            item_key, "external_action", index, compact_scope, state=state
        )
        item_fingerprint = _item_input_fingerprint(item_definition)
        if existing:
            if str(existing["input_fingerprint"]) != item_fingerprint:
                refreshed_state = (
                    "invalidated"
                    if existing["state"] in FINISHED_WORK_STATES
                    else str(existing["state"])
                )
                conn.execute(
                    """
                    UPDATE daily_run_work_items SET input_fingerprint = ?, scope_json = ?,
                        state = ?, completed_at = CASE WHEN ? = 'invalidated' THEN NULL ELSE completed_at END,
                        updated_at = ?
                    WHERE run_id = ? AND step_key = 'external_action_reconciliation'
                      AND item_key = ?
                    """,
                    (
                        item_fingerprint,
                        canonical_json(compact_scope),
                        refreshed_state,
                        refreshed_state,
                        timestamp,
                        run_id,
                        item_key,
                    ),
                )
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="work_item",
                    entity_key=f"external_action_reconciliation/{item_key}",
                    event_type=(
                        "invalidated" if refreshed_state == "invalidated" else "scope_refreshed"
                    ),
                    from_state=str(existing["state"]),
                    to_state=refreshed_state,
                    reason="authoritative_queue_scope_changed",
                    details={
                        "old_input_fingerprint": existing["input_fingerprint"],
                        "new_input_fingerprint": item_fingerprint,
                    },
                )
                changed_item_keys.append(("external_action_reconciliation", item_key))
            continue
        conn.execute(
            """
            INSERT INTO daily_run_work_items (
                run_id, step_key, item_key, item_kind, order_no, required, state,
                plan_revision_added, input_fingerprint, updated_at, scope_json
            ) VALUES (?, 'external_action_reconciliation', ?, 'external_action', ?, 1, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                item_key,
                index,
                "pending" if state == "needs_verification" else state,
                run["plan_revision"],
                item_fingerprint,
                timestamp,
                canonical_json(compact_scope),
            ),
        )
        added["external_actions"] += 1
        added_item_keys.append(("external_action_reconciliation", item_key))
        append_transition(
            conn,
            run_id=run_id,
            entity_type="work_item",
            entity_key=f"external_action_reconciliation/{item_key}",
            event_type="added_to_plan",
            from_state=None,
            to_state="pending",
            reason="authoritative_queue_extended",
            details={"plan_revision": int(run["plan_revision"]) + 1},
        )
    if added_item_keys or changed_item_keys:
        _record_dynamic_plan_revision(
            conn,
            run_id=run_id,
            added_items=added_item_keys,
            changed_items=changed_item_keys,
        )
    for step_key in ("due_followups", "external_action_reconciliation"):
        if added[step_key if step_key == "due_followups" else "external_actions"]:
            step = conn.execute(
                "SELECT state FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
                (run_id, step_key),
            ).fetchone()
            if step and step["state"] == "completed":
                conn.execute(
                    "UPDATE daily_run_steps SET state = 'invalidated', completed_at = NULL, updated_at = ? WHERE run_id = ? AND step_key = ?",
                    (timestamp, run_id, step_key),
                )
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="step",
                    entity_key=step_key,
                    event_type="invalidated",
                    from_state="completed",
                    to_state="invalidated",
                    reason="authoritative_queue_extended",
                )
                for descendant in _descendants(conn, run_id, [step_key]):
                    descendant_state = conn.execute(
                        "SELECT state FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
                        (run_id, descendant),
                    ).fetchone()
                    if descendant_state and descendant_state["state"] == "completed":
                        conn.execute(
                            "UPDATE daily_run_steps SET state = 'invalidated', completed_at = NULL, updated_at = ? WHERE run_id = ? AND step_key = ?",
                            (timestamp, run_id, descendant),
                        )
    for row in conn.execute(
        """
        SELECT step_key, item_key, item_kind, scope_json, state
        FROM daily_run_work_items
        WHERE run_id = ? AND step_key IN ('due_followups','external_action_reconciliation')
          AND state <> 'completed'
        ORDER BY step_key, order_no, item_key
        """,
        (run_id,),
    ).fetchall():
        if row["item_kind"] == "due_followup":
            valid, resolution = due_followup_resolution(
                conn, run_id, str(row["step_key"]), str(row["item_key"])
            )
        else:
            valid, resolution = external_action_resolution(
                conn, run_id, str(row["step_key"]), str(row["item_key"])
            )
        if valid:
            manifest = _programmatic_manifest(
                run_id,
                str(row["step_key"]),
                str(row["item_key"]),
                str(row["item_kind"]),
                json.loads(str(row["scope_json"])),
                resolution,
            )
            complete_work(
                conn,
                settings,
                run_id=run_id,
                step_key=str(row["step_key"]),
                item_key=str(row["item_key"]),
                manifest=manifest,
                record_type="programmatic",
            )
            added["resolved"] += 1
        elif row["item_kind"] == "external_action" and resolution.get("state") == "attempted":
            if row["state"] != "needs_verification":
                mark_uncertain(
                    conn,
                    run_id=run_id,
                    step_key=str(row["step_key"]),
                    item_key=str(row["item_key"]),
                    reason="Действие могло быть отправлено, но видимое подтверждение не зафиксировано.",
                )
                added["uncertain"] += 1
    _aggregate_step(conn, run_id, "due_followups")
    _aggregate_step(conn, run_id, "external_action_reconciliation")
    refresh_run_snapshot(conn, run_id)
    return added


def refresh_plan(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    reason: str,
) -> dict[str, Any]:
    reason = reason.strip()
    if not reason:
        raise ValueError("Для обновления плана требуется точная причина.")
    run = get_run(conn, run_id)
    if run is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    if run["status"] == "completed":
        raise RuntimeError("Завершённый ежедневный запуск нельзя обновлять.")
    plan = build_plan_definition(
        conn,
        settings,
        run_date=str(run["run_date"]),
        timezone=str(run["timezone"]),
    )
    new_revision = int(run["plan_revision"]) + 1
    timestamp = now_iso()
    changed_roots: set[str] = set()
    added_items = 0
    added_steps = 0
    new_uncertain_items: list[tuple[str, str]] = []
    for step in plan["steps"]:
        existing = conn.execute(
            "SELECT * FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
            (run_id, step["key"]),
        ).fetchone()
        fingerprint = _step_input_fingerprint(step)
        if existing is None:
            state = _initial_step_state(step)
            conn.execute(
                """
                INSERT INTO daily_run_steps (
                    run_id, step_key, step_kind, order_no, required, state,
                    plan_revision_added, input_fingerprint, updated_at,
                    completed_at, scope_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    step["key"],
                    step["kind"],
                    step["order"],
                    int(step["required"]),
                    state,
                    new_revision,
                    fingerprint,
                    timestamp,
                    timestamp if state in FINISHED_WORK_STATES else None,
                    canonical_json(step["scope"]),
                ),
            )
            added_steps += 1
            changed_roots.add(step["key"])
            append_transition(
                conn,
                run_id=run_id,
                entity_type="step",
                entity_key=step["key"],
                event_type="added_to_plan",
                from_state=None,
                to_state=state,
                reason=f"plan_refresh:{reason}",
                details={"plan_revision": new_revision},
            )
            if state in FINISHED_WORK_STATES:
                manifest = {
                    "manifest_version": 1,
                    "kind": "scope_empty",
                    "run_id": run_id,
                    "step_key": step["key"],
                    "item_key": "",
                    "observed_at": timestamp,
                    "captured_scope": step["scope"],
                    "remote_boundary_verified": False,
                    "completion_boundary": (
                        "captured_disabled_scope"
                        if state == "not_applicable"
                        else "captured_empty_scope"
                    ),
                }
                digest, _ = _insert_manifest(
                    conn,
                    manifest=manifest,
                    record_type="scope_empty",
                    validation_status="validated",
                )
                conn.execute(
                    """
                    UPDATE daily_run_steps
                    SET manifest_hash = ?, evidence_hash = ?, output_fingerprint = ?
                    WHERE run_id = ? AND step_key = ?
                    """,
                    (digest, digest, digest, run_id, step["key"]),
                )
        elif str(existing["input_fingerprint"]) != fingerprint:
            changed_roots.add(step["key"])
            if existing["state"] == "completed" or (
                existing["state"] == "not_applicable" and step["enabled"]
            ):
                conn.execute(
                    "UPDATE daily_run_steps SET state = 'invalidated', completed_at = NULL, updated_at = ? WHERE run_id = ? AND step_key = ?",
                    (timestamp, run_id, step["key"]),
                )
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="step",
                    entity_key=step["key"],
                    event_type="invalidated",
                    from_state=str(existing["state"]),
                    to_state="invalidated",
                    reason=f"plan_refresh:{reason}",
                    details={
                        "old_input_fingerprint": existing["input_fingerprint"],
                        "new_input_fingerprint": fingerprint,
                        "plan_revision": new_revision,
                    },
                )
            for child in conn.execute(
                """
                SELECT item_key, state FROM daily_run_work_items
                WHERE run_id = ? AND step_key = ? AND state = 'completed'
                ORDER BY order_no, item_key
                """,
                (run_id, step["key"]),
            ).fetchall():
                conn.execute(
                    """
                    UPDATE daily_run_work_items SET state = 'invalidated',
                        completed_at = NULL, updated_at = ?
                    WHERE run_id = ? AND step_key = ? AND item_key = ?
                    """,
                    (timestamp, run_id, step["key"], child["item_key"]),
                )
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="work_item",
                    entity_key=f"{step['key']}/{child['item_key']}",
                    event_type="invalidated",
                    from_state="completed",
                    to_state="invalidated",
                    reason=f"parent_plan_refresh:{reason}",
                    details={
                        "old_parent_input_fingerprint": existing["input_fingerprint"],
                        "new_parent_input_fingerprint": fingerprint,
                        "plan_revision": new_revision,
                    },
                )
            conn.execute(
                """
                UPDATE daily_run_steps SET input_fingerprint = ?, scope_json = ?,
                    step_kind = ?, order_no = ?, required = MAX(required, ?), updated_at = ?
                WHERE run_id = ? AND step_key = ?
                """,
                (
                    fingerprint,
                    canonical_json(step["scope"]),
                    step["kind"],
                    step["order"],
                    int(step["required"]),
                    timestamp,
                    run_id,
                    step["key"],
                ),
            )
        for dependency in step["depends_on"]:
            conn.execute(
                """
                INSERT OR IGNORE INTO daily_run_step_dependencies (
                    run_id, step_key, depends_on_step_key, plan_revision_added, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, step["key"], dependency, new_revision, timestamp),
            )
        for item in step["items"]:
            existing_item = conn.execute(
                """
                SELECT * FROM daily_run_work_items
                WHERE run_id = ? AND step_key = ? AND item_key = ?
                """,
                (run_id, step["key"], item["key"]),
            ).fetchone()
            item_fingerprint = _item_input_fingerprint(item)
            if existing_item is None:
                stored_item_state = (
                    "pending"
                    if item["state"] == "needs_verification"
                    else item["state"]
                )
                conn.execute(
                    """
                    INSERT INTO daily_run_work_items (
                        run_id, step_key, item_key, item_kind, order_no, required,
                        state, plan_revision_added, input_fingerprint, updated_at,
                        scope_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        step["key"],
                        item["key"],
                        item["kind"],
                        item["order"],
                        int(item["required"]),
                        stored_item_state,
                        new_revision,
                        item_fingerprint,
                        timestamp,
                        canonical_json(item["scope"]),
                    ),
                )
                added_items += 1
                changed_roots.add(step["key"])
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="work_item",
                    entity_key=f"{step['key']}/{item['key']}",
                    event_type="added_to_plan",
                    from_state=None,
                    to_state=str(stored_item_state),
                    reason=f"plan_refresh:{reason}",
                    details={"plan_revision": new_revision},
                )
                if item["state"] == "needs_verification":
                    new_uncertain_items.append((step["key"], item["key"]))
            elif str(existing_item["input_fingerprint"]) != item_fingerprint:
                changed_roots.add(step["key"])
                state = (
                    "invalidated"
                    if existing_item["state"] in FINISHED_WORK_STATES
                    else existing_item["state"]
                )
                conn.execute(
                    """
                    UPDATE daily_run_work_items SET input_fingerprint = ?, scope_json = ?,
                        item_kind = ?, order_no = ?, required = MAX(required, ?),
                        state = ?, completed_at = CASE WHEN ? = 'invalidated' THEN NULL ELSE completed_at END,
                        updated_at = ?
                    WHERE run_id = ? AND step_key = ? AND item_key = ?
                    """,
                    (
                        item_fingerprint,
                        canonical_json(item["scope"]),
                        item["kind"],
                        item["order"],
                        int(item["required"]),
                        state,
                        state,
                        timestamp,
                        run_id,
                        step["key"],
                        item["key"],
                    ),
                )
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="work_item",
                    entity_key=f"{step['key']}/{item['key']}",
                    event_type="invalidated" if state == "invalidated" else "scope_refreshed",
                    from_state=str(existing_item["state"]),
                    to_state=str(state),
                    reason=f"plan_refresh:{reason}",
                    details={
                        "old_input_fingerprint": existing_item["input_fingerprint"],
                        "new_input_fingerprint": item_fingerprint,
                        "plan_revision": new_revision,
                    },
                )
    for step_key, item_key in new_uncertain_items:
        mark_uncertain(
            conn,
            run_id=run_id,
            step_key=step_key,
            item_key=item_key,
            reason="Попытка внешнего действия не имеет видимого подтверждения.",
        )
    for root in sorted(changed_roots):
        for descendant in _descendants(conn, run_id, [root]):
            descendant_row = conn.execute(
                "SELECT state FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
                (run_id, descendant),
            ).fetchone()
            if descendant_row and descendant_row["state"] == "completed":
                conn.execute(
                    "UPDATE daily_run_steps SET state = 'invalidated', completed_at = NULL, updated_at = ? WHERE run_id = ? AND step_key = ?",
                    (timestamp, run_id, descendant),
                )
                append_transition(
                    conn,
                    run_id=run_id,
                    entity_type="step",
                    entity_key=descendant,
                    event_type="invalidated",
                    from_state="completed",
                    to_state="invalidated",
                    reason=f"plan_refresh:{reason}",
                    details={"upstream_step": root, "plan_revision": new_revision},
                )
    current_items = {
        (str(step["key"]), str(item["key"]))
        for step in plan["steps"]
        for item in step["items"]
        if item["required"]
    }
    retained_required = [
        {"step_key": str(row["step_key"]), "item_key": str(row["item_key"])}
        for row in conn.execute(
            """
            SELECT step_key, item_key FROM daily_run_work_items
            WHERE run_id = ? AND required = 1
            ORDER BY step_key, order_no, item_key
            """,
            (run_id,),
        ).fetchall()
        if (str(row["step_key"]), str(row["item_key"])) not in current_items
    ]
    current_steps = {str(step["key"]) for step in plan["steps"] if step["required"]}
    retained_required_steps = [
        str(row["step_key"])
        for row in conn.execute(
            """
            SELECT step_key FROM daily_run_steps
            WHERE run_id = ? AND required = 1
            ORDER BY order_no, step_key
            """,
            (run_id,),
        ).fetchall()
        if str(row["step_key"]) not in current_steps
    ]
    merged_scope = dict(plan["scope"])
    if retained_required:
        merged_scope["retained_prior_requirements"] = retained_required
    if retained_required_steps:
        merged_scope["retained_prior_required_steps"] = retained_required_steps
    merged_fingerprint = payload_hash(merged_scope)
    if (
        not changed_roots
        and added_steps == 0
        and added_items == 0
        and merged_fingerprint == str(run["plan_fingerprint"])
        and plan["configuration_fingerprint"]
        == str(run["configuration_fingerprint"])
    ):
        refresh_run_snapshot(conn, run_id)
        return {
            "changed": False,
            "plan_revision": int(run["plan_revision"]),
            "plan_fingerprint": str(run["plan_fingerprint"]),
            "configuration_fingerprint": str(run["configuration_fingerprint"]),
            "changed_roots": [],
            "added_steps": 0,
            "added_items": 0,
            "retained_prior_requirements": len(retained_required),
            "retained_prior_required_steps": len(retained_required_steps),
        }
    conn.execute(
        """
        INSERT INTO daily_run_plan_revisions (
            run_id, revision, plan_version, plan_fingerprint,
            configuration_fingerprint, scope_json, reason, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            new_revision,
            plan["plan_version"],
            merged_fingerprint,
            plan["configuration_fingerprint"],
            canonical_json(merged_scope),
            reason,
            timestamp,
        ),
    )
    conn.execute(
        """
        UPDATE daily_runs SET plan_revision = ?, plan_version = ?,
            plan_fingerprint = ?, configuration_fingerprint = ?, scope_json = ?,
            status = 'running', updated_at = ? WHERE run_id = ?
        """,
        (
            new_revision,
            plan["plan_version"],
            merged_fingerprint,
            plan["configuration_fingerprint"],
            canonical_json(merged_scope),
            timestamp,
            run_id,
        ),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="plan",
        entity_key=str(new_revision),
        event_type="refreshed",
        from_state=str(run["plan_revision"]),
        to_state=str(new_revision),
        reason=reason,
        details={
            "changed_roots": sorted(changed_roots),
            "added_steps": added_steps,
            "added_items": added_items,
            "retained_prior_requirements": len(retained_required),
            "retained_prior_required_steps": len(retained_required_steps),
        },
    )
    for step in plan["steps"]:
        _aggregate_step(conn, run_id, step["key"])
    refresh_run_snapshot(conn, run_id)
    return {
        "changed": True,
        "plan_revision": new_revision,
        "plan_fingerprint": merged_fingerprint,
        "configuration_fingerprint": plan["configuration_fingerprint"],
        "changed_roots": sorted(changed_roots),
        "added_steps": added_steps,
        "added_items": added_items,
        "retained_prior_requirements": len(retained_required),
        "retained_prior_required_steps": len(retained_required_steps),
    }


def _coverage_closeout_issues(conn: sqlite3.Connection, run: sqlite3.Row) -> list[str]:
    issues: list[str] = []
    for source, step_key in (("hh", "hh_coverage"), ("telegram", "telegram_coverage")):
        step = conn.execute(
            "SELECT required, state FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
            (run["run_id"], step_key),
        ).fetchone()
        if step is None or not step["required"]:
            continue
        # P2 HH completion is proved directly by one immutable, validated v2
        # manifest per required stream. Keep the legacy P1/search_runs contract
        # unchanged for Telegram and for HH work that did not use P2.
        if source == "hh" and step["state"] == "completed":
            required_items = conn.execute(
                """
                SELECT item_key, manifest_hash
                FROM daily_run_work_items
                WHERE run_id = ? AND step_key = ? AND required = 1
                ORDER BY order_no, item_key
                """,
                (run["run_id"], step_key),
            ).fetchall()
            p2_complete = bool(required_items)
            for item in required_items:
                manifest = conn.execute(
                    """
                    SELECT payload_json FROM daily_run_manifests
                    WHERE run_id = ? AND step_key = ? AND item_key = ?
                      AND payload_hash = ? AND manifest_kind = 'hh_stream'
                      AND validation_status = 'validated'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (
                        run["run_id"],
                        step_key,
                        item["item_key"],
                        item["manifest_hash"] or "",
                    ),
                ).fetchone()
                payload = json.loads(str(manifest["payload_json"])) if manifest else {}
                if not (
                    payload.get("manifest_version") == 2
                    and payload.get("remote_boundary_verified") is True
                ):
                    p2_complete = False
                    break
            if p2_complete:
                continue
        if run["status"] == "completed":
            manifest_kind = "hh_stream" if source == "hh" else "telegram_channel"
            missing_manifests = 0
            for item in conn.execute(
                """
                SELECT item_key, manifest_hash
                FROM daily_run_work_items
                WHERE run_id = ? AND step_key = ? AND required = 1
                ORDER BY order_no, item_key
                """,
                (run["run_id"], step_key),
            ).fetchall():
                manifest = conn.execute(
                    """
                    SELECT payload_json FROM daily_run_manifests
                    WHERE run_id = ? AND step_key = ? AND item_key = ?
                      AND payload_hash = ? AND manifest_kind = ?
                      AND validation_status = 'validated'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (
                        run["run_id"],
                        step_key,
                        item["item_key"],
                        item["manifest_hash"] or "",
                        manifest_kind,
                    ),
                ).fetchone()
                if manifest is None or json.loads(str(manifest["payload_json"])).get(
                    "remote_boundary_verified"
                ) is not True:
                    missing_manifests += 1
            if missing_manifests:
                issues.append(
                    f"Для завершённого source={source} отсутствуют неизменяемые манифесты запуска: "
                    f"{missing_manifests}."
                )
            continue
        search_run = conn.execute(
            """
            SELECT id, status, issue_count FROM search_runs
            WHERE run_date = ? AND source = ?
            """,
            (run["run_date"], source),
        ).fetchone()
        if search_run is None or search_run["status"] != "completed" or int(search_run["issue_count"]) != 0:
            issues.append(f"Для source={source} нет успешного манифеста покрытия за run_date.")
            continue
        expected = {
            str(json.loads(str(row["scope_json"])).get("stream_key", ""))
            for row in conn.execute(
                """
                SELECT scope_json FROM daily_run_work_items
                WHERE run_id = ? AND step_key = ? AND required = 1
                """,
                (run["run_id"], step_key),
            ).fetchall()
        }
        actual = {
            str(row["stream_key"])
            for row in conn.execute(
                """
                SELECT stream_key FROM search_coverage
                WHERE search_run_id = ? AND status = 'completed'
                  AND COALESCE(issues, '[]') = '[]'
                """,
                (search_run["id"],),
            ).fetchall()
        }
        missing = sorted(expected - actual)
        if missing:
            issues.append(
                f"Покрытие source={source} не закрыло зафиксированные элементы: "
                + ", ".join(missing)
            )
    return issues


def _manifest_integrity_issues(conn: sqlite3.Connection, run_id: str) -> list[str]:
    issues: list[str] = []
    for item in _leaf_rows(conn, run_id):
        if not item["required"] or item["state"] not in FINISHED_WORK_STATES:
            continue
        if item["item_key"]:
            row = conn.execute(
                """
                SELECT manifest_hash FROM daily_run_work_items
                WHERE run_id = ? AND step_key = ? AND item_key = ?
                """,
                (run_id, item["step_key"], item["item_key"]),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT manifest_hash FROM daily_run_steps
                WHERE run_id = ? AND step_key = ?
                """,
                (run_id, item["step_key"]),
            ).fetchone()
        digest = str(row["manifest_hash"] or "") if row else ""
        manifest = (
            conn.execute(
                """
                SELECT 1 FROM daily_run_manifests
                WHERE run_id = ? AND step_key = ? AND item_key = ?
                  AND payload_hash = ? AND validation_status = 'validated'
                LIMIT 1
                """,
                (run_id, item["step_key"], item["item_key"], digest),
            ).fetchone()
            if digest
            else None
        )
        if manifest is None:
            label = f"{item['step_key']}/{item['item_key']}".rstrip("/")
            issues.append(f"Завершённая работа {label} не имеет проверенного манифеста запуска.")
    return issues


def _dynamic_scope_issues(conn: sqlite3.Connection, run: sqlite3.Row) -> list[str]:
    if run["status"] == "completed":
        return []
    existing_due = {
        str(row["item_key"])
        for row in conn.execute(
            """
            SELECT item_key FROM daily_run_work_items
            WHERE run_id = ? AND step_key = 'due_followups' AND required = 1
            """,
            (run["run_id"],),
        ).fetchall()
    }
    authoritative_due = {
        f"due:{row['vacancy_id']}:{row['application_id']}:{row['follow_up_date']}"
        for row in authoritative_due_followups(conn, str(run["run_date"]))
    }
    existing_actions = {
        str(row["item_key"])
        for row in conn.execute(
            """
            SELECT item_key FROM daily_run_work_items
            WHERE run_id = ? AND step_key = 'external_action_reconciliation'
              AND required = 1
            """,
            (run["run_id"],),
        ).fetchall()
    }
    authoritative_actions = {
        stable_item_key("external", str(row["action_key"]))
        for row in latest_nonterminal_external_actions(conn)
    }
    issues: list[str] = []
    if missing := sorted(authoritative_due - existing_due):
        issues.append(
            f"Авторитетная очередь повторных обращений содержит незафиксированные элементы: {len(missing)}."
        )
    if missing := sorted(authoritative_actions - existing_actions):
        issues.append(
            f"Авторитетная очередь внешних действий содержит незафиксированные элементы: {len(missing)}."
        )
    return issues


def closeout_readiness(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    mutate_dynamic: bool,
) -> dict[str, Any]:
    run = get_run(conn, run_id)
    if run is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    dynamic = (
        refresh_dynamic_work(conn, settings, run_id=run_id)
        if mutate_dynamic and run["status"] != "completed"
        else {"due_followups": 0, "external_actions": 0, "resolved": 0, "uncertain": 0}
    )
    run = get_run(conn, run_id)
    assert run is not None
    issues: list[str] = []
    current_config = configuration_fingerprint(settings, str(run["timezone"]))
    if (
        run["status"] != "completed"
        and current_config != str(run["configuration_fingerprint"])
    ):
        issues.append(
            "Конфигурация изменилась после фиксации плана; требуется аудируемое обновление плана."
        )
    issues.extend(_dynamic_scope_issues(conn, run))
    issues.extend(_coverage_closeout_issues(conn, run))
    issues.extend(_manifest_integrity_issues(conn, run_id))
    quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
    if quick_check != "ok":
        issues.append(f"PRAGMA quick_check вернул: {quick_check}.")
    foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        issues.append(f"PRAGMA foreign_key_check вернул нарушений: {len(foreign_keys)}.")
    for item in _leaf_rows(conn, run_id):
        if not item["required"]:
            continue
        if item["step_key"] in {"sqlite_reconciliation", "closeout"}:
            continue
        if item["state"] not in FINISHED_WORK_STATES:
            label = f"{item['step_key']}/{item['item_key']}".rstrip("/")
            issues.append(f"Обязательная работа {label} имеет состояние {item['state']}.")
    sqlite_step = conn.execute(
        "SELECT * FROM daily_run_steps WHERE run_id = ? AND step_key = 'sqlite_reconciliation'",
        (run_id,),
    ).fetchone()
    if (
        mutate_dynamic
        and not issues
        and sqlite_step is not None
        and sqlite_step["state"] != "completed"
    ):
        manifest = {
            "manifest_version": 1,
            "kind": "sqlite_reconciliation",
            "run_id": run_id,
            "step_key": "sqlite_reconciliation",
            "item_key": "",
            "observed_at": now_iso(),
            "captured_scope": json.loads(str(sqlite_step["scope_json"])),
            "completion_boundary": "authoritative_sqlite_checks_passed",
            "remote_boundary_verified": False,
            "blockers": [],
        }
        complete_work(
            conn,
            settings,
            run_id=run_id,
            step_key="sqlite_reconciliation",
            item_key="",
            manifest=manifest,
            record_type="programmatic",
        )
    refresh_run_snapshot(conn, run_id)
    return {
        "ready": not issues,
        "run_id": run_id,
        "status": run["status"],
        "issues": issues,
        "dynamic_refresh": dynamic,
        "quick_check": quick_check,
        "foreign_key_issues": len(foreign_keys),
    }


def enter_finalizing(conn: sqlite3.Connection, *, run_id: str, dirty_revision: int) -> None:
    run = get_run(conn, run_id)
    if run is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    if run["status"] == "completed":
        return
    previous = str(run["status"])
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE daily_runs SET status = 'finalizing',
            projection_revision_finalizing = COALESCE(projection_revision_finalizing, ?),
            finalization_started_at = COALESCE(finalization_started_at, ?),
            updated_at = ? WHERE run_id = ?
        """,
        (dirty_revision, timestamp, timestamp, run_id),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="run",
        entity_key=run_id,
        event_type="finalization_started",
        from_state=previous,
        to_state="finalizing",
        reason="all_programmatic_preconditions_passed",
        details={"projection_revision_finalizing": dirty_revision},
    )


def final_render_already_published(
    conn: sqlite3.Connection, *, run_id: str, projection_state: Mapping[str, Any]
) -> bool:
    run = get_run(conn, run_id)
    if run is None or run["projection_revision_finalizing"] is None:
        return False
    return (
        not bool(projection_state["dirty"])
        and int(projection_state["rendered_revision"])
        >= int(run["projection_revision_finalizing"])
    )


def mark_completed(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    lease_token: str,
    projection_revision: int,
) -> None:
    run = get_run(conn, run_id)
    if run is None:
        raise RuntimeError(f"Ежедневный запуск {run_id} не найден.")
    if run["status"] == "completed":
        return
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE daily_runs SET status = 'completed', completed_at = ?, updated_at = ?,
            projection_revision_completed = ?, current_lease_token = NULL,
            last_lease_token = ? WHERE run_id = ?
        """,
        (timestamp, timestamp, projection_revision, lease_token, run_id),
    )
    conn.execute(
        """
        UPDATE daily_run_steps SET state = 'completed', completed_at = ?,
            updated_at = ? WHERE run_id = ? AND step_key = 'closeout'
        """,
        (timestamp, timestamp, run_id),
    )
    append_transition(
        conn,
        run_id=run_id,
        entity_type="run",
        entity_key=run_id,
        event_type="completed",
        from_state=str(run["status"]),
        to_state="completed",
        reason="atomic_projection_published",
        details={"projection_revision": projection_revision},
    )
    refresh_run_snapshot(conn, run_id)


def note_external_action_event(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    action_key: str,
) -> None:
    """Reconcile a newly appended action and invalidate freshness-dependent closeout."""

    refresh_dynamic_work(conn, settings, run_id=run_id)
    latest_action = conn.execute(
        """
        SELECT state FROM external_actions
        WHERE action_key = ? ORDER BY event_at DESC, id DESC LIMIT 1
        """,
        (action_key,),
    ).fetchone()
    if latest_action is not None and latest_action["state"] in {
        "attempted",
        "visibly_confirmed",
    }:
        inbound = conn.execute(
            """
            SELECT state FROM daily_run_steps
            WHERE run_id = ? AND step_key = 'inbound_reconciliation'
            """,
            (run_id,),
        ).fetchone()
        if inbound is not None and inbound["state"] == "completed":
            conn.execute(
                """
                UPDATE daily_run_steps SET state = 'invalidated', completed_at = NULL,
                    updated_at = ? WHERE run_id = ? AND step_key = 'inbound_reconciliation'
                """,
                (now_iso(), run_id),
            )
            append_transition(
                conn,
                run_id=run_id,
                entity_type="step",
                entity_key="inbound_reconciliation",
                event_type="invalidated",
                from_state="completed",
                to_state="invalidated",
                reason="external_action_after_inbound_checkpoint",
                details={"action_key": action_key, "state": latest_action["state"]},
            )
    item_key = stable_item_key("external", action_key)
    row = conn.execute(
        """
        SELECT state FROM daily_run_work_items
        WHERE run_id = ? AND step_key = 'external_action_reconciliation'
          AND item_key = ?
        """,
        (run_id, item_key),
    ).fetchone()
    if row is None:
        return
    valid, resolution = external_action_resolution(
        conn, run_id, "external_action_reconciliation", item_key
    )
    if valid and row["state"] != "completed":
        _, item = _work_row(conn, run_id, "external_action_reconciliation", item_key)
        manifest = _programmatic_manifest(
            run_id,
            "external_action_reconciliation",
            item_key,
            "external_action",
            json.loads(str(item["scope_json"])),
            resolution,
        )
        complete_work(
            conn,
            settings,
            run_id=run_id,
            step_key="external_action_reconciliation",
            item_key=item_key,
            manifest=manifest,
            record_type="programmatic",
        )
    elif resolution.get("state") == "attempted" and row["state"] != "needs_verification":
        mark_uncertain(
            conn,
            run_id=run_id,
            step_key="external_action_reconciliation",
            item_key=item_key,
            reason="Попытка внешнего действия не имеет видимого подтверждения.",
        )
    for step_key in ("sqlite_reconciliation", "closeout"):
        step = conn.execute(
            "SELECT state FROM daily_run_steps WHERE run_id = ? AND step_key = ?",
            (run_id, step_key),
        ).fetchone()
        if step and step["state"] == "completed":
            conn.execute(
                "UPDATE daily_run_steps SET state = 'invalidated', completed_at = NULL, updated_at = ? WHERE run_id = ? AND step_key = ?",
                (now_iso(), run_id, step_key),
            )
            append_transition(
                conn,
                run_id=run_id,
                entity_type="step",
                entity_key=step_key,
                event_type="invalidated",
                from_state="completed",
                to_state="invalidated",
                reason="external_action_after_freshness_checkpoint",
                details={"action_key": action_key},
            )
    refresh_run_snapshot(conn, run_id)
