#!/usr/bin/env python3
"""Safe, deterministic HeadHunter DOM acquisition for schema v10.

The module is deliberately browser-neutral and standard-library-only.  It
accepts bounded evidence produced by the checked-in read-only DOM adapter,
validates that evidence fail-closed, reconciles canonical IDs in SQLite, and
maintains resumable per-stream acquisition state.  It never opens a browser,
uses an HTTP endpoint, or performs an external mutation.
"""

from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from jobsearch_config import Settings


SCHEMA_VERSION = 10
CAPTURE_CONTRACT_VERSION = 1
MANIFEST_VERSION = 2
ADAPTER_VERSION = "hh-dom-v1.0.2"
PAGE_CAPTURE_KIND = "hh_page_capture_v1"
DETAIL_CAPTURE_KIND = "hh_detail_capture_v1"
SOURCE_KINDS = {"ordinary_search", "personal_recommendations"}
ACQUISITION_MODES = {"full", "shadow", "delta", "resume", "audit"}
SESSION_STATES = {"exposed", "not_exposed"}
BLOCKER_TYPES = {"none", "login", "captcha", "access_denied", "error", "loading_timeout"}
HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
VACANCY_ID_RE = re.compile(r"^[0-9]{1,32}$")
FORBIDDEN_CAPTURE_KEYS = {
    "cookie",
    "cookies",
    "authorization",
    "access_token",
    "refresh_token",
    "auth_token",
    "account_id",
    "user_id",
    "email",
    "phone",
}
MAX_CAPTURE_BYTES = 1_000_000
MAX_DETAIL_DESCRIPTION_CHARS = 250_000
MAX_PAGE_CARDS = 500
BOUNDARY_SAMPLE_LIMIT = 250
ZERO_EVIDENCE_INVALIDATION_EVENT = "zero_evidence_plan_invalidated"
ZERO_EVIDENCE_REPLAN_EVENT = "zero_evidence_plan_replanned"
ZERO_EVIDENCE_NON_SOURCE_EVENTS = {
    "acquisition_planned",
    ZERO_EVIDENCE_INVALIDATION_EVENT,
    ZERO_EVIDENCE_REPLAN_EVENT,
}
P1_AUDIT_ONLY_MANIFEST_RECORD_TYPES = {"block", "invalidation"}
P1_AUDIT_ONLY_TRANSITION_EVENTS = {
    "planned",
    "started",
    "blocked",
    "reopened",
    "invalidated",
}
ZERO_EVIDENCE_RECOVERY_BOOKKEEPING_BLOCKER_CODES = {
    "hh_zero_evidence_recovery_rejects_prior_blocker_audit",
    "hh_v102_recovery_rejects_superseded_map_link_audit",
}


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def payload_hash(value: Any) -> str:
    return sha256_text(canonical_json(value))


def _json_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label}: требуется объект JSON.")
    return dict(value)


def _json_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label}: требуется массив JSON.")
    return list(value)


def _nonempty_string(value: Any, label: str, maximum: int = 10_000) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label}: требуется непустая строка.")
    cleaned = value.strip()
    if len(cleaned) > maximum:
        raise ValueError(f"{label}: строка длиннее допустимого предела {maximum}.")
    return cleaned


def _optional_string(value: Any, label: str, maximum: int = 10_000) -> str:
    if value in (None, ""):
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{label}: требуется строка.")
    cleaned = value.strip()
    if len(cleaned) > maximum:
        raise ValueError(f"{label}: строка длиннее допустимого предела {maximum}.")
    return cleaned


def _nonnegative_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{label}: требуется неотрицательное целое число.")
    return value


def _parse_iso(value: Any, label: str) -> str:
    text = _nonempty_string(value, label, 128)
    try:
        dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label}: требуется дата и время ISO 8601.") from exc
    return text


def _parse_date(value: Any, label: str) -> str:
    text = _nonempty_string(value, label, 32)
    try:
        dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{label}: требуется дата ГГГГ-ММ-ДД.") from exc
    return text


def _walk_forbidden_keys(value: Any, path: str = "capture") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().lower()
            if normalized in FORBIDDEN_CAPTURE_KEYS:
                raise ValueError(
                    f"{path}.{key}: секреты и идентификаторы аккаунта запрещены в снимке."
                )
            _walk_forbidden_keys(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk_forbidden_keys(child, f"{path}[{index}]")


def _canonical_url(value: Any, *, vacancy_id: str | None = None) -> str:
    text = _nonempty_string(value, "canonical_url", 4096)
    split = urlsplit(text)
    if split.scheme not in {"http", "https"} or not split.netloc or split.username or split.password:
        raise ValueError("canonical_url: требуется безопасный абсолютный HTTP(S)-адрес без учётных данных.")
    query = [
        (key, val)
        for key, val in parse_qsl(split.query, keep_blank_values=True)
        if key.lower()
        not in {
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_term",
            "utm_content",
            "from",
            "hhtmfrom",
            "hhtmfromlabel",
        }
    ]
    normalized = urlunsplit(
        (
            split.scheme.lower(),
            split.netloc.lower(),
            re.sub(r"/{2,}", "/", split.path) or "/",
            urlencode(query, doseq=True),
            "",
        )
    )
    if vacancy_id is not None:
        match = re.search(r"/vacancy/([0-9]{1,32})(?:/|$)", split.path)
        if match is None or match.group(1) != vacancy_id:
            raise ValueError(
                "canonical_url: путь вакансии не подтверждает переданный vacancy_id."
            )
        return f"{split.scheme.lower()}://{split.netloc.lower()}/vacancy/{vacancy_id}"
    return normalized


def canonical_external_id(value: Any) -> tuple[str, str]:
    text = _nonempty_string(value, "vacancy_id", 64)
    raw = text[3:] if text.lower().startswith("hh:") else text
    if not VACANCY_ID_RE.fullmatch(raw):
        raise ValueError("vacancy_id: требуется точный числовой идентификатор HH.")
    normalized = raw.lstrip("0") or "0"
    return normalized, f"hh:{normalized}"


def _normalize_text(value: Any) -> str:
    text = str(value or "").casefold()
    return " ".join(text.split())


def list_material_fingerprint(card: Mapping[str, Any]) -> str:
    identity = {
        "title": _normalize_text(card.get("title")),
        "company": _normalize_text(card.get("company")),
        "publication": _normalize_text(card.get("publication_evidence")),
        "url": str(card.get("canonical_url") or ""),
    }
    # Source ranking/position is durable source evidence, but is deliberately not
    # a material vacancy change and must not enqueue detail review by itself.
    return "hh-list:v2:" + payload_hash(identity)


def detail_material_fingerprint(fields: Mapping[str, Any]) -> str:
    relevant = {
        key: _normalize_text(fields.get(key))
        for key in (
            "title",
            "company",
            "salary",
            "location",
            "schedule",
            "employment_format",
            "requirements",
            "description",
        )
    }
    return "hh-detail:v1:" + payload_hash(relevant)


def acquisition_config_payload(
    settings: Settings,
    *,
    source_kind: str,
    stream_key: str,
    query_fingerprint: str,
) -> dict[str, Any]:
    cfg = settings.search.hh_acquisition
    payload = {
        "source_kind": source_kind,
        "stream_key": stream_key.casefold(),
        "query_fingerprint": query_fingerprint,
        "adapter_version": ADAPTER_VERSION,
        "search_period_days": settings.search.default_period_days,
        "items_per_page": settings.search.items_per_page,
        # The rollout policy is deliberately not part of the evidence
        # fingerprint. A stream must be able to accumulate clean shadow
        # evidence and then switch from shadow to enabled without discarding
        # that evidence. The planner still applies incremental_mode as a
        # separate, fail-closed policy gate.
        "minimum_overlap_pages": cfg.minimum_overlap_pages,
        "consecutive_known_boundary_pages": cfg.consecutive_known_boundary_pages,
        "guard_page_required": cfg.guard_page_required,
        "checkpoint_staleness_days": cfg.checkpoint_staleness_days,
        "shadow_runs_required": cfg.shadow_runs_required,
        "full_audit_interval_days": cfg.full_audit_interval_days,
        "page_stability_samples": cfg.page_stability_samples,
        "page_stability_delay_ms": cfg.page_stability_delay_ms,
        "page_stability_timeout_ms": cfg.page_stability_timeout_ms,
        "count_drift_recaptures": cfg.count_drift_recaptures,
        "max_pages_per_stream": cfg.max_pages_per_stream,
    }
    if source_kind == "personal_recommendations":
        payload.update(
            {
                "personal_initial_depth_pages": cfg.personal_initial_depth_pages,
                "personal_minimum_stable_pages": cfg.personal_minimum_stable_pages,
                "personal_consecutive_known_pages": cfg.personal_consecutive_known_pages,
                "personal_max_pages": cfg.personal_max_pages,
                "personal_max_is_completion_boundary": cfg.personal_max_is_completion_boundary,
            }
        )
    return payload


def acquisition_configuration_fingerprint(
    settings: Settings,
    *,
    source_kind: str,
    stream_key: str,
    query_fingerprint: str,
) -> str:
    return payload_hash(
        acquisition_config_payload(
            settings,
            source_kind=source_kind,
            stream_key=stream_key,
            query_fingerprint=query_fingerprint,
        )
    )


def ensure_v10_schema(conn: sqlite3.Connection) -> None:
    """Add P2 acquisition state without rewriting v9 evidence."""

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS hh_stream_checkpoints (
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            checkpoint_version INTEGER NOT NULL,
            last_successful_run_id TEXT NOT NULL,
            last_successful_date TEXT NOT NULL,
            acquisition_mode TEXT NOT NULL,
            query_fingerprint TEXT NOT NULL,
            configuration_fingerprint TEXT NOT NULL,
            adapter_version TEXT NOT NULL,
            newest_publication TEXT,
            oldest_publication TEXT,
            covered_range_json TEXT NOT NULL,
            boundary_id_hash TEXT NOT NULL,
            boundary_sample_json TEXT NOT NULL,
            last_full_scan_at TEXT,
            last_audit_scan_at TEXT,
            shadow_clean_runs INTEGER NOT NULL DEFAULT 0,
            shadow_runs_required INTEGER NOT NULL,
            last_shadow_result_json TEXT NOT NULL DEFAULT '{}',
            last_audit_result_json TEXT NOT NULL DEFAULT '{}',
            session_id_state TEXT NOT NULL,
            session_fingerprint TEXT NOT NULL,
            anomaly_state TEXT NOT NULL DEFAULT '',
            eligibility_state TEXT NOT NULL,
            cursor_json TEXT NOT NULL,
            invalidated_at TEXT,
            invalidation_reason TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(source, stream_key),
            CHECK(source_kind IN ('ordinary_search','personal_recommendations')),
            CHECK(acquisition_mode IN ('full','shadow','delta','resume','audit')),
            CHECK(session_id_state IN ('exposed','not_exposed')),
            CHECK(checkpoint_version >= 1),
            CHECK(shadow_clean_runs >= 0)
        );

        CREATE INDEX IF NOT EXISTS idx_hh_stream_checkpoints_eligibility
            ON hh_stream_checkpoints(eligibility_state, updated_at, stream_key);

        CREATE TABLE IF NOT EXISTS hh_stream_checkpoint_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            run_id TEXT NOT NULL,
            checkpoint_version INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            snapshot_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(source, stream_key, run_id, event_type, snapshot_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_hh_checkpoint_history_lookup
            ON hh_stream_checkpoint_history(source, stream_key, id);

        CREATE TABLE IF NOT EXISTS hh_stream_runs (
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            p1_step_key TEXT NOT NULL,
            p1_item_key TEXT NOT NULL,
            requested_mode TEXT NOT NULL,
            effective_mode TEXT NOT NULL,
            resume_from_mode TEXT,
            query_fingerprint TEXT NOT NULL,
            configuration_fingerprint TEXT NOT NULL,
            adapter_version TEXT NOT NULL,
            state TEXT NOT NULL,
            next_page INTEGER NOT NULL DEFAULT 0,
            last_verified_page INTEGER NOT NULL DEFAULT -1,
            predicted_boundary_page INTEGER,
            boundary_candidate_page INTEGER,
            boundary_proven_page INTEGER,
            known_page_streak INTEGER NOT NULL DEFAULT 0,
            guard_pages_verified INTEGER NOT NULL DEFAULT 0,
            source_exhausted INTEGER NOT NULL DEFAULT 0,
            session_id_state TEXT,
            session_fingerprint TEXT,
            newest_publication TEXT,
            oldest_publication TEXT,
            source_reported_count INTEGER,
            raw_count INTEGER NOT NULL DEFAULT 0,
            unique_count INTEGER NOT NULL DEFAULT 0,
            known_unchanged_count INTEGER NOT NULL DEFAULT 0,
            known_changed_count INTEGER NOT NULL DEFAULT 0,
            new_count INTEGER NOT NULL DEFAULT 0,
            duplicate_on_page_count INTEGER NOT NULL DEFAULT 0,
            duplicate_across_pages_count INTEGER NOT NULL DEFAULT 0,
            duplicate_across_streams_count INTEGER NOT NULL DEFAULT 0,
            unresolved_drift_page INTEGER,
            fallback_reason TEXT,
            blocker_code TEXT,
            blocker_reason TEXT,
            completion_manifest_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            PRIMARY KEY(run_id, source, stream_key),
            CHECK(source_kind IN ('ordinary_search','personal_recommendations')),
            CHECK(requested_mode IN ('full','shadow','delta','resume','audit')),
            CHECK(effective_mode IN ('full','shadow','delta','resume','audit')),
            CHECK(state IN ('planned','capturing','checkpointed','details_pending','ready_to_finalize','completed','blocked')),
            FOREIGN KEY(run_id) REFERENCES daily_runs(run_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_hh_stream_runs_state
            ON hh_stream_runs(run_id, state, source_kind, stream_key);

        CREATE TABLE IF NOT EXISTS hh_page_captures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            page_index INTEGER NOT NULL,
            recapture_no INTEGER NOT NULL,
            capture_hash TEXT NOT NULL UNIQUE,
            adapter_version TEXT NOT NULL,
            query_fingerprint TEXT NOT NULL,
            canonical_url_hash TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            canonical_id_set_hash TEXT NOT NULL,
            source_reported_count INTEGER,
            source_expected_page_count INTEGER,
            raw_card_count INTEGER NOT NULL,
            canonical_unique_count INTEGER NOT NULL,
            stability_json TEXT NOT NULL,
            navigation_json TEXT NOT NULL,
            ordering_json TEXT NOT NULL,
            session_id_state TEXT NOT NULL,
            session_fingerprint TEXT NOT NULL,
            warnings_json TEXT NOT NULL,
            blockers_json TEXT NOT NULL,
            count_drift_state TEXT NOT NULL,
            verified INTEGER NOT NULL,
            artifact_path TEXT NOT NULL,
            artifact_sha256 TEXT NOT NULL,
            reconciliation_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id, source, stream_key)
                REFERENCES hh_stream_runs(run_id, source, stream_key) ON DELETE CASCADE,
            UNIQUE(run_id, source, stream_key, page_index, recapture_no),
            CHECK(verified IN (0,1)),
            CHECK(session_id_state IN ('exposed','not_exposed')),
            CHECK(count_drift_state IN ('none','awaiting_recapture','verified','conflict'))
        );

        CREATE INDEX IF NOT EXISTS idx_hh_page_captures_lookup
            ON hh_page_captures(run_id, source, stream_key, page_index, recapture_no);

        CREATE TABLE IF NOT EXISTS hh_page_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            capture_id INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            page_index INTEGER NOT NULL,
            ordinal INTEGER NOT NULL,
            external_id TEXT NOT NULL,
            vacancy_id INTEGER,
            canonical_url TEXT NOT NULL,
            title TEXT,
            company TEXT,
            position_text TEXT,
            publication_evidence TEXT,
            promoted INTEGER NOT NULL DEFAULT 0,
            pinned INTEGER NOT NULL DEFAULT 0,
            material_fingerprint TEXT NOT NULL,
            base_classification TEXT NOT NULL,
            classification TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(capture_id) REFERENCES hh_page_captures(id) ON DELETE CASCADE,
            FOREIGN KEY(vacancy_id) REFERENCES vacancies(id) ON DELETE SET NULL,
            CHECK(base_classification IN ('known_unchanged','known_changed','new','duplicate_on_page')),
            CHECK(classification IN ('known_unchanged','known_changed','new','duplicate_on_page','duplicate_across_pages','duplicate_across_streams'))
        );

        CREATE INDEX IF NOT EXISTS idx_hh_page_items_external
            ON hh_page_items(run_id, external_id, stream_key, page_index);

        CREATE TABLE IF NOT EXISTS hh_vacancy_snapshots (
            external_id TEXT PRIMARY KEY,
            vacancy_id INTEGER,
            list_material_fingerprint TEXT NOT NULL,
            detail_material_fingerprint TEXT,
            title_normalized TEXT,
            company_normalized TEXT,
            publication_evidence TEXT,
            evidence_level TEXT NOT NULL,
            last_capture_hash TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(vacancy_id) REFERENCES vacancies(id) ON DELETE SET NULL,
            CHECK(evidence_level IN ('list','detail'))
        );

        CREATE TABLE IF NOT EXISTS hh_detail_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            external_id TEXT NOT NULL,
            vacancy_id INTEGER,
            canonical_url TEXT NOT NULL,
            reason TEXT NOT NULL,
            first_page INTEGER NOT NULL,
            last_page INTEGER NOT NULL,
            material_fingerprint TEXT NOT NULL,
            state TEXT NOT NULL,
            detail_capture_hash TEXT,
            detail_artifact_path TEXT,
            detail_artifact_sha256 TEXT,
            detail_payload_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(run_id, source, stream_key)
                REFERENCES hh_stream_runs(run_id, source, stream_key) ON DELETE CASCADE,
            FOREIGN KEY(vacancy_id) REFERENCES vacancies(id) ON DELETE SET NULL,
            UNIQUE(run_id, source, stream_key, external_id),
            CHECK(reason IN ('new','materially_changed')),
            CHECK(state IN ('pending','captured','blocked'))
        );

        CREATE INDEX IF NOT EXISTS idx_hh_detail_queue_state
            ON hh_detail_queue(run_id, state, stream_key, id);

        CREATE TABLE IF NOT EXISTS hh_incremental_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            details_json TEXT NOT NULL,
            event_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            CHECK(severity IN ('info','warning','failure'))
        );

        CREATE INDEX IF NOT EXISTS idx_hh_incremental_events_lookup
            ON hh_incremental_events(run_id, stream_key, id);
        """
    )


def schema_v10_issues(conn: sqlite3.Connection) -> list[str]:
    required = {
        "hh_stream_checkpoints": {
            "source",
            "stream_key",
            "checkpoint_version",
            "query_fingerprint",
            "eligibility_state",
        },
        "hh_stream_checkpoint_history": {"stream_key", "snapshot_hash", "event_type"},
        "hh_stream_runs": {"run_id", "stream_key", "effective_mode", "next_page"},
        "hh_page_captures": {"capture_hash", "verified", "count_drift_state"},
        "hh_page_items": {"external_id", "base_classification", "classification"},
        "hh_vacancy_snapshots": {"external_id", "list_material_fingerprint"},
        "hh_detail_queue": {"run_id", "external_id", "state"},
        "hh_incremental_events": {"run_id", "event_type", "event_hash"},
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


def _append_event(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    event_type: str,
    severity: str,
    details: Mapping[str, Any],
) -> bool:
    identity = {
        "run_id": run_id,
        "source": "hh",
        "stream_key": stream_key,
        "event_type": event_type,
        "severity": severity,
        "details": dict(details),
    }
    digest = payload_hash(identity)
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO hh_incremental_events (
            run_id, source, stream_key, event_type, severity,
            details_json, event_hash, created_at
        ) VALUES (?, 'hh', ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            stream_key,
            event_type,
            severity,
            canonical_json(dict(details)),
            digest,
            now_iso(),
        ),
    )
    return conn.total_changes > before


def _zero_evidence_recovery_events(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT id, event_type, details_json, event_hash, created_at
        FROM hh_incremental_events
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
          AND event_type IN (?, ?)
        ORDER BY id
        """,
        (
            run_id,
            stream_key,
            ZERO_EVIDENCE_INVALIDATION_EVENT,
            ZERO_EVIDENCE_REPLAN_EVENT,
        ),
    ).fetchall()
    invalidations: dict[int, dict[str, Any]] = {}
    replans: list[dict[str, Any]] = []
    consumed: set[int] = set()
    for row in rows:
        try:
            details = json.loads(str(row["details_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("Журнал recovery HH содержит повреждённое событие.") from exc
        event = {"row": row, "details": details}
        if str(row["event_type"]) == ZERO_EVIDENCE_INVALIDATION_EVENT:
            invalidations[int(row["id"])] = event
            continue
        invalidation_event_id = details.get("invalidation_event_id")
        if isinstance(invalidation_event_id, int) and not isinstance(
            invalidation_event_id, bool
        ):
            consumed.add(invalidation_event_id)
        replans.append(event)
    pending = next(
        (
            invalidations[event_id]
            for event_id in sorted(invalidations, reverse=True)
            if event_id not in consumed
        ),
        None,
    )
    return {
        "invalidations": invalidations,
        "replans": replans,
        "pending": pending,
    }


def _recovery_event_result(
    event: Mapping[str, Any],
    *,
    idempotent: bool,
    replan_required: bool,
) -> dict[str, Any]:
    row = event["row"]
    details = dict(event["details"])
    return {
        "run_id": details["run_id"],
        "stream_key": details["stream_key"],
        "source_kind": details["source_kind"],
        "invalidated": True,
        "idempotent": idempotent,
        "replan_required": replan_required,
        "previous_adapter_version": details["previous_adapter_version"],
        "previous_configuration_fingerprint": details[
            "previous_configuration_fingerprint"
        ],
        "target_adapter_version": details["target_adapter_version"],
        "target_configuration_fingerprint": details[
            "target_configuration_fingerprint"
        ],
        "reason": details["operator_reason"],
        "no_source_evidence_discarded": details[
            "no_source_evidence_discarded"
        ],
        "historical_checkpoint_preserved": details[
            "historical_checkpoint_preserved"
        ],
        "superseded_p1_audit": details.get("superseded_p1_audit", {}),
        "audit_event": {
            "id": int(row["id"]),
            "event_type": str(row["event_type"]),
            "event_hash": str(row["event_hash"]),
            "timestamp": str(row["created_at"]),
        },
    }


def _p1_target(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    source_kind: str,
) -> tuple[str, str, str]:
    if source_kind == "personal_recommendations":
        row = conn.execute(
            "SELECT step_kind, scope_json FROM daily_run_steps WHERE run_id = ? AND step_key = 'personal_recommendations'",
            (run_id,),
        ).fetchone()
        if row is None:
            raise ValueError("В плане P1 отсутствует шаг персональных рекомендаций.")
        scope = json.loads(str(row["scope_json"]))
        if str(scope.get("stream_key", "")).casefold() != stream_key.casefold():
            raise ValueError("Поток не совпадает с зафиксированными персональными рекомендациями P1.")
        if not bool(scope.get("enabled", False)):
            raise ValueError("Персональные рекомендации отключены в зафиксированном плане P1.")
        return "personal_recommendations", "", str(row["step_kind"])

    rows = conn.execute(
        """
        SELECT item_key, item_kind, scope_json
        FROM daily_run_work_items
        WHERE run_id = ? AND step_key = 'hh_coverage'
        """,
        (run_id,),
    ).fetchall()
    for row in rows:
        scope = json.loads(str(row["scope_json"]))
        if str(scope.get("stream_key", "")).casefold() == stream_key.casefold():
            return "hh_coverage", str(row["item_key"]), str(row["item_kind"])
    raise ValueError("Поток HH отсутствует в зафиксированных рабочих элементах P1.")


def _p1_audit_scope_matches(
    *,
    step_key: str,
    captured_scope: Any,
    expected_scope: Mapping[str, Any],
) -> bool:
    """Match audit identity across additive personal-gate configuration fields."""

    expected = dict(expected_scope)
    if captured_scope == expected:
        return True
    if step_key != "personal_recommendations":
        return False
    if not isinstance(captured_scope, dict):
        return False
    identity_keys = {"enabled", "hh_acquisition", "stream_key"}
    if set(captured_scope) != identity_keys or set(expected) != identity_keys:
        return False
    if captured_scope.get("enabled") != expected.get("enabled"):
        return False
    if captured_scope.get("stream_key") != expected.get("stream_key"):
        return False
    captured_acquisition = captured_scope.get("hh_acquisition")
    expected_acquisition = expected.get("hh_acquisition")
    if not isinstance(captured_acquisition, dict) or not isinstance(
        expected_acquisition, dict
    ):
        return False
    if not set(captured_acquisition) < set(expected_acquisition):
        return False
    return all(
        expected_acquisition.get(key) == value
        for key, value in captured_acquisition.items()
    )


def _p1_manifest_evidence_taxonomy(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    step_key: str,
    item_key: str,
    expected_scope: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Separate immutable P1 audit history from source-bearing evidence."""

    rows = conn.execute(
        """
        SELECT id, record_type, manifest_kind, validation_status,
               payload_json, payload_hash, observed_at
        FROM daily_run_manifests
        WHERE run_id = ? AND step_key = ? AND item_key = ?
        ORDER BY id
        """,
        (run_id, step_key, item_key),
    ).fetchall()
    audit_only: list[dict[str, Any]] = []
    source_bearing: list[dict[str, Any]] = []
    common_keys = {
        "manifest_version",
        "kind",
        "run_id",
        "step_key",
        "item_key",
        "observed_at",
        "captured_scope",
    }
    for row in rows:
        entry = {
            "id": int(row["id"]),
            "record_type": str(row["record_type"]),
            "manifest_kind": str(row["manifest_kind"]),
            "payload_hash": str(row["payload_hash"]),
            "observed_at": str(row["observed_at"]),
        }
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            entry["classification_reason"] = "invalid_payload_json"
            source_bearing.append(entry)
            continue
        if not isinstance(payload, dict):
            entry["classification_reason"] = "payload_not_object"
            source_bearing.append(entry)
            continue
        identity_matches = (
            payload.get("manifest_version") == 1
            and payload.get("run_id") == run_id
            and payload.get("step_key") == step_key
            and payload.get("item_key", "") == item_key
            and _p1_audit_scope_matches(
                step_key=step_key,
                captured_scope=payload.get("captured_scope"),
                expected_scope=expected_scope,
            )
        )
        record_type = str(row["record_type"])
        audit_kind = ""
        if record_type == "block":
            blockers = payload.get("blockers")
            valid_blockers = (
                isinstance(blockers, list)
                and bool(blockers)
                and all(
                    isinstance(blocker, dict)
                    and set(blocker) <= {"code", "reason", "retryable"}
                    and isinstance(blocker.get("code"), str)
                    and bool(str(blocker.get("code", "")).strip())
                    and isinstance(blocker.get("reason"), str)
                    and bool(str(blocker.get("reason", "")).strip())
                    and isinstance(blocker.get("retryable"), bool)
                    for blocker in blockers
                )
            )
            if (
                identity_matches
                and str(row["validation_status"]) == "blocked"
                and set(payload) <= common_keys | {"blockers"}
                and valid_blockers
            ):
                audit_kind = "audit_only_blocker"
        elif record_type == "invalidation":
            if (
                identity_matches
                and str(row["validation_status"]) == "invalidated"
                and set(payload)
                <= common_keys | {"reason", "previous_manifest_hash"}
                and isinstance(payload.get("reason"), str)
                and bool(str(payload.get("reason", "")).strip())
                and isinstance(payload.get("previous_manifest_hash", ""), str)
            ):
                audit_kind = "audit_only_invalidation"
        if audit_kind:
            entry["audit_kind"] = audit_kind
            if audit_kind == "audit_only_blocker":
                entry["blockers"] = [dict(blocker) for blocker in payload["blockers"]]
            elif audit_kind == "audit_only_invalidation":
                entry["previous_manifest_hash"] = str(
                    payload.get("previous_manifest_hash", "")
                )
            audit_only.append(entry)
        else:
            entry["classification_reason"] = (
                "unknown_or_source_bearing_manifest"
                if record_type in P1_AUDIT_ONLY_MANIFEST_RECORD_TYPES
                else "source_bearing_record_type"
            )
            source_bearing.append(entry)
    return {"audit_only": audit_only, "source_bearing": source_bearing}


def _p1_transition_evidence_taxonomy(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    entity_type: str,
    entity_key: str,
    blocker_manifest_hashes: set[str],
    invalidation_manifest_hashes: set[str],
) -> dict[str, list[dict[str, Any]]]:
    rows = conn.execute(
        """
        SELECT id, event_type, reason, details_json, event_hash, occurred_at
        FROM daily_run_transitions
        WHERE run_id = ? AND entity_type = ? AND entity_key = ?
        ORDER BY id
        """,
        (run_id, entity_type, entity_key),
    ).fetchall()
    audit_only: list[dict[str, Any]] = []
    source_bearing: list[dict[str, Any]] = []
    for row in rows:
        event_type = str(row["event_type"])
        entry = {
            "id": int(row["id"]),
            "event_type": event_type,
            "event_hash": str(row["event_hash"]),
            "occurred_at": str(row["occurred_at"]),
        }
        try:
            details = json.loads(str(row["details_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            details = None
        audit_kind = ""
        if isinstance(details, dict) and event_type == "planned":
            if set(details) <= {"plan_revision", "required"}:
                audit_kind = "audit_only_plan"
        elif isinstance(details, dict) and event_type == "started":
            if set(details) <= {"attempt_count"}:
                audit_kind = "audit_only_start"
        elif isinstance(details, dict) and event_type == "blocked":
            manifest_hash = details.get("manifest_hash")
            if (
                isinstance(manifest_hash, str)
                and manifest_hash in blocker_manifest_hashes
                and set(details)
                <= {"code", "retryable", "manifest_hash", "new_manifest"}
            ):
                audit_kind = "audit_only_blocker"
        elif isinstance(details, dict) and event_type in {"reopened", "invalidated"}:
            invalidation_hash = details.get("invalidation_hash")
            if (
                isinstance(invalidation_hash, str)
                and invalidation_hash in invalidation_manifest_hashes
                and set(details) <= {"invalidation_hash"}
            ):
                audit_kind = f"audit_only_{event_type}"
        if audit_kind and event_type in P1_AUDIT_ONLY_TRANSITION_EVENTS:
            entry["audit_kind"] = audit_kind
            audit_only.append(entry)
        else:
            entry["classification_reason"] = (
                "unknown_or_source_bearing_transition"
            )
            source_bearing.append(entry)
    return {"audit_only": audit_only, "source_bearing": source_bearing}


def _zero_evidence_recovery_summary(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
) -> dict[str, Any]:
    run_id = str(row["run_id"])
    stream_key = str(row["stream_key"])
    step_key = str(row["p1_step_key"])
    item_key = str(row["p1_item_key"])

    p1_table = "daily_run_work_items" if item_key else "daily_run_steps"
    p1_where = "run_id = ? AND step_key = ?" + (
        " AND item_key = ?" if item_key else ""
    )
    p1_row = conn.execute(
        f"""
        SELECT state, manifest_hash, evidence_hash, last_checkpoint_json, scope_json
        FROM {p1_table} WHERE {p1_where}
        """,
        (run_id, step_key, *([item_key] if item_key else [])),
    ).fetchone()
    expected_scope = (
        json.loads(str(p1_row["scope_json"])) if p1_row is not None else {}
    )
    manifest_taxonomy = _p1_manifest_evidence_taxonomy(
        conn,
        run_id=run_id,
        step_key=step_key,
        item_key=item_key,
        expected_scope=expected_scope,
    )
    blocker_manifests = [
        item
        for item in manifest_taxonomy["audit_only"]
        if item["audit_kind"] == "audit_only_blocker"
    ]
    invalidation_manifests = [
        item
        for item in manifest_taxonomy["audit_only"]
        if item["audit_kind"] == "audit_only_invalidation"
    ]
    entity_type = "work_item" if item_key else "step"
    entity_key = f"{step_key}/{item_key}" if item_key else step_key
    transition_taxonomy = _p1_transition_evidence_taxonomy(
        conn,
        run_id=run_id,
        entity_type=entity_type,
        entity_key=entity_key,
        blocker_manifest_hashes={
            str(item["payload_hash"]) for item in blocker_manifests
        },
        invalidation_manifest_hashes={
            str(item["payload_hash"]) for item in invalidation_manifests
        },
    )

    counts = {
        "hh_page_captures": int(
            conn.execute(
                """
                SELECT COUNT(*) FROM hh_page_captures
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (run_id, stream_key),
            ).fetchone()[0]
        ),
        "hh_page_items": int(
            conn.execute(
                """
                SELECT COUNT(*) FROM hh_page_items
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (run_id, stream_key),
            ).fetchone()[0]
        ),
        "hh_detail_queue": int(
            conn.execute(
                """
                SELECT COUNT(*) FROM hh_detail_queue
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (run_id, stream_key),
            ).fetchone()[0]
        ),
        "hh_vacancy_snapshots_current_run": int(
            conn.execute(
                """
                SELECT COUNT(DISTINCT snapshot.external_id)
                FROM hh_vacancy_snapshots snapshot
                WHERE snapshot.last_capture_hash IN (
                    SELECT capture_hash FROM hh_page_captures
                    WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                    UNION
                    SELECT detail_capture_hash FROM hh_detail_queue
                    WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                      AND COALESCE(detail_capture_hash, '') <> ''
                )
                """,
                (run_id, stream_key, run_id, stream_key),
            ).fetchone()[0]
        ),
        "current_run_checkpoint_history": int(
            conn.execute(
                """
                SELECT COUNT(*) FROM hh_stream_checkpoint_history
                WHERE source = 'hh' AND stream_key = ? AND run_id = ?
                """,
                (stream_key, run_id),
            ).fetchone()[0]
        ),
        "current_run_successful_checkpoint": int(
            conn.execute(
                """
                SELECT COUNT(*) FROM hh_stream_checkpoints
                WHERE source = 'hh' AND stream_key = ?
                  AND last_successful_run_id = ?
                """,
                (stream_key, run_id),
            ).fetchone()[0]
        ),
        "p1_source_manifests": len(manifest_taxonomy["source_bearing"]),
        "p1_source_progress_transitions": len(
            transition_taxonomy["source_bearing"]
        ),
    }
    placeholders = ",".join("?" for _ in ZERO_EVIDENCE_NON_SOURCE_EVENTS)
    counts["source_progress_events"] = int(
        conn.execute(
            f"""
            SELECT COUNT(*) FROM hh_incremental_events
            WHERE run_id = ? AND source = 'hh' AND stream_key = ?
              AND event_type NOT IN ({placeholders})
            """,
            (run_id, stream_key, *sorted(ZERO_EVIDENCE_NON_SOURCE_EVENTS)),
        ).fetchone()[0]
    )

    progress_fields: list[str] = []
    if str(row["state"]) != "planned":
        progress_fields.append(f"state={row['state']}")
    if int(row["next_page"]) != 0:
        progress_fields.append(f"next_page={row['next_page']}")
    if int(row["last_verified_page"]) != -1:
        progress_fields.append(f"last_verified_page={row['last_verified_page']}")
    if row["resume_from_mode"] not in (None, ""):
        progress_fields.append("resume_from_mode")
    for field in (
        "predicted_boundary_page",
        "boundary_candidate_page",
        "boundary_proven_page",
        "source_reported_count",
        "unresolved_drift_page",
    ):
        if row[field] is not None:
            progress_fields.append(field)
    for field in (
        "known_page_streak",
        "guard_pages_verified",
        "source_exhausted",
        "raw_count",
        "unique_count",
        "known_unchanged_count",
        "known_changed_count",
        "new_count",
        "duplicate_on_page_count",
        "duplicate_across_pages_count",
        "duplicate_across_streams_count",
    ):
        if int(row[field]) != 0:
            progress_fields.append(f"{field}={row[field]}")
    for field in (
        "session_id_state",
        "session_fingerprint",
        "newest_publication",
        "oldest_publication",
        "fallback_reason",
        "blocker_code",
        "blocker_reason",
        "completed_at",
    ):
        if row[field] not in (None, ""):
            progress_fields.append(field)
    completion_manifest_exists = row["completion_manifest_json"] not in (None, "")
    if completion_manifest_exists:
        progress_fields.append("completion_manifest_json")

    p1_progress_fields: list[str] = []
    runtime_blocker_verified = None
    runtime_blocker_audit: dict[str, Any] = {}
    if str(row["adapter_version"]) == "hh-dom-v1.0.1":
        blocker_entries = [
            item
            for item in manifest_taxonomy["audit_only"]
            if item.get("audit_kind") == "audit_only_blocker"
        ]
        classified_entries: list[tuple[dict[str, Any], str]] = []
        for entry in blocker_entries:
            blocker_kinds: set[str] = set()
            for blocker in entry.get("blockers", []):
                code = str(blocker.get("code", ""))
                reason = (
                    str(blocker.get("reason", ""))
                    .replace(" ", "")
                    .casefold()
                )
                if (
                    "mutationobserver" in reason
                    or code
                    in {
                        "hh_dom_runtime_missing_mutation_observer",
                        "hh_mutation_observer_unavailable",
                    }
                ):
                    blocker_kinds.add("missing_mutation_observer")
                elif code in ZERO_EVIDENCE_RECOVERY_BOOKKEEPING_BLOCKER_CODES:
                    blocker_kinds.add("recovery_bookkeeping")
                else:
                    blocker_kinds.add("other")
            if blocker_kinds == {"missing_mutation_observer"}:
                classification = "missing_mutation_observer"
            elif blocker_kinds and blocker_kinds <= {"recovery_bookkeeping"}:
                classification = "recovery_bookkeeping"
            else:
                classification = "other"
            classified_entries.append((entry, classification))
        relevant_entries = [
            (entry, classification)
            for entry, classification in classified_entries
            if classification != "recovery_bookkeeping"
        ]
        latest_relevant = relevant_entries[-1] if relevant_entries else None
        runtime_blocker_verified = bool(
            latest_relevant
            and latest_relevant[1] == "missing_mutation_observer"
        )
        runtime_blocker_audit = {
            "verified_manifest_hash": (
                str(latest_relevant[0]["payload_hash"])
                if runtime_blocker_verified and latest_relevant is not None
                else ""
            ),
            "ignored_recovery_bookkeeping_manifest_hashes": [
                str(entry["payload_hash"])
                for entry, classification in classified_entries
                if classification == "recovery_bookkeeping"
            ],
        }
        if not runtime_blocker_verified:
            p1_progress_fields.append(
                "v1.0.1_without_exclusive_missing_mutation_observer_blocker"
            )
    if p1_row is None:
        p1_progress_fields.append("missing_p1_target")
    else:
        if str(p1_row["state"]) not in {"pending", "in_progress"}:
            p1_progress_fields.append(f"state={p1_row['state']}")
        manifest_hash = str(p1_row["manifest_hash"] or "")
        audit_manifest_hashes = {
            str(item["payload_hash"]) for item in manifest_taxonomy["audit_only"]
        }
        if manifest_hash and manifest_hash not in audit_manifest_hashes:
            p1_progress_fields.append("manifest_hash")
        for field in ("evidence_hash", "last_checkpoint_json"):
            if p1_row[field] not in (None, "", "{}"):
                p1_progress_fields.append(field)

    historical = conn.execute(
        """
        SELECT checkpoint_version, last_successful_run_id, updated_at
        FROM hh_stream_checkpoints
        WHERE source = 'hh' AND stream_key = ?
        """,
        (stream_key,),
    ).fetchone()
    historical_checkpoint = (
        {
            "present": True,
            "checkpoint_version": int(historical["checkpoint_version"]),
            "last_successful_run_id": str(historical["last_successful_run_id"]),
            "updated_at": str(historical["updated_at"]),
        }
        if historical is not None
        and str(historical["last_successful_run_id"]) != run_id
        else {"present": False}
    )
    return {
        "counts": counts,
        "progress_fields": progress_fields,
        "p1_progress_fields": p1_progress_fields,
        "p1_audit_history": {
            "audit_only_manifest_count": len(manifest_taxonomy["audit_only"]),
            "audit_only_transition_count": len(
                transition_taxonomy["audit_only"]
            ),
            "superseded_blocker_manifest_hashes": [
                str(item["payload_hash"]) for item in blocker_manifests
            ],
            "superseded_invalidation_manifest_hashes": [
                str(item["payload_hash"]) for item in invalidation_manifests
            ],
            "superseded_transition_ids": [
                int(item["id"])
                for item in transition_taxonomy["audit_only"]
                if item["event_type"] in {"blocked", "reopened", "invalidated"}
            ],
            "audit_only_transition_ids": [
                int(item["id"]) for item in transition_taxonomy["audit_only"]
            ],
        },
        "historical_checkpoint": historical_checkpoint,
        "runtime_blocker_verified": runtime_blocker_verified,
        "runtime_blocker_audit": runtime_blocker_audit,
    }


def _require_zero_evidence_recovery(
    conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
) -> dict[str, Any]:
    summary = _zero_evidence_recovery_summary(conn, row=row)
    issues = [
        f"{name}={count}"
        for name, count in summary["counts"].items()
        if int(count) != 0
    ]
    issues.extend(str(item) for item in summary["progress_fields"])
    issues.extend(
        f"p1.{item}" for item in summary["p1_progress_fields"]
    )
    if issues:
        raise ValueError(
            "Recovery zero-evidence плана HH запрещён: обнаружены доказательства "
            "или прогресс источника (" + ", ".join(issues) + ")."
        )
    return summary


def invalidate_zero_evidence_plan(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    reason: str,
) -> dict[str, Any]:
    """Invalidate only a frozen HH plan that has no source evidence at all."""

    reason = _nonempty_string(reason, "reason", 1000)
    stream_key = _nonempty_string(stream_key, "stream_key", 256)
    run = conn.execute(
        "SELECT status FROM daily_runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if run is None or str(run["status"]) == "completed":
        raise ValueError("Точный незавершённый ежедневный запуск P1 не найден.")

    recovery = _zero_evidence_recovery_events(
        conn, run_id=run_id, stream_key=stream_key
    )
    row = conn.execute(
        """
        SELECT * FROM hh_stream_runs
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        pending = recovery["pending"]
        if pending is not None:
            return {
                **_recovery_event_result(
                    pending, idempotent=True, replan_required=True
                ),
                "changed": False,
            }
        raise ValueError("Текущий план потока HH не найден.")

    source_kind = str(row["source_kind"])
    _p1_target(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        source_kind=source_kind,
    )
    target_configuration_fingerprint = acquisition_configuration_fingerprint(
        settings,
        source_kind=source_kind,
        stream_key=stream_key,
        query_fingerprint=str(row["query_fingerprint"]),
    )
    already_current = (
        str(row["adapter_version"]) == ADAPTER_VERSION
        and str(row["configuration_fingerprint"])
        == target_configuration_fingerprint
    )
    if already_current:
        for replan in reversed(recovery["replans"]):
            details = replan["details"]
            invalidation_event_id = details.get("invalidation_event_id")
            invalidation = recovery["invalidations"].get(invalidation_event_id)
            if (
                invalidation is not None
                and details.get("target_adapter_version") == ADAPTER_VERSION
                and details.get("target_configuration_fingerprint")
                == target_configuration_fingerprint
                and details.get("query_fingerprint") == row["query_fingerprint"]
                and details.get("source_kind") == source_kind
            ):
                _require_zero_evidence_recovery(conn, row=row)
                return {
                    **_recovery_event_result(
                        invalidation, idempotent=True, replan_required=False
                    ),
                    "replanned": True,
                    "changed": False,
                }
        raise ValueError(
            "Текущий план HH уже соответствует текущему адаптеру и конфигурации; "
            "recovery не требуется."
        )

    pending = recovery["pending"]
    if pending is not None:
        details = pending["details"]
        if (
            details.get("previous_adapter_version")
            != str(row["adapter_version"])
            or details.get("previous_configuration_fingerprint")
            != str(row["configuration_fingerprint"])
            or details.get("query_fingerprint") != str(row["query_fingerprint"])
            or details.get("source_kind") != source_kind
        ):
            raise RuntimeError(
                "Незавершённое recovery-событие не совпадает с текущей строкой плана HH."
            )
        return {
            **_recovery_event_result(
                pending, idempotent=True, replan_required=True
            ),
            "changed": False,
        }

    summary = _require_zero_evidence_recovery(conn, row=row)
    timestamp = now_iso()
    details = {
        "run_id": run_id,
        "stream_key": stream_key,
        "source_kind": source_kind,
        "query_fingerprint": str(row["query_fingerprint"]),
        "previous_adapter_version": str(row["adapter_version"]),
        "previous_configuration_fingerprint": str(
            row["configuration_fingerprint"]
        ),
        "previous_requested_mode": str(row["requested_mode"]),
        "previous_effective_mode": str(row["effective_mode"]),
        "previous_plan_created_at": str(row["created_at"]),
        "target_adapter_version": ADAPTER_VERSION,
        "target_configuration_fingerprint": target_configuration_fingerprint,
        "operator_reason": reason,
        "invalidated_at": timestamp,
        "no_source_evidence_discarded": True,
        "source_evidence_discarded": False,
        "superseded_p1_audit": summary["p1_audit_history"],
        "verified_zero_evidence": {
            "state": str(row["state"]),
            "next_page": int(row["next_page"]),
            "last_verified_page": int(row["last_verified_page"]),
            "completion_manifest_present": False,
            "table_and_event_counts": summary["counts"],
            "source_progress_fields": summary["progress_fields"],
            "p1_progress_fields": summary["p1_progress_fields"],
            "p1_audit_history": summary["p1_audit_history"],
            "runtime_blocker_verified": summary["runtime_blocker_verified"],
            "runtime_blocker_audit": summary["runtime_blocker_audit"],
        },
        "historical_checkpoint_preserved": summary["historical_checkpoint"],
    }
    if not _append_event(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        event_type=ZERO_EVIDENCE_INVALIDATION_EVENT,
        severity="warning",
        details=details,
    ):
        raise RuntimeError("Не удалось сохранить audit-событие recovery HH.")
    recovery = _zero_evidence_recovery_events(
        conn, run_id=run_id, stream_key=stream_key
    )
    pending = recovery["pending"]
    if pending is None:
        raise RuntimeError("Audit-событие recovery HH не найдено после записи.")

    cursor = conn.execute(
        """
        DELETE FROM hh_stream_runs
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
          AND state = 'planned' AND next_page = 0 AND last_verified_page = -1
        """,
        (run_id, stream_key),
    )
    if cursor.rowcount != 1:
        raise RuntimeError(
            "Пустая строка плана HH изменилась во время recovery; операция отменена."
        )
    return {
        **_recovery_event_result(
            pending, idempotent=False, replan_required=True
        ),
        "changed": True,
    }


def _checkpoint_is_stale(row: sqlite3.Row, *, run_date: str, days: int) -> bool:
    previous = dt.date.fromisoformat(str(row["last_successful_date"]))
    current = dt.date.fromisoformat(run_date)
    return (current - previous).days > days or current < previous


def _audit_due(row: sqlite3.Row, *, run_date: str, interval_days: int) -> bool:
    raw = str(row["last_audit_scan_at"] or row["last_full_scan_at"] or "")
    if not raw:
        return True
    previous = dt.datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    current = dt.date.fromisoformat(run_date)
    return (current - previous).days >= interval_days or current < previous


def build_acquisition_plan(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    source_kind: str,
    query_fingerprint: str,
) -> dict[str, Any]:
    """Persist and return the deterministic safe mode for one exact P1 stream."""

    source_kind = _nonempty_string(source_kind, "source_kind", 64)
    if source_kind not in SOURCE_KINDS:
        raise ValueError("source_kind должен быть ordinary_search или personal_recommendations.")
    stream_key = _nonempty_string(stream_key, "stream_key", 256)
    query_fingerprint = _nonempty_string(query_fingerprint, "query_fingerprint", 64).lower()
    if not HEX_64_RE.fullmatch(query_fingerprint):
        raise ValueError("query_fingerprint должен быть SHA-256 в нижнем регистре.")
    run = conn.execute("SELECT * FROM daily_runs WHERE run_id = ?", (run_id,)).fetchone()
    if run is None or str(run["status"]) == "completed":
        raise ValueError("Точный незавершённый ежедневный запуск P1 не найден.")
    step_key, item_key, _ = _p1_target(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        source_kind=source_kind,
    )
    config_fingerprint = acquisition_configuration_fingerprint(
        settings,
        source_kind=source_kind,
        stream_key=stream_key,
        query_fingerprint=query_fingerprint,
    )
    existing = conn.execute(
        """
        SELECT * FROM hh_stream_runs
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (run_id, stream_key),
    ).fetchone()
    if existing is not None:
        if (
            str(existing["query_fingerprint"]) != query_fingerprint
            or str(existing["configuration_fingerprint"]) != config_fingerprint
            or str(existing["source_kind"]) != source_kind
        ):
            raise ValueError(
                "Параметры уже начатого сбора не совпадают; сначала явно инвалидируйте работу."
            )
        if str(existing["state"]) != "completed" and int(existing["last_verified_page"]) >= 0:
            mode = "resume"
            conn.execute(
                """
                UPDATE hh_stream_runs SET requested_mode = 'resume', effective_mode = 'resume',
                    resume_from_mode = COALESCE(resume_from_mode, ?), updated_at = ?
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (
                    str(existing["effective_mode"]),
                    now_iso(),
                    run_id,
                    stream_key,
                ),
            )
            return _plan_result(conn, run_id=run_id, stream_key=stream_key, mode=mode)
        return _plan_result(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            mode=str(existing["effective_mode"]),
        )

    recovery_state = _zero_evidence_recovery_events(
        conn, run_id=run_id, stream_key=stream_key
    )
    recovery = recovery_state["pending"]
    if recovery is None and recovery_state["invalidations"]:
        raise RuntimeError(
            "Audit recovery уже содержит событие перепланирования, но текущая строка "
            "плана HH отсутствует; требуется ручная проверка, новый план не создан."
        )
    if recovery is not None:
        recovery_details = recovery["details"]
        if (
            recovery_details.get("source_kind") != source_kind
            or recovery_details.get("query_fingerprint") != query_fingerprint
        ):
            raise ValueError(
                "Recovery zero-evidence разрешает только прежние source_kind и "
                "query_fingerprint; изменение запроса требует отдельного решения."
            )
        if (
            recovery_details.get("target_adapter_version") != ADAPTER_VERSION
            or recovery_details.get("target_configuration_fingerprint")
            != config_fingerprint
            or recovery_details.get("no_source_evidence_discarded") is not True
        ):
            raise ValueError(
                "Audit recovery не соответствует текущему адаптеру или конфигурации; "
                "перепланирование остановлено."
            )

    checkpoint = conn.execute(
        "SELECT * FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
        (stream_key,),
    ).fetchone()
    cfg = settings.search.hh_acquisition
    reason = ""
    mode = "full"
    eligibility: dict[str, bool] = {
        "checkpoint_present": checkpoint is not None,
        "completion_boundary_valid": False,
        "query_unchanged": False,
        "configuration_unchanged": False,
        "checkpoint_fresh": False,
        "ordering_compatible": False,
        "anomaly_free": False,
        "shadow_requirement_met": False,
    }
    if checkpoint is None:
        reason = "initial_scan_without_checkpoint"
    else:
        cursor = json.loads(str(checkpoint["cursor_json"]))
        eligibility.update(
            {
                "completion_boundary_valid": bool(cursor.get("valid_completion_boundary")),
                "query_unchanged": str(checkpoint["query_fingerprint"]) == query_fingerprint,
                "configuration_unchanged": str(checkpoint["configuration_fingerprint"])
                == config_fingerprint,
                "checkpoint_fresh": not _checkpoint_is_stale(
                    checkpoint,
                    run_date=str(run["run_date"]),
                    days=cfg.checkpoint_staleness_days,
                ),
                "ordering_compatible": bool(cursor.get("ordering_compatible")),
                "anomaly_free": not str(checkpoint["anomaly_state"] or "")
                and not str(checkpoint["invalidated_at"] or ""),
                "shadow_requirement_met": int(checkpoint["shadow_clean_runs"])
                >= cfg.shadow_runs_required,
            }
        )
        fundamental_gates = {
            key: eligibility[key]
            for key in (
                "completion_boundary_valid",
                "query_unchanged",
                "configuration_unchanged",
                "checkpoint_fresh",
                "ordering_compatible",
                "anomaly_free",
            )
        }
        if cfg.incremental_mode == "disabled":
            mode = "full"
            reason = "incremental_disabled"
        elif not all(fundamental_gates.values()):
            mode = "shadow"
            failed = sorted(key for key, ok in fundamental_gates.items() if not ok)
            reason = "incremental_ineligible:" + ",".join(failed)
        elif (
            str(checkpoint["eligibility_state"]) == "eligible"
            and _audit_due(
            checkpoint,
            run_date=str(run["run_date"]),
            interval_days=cfg.full_audit_interval_days,
            )
        ):
            mode = "audit"
            reason = "periodic_full_audit_due"
        elif cfg.incremental_mode == "shadow":
            mode = "shadow"
            reason = "public_shadow_mode"
        elif all(eligibility.values()) and str(checkpoint["eligibility_state"]) == "eligible":
            mode = "delta"
            reason = "all_incremental_safety_gates_passed"
        else:
            mode = "shadow"
            failed = sorted(key for key, ok in eligibility.items() if not ok)
            reason = "incremental_ineligible:" + ",".join(failed)

    timestamp = now_iso()
    conn.execute(
        """
        INSERT INTO hh_stream_runs (
            run_id, source, stream_key, source_kind, p1_step_key, p1_item_key,
            requested_mode, effective_mode, query_fingerprint,
            configuration_fingerprint, adapter_version, state, created_at, updated_at
        ) VALUES (?, 'hh', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?)
        """,
        (
            run_id,
            stream_key,
            source_kind,
            step_key,
            item_key,
            mode,
            mode,
            query_fingerprint,
            config_fingerprint,
            ADAPTER_VERSION,
            timestamp,
            timestamp,
        ),
    )
    _append_event(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        event_type="acquisition_planned",
        severity="info",
        details={"mode": mode, "reason": reason, "eligibility": eligibility},
    )
    result = _plan_result(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        mode=mode,
        reason=reason,
        eligibility=eligibility,
    )
    if recovery is not None:
        recovery_details = recovery["details"]
        _append_event(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            event_type=ZERO_EVIDENCE_REPLAN_EVENT,
            severity="info",
            details={
                "run_id": run_id,
                "stream_key": stream_key,
                "source_kind": source_kind,
                "query_fingerprint": query_fingerprint,
                "invalidation_event_id": int(recovery["row"]["id"]),
                "previous_adapter_version": recovery_details[
                    "previous_adapter_version"
                ],
                "previous_configuration_fingerprint": recovery_details[
                    "previous_configuration_fingerprint"
                ],
                "target_adapter_version": ADAPTER_VERSION,
                "target_configuration_fingerprint": config_fingerprint,
                "operator_reason": recovery_details["operator_reason"],
                "replanned_at": timestamp,
                "no_source_evidence_discarded": True,
            },
        )
        result["recovery"] = {
            "invalidation_event_id": int(recovery["row"]["id"]),
            "previous_adapter_version": recovery_details[
                "previous_adapter_version"
            ],
            "previous_configuration_fingerprint": recovery_details[
                "previous_configuration_fingerprint"
            ],
            "no_source_evidence_discarded": True,
        }
    return result


def _plan_result(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    mode: str,
    reason: str = "",
    eligibility: Mapping[str, bool] | None = None,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        raise RuntimeError("Внутренняя ошибка: план сбора не сохранён.")
    return {
        "run_id": run_id,
        "stream_key": stream_key,
        "source_kind": row["source_kind"],
        "acquisition_mode": mode,
        "reason": reason,
        "eligibility": dict(eligibility or {}),
        "adapter_version": row["adapter_version"],
        "query_fingerprint": row["query_fingerprint"],
        "next_page": int(row["next_page"]),
        "last_verified_page": int(row["last_verified_page"]),
        "p1": {"step_key": row["p1_step_key"], "item_key": row["p1_item_key"]},
        "next_safe_action": (
            "stream_complete" if str(row["state"]) == "completed" else "capture_stable_page"
        ),
    }


def _normalize_session(raw: Any, *, source_kind: str) -> dict[str, str]:
    session = _json_object(raw, "session")
    state = _nonempty_string(session.get("session_id_state"), "session.session_id_state", 32)
    if state not in SESSION_STATES:
        raise ValueError("session.session_id_state должен быть exposed или not_exposed.")
    evidence = _json_list(session.get("evidence", []), "session.evidence")
    if not evidence or not all(isinstance(item, str) and item.strip() for item in evidence):
        raise ValueError("session.evidence должен содержать видимое доказательство доступности ID сессии.")
    if state == "exposed":
        raw_id = _nonempty_string(
            session.get("search_session_id"), "session.search_session_id", 512
        )
        fingerprint = sha256_text("hh-search-session:v1:" + raw_id)
    else:
        if session.get("search_session_id") not in (None, ""):
            raise ValueError(
                "При session_id_state=not_exposed поле search_session_id должно отсутствовать."
            )
        fingerprint = _nonempty_string(
            session.get("alternative_capture_session_fingerprint"),
            "session.alternative_capture_session_fingerprint",
            64,
        ).lower()
        if not HEX_64_RE.fullmatch(fingerprint):
            raise ValueError(
                "Для not_exposed требуется детерминированный alternative fingerprint SHA-256."
            )
    return {
        "session_id_state": state,
        "session_fingerprint": fingerprint,
        "evidence_hash": payload_hash(evidence),
        "source_kind": source_kind,
    }


def _session_id_matches_visible_url(payload: Mapping[str, Any], canonical_url: str) -> bool:
    raw_session = payload.get("session")
    if not isinstance(raw_session, Mapping):
        return False
    if raw_session.get("session_id_state") != "exposed":
        return False
    raw_id = raw_session.get("search_session_id")
    if not isinstance(raw_id, str) or not raw_id:
        return False
    return any(
        key in {"searchSessionId", "search_session_id"} and value == raw_id
        for key, value in parse_qsl(urlsplit(canonical_url).query, keep_blank_values=True)
    )


def _is_visible_navigation_session_enrichment(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    page_index: int,
    current_session_state: str,
    current_session_fingerprint: str,
    payload: Mapping[str, Any],
    normalized: Mapping[str, Any],
) -> bool:
    if (
        page_index <= 0
        or current_session_state != "not_exposed"
        or normalized["session"]["session_id_state"] != "exposed"
        or not _session_id_matches_visible_url(payload, str(normalized["canonical_url"]))
    ):
        return False
    previous = conn.execute(
        """
        SELECT navigation_json, session_id_state, session_fingerprint
        FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
          AND page_index = ? AND verified = 1
        ORDER BY recapture_no DESC LIMIT 1
        """,
        (run_id, stream_key, page_index - 1),
    ).fetchone()
    if previous is None:
        return False
    if (
        str(previous["session_id_state"]) != current_session_state
        or str(previous["session_fingerprint"]) != current_session_fingerprint
    ):
        return False
    navigation = json.loads(str(previous["navigation_json"] or "{}"))
    next_page = navigation.get("next", {})
    return bool(
        next_page.get("present")
        and next_page.get("page_index") == page_index
        and next_page.get("url_hash") == normalized["canonical_url_hash"]
    )


def _normalize_blocker(raw: Any) -> dict[str, Any]:
    blocker = _json_object(raw, "blocker")
    kind = _nonempty_string(blocker.get("type", "none"), "blocker.type", 64)
    if kind not in BLOCKER_TYPES:
        raise ValueError("blocker.type содержит неподдерживаемый вид блокировки.")
    evidence = _json_list(blocker.get("evidence", []), "blocker.evidence")
    if not all(isinstance(item, str) for item in evidence):
        raise ValueError("blocker.evidence должен быть массивом строк.")
    if kind != "none" and not any(item.strip() for item in evidence):
        raise ValueError("Для блокировки требуется точное видимое доказательство.")
    return {"type": kind, "evidence": [item.strip()[:512] for item in evidence if item.strip()]}


def _normalize_navigation(raw: Any, *, page_index: int) -> dict[str, Any]:
    navigation = _json_object(raw, "navigation")
    if navigation.get("consistent") is not True:
        raise ValueError("navigation.consistent должен явно подтверждать согласованность пагинации.")
    result: dict[str, Any] = {"consistent": True}
    for direction in ("previous", "next"):
        item = _json_object(navigation.get(direction, {}), f"navigation.{direction}")
        present = item.get("present")
        if not isinstance(present, bool):
            raise ValueError(f"navigation.{direction}.present должен быть boolean.")
        page = item.get("page_index")
        if page is not None:
            page = _nonnegative_int(page, f"navigation.{direction}.page_index")
        url_hash = ""
        if present:
            url_hash = sha256_text(_canonical_url(item.get("url")))
            expected = page_index - 1 if direction == "previous" else page_index + 1
            if page is not None and page != expected:
                raise ValueError(
                    f"navigation.{direction}.page_index не согласован с текущей страницей."
                )
        elif item.get("url") not in (None, ""):
            raise ValueError(f"navigation.{direction}.url не должен быть заполнен без ссылки.")
        result[direction] = {"present": present, "page_index": page, "url_hash": url_hash}
    if page_index == 0 and result["previous"]["present"]:
        raise ValueError("Первая страница не может содержать доказательство предыдущей страницы.")
    return result


def _normalize_ordering(raw: Any) -> dict[str, Any]:
    ordering = _json_object(raw, "ordering")
    kind = _nonempty_string(ordering.get("kind"), "ordering.kind", 64)
    monotonic = ordering.get("monotonic")
    if not isinstance(monotonic, bool):
        raise ValueError("ordering.monotonic должен быть логическим значением.")
    return {
        "kind": kind,
        "monotonic": monotonic,
        "newest_publication": _optional_string(
            ordering.get("newest_publication"), "ordering.newest_publication", 256
        ),
        "oldest_publication": _optional_string(
            ordering.get("oldest_publication"), "ordering.oldest_publication", 256
        ),
        "evidence": _optional_string(ordering.get("evidence"), "ordering.evidence", 1024),
    }


def _normalize_stability(
    raw: Any,
    *,
    final_id_hash: str,
    final_ordered_ids: Sequence[str],
    required_samples: int,
    required_interval_ms: int,
    required_timeout_ms: int,
) -> dict[str, Any]:
    stability = _json_object(raw, "stability")
    method = _nonempty_string(
        stability.get("stability_method"), "stability.stability_method", 64
    )
    if method not in {
        "mutation_observer_visible_dom",
        "timed_visible_dom_sampling",
    }:
        raise ValueError("stability.stability_method содержит неподдерживаемый метод.")
    observer_available = stability.get("mutation_observer_available")
    if not isinstance(observer_available, bool):
        raise ValueError("stability.mutation_observer_available должен быть boolean.")
    if observer_available != (method == "mutation_observer_visible_dom"):
        raise ValueError("Метод стабильности не совпадает с доступностью MutationObserver.")
    if stability.get("adapter_version") != ADAPTER_VERSION:
        raise ValueError(
            f"stability.adapter_version должен быть равен {ADAPTER_VERSION}."
        )
    configured_samples = _nonnegative_int(
        stability.get("required_stable_sample_count"),
        "stability.required_stable_sample_count",
    )
    if configured_samples != required_samples:
        raise ValueError(
            "required_stable_sample_count не совпадает с зафиксированной конфигурацией."
        )
    interval_ms = _nonnegative_int(
        stability.get("sampling_interval_ms"), "stability.sampling_interval_ms"
    )
    timeout_ms = _nonnegative_int(
        stability.get("timeout_ms"), "stability.timeout_ms"
    )
    if interval_ms != required_interval_ms or timeout_ms != required_timeout_ms:
        raise ValueError(
            "Интервал или timeout стабильности не совпадает с зафиксированной конфигурацией."
        )
    samples = _json_list(stability.get("samples"), "stability.samples")
    actual_sample_count = _nonnegative_int(
        stability.get("actual_sample_count"), "stability.actual_sample_count"
    )
    if actual_sample_count != len(samples):
        raise ValueError("actual_sample_count не совпадает с массивом samples.")
    if len(samples) < required_samples + 1:
        raise ValueError(
            f"stability.samples: требуется {required_samples} устойчивых снимка и отдельная финальная проверка."
        )
    normalized_samples: list[dict[str, Any]] = []
    previous_offset = -1
    for index, raw_sample in enumerate(samples):
        sample = _json_object(raw_sample, f"stability.samples[{index}]")
        sample_index = _nonnegative_int(
            sample.get("sample_index"), f"stability.samples[{index}].sample_index"
        )
        if sample_index != index:
            raise ValueError("Индексы stability.samples должны быть непрерывными с нуля.")
        sampled_at = _parse_iso(
            sample.get("sampled_at"), f"stability.samples[{index}].sampled_at"
        )
        relative_offset_ms = _nonnegative_int(
            sample.get("relative_offset_ms"),
            f"stability.samples[{index}].relative_offset_ms",
        )
        if relative_offset_ms < previous_offset:
            raise ValueError("relative_offset_ms должен быть монотонным.")
        previous_offset = relative_offset_ms
        ordered_ids = _json_list(
            sample.get("canonical_ordered_ids"),
            f"stability.samples[{index}].canonical_ordered_ids",
        )
        if not all(
            isinstance(value, str) and re.fullmatch(r"hh:[0-9]{1,32}", value)
            for value in ordered_ids
        ):
            raise ValueError("canonical_ordered_ids должен содержать канонические ID HH.")
        ordered_hash = _nonempty_string(
            sample.get("canonical_ordered_id_hash"),
            f"stability.samples[{index}].canonical_ordered_id_hash",
            64,
        ).lower()
        if ordered_hash != payload_hash(ordered_ids):
            raise ValueError("canonical_ordered_id_hash не совпадает с ordered IDs.")
        set_hash = _nonempty_string(
            sample.get("canonical_id_set_hash"),
            f"stability.samples[{index}].canonical_id_set_hash",
            64,
        ).lower()
        if set_hash != payload_hash(sorted(set(ordered_ids))):
            raise ValueError("canonical_id_set_hash не совпадает с ordered IDs.")
        visible_card_count = _nonnegative_int(
            sample.get("visible_card_count"),
            f"stability.samples[{index}].visible_card_count",
        )
        if visible_card_count != len(ordered_ids):
            raise ValueError("visible_card_count не совпадает с ordered IDs.")
        height = _nonnegative_int(
            sample.get("scroll_height"), f"stability.samples[{index}].scroll_height"
        )
        scroll_position = _nonnegative_int(
            sample.get("scroll_position"),
            f"stability.samples[{index}].scroll_position",
        )
        maximum_position = sample.get("maximum_observed_card_position")
        if maximum_position is not None:
            maximum_position = _nonnegative_int(
                maximum_position,
                f"stability.samples[{index}].maximum_observed_card_position",
            )
        loader_active = sample.get("loader_active")
        if not isinstance(loader_active, bool):
            raise ValueError("Снимок стабильности требует логическое поле loader_active.")
        normalized_samples.append(
            {
                "sample_index": sample_index,
                "sampled_at": sampled_at,
                "relative_offset_ms": relative_offset_ms,
                "canonical_ordered_ids": list(ordered_ids),
                "canonical_ordered_id_hash": ordered_hash,
                "canonical_id_set_hash": set_hash,
                "visible_card_count": visible_card_count,
                "scroll_height": height,
                "scroll_position": scroll_position,
                "maximum_observed_card_position": maximum_position,
                "loader_active": loader_active,
                "mutation_count": _nonnegative_int(
                    sample.get("mutation_count", 0),
                    f"stability.samples[{index}].mutation_count",
                ),
            }
        )
    stable_indexes = _json_list(
        stability.get("stable_window_sample_indexes"),
        "stability.stable_window_sample_indexes",
    )
    if len(stable_indexes) != required_samples or not all(
        isinstance(value, int) and not isinstance(value, bool) for value in stable_indexes
    ):
        raise ValueError("stable_window_sample_indexes должен точно описывать устойчивое окно.")
    if stable_indexes != list(
        range(stable_indexes[0], stable_indexes[0] + required_samples)
    ):
        raise ValueError("Устойчивые samples должны быть последовательными.")
    if stable_indexes[-1] >= len(normalized_samples) - 1:
        raise ValueError("Финальная проверка должна быть отдельной от устойчивого окна.")
    stable_samples = [normalized_samples[index] for index in stable_indexes]
    stable_signature = {
        (
            sample["canonical_ordered_id_hash"],
            sample["canonical_id_set_hash"],
            sample["visible_card_count"],
            sample["scroll_height"],
            sample["loader_active"],
        )
        for sample in stable_samples
    }
    if len(stable_signature) != 1 or any(
        sample["loader_active"] for sample in stable_samples
    ):
        raise ValueError("Устойчивое окно содержит различающиеся DOM samples или loader.")
    final_verification = _json_object(
        stability.get("final_verification"), "stability.final_verification"
    )
    if final_verification.get("performed") is not True or final_verification.get(
        "matched"
    ) is not True:
        raise ValueError("Финальная независимая проверка стабильности не подтверждена.")
    final_index = _nonnegative_int(
        final_verification.get("sample_index"),
        "stability.final_verification.sample_index",
    )
    if final_index != len(normalized_samples) - 1:
        raise ValueError("Финальная проверка должна ссылаться на последний sample.")
    final_sample = normalized_samples[final_index]
    final_signature = (
        final_sample["canonical_ordered_id_hash"],
        final_sample["canonical_id_set_hash"],
        final_sample["visible_card_count"],
        final_sample["scroll_height"],
        final_sample["loader_active"],
    )
    if final_signature != next(iter(stable_signature)):
        raise ValueError("Финальная проверка отличается от устойчивого окна.")
    if (
        final_sample["canonical_id_set_hash"] != final_id_hash
        or final_sample["canonical_ordered_ids"] != list(final_ordered_ids)
    ):
        raise ValueError("Финальная проверка не совпадает с сохранёнными карточками.")
    if stability.get("bottom_scroll_attempted") is not True:
        raise ValueError("Протокол стабильности требует дополнительную прокрутку вниз.")
    observer_evidence = stability.get("observer_mutation_evidence_available")
    if observer_evidence is not observer_available:
        raise ValueError("Доказательство мутаций не совпадает с методом стабильности.")
    no_mutation = stability.get("no_relevant_dom_mutation_after_bottom")
    observer_mutation_count = final_verification.get("observer_mutation_count")
    if observer_available:
        if no_mutation is not True or observer_mutation_count != 0:
            raise ValueError("Observer-путь требует нулевую релевантную мутацию после прокрутки.")
    elif no_mutation is not None or observer_mutation_count is not None:
        raise ValueError("Timer sampling не должен выдавать observer evidence.")
    end_evidence = stability.get("end_of_list_evidence") is True
    return {
        "stability_method": method,
        "mutation_observer_available": observer_available,
        "adapter_version": ADAPTER_VERSION,
        "results_root_selector": _nonempty_string(
            stability.get("results_root_selector"),
            "stability.results_root_selector",
            256,
        ),
        "sample_count": len(samples),
        "required_sample_count": required_samples,
        "actual_sample_count": actual_sample_count,
        "sampling_interval_ms": interval_ms,
        "timeout_ms": timeout_ms,
        "samples": normalized_samples,
        "stable_window_sample_indexes": stable_indexes,
        "stable_scroll_height": True,
        "final_verification": {
            "performed": True,
            "matched": True,
            "sample_index": final_index,
            "observer_mutation_count": observer_mutation_count,
        },
        "bottom_scroll_attempted": True,
        "observer_mutation_evidence_available": observer_available,
        "no_relevant_dom_mutation_after_bottom": no_mutation,
        "end_of_list_evidence": end_evidence,
    }


def _normalize_card(raw: Any, *, index: int) -> dict[str, Any]:
    card = _json_object(raw, f"cards[{index}]")
    raw_id, external_id = canonical_external_id(card.get("vacancy_id"))
    canonical_url = _canonical_url(card.get("canonical_url"), vacancy_id=raw_id)
    position_value = card.get("position")
    if isinstance(position_value, int) and not isinstance(position_value, bool):
        position = str(position_value)
    else:
        position = _optional_string(position_value, f"cards[{index}].position", 256)
    result = {
        "vacancy_id": raw_id,
        "external_id": external_id,
        "canonical_url": canonical_url,
        "title": _optional_string(card.get("title"), f"cards[{index}].title", 1024),
        "company": _optional_string(card.get("company"), f"cards[{index}].company", 1024),
        "position": position,
        "publication_evidence": _optional_string(
            card.get("publication_evidence"),
            f"cards[{index}].publication_evidence",
            1024,
        ),
        "promoted": bool(card.get("promoted", False)),
        "pinned": bool(card.get("pinned", False)),
    }
    for flag in ("promoted", "pinned"):
        if card.get(flag, False) not in (True, False):
            raise ValueError(f"cards[{index}].{flag} должен быть boolean.")
    result["material_fingerprint"] = list_material_fingerprint(result)
    return result


def _expected_page_count(
    source_reported_count: int | None, *, page_index: int, page_size: int
) -> int | None:
    if source_reported_count is None:
        return None
    remaining = source_reported_count - page_index * page_size
    return max(0, min(page_size, remaining))


def validate_page_capture(
    payload: Mapping[str, Any],
    settings: Settings,
    *,
    expected_source_kind: str | None = None,
    expected_query_fingerprint: str | None = None,
    expected_page_index: int | None = None,
) -> dict[str, Any]:
    """Validate one adapter payload without relying on a fixed card count."""

    if not isinstance(payload, Mapping):
        raise ValueError("Снимок DOM должен быть объектом JSON.")
    if len(canonical_json(payload).encode("utf-8")) > MAX_CAPTURE_BYTES:
        raise ValueError("Снимок DOM превышает безопасный предел 1 МБ.")
    _walk_forbidden_keys(payload)
    if payload.get("capture_contract") != PAGE_CAPTURE_KIND:
        raise ValueError(f"capture_contract должен быть {PAGE_CAPTURE_KIND}.")
    if payload.get("contract_version") != CAPTURE_CONTRACT_VERSION:
        raise ValueError("Неподдерживаемая версия контракта снимка страницы.")
    adapter_version = _nonempty_string(payload.get("adapter_version"), "adapter_version", 64)
    if adapter_version != ADAPTER_VERSION:
        raise ValueError(
            f"Требуется adapter_version={ADAPTER_VERSION}; получено {adapter_version}."
        )
    source_kind = _nonempty_string(payload.get("source_kind"), "source_kind", 64)
    if source_kind not in SOURCE_KINDS:
        raise ValueError("Неподдерживаемый source_kind.")
    if expected_source_kind is not None and source_kind != expected_source_kind:
        raise ValueError("source_kind снимка не совпадает с планом сбора.")
    canonical_url = _canonical_url(payload.get("canonical_url"))
    query_fingerprint = _nonempty_string(
        payload.get("query_fingerprint"), "query_fingerprint", 64
    ).lower()
    if not HEX_64_RE.fullmatch(query_fingerprint):
        raise ValueError("query_fingerprint должен быть SHA-256.")
    if expected_query_fingerprint is not None and query_fingerprint != expected_query_fingerprint:
        raise ValueError("query_fingerprint снимка не совпадает с зафиксированным планом.")
    page_index = _nonnegative_int(payload.get("page_index"), "page_index")
    if expected_page_index is not None and page_index != expected_page_index:
        raise ValueError(
            f"Ожидалась страница {expected_page_index}, снимок относится к {page_index}."
        )
    captured_at = _parse_iso(payload.get("captured_at"), "captured_at")
    raw_cards = _json_list(payload.get("cards"), "cards")
    if len(raw_cards) > MAX_PAGE_CARDS:
        raise ValueError(f"Снимок содержит больше {MAX_PAGE_CARDS} карточек.")
    cards = [_normalize_card(card, index=index) for index, card in enumerate(raw_cards)]
    ordered_ids = [str(card["external_id"]) for card in cards]
    canonical_ids = sorted({str(card["external_id"]) for card in cards})
    id_set_hash = payload_hash(canonical_ids)
    supplied_hash = _nonempty_string(
        payload.get("canonical_id_set_hash"), "canonical_id_set_hash", 64
    ).lower()
    if supplied_hash != id_set_hash:
        raise ValueError("canonical_id_set_hash не совпадает с извлечёнными IDs.")
    blocker = _normalize_blocker(payload.get("blocker", {"type": "none", "evidence": []}))
    loader = _json_object(payload.get("loader"), "loader")
    if not isinstance(loader.get("active"), bool):
        raise ValueError("loader.active должен быть логическим значением.")
    if loader.get("active") and blocker["type"] == "none":
        blocker = {
            "type": "loading_timeout",
            "evidence": ["индикатор загрузки остался активным"],
        }
    navigation = _normalize_navigation(payload.get("navigation"), page_index=page_index)
    if source_kind == "ordinary_search" and page_index > 0 and not navigation["previous"]["present"]:
        raise ValueError(
            "Обычная поисковая выдача HH после первой страницы требует доказательство предыдущей страницы."
        )
    ordering = _normalize_ordering(payload.get("ordering"))
    session = _normalize_session(payload.get("session"), source_kind=source_kind)
    warnings = _json_list(payload.get("warnings", []), "warnings")
    if not all(isinstance(item, (str, dict)) for item in warnings):
        raise ValueError("warnings должен быть массивом строк или объектов.")
    normalized_warnings = [
        item.strip()[:1024] if isinstance(item, str) else dict(item) for item in warnings
    ]
    source_reported = payload.get("source_reported_result_count")
    if source_reported is not None:
        source_reported = _nonnegative_int(
            source_reported, "source_reported_result_count"
        )
    stability_error = ""
    try:
        stability = _normalize_stability(
            payload.get("stability"),
            final_id_hash=id_set_hash,
            final_ordered_ids=ordered_ids,
            required_samples=settings.search.hh_acquisition.page_stability_samples,
            required_interval_ms=settings.search.hh_acquisition.page_stability_delay_ms,
            required_timeout_ms=settings.search.hh_acquisition.page_stability_timeout_ms,
        )
    except ValueError as exc:
        stability_error = str(exc)
        stability = {
            "sample_count": len(
                payload.get("stability", {}).get("samples", [])
                if isinstance(payload.get("stability"), dict)
                else []
            ),
            "stable": False,
        }
    expected_count = _expected_page_count(
        source_reported,
        page_index=page_index,
        page_size=settings.search.items_per_page,
    )
    count_drift = expected_count is not None and len(canonical_ids) != expected_count
    if count_drift:
        normalized_warnings.append(
            {
                "code": "source_reported_count_drift",
                "source_expected_page_count": expected_count,
                "canonical_unique_count": len(canonical_ids),
            }
        )
    blockers: list[dict[str, Any]] = []
    if blocker["type"] != "none":
        blockers.append(
            {
                "code": f"source_{blocker['type']}",
                "reason": "; ".join(blocker["evidence"]) or blocker["type"],
                "retryable": blocker["type"] in {"login", "captcha", "loading_timeout"},
            }
        )
    if stability_error:
        blockers.append(
            {
                "code": "unstable_dom",
                "reason": stability_error,
                "retryable": True,
            }
        )
    if not ordering["monotonic"]:
        normalized_warnings.append(
            {"code": "source_order_anomaly", "evidence": ordering.get("evidence", "")}
        )
    normalized = {
        "capture_contract": PAGE_CAPTURE_KIND,
        "contract_version": CAPTURE_CONTRACT_VERSION,
        "adapter_version": adapter_version,
        "source_kind": source_kind,
        "canonical_url": canonical_url,
        "canonical_url_hash": sha256_text(canonical_url),
        "query_fingerprint": query_fingerprint,
        "page_index": page_index,
        "captured_at": captured_at,
        "source_reported_result_count": source_reported,
        "source_expected_page_count": expected_count,
        "navigation": navigation,
        "ordering": ordering,
        "cards": cards,
        "canonical_id_set_hash": id_set_hash,
        "raw_card_count": len(cards),
        "canonical_unique_count": len(canonical_ids),
        "loader": {"active": bool(loader.get("active"))},
        "blocker": blocker,
        "stability": stability,
        "session": session,
        "warnings": normalized_warnings,
        "blockers": blockers,
        "requires_count_drift_recapture": count_drift,
        "stable": not stability_error and blocker["type"] == "none" and not loader.get("active"),
    }
    normalized["capture_hash"] = payload_hash(normalized)
    return normalized


def validate_detail_capture(
    payload: Mapping[str, Any],
    *,
    expected_external_id: str | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("Снимок вакансии должен быть объектом JSON.")
    if len(canonical_json(payload).encode("utf-8")) > MAX_CAPTURE_BYTES:
        raise ValueError("Снимок вакансии превышает безопасный предел 1 МБ.")
    _walk_forbidden_keys(payload)
    if payload.get("capture_contract") != DETAIL_CAPTURE_KIND:
        raise ValueError(f"capture_contract должен быть {DETAIL_CAPTURE_KIND}.")
    if payload.get("contract_version") != CAPTURE_CONTRACT_VERSION:
        raise ValueError("Неподдерживаемая версия контракта снимка вакансии.")
    if payload.get("adapter_version") != ADAPTER_VERSION:
        raise ValueError(f"Снимок вакансии требует adapter_version={ADAPTER_VERSION}.")
    raw_id, external_id = canonical_external_id(payload.get("vacancy_id"))
    if expected_external_id is not None and external_id != expected_external_id:
        raise ValueError("Снимок относится к другой вакансии.")
    blocker = _normalize_blocker(payload.get("blocker", {"type": "none", "evidence": []}))
    if blocker["type"] != "none":
        raise ValueError(
            "Снимок вакансии заблокирован: " + "; ".join(blocker.get("evidence", []))
        )
    loader = _json_object(payload.get("loader"), "loader")
    if loader.get("active") is not False:
        raise ValueError("Снимок вакансии нельзя принять при активном индикаторе загрузки.")
    if payload.get("availability") is not None:
        if "fields" in payload:
            raise ValueError("Недоступная вакансия не должна содержать вымышленные поля.")
        availability = _json_object(payload.get("availability"), "availability")
        if set(availability) != {"state", "reason", "observed_url"}:
            raise ValueError("availability содержит неподдерживаемые поля.")
        if availability.get("state") != "unavailable":
            raise ValueError("availability.state должен быть unavailable.")
        if availability.get("reason") != "same_origin_lead_gen_redirect":
            raise ValueError("Неподдерживаемая причина недоступности вакансии.")
        observed_url = _optional_string(
            availability.get("observed_url"), "availability.observed_url", 10_000
        )
        if not observed_url:
            raise ValueError("availability.observed_url обязателен.")
        parsed = urlsplit(observed_url)
        host = (parsed.hostname or "").casefold()
        redirect_ids = [
            value.lstrip("0") or "0"
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key == "utm_redirect_vacancy_id"
        ]
        supported_lead_gen_path = bool(
            re.fullmatch(r"/article/[0-9]+/?", parsed.path)
            or re.fullmatch(r"/vrsurvey/[A-Za-z0-9_-]+/?", parsed.path)
        )
        if (
            parsed.scheme != "https"
            or not (host == "hh.ru" or host.endswith(".hh.ru"))
            or not supported_lead_gen_path
            or redirect_ids != [raw_id]
            or parsed.fragment
        ):
            raise ValueError(
                "Недоступность требует точный same-origin HH lead-gen redirect с тем же ID."
            )
        normalized = {
            "capture_contract": DETAIL_CAPTURE_KIND,
            "contract_version": CAPTURE_CONTRACT_VERSION,
            "adapter_version": ADAPTER_VERSION,
            "captured_at": _parse_iso(payload.get("captured_at"), "captured_at"),
            "vacancy_id": raw_id,
            "external_id": external_id,
            "canonical_url": _canonical_url(payload.get("canonical_url"), vacancy_id=raw_id),
            "availability": {
                "state": "unavailable",
                "reason": "same_origin_lead_gen_redirect",
                "observed_url": observed_url,
            },
            "source_evidence": _json_list(
                payload.get("source_evidence", []), "source_evidence"
            ),
        }
        normalized["capture_hash"] = payload_hash(normalized)
        return normalized
    fields = _json_object(payload.get("fields"), "fields")
    allowed_fields = (
        "title",
        "company",
        "description",
        "salary",
        "location",
        "schedule",
        "employment_format",
        "requirements",
        "experience",
        "skills",
        "publication_evidence",
    )
    normalized_fields: dict[str, Any] = {}
    for key in allowed_fields:
        value = fields.get(key)
        if key == "skills" and value is not None:
            items = _json_list(value, "fields.skills")
            if not all(isinstance(item, str) for item in items) or len(items) > 200:
                raise ValueError("fields.skills должен быть ограниченным массивом строк.")
            normalized_fields[key] = [item.strip()[:512] for item in items if item.strip()]
        else:
            maximum = MAX_DETAIL_DESCRIPTION_CHARS if key in {"description", "requirements"} else 10_000
            normalized_fields[key] = _optional_string(value, f"fields.{key}", maximum)
    if not normalized_fields.get("title") or not normalized_fields.get("company"):
        raise ValueError("Снимок вакансии требует видимые название и компанию.")
    normalized = {
        "capture_contract": DETAIL_CAPTURE_KIND,
        "contract_version": CAPTURE_CONTRACT_VERSION,
        "adapter_version": ADAPTER_VERSION,
        "captured_at": _parse_iso(payload.get("captured_at"), "captured_at"),
        "vacancy_id": raw_id,
        "external_id": external_id,
        "canonical_url": _canonical_url(payload.get("canonical_url"), vacancy_id=raw_id),
        "fields": normalized_fields,
        "source_evidence": _json_list(payload.get("source_evidence", []), "source_evidence"),
    }
    normalized["material_fingerprint"] = detail_material_fingerprint(normalized_fields)
    normalized["capture_hash"] = payload_hash(normalized)
    return normalized


def _artifact_directory(workspace_root: Path, *, run_id: str, stream_key: str) -> Path:
    safe_run = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)[:96]
    stream_digest = sha256_text(stream_key.casefold())[:20]
    directory = workspace_root / "artifacts" / "hh" / safe_run / stream_digest
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    return directory


def _write_gzip_artifact(
    workspace_root: Path,
    *,
    run_id: str,
    stream_key: str,
    filename: str,
    payload: Mapping[str, Any],
) -> tuple[str, str]:
    directory = _artifact_directory(workspace_root, run_id=run_id, stream_key=stream_key)
    path = directory / filename
    data = canonical_json(dict(payload)).encode("utf-8")
    compressed = gzip.compress(data, compresslevel=9, mtime=0)
    if path.exists():
        existing = path.read_bytes()
        if existing != compressed:
            raise RuntimeError("Путь артефакта уже занят другим содержимым.")
    else:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".hh-artifact-", suffix=".tmp", dir=directory
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(compressed)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, path)
        finally:
            temporary_path.unlink(missing_ok=True)
    digest = hashlib.sha256(compressed).hexdigest()
    return str(path.relative_to(workspace_root)), digest


def _batch_resolve_ids(
    conn: sqlite3.Connection, external_ids: Sequence[str]
) -> dict[str, dict[str, Any]]:
    if not external_ids:
        return {}
    placeholders = ",".join("?" for _ in external_ids)
    resolved: dict[str, dict[str, Any]] = {}
    for row in conn.execute(
        f"""
        SELECT external_id, id AS vacancy_id, title, company, url,
               first_seen_date, last_seen_date, 0 AS matched_via_alias
        FROM vacancies WHERE external_id IN ({placeholders})
        """,
        tuple(external_ids),
    ).fetchall():
        resolved[str(row["external_id"])] = dict(row)
    for row in conn.execute(
        f"""
        SELECT alias.external_id, vacancy.id AS vacancy_id, vacancy.title,
               vacancy.company, vacancy.url, vacancy.first_seen_date,
               vacancy.last_seen_date, 1 AS matched_via_alias
        FROM vacancy_external_aliases alias
        JOIN vacancies vacancy ON vacancy.id = alias.vacancy_id
        WHERE alias.external_id IN ({placeholders})
        """,
        tuple(external_ids),
    ).fetchall():
        external_id = str(row["external_id"])
        current = resolved.get(external_id)
        if current is not None and int(current["vacancy_id"]) != int(row["vacancy_id"]):
            raise RuntimeError(
                f"Псевдоним {external_id} неоднозначно разрешается в несколько вакансий."
            )
        resolved[external_id] = dict(row)
    return resolved


def _initial_known_changed(card: Mapping[str, Any], vacancy: Mapping[str, Any]) -> bool:
    comparisons = (
        (_normalize_text(card.get("title")), _normalize_text(vacancy.get("title"))),
        (_normalize_text(card.get("company")), _normalize_text(vacancy.get("company"))),
    )
    for captured, stored in comparisons:
        if captured and stored and captured != stored:
            return True
    stored_url = str(vacancy.get("url") or "")
    if stored_url and not bool(vacancy.get("matched_via_alias")):
        try:
            stored_split = urlsplit(stored_url)
            stored_match = re.search(r"/vacancy/([0-9]{1,32})(?:/|$)", stored_split.path)
            card_raw, _ = canonical_external_id(card["external_id"])
            if stored_match and stored_match.group(1).lstrip("0") != card_raw:
                return True
        except ValueError:
            return True
    return False


def _update_last_seen(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    external_id: str,
    seen_date: str,
) -> None:
    conn.execute(
        """
        UPDATE vacancies
        SET last_seen_date = CASE
                WHEN COALESCE(last_seen_date, '') < ? THEN ? ELSE last_seen_date END,
            updated_at = ?
        WHERE id = ?
        """,
        (seen_date, seen_date, now_iso(), vacancy_id),
    )
    conn.execute(
        """
        UPDATE vacancy_external_aliases
        SET last_seen_date = CASE
                WHEN COALESCE(last_seen_date, '') < ? THEN ? ELSE last_seen_date END,
            updated_at = ?
        WHERE vacancy_id = ? AND external_id = ?
        """,
        (seen_date, seen_date, now_iso(), vacancy_id, external_id),
    )


def _reconcile_cards(
    conn: sqlite3.Connection,
    *,
    capture_id: int,
    capture_hash: str,
    run_id: str,
    stream_key: str,
    page_index: int,
    captured_at: str,
    cards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    external_ids = [str(card["external_id"]) for card in cards]
    resolved = _batch_resolve_ids(conn, sorted(set(external_ids)))
    snapshots = {
        str(row["external_id"]): dict(row)
        for row in conn.execute(
            "SELECT * FROM hh_vacancy_snapshots WHERE external_id IN ("
            + ",".join("?" for _ in sorted(set(external_ids)))
            + ")",
            tuple(sorted(set(external_ids))),
        ).fetchall()
    } if external_ids else {}
    unique_external_ids = sorted(set(external_ids))
    current_vacancy_ids = sorted(
        {
            int(item["vacancy_id"])
            for item in resolved.values()
            if item.get("vacancy_id") is not None
        }
    )
    identity_clauses = [
        "external_id IN (" + ",".join("?" for _ in unique_external_ids) + ")"
    ]
    identity_params: list[Any] = list(unique_external_ids)
    if current_vacancy_ids:
        identity_clauses.append(
            "vacancy_id IN (" + ",".join("?" for _ in current_vacancy_ids) + ")"
        )
        identity_params.extend(current_vacancy_ids)
    across_rows = conn.execute(
        """
        SELECT DISTINCT external_id, vacancy_id FROM hh_page_items
        WHERE run_id = ? AND source = 'hh' AND stream_key <> ? AND ("""
        + " OR ".join(identity_clauses)
        + ")",
        (run_id, stream_key, *identity_params),
    ).fetchall() if external_ids else []
    across = {
        str(row["external_id"])
        for row in across_rows
    }
    across_vacancies = {
        int(row["vacancy_id"])
        for row in across_rows
        if row["vacancy_id"] is not None
    }
    across_page_rows = conn.execute(
        """
        SELECT DISTINCT item.external_id, item.vacancy_id
        FROM hh_page_items item
        JOIN hh_page_captures capture ON capture.id = item.capture_id
        WHERE item.run_id = ? AND item.source = 'hh' AND item.stream_key = ?
          AND item.page_index <> ? AND capture.verified = 1 AND ("""
        + " OR ".join("item." + clause for clause in identity_clauses)
        + ")",
        (run_id, stream_key, page_index, *identity_params),
    ).fetchall() if external_ids else []
    across_pages = {
        str(row["external_id"])
        for row in across_page_rows
    }
    across_page_vacancies = {
        int(row["vacancy_id"])
        for row in across_page_rows
        if row["vacancy_id"] is not None
    }
    seen_on_page: set[str] = set()
    classifications: list[dict[str, Any]] = []
    unique_base: dict[str, str] = {}
    duplicate_on_page = 0
    duplicate_across_pages = 0
    duplicate_across_streams = 0
    seen_date = captured_at[:10]
    timestamp = now_iso()
    for ordinal, card in enumerate(cards, start=1):
        external_id = str(card["external_id"])
        vacancy = resolved.get(external_id)
        snapshot = snapshots.get(external_id)
        vacancy_id = int(vacancy["vacancy_id"]) if vacancy is not None else None
        identity = f"vacancy:{vacancy_id}" if vacancy_id is not None else external_id
        detail_enqueued = False
        if identity in seen_on_page:
            base = "duplicate_on_page"
            classification = "duplicate_on_page"
            duplicate_on_page += 1
        else:
            seen_on_page.add(identity)
            if vacancy is None and snapshot is None:
                base = "new"
            elif snapshot is not None:
                base = (
                    "known_unchanged"
                    if str(snapshot["list_material_fingerprint"])
                    == str(card["material_fingerprint"])
                    else "known_changed"
                )
            else:
                base = "known_changed" if _initial_known_changed(card, vacancy) else "known_unchanged"
            unique_base[external_id] = base
            if external_id in across or (
                vacancy_id is not None and vacancy_id in across_vacancies
            ):
                classification = "duplicate_across_streams"
                duplicate_across_streams += 1
            elif external_id in across_pages or (
                vacancy_id is not None and vacancy_id in across_page_vacancies
            ):
                classification = "duplicate_across_pages"
                duplicate_across_pages += 1
            else:
                classification = base
        conn.execute(
            """
            INSERT INTO hh_page_items (
                capture_id, run_id, source, stream_key, page_index, ordinal,
                external_id, vacancy_id, canonical_url, title, company,
                position_text, publication_evidence, promoted, pinned,
                material_fingerprint, base_classification, classification, created_at
            ) VALUES (?, ?, 'hh', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                capture_id,
                run_id,
                stream_key,
                page_index,
                ordinal,
                external_id,
                vacancy_id,
                card["canonical_url"],
                card.get("title", ""),
                card.get("company", ""),
                card.get("position", ""),
                card.get("publication_evidence", ""),
                1 if card.get("promoted") else 0,
                1 if card.get("pinned") else 0,
                card["material_fingerprint"],
                base,
                classification,
                timestamp,
            ),
        )
        if base != "duplicate_on_page":
            conn.execute(
                """
                INSERT INTO hh_vacancy_snapshots (
                    external_id, vacancy_id, list_material_fingerprint,
                    title_normalized, company_normalized, publication_evidence,
                    evidence_level, last_capture_hash, last_seen_date, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'list', ?, ?, ?)
                ON CONFLICT(external_id) DO UPDATE SET
                    vacancy_id = COALESCE(excluded.vacancy_id, hh_vacancy_snapshots.vacancy_id),
                    list_material_fingerprint = excluded.list_material_fingerprint,
                    title_normalized = excluded.title_normalized,
                    company_normalized = excluded.company_normalized,
                    publication_evidence = excluded.publication_evidence,
                    last_capture_hash = excluded.last_capture_hash,
                    last_seen_date = excluded.last_seen_date,
                    updated_at = excluded.updated_at
                """,
                (
                    external_id,
                    vacancy_id,
                    card["material_fingerprint"],
                    _normalize_text(card.get("title")),
                    _normalize_text(card.get("company")),
                    card.get("publication_evidence", ""),
                    capture_hash,
                    seen_date,
                    timestamp,
                ),
            )
            if vacancy_id is not None:
                _update_last_seen(
                    conn,
                    vacancy_id=vacancy_id,
                    external_id=external_id,
                    seen_date=seen_date,
                )
            if base in {"new", "known_changed"}:
                reason = "new" if base == "new" else "materially_changed"
                prior_detail = conn.execute(
                    """
                    SELECT 1 FROM hh_detail_queue
                    WHERE run_id = ? AND source = 'hh'
                      AND (external_id = ? OR (? IS NOT NULL AND vacancy_id = ?))
                    LIMIT 1
                    """,
                    (run_id, external_id, vacancy_id, vacancy_id),
                ).fetchone()
                if prior_detail is None:
                    conn.execute(
                        """
                        INSERT INTO hh_detail_queue (
                            run_id, source, stream_key, external_id, vacancy_id,
                            canonical_url, reason, first_page, last_page,
                            material_fingerprint, state, created_at, updated_at
                        ) VALUES (?, 'hh', ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                        ON CONFLICT(run_id, source, stream_key, external_id) DO UPDATE SET
                            vacancy_id = COALESCE(excluded.vacancy_id, hh_detail_queue.vacancy_id),
                            canonical_url = excluded.canonical_url,
                            reason = CASE WHEN hh_detail_queue.reason = 'new' THEN 'new' ELSE excluded.reason END,
                            last_page = excluded.last_page,
                            material_fingerprint = excluded.material_fingerprint,
                            updated_at = excluded.updated_at
                        """,
                        (
                            run_id,
                            stream_key,
                            external_id,
                            vacancy_id,
                            card["canonical_url"],
                            reason,
                            page_index,
                            page_index,
                            card["material_fingerprint"],
                            timestamp,
                            timestamp,
                        ),
                    )
                    detail_enqueued = True
        classifications.append(
            {
                "external_id": external_id,
                "vacancy_id": vacancy_id,
                "base_classification": base,
                "classification": classification,
                "detail_enqueued": detail_enqueued,
            }
        )
    summary = {
        "unique": len(unique_base),
        "known_unchanged": sum(value == "known_unchanged" for value in unique_base.values()),
        "known_changed": sum(value == "known_changed" for value in unique_base.values()),
        "new": sum(value == "new" for value in unique_base.values()),
        "duplicate_on_page": duplicate_on_page,
        "duplicate_across_pages": duplicate_across_pages,
        "duplicate_across_streams": duplicate_across_streams,
    }
    return {
        "counts": summary,
        "items": classifications,
        "all_unique_known_unchanged": bool(unique_base)
        and all(value == "known_unchanged" for value in unique_base.values()),
        "unique_external_ids": sorted(unique_base),
    }


def _navigation_equivalent(first: Mapping[str, Any], second: Mapping[str, Any]) -> bool:
    return all(
        first.get(direction, {}).get(key) == second.get(direction, {}).get(key)
        for direction in ("previous", "next")
        for key in ("present", "page_index", "url_hash")
    )


def _ordering_equivalent(first: Mapping[str, Any], second: Mapping[str, Any]) -> bool:
    return all(
        first.get(key) == second.get(key)
        for key in ("kind", "monotonic", "newest_publication", "oldest_publication")
    )


def _checkpoint_boundary_sample(conn: sqlite3.Connection, stream_key: str) -> set[str]:
    row = conn.execute(
        "SELECT boundary_sample_json FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
        (stream_key,),
    ).fetchone()
    if row is None:
        return set()
    value = json.loads(str(row[0]))
    return {str(item) for item in value if isinstance(item, str)} if isinstance(value, list) else set()


def _page_overlaps_checkpoint(
    conn: sqlite3.Connection,
    *,
    stream_key: str,
    external_ids: Iterable[str],
    ordering: Mapping[str, Any],
) -> bool:
    sample = _checkpoint_boundary_sample(conn, stream_key)
    ids = set(external_ids)
    if sample and sample & ids:
        return True
    row = conn.execute(
        "SELECT covered_range_json FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
        (stream_key,),
    ).fetchone()
    if row is None:
        return False
    covered = json.loads(str(row[0]))
    old = str(ordering.get("oldest_publication") or "")
    newest = str(covered.get("newest_publication") or "") if isinstance(covered, dict) else ""
    oldest = str(covered.get("oldest_publication") or "") if isinstance(covered, dict) else ""
    return bool(old and oldest and newest and oldest <= old <= newest)


def _effective_rule_mode(row: Mapping[str, Any]) -> str:
    """Recover the acquisition rule behind resume, honoring a full fallback."""

    effective = str(row["effective_mode"])
    if effective == "full" and str(row["fallback_reason"] or ""):
        return "full"
    return str(row["resume_from_mode"] or effective)


def _set_stream_blocked(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    code: str,
    reason: str,
) -> None:
    conn.execute(
        """
        UPDATE hh_stream_runs SET state = 'blocked', blocker_code = ?,
            blocker_reason = ?, updated_at = ?
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (code, reason[:1000], now_iso(), run_id, stream_key),
    )
    _append_event(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        event_type="capture_blocked",
        severity="failure",
        details={"code": code, "reason": reason[:1000]},
    )


def _next_action_for_stream(
    conn: sqlite3.Connection, *, run_id: str, stream_key: str, limit: int
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        raise ValueError("План сбора для потока не найден.")
    if row["state"] == "blocked":
        return {
            "action": "resolve_blocker",
            "code": row["blocker_code"] or "",
            "reason": row["blocker_reason"] or "",
        }
    if row["state"] == "completed":
        return {"action": "stream_complete"}
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
    details = conn.execute(
        """
        SELECT external_id, canonical_url, reason, first_page
        FROM hh_detail_queue
        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND state = 'pending'
        ORDER BY id LIMIT ?
        """,
        (run_id, stream_key, limit),
    ).fetchall()
    if details:
        return {
            "action": "fetch_details",
            "items": [dict(item) for item in details],
            "truncated": max(
                int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM hh_detail_queue
                        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND state = 'pending'
                        """,
                        (run_id, stream_key),
                    ).fetchone()[0]
                )
                - len(details),
                0,
            ),
        }
    if row["state"] == "ready_to_finalize":
        return {"action": "finalize_stream"}
    action = "continue_stable_page_capture"
    if int(row["last_verified_page"]) >= 0:
        action = "continue_from_page"
    if row["fallback_reason"]:
        action = "continue_full_scan_after_fallback"
    return {
        "action": action,
        "page_index": int(row["next_page"]),
        "effective_mode": row["effective_mode"],
    }


def _recompute_stream_counts(
    conn: sqlite3.Connection, *, run_id: str, stream_key: str
) -> dict[str, int]:
    captures = conn.execute(
        """
        SELECT COALESCE(SUM(raw_card_count), 0) AS raw_count
        FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND verified = 1
        """,
        (run_id, stream_key),
    ).fetchone()
    rows = conn.execute(
        """
        SELECT item.external_id, item.base_classification, item.classification
        FROM hh_page_items item
        JOIN hh_page_captures capture ON capture.id = item.capture_id
        WHERE item.run_id = ? AND item.source = 'hh' AND item.stream_key = ?
          AND capture.verified = 1
        """,
        (run_id, stream_key),
    ).fetchall()
    by_id: dict[str, set[str]] = {}
    duplicate_on_page = 0
    duplicate_across_pages = 0
    duplicate_across_streams = 0
    for row in rows:
        base = str(row["base_classification"])
        classification = str(row["classification"])
        if base == "duplicate_on_page":
            duplicate_on_page += 1
            continue
        by_id.setdefault(str(row["external_id"]), set()).add(base)
        if classification == "duplicate_across_pages":
            duplicate_across_pages += 1
        elif classification == "duplicate_across_streams":
            duplicate_across_streams += 1
    known_unchanged = known_changed = new = 0
    for classifications in by_id.values():
        if "new" in classifications:
            new += 1
        elif "known_changed" in classifications:
            known_changed += 1
        else:
            known_unchanged += 1
    result = {
        "raw": int(captures["raw_count"] if captures else 0),
        "unique": len(by_id),
        "known_unchanged": known_unchanged,
        "known_changed": known_changed,
        "new": new,
        "duplicate_on_page": duplicate_on_page,
        "duplicate_across_pages": duplicate_across_pages,
        "duplicate_across_streams": duplicate_across_streams,
    }
    conn.execute(
        """
        UPDATE hh_stream_runs SET raw_count = ?, unique_count = ?,
            known_unchanged_count = ?, known_changed_count = ?, new_count = ?,
            duplicate_on_page_count = ?, duplicate_across_pages_count = ?,
            duplicate_across_streams_count = ?, updated_at = ?
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (
            result["raw"],
            result["unique"],
            result["known_unchanged"],
            result["known_changed"],
            result["new"],
            result["duplicate_on_page"],
            result["duplicate_across_pages"],
            result["duplicate_across_streams"],
            now_iso(),
            run_id,
            stream_key,
        ),
    )
    return result


def run_reconciliation_totals(
    conn: sqlite3.Connection, *, run_id: str
) -> dict[str, int]:
    """Return canonical run totals without expanding model-visible item lists.

    Known aliases collapse through vacancy_id. New IDs, which deliberately do
    not create lifecycle rows during list capture, collapse by canonical HH ID.
    Classification precedence is new, changed, unchanged.
    """

    rows = conn.execute(
        """
        SELECT item.external_id, item.vacancy_id, item.base_classification,
               item.classification
        FROM hh_page_items item
        JOIN hh_page_captures capture ON capture.id = item.capture_id
        WHERE item.run_id = ? AND item.source = 'hh' AND capture.verified = 1
        """,
        (run_id,),
    ).fetchall()
    by_identity: dict[str, set[str]] = {}
    duplicate_on_page = duplicate_across_pages = duplicate_across_streams = 0
    for row in rows:
        base = str(row["base_classification"])
        classification = str(row["classification"])
        if base == "duplicate_on_page":
            duplicate_on_page += 1
            continue
        identity = (
            f"vacancy:{int(row['vacancy_id'])}"
            if row["vacancy_id"] is not None
            else str(row["external_id"])
        )
        by_identity.setdefault(identity, set()).add(base)
        if classification == "duplicate_across_pages":
            duplicate_across_pages += 1
        elif classification == "duplicate_across_streams":
            duplicate_across_streams += 1
    known_unchanged = known_changed = new = 0
    for classifications in by_identity.values():
        if "new" in classifications:
            new += 1
        elif "known_changed" in classifications:
            known_changed += 1
        else:
            known_unchanged += 1
    raw = int(
        conn.execute(
            """
            SELECT COALESCE(SUM(raw_card_count), 0) FROM hh_page_captures
            WHERE run_id = ? AND source = 'hh' AND verified = 1
            """,
            (run_id,),
        ).fetchone()[0]
    )
    return {
        "raw": raw,
        "unique": len(by_identity),
        "known_unchanged": known_unchanged,
        "known_changed": known_changed,
        "new": new,
        "duplicate_on_page": duplicate_on_page,
        "duplicate_across_pages": duplicate_across_pages,
        "duplicate_across_streams": duplicate_across_streams,
    }


def _update_boundary_state(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    page_index: int,
    reconciliation: Mapping[str, Any],
    navigation: Mapping[str, Any],
    ordering: Mapping[str, Any],
) -> None:
    row = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        raise RuntimeError("План потока исчез во время записи снимка.")
    cfg = settings.search.hh_acquisition
    source_kind = str(row["source_kind"])
    effective_mode = str(row["effective_mode"])
    rule_mode = _effective_rule_mode(row)
    ordering_ok = bool(ordering.get("monotonic"))
    all_known = bool(reconciliation.get("all_unique_known_unchanged"))
    known_streak = int(row["known_page_streak"])
    known_streak = known_streak + 1 if all_known else 0
    overlap = _page_overlaps_checkpoint(
        conn,
        stream_key=stream_key,
        external_ids=reconciliation.get("unique_external_ids", []),
        ordering=ordering,
    )
    candidate = row["boundary_candidate_page"]
    predicted = row["predicted_boundary_page"]
    proven = row["boundary_proven_page"]
    guard_pages = int(row["guard_pages_verified"])
    pages_seen = page_index + 1
    if source_kind == "personal_recommendations":
        minimum_pages = cfg.personal_minimum_stable_pages
        required_known = cfg.personal_consecutive_known_pages
        if pages_seen >= minimum_pages and known_streak >= required_known and ordering_ok:
            candidate = candidate if candidate is not None else page_index
            predicted = predicted if predicted is not None else page_index
            proven = page_index
    elif rule_mode in {"delta", "shadow", "audit"}:
        # The candidate page must itself overlap the last successful checkpoint.
        # A later guard page is independent confirmation: it must remain stable,
        # ordered and wholly known, but it need not repeat the bounded checkpoint
        # sample verbatim. Requiring that repeat would make a normal ordered source
        # unable to advance past the candidate boundary.
        if (
            candidate is None
            and pages_seen >= cfg.minimum_overlap_pages
            and known_streak >= cfg.consecutive_known_boundary_pages
            and overlap
            and ordering_ok
        ):
            candidate = page_index
            if not cfg.guard_page_required:
                predicted = page_index
                proven = page_index
        elif (
            candidate is not None
            and proven is None
            and cfg.guard_page_required
            and page_index > int(candidate)
            and all_known
            and ordering_ok
        ):
            guard_pages += 1
            predicted = page_index
            proven = page_index
    source_exhausted = (
        not bool(navigation.get("next", {}).get("present"))
        and bool(
            json.loads(
                str(
                    conn.execute(
                        """
                        SELECT stability_json FROM hh_page_captures
                        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                          AND page_index = ? AND verified = 1
                        ORDER BY recapture_no DESC LIMIT 1
                        """,
                        (run_id, stream_key, page_index),
                    ).fetchone()[0]
                )
            ).get("end_of_list_evidence")
        )
    )
    fallback_reason = str(row["fallback_reason"] or "")
    next_state = "checkpointed"
    max_pages = (
        cfg.personal_max_pages
        if source_kind == "personal_recommendations"
        else cfg.max_pages_per_stream
    )
    if not ordering_ok and rule_mode == "delta":
        effective_mode = "full"
        rule_mode = "full"
        fallback_reason = "source_order_anomaly"
        candidate = predicted = proven = None
        _append_event(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            event_type="delta_fallback_to_full",
            severity="warning",
            details={"reason": fallback_reason, "page_index": page_index},
        )
    elif not ordering_ok:
        # Exhaustive collection may continue, but this source-order anomaly is
        # durable safety evidence and must prevent a future early stop until a
        # new clean shadow sequence is completed.
        fallback_reason = fallback_reason or "source_order_anomaly"
        if rule_mode in {"shadow", "audit"}:
            candidate = predicted = proven = None
        _append_event(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            event_type="source_order_anomaly",
            severity="warning",
            details={"page_index": page_index},
        )
    if source_kind == "personal_recommendations":
        checkpoint = conn.execute(
            "SELECT 1 FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
            (stream_key,),
        ).fetchone()
        if source_exhausted or proven is not None:
            next_state = "ready_to_finalize"
        elif checkpoint is None and pages_seen >= cfg.personal_initial_depth_pages:
            proven = page_index
            predicted = page_index
            next_state = "ready_to_finalize"
        elif pages_seen >= max_pages:
            if cfg.personal_max_is_completion_boundary:
                proven = page_index
                predicted = page_index
                next_state = "ready_to_finalize"
            elif bool(navigation.get("next", {}).get("present")):
                _set_stream_blocked(
                    conn,
                    run_id=run_id,
                    stream_key=stream_key,
                    code="personal_boundary_not_reached",
                    reason=(
                        "Достигнут настроенный предел персональных рекомендаций без "
                        "доказанной границы новизны; предел не объявлен допустимой "
                        "границей завершения."
                    ),
                )
                return
    elif rule_mode in {"full", "audit"}:
        if source_exhausted:
            next_state = "ready_to_finalize"
    elif rule_mode == "shadow":
        if proven is not None and predicted is None:
            predicted = proven
        if source_exhausted:
            next_state = "ready_to_finalize"
    elif rule_mode == "delta":
        if proven is not None:
            next_state = "ready_to_finalize"
        elif source_exhausted:
            effective_mode = "full"
            fallback_reason = "known_boundary_not_proven_before_source_exhaustion"
            next_state = "ready_to_finalize"
            _append_event(
                conn,
                run_id=run_id,
                stream_key=stream_key,
                event_type="delta_fallback_to_full",
                severity="warning",
                details={"reason": fallback_reason, "page_index": page_index},
            )
    if pages_seen >= max_pages and next_state != "ready_to_finalize" and bool(
        navigation.get("next", {}).get("present")
    ):
        _set_stream_blocked(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            code="safety_page_cap_reached",
            reason=(
                "Достигнуто предельное число страниц без доказанного исчерпания "
                "источника или границы известных результатов; успешная граница не создаётся."
            ),
        )
        return
    conn.execute(
        """
        UPDATE hh_stream_runs SET effective_mode = ?, state = ?,
            next_page = ?, last_verified_page = ?, known_page_streak = ?,
            boundary_candidate_page = ?, predicted_boundary_page = ?,
            boundary_proven_page = ?, guard_pages_verified = ?, source_exhausted = ?,
            fallback_reason = ?, newest_publication = COALESCE(newest_publication, ?),
            oldest_publication = CASE WHEN ? <> '' THEN ? ELSE oldest_publication END,
            updated_at = ?
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (
            effective_mode,
            next_state,
            page_index + 1,
            page_index,
            known_streak,
            candidate,
            predicted,
            proven,
            guard_pages,
            1 if source_exhausted else 0,
            fallback_reason,
            ordering.get("newest_publication", ""),
            ordering.get("oldest_publication", ""),
            ordering.get("oldest_publication", ""),
            now_iso(),
            run_id,
            stream_key,
        ),
    )


def record_page_capture(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Record one page idempotently and return only bounded new/changed work."""

    stream = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if stream is None:
        raise ValueError("Сначала постройте план сбора HH для точного запуска и потока.")
    expected_page = (
        int(stream["unresolved_drift_page"])
        if stream["unresolved_drift_page"] is not None
        else int(stream["next_page"])
    )
    normalized = validate_page_capture(
        payload,
        settings,
        expected_source_kind=str(stream["source_kind"]),
        expected_query_fingerprint=str(stream["query_fingerprint"]),
    )
    existing = conn.execute(
        "SELECT * FROM hh_page_captures WHERE capture_hash = ?",
        (normalized["capture_hash"],),
    ).fetchone()
    if existing is not None:
        if (
            str(existing["run_id"]) != run_id
            or str(existing["stream_key"]) != stream_key
        ):
            raise RuntimeError("Одинаковый хеш снимка уже связан с другим потоком.")
        stored = json.loads(str(existing["reconciliation_json"] or "{}"))
        return {
            "run_id": run_id,
            "stream_key": stream_key,
            "capture_hash": normalized["capture_hash"],
            "idempotent": True,
            "verified": bool(existing["verified"]),
            "reconciliation": _bounded_reconciliation(
                conn,
                stored,
                settings,
                run_id=run_id,
                stream_key=stream_key,
            ),
            "next_safe_action": _next_action_for_stream(
                conn,
                run_id=run_id,
                stream_key=stream_key,
                limit=settings.search.hh_acquisition.max_returned_ids,
            ),
        }
    if str(stream["state"]) == "completed":
        raise ValueError("Завершённый поток нельзя дополнить без явной инвалидации.")
    if int(normalized["page_index"]) != expected_page:
        raise ValueError(
            f"Ожидалась страница {expected_page}, снимок относится к {normalized['page_index']}."
        )
    page_index = int(normalized["page_index"])
    previous_rows = conn.execute(
        """
        SELECT * FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND page_index = ?
        ORDER BY recapture_no
        """,
        (run_id, stream_key, page_index),
    ).fetchall()
    recapture_no = len(previous_rows) + 1
    artifact_path, artifact_sha = _write_gzip_artifact(
        settings.workspace_root,
        run_id=run_id,
        stream_key=stream_key,
        filename=(
            f"page-{page_index:04d}-r{recapture_no}-"
            f"{normalized['capture_hash'][:16]}.json.gz"
        ),
        payload=normalized,
    )
    source_blockers = [
        item for item in normalized["blockers"] if item.get("code") != "unstable_dom"
    ]
    unstable_only = bool(normalized["blockers"]) and not source_blockers
    previous_reported = conn.execute(
        """
        SELECT source_reported_count FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
          AND page_index <> ? AND verified = 1 AND source_reported_count IS NOT NULL
        ORDER BY page_index DESC, recapture_no DESC LIMIT 1
        """,
        (run_id, stream_key, page_index),
    ).fetchone()
    total_changed = (
        previous_reported is not None
        and normalized["source_reported_result_count"] is not None
        and int(previous_reported[0]) != int(normalized["source_reported_result_count"])
    )
    full_personal_continuation = bool(
        normalized["source_kind"] == "personal_recommendations"
        and normalized["source_expected_page_count"]
        == settings.search.items_per_page
        and normalized["canonical_unique_count"] == settings.search.items_per_page
        and normalized["navigation"]["consistent"]
        and normalized["navigation"]["next"]["present"]
    )
    drift_required = bool(
        normalized["requires_count_drift_recapture"]
        or (total_changed and not full_personal_continuation)
    )
    drift_state = "none"
    verified = bool(normalized["stable"] and not normalized["blockers"] and not drift_required)
    if drift_required and normalized["stable"] and not normalized["blockers"]:
        drift_state = "awaiting_recapture"
        stable_previous = []
        for row in previous_rows:
            previous_stability = json.loads(str(row["stability_json"] or "{}"))
            previous_blockers = json.loads(str(row["blockers_json"] or "[]"))
            if previous_blockers:
                continue
            if int(previous_stability.get("sample_count", 0)) < int(
                previous_stability.get("required_sample_count", 1)
            ):
                continue
            if not (
                previous_stability.get("stable_scroll_height")
                or previous_stability.get("end_of_list_evidence")
            ):
                continue
            stability_method = str(
                previous_stability.get("stability_method", "")
            )
            if stability_method == "mutation_observer_visible_dom":
                if (
                    previous_stability.get(
                        "no_relevant_dom_mutation_after_bottom"
                    )
                    is not True
                ):
                    continue
            elif stability_method == "timed_visible_dom_sampling":
                if not (
                    previous_stability.get("mutation_observer_available") is False
                    and previous_stability.get(
                        "observer_mutation_evidence_available"
                    )
                    is False
                    and previous_stability.get(
                        "no_relevant_dom_mutation_after_bottom"
                    )
                    is None
                ):
                    continue
            else:
                continue
            stable_previous.append(row)
        matching = [
            row
            for row in stable_previous
            if str(row["canonical_id_set_hash"]) == normalized["canonical_id_set_hash"]
            and _navigation_equivalent(
                json.loads(str(row["navigation_json"])), normalized["navigation"]
            )
            and _ordering_equivalent(
                json.loads(str(row["ordering_json"])), normalized["ordering"]
            )
            and str(row["session_id_state"]) == normalized["session"]["session_id_state"]
            and str(row["session_fingerprint"]) == normalized["session"]["session_fingerprint"]
        ]
        conflicting = bool(stable_previous) and len(matching) != len(stable_previous)
        matching_ids = {int(row["id"]) for row in matching}
        matching_tail = 0
        for row in reversed(stable_previous):
            if int(row["id"]) not in matching_ids:
                break
            matching_tail += 1
        if (
            matching_tail + 1
            >= settings.search.hh_acquisition.count_drift_recaptures
        ):
            drift_state = "verified"
            verified = True
        elif conflicting:
            drift_state = "conflict"
            verified = False
    cursor = conn.execute(
        """
        INSERT INTO hh_page_captures (
            run_id, source, stream_key, source_kind, page_index, recapture_no,
            capture_hash, adapter_version, query_fingerprint, canonical_url_hash,
            captured_at, canonical_id_set_hash, source_reported_count,
            source_expected_page_count, raw_card_count, canonical_unique_count,
            stability_json, navigation_json, ordering_json, session_id_state,
            session_fingerprint, warnings_json, blockers_json, count_drift_state,
            verified, artifact_path, artifact_sha256, reconciliation_json, created_at
        ) VALUES (?, 'hh', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)
        """,
        (
            run_id,
            stream_key,
            normalized["source_kind"],
            page_index,
            recapture_no,
            normalized["capture_hash"],
            normalized["adapter_version"],
            normalized["query_fingerprint"],
            normalized["canonical_url_hash"],
            normalized["captured_at"],
            normalized["canonical_id_set_hash"],
            normalized["source_reported_result_count"],
            normalized["source_expected_page_count"],
            normalized["raw_card_count"],
            normalized["canonical_unique_count"],
            canonical_json(normalized["stability"]),
            canonical_json(normalized["navigation"]),
            canonical_json(normalized["ordering"]),
            normalized["session"]["session_id_state"],
            normalized["session"]["session_fingerprint"],
            canonical_json(normalized["warnings"]),
            canonical_json(normalized["blockers"]),
            drift_state,
            1 if verified else 0,
            artifact_path,
            artifact_sha,
            now_iso(),
        ),
    )
    capture_id = int(cursor.lastrowid)
    current_session_state = str(stream["session_id_state"] or "")
    current_session_fp = str(stream["session_fingerprint"] or "")
    session_changed = bool(current_session_state) and (
        current_session_state != normalized["session"]["session_id_state"]
        or current_session_fp != normalized["session"]["session_fingerprint"]
    )
    navigation_session_enrichment = session_changed and _is_visible_navigation_session_enrichment(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        page_index=page_index,
        current_session_state=current_session_state,
        current_session_fingerprint=current_session_fp,
        payload=payload,
        normalized=normalized,
    )
    if session_changed and not navigation_session_enrichment:
        _set_stream_blocked(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            code="session_identity_changed",
            reason=(
                "Session capability или её fingerprint изменились внутри потока; "
                "необъяснимое исчезновение/изменение session identity блокирует покрытие."
            ),
        )
        return _record_result(
            conn,
            settings,
            run_id=run_id,
            stream_key=stream_key,
            normalized=normalized,
            verified=False,
            reconciliation={},
            idempotent=False,
        )
    conn.execute(
        """
        UPDATE hh_stream_runs SET state = CASE WHEN state = 'planned' THEN 'capturing' ELSE state END,
            session_id_state = ?,
            session_fingerprint = ?,
            source_reported_count = ?, updated_at = ?
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (
            normalized["session"]["session_id_state"],
            normalized["session"]["session_fingerprint"],
            normalized["source_reported_result_count"],
            now_iso(),
            run_id,
            stream_key,
        ),
    )
    if source_blockers:
        first = source_blockers[0]
        _set_stream_blocked(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            code=str(first["code"]),
            reason=str(first["reason"]),
        )
    elif unstable_only:
        conn.execute(
            """
            UPDATE hh_stream_runs SET state = 'checkpointed', updated_at = ?
            WHERE run_id = ? AND source = 'hh' AND stream_key = ?
            """,
            (now_iso(), run_id, stream_key),
        )
    elif drift_state == "conflict":
        conn.execute(
            """
            UPDATE hh_stream_runs SET state = 'checkpointed', unresolved_drift_page = ?,
                updated_at = ? WHERE run_id = ? AND source = 'hh' AND stream_key = ?
            """,
            (page_index, now_iso(), run_id, stream_key),
        )
        _append_event(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            event_type="source_reported_count_drift_conflict",
            severity="warning",
            details={
                "page_index": page_index,
                "recapture_no": recapture_no,
                "next_action": "verify_count_drift",
            },
        )
    elif not verified:
        conn.execute(
            """
            UPDATE hh_stream_runs SET state = 'checkpointed', unresolved_drift_page = ?,
                updated_at = ? WHERE run_id = ? AND source = 'hh' AND stream_key = ?
            """,
            (page_index, now_iso(), run_id, stream_key),
        )
    reconciliation: dict[str, Any] = {}
    if verified:
        reconciliation = _reconcile_cards(
            conn,
            capture_id=capture_id,
            capture_hash=normalized["capture_hash"],
            run_id=run_id,
            stream_key=stream_key,
            page_index=page_index,
            captured_at=normalized["captured_at"],
            cards=normalized["cards"],
        )
        conn.execute(
            "UPDATE hh_page_captures SET reconciliation_json = ? WHERE id = ?",
            (canonical_json(reconciliation), capture_id),
        )
        conn.execute(
            """
            UPDATE hh_stream_runs SET unresolved_drift_page = NULL, updated_at = ?
            WHERE run_id = ? AND source = 'hh' AND stream_key = ?
            """,
            (now_iso(), run_id, stream_key),
        )
        if drift_state == "verified":
            _append_event(
                conn,
                run_id=run_id,
                stream_key=stream_key,
                event_type="source_reported_count_drift_verified",
                severity="warning",
                details={
                    "page_index": page_index,
                    "canonical_id_set_hash": normalized["canonical_id_set_hash"],
                    "canonical_unique_count": normalized["canonical_unique_count"],
                    "source_expected_page_count": normalized["source_expected_page_count"],
                },
            )
        _recompute_stream_counts(conn, run_id=run_id, stream_key=stream_key)
        _update_boundary_state(
            conn,
            settings,
            run_id=run_id,
            stream_key=stream_key,
            page_index=page_index,
            reconciliation=reconciliation,
            navigation=normalized["navigation"],
            ordering=normalized["ordering"],
        )
        pending = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM hh_detail_queue
                WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND state = 'pending'
                """,
                (run_id, stream_key),
            ).fetchone()[0]
        )
        state = conn.execute(
            "SELECT state FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
            (run_id, stream_key),
        ).fetchone()[0]
        if pending and state == "ready_to_finalize":
            conn.execute(
                """
                UPDATE hh_stream_runs SET state = 'details_pending', updated_at = ?
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (now_iso(), run_id, stream_key),
            )
    return _record_result(
        conn,
        settings,
        run_id=run_id,
        stream_key=stream_key,
        normalized=normalized,
        verified=verified,
        reconciliation=reconciliation,
        idempotent=False,
    )


def _bounded_reconciliation(
    conn: sqlite3.Connection,
    reconciliation: Mapping[str, Any],
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
) -> dict[str, Any]:
    limit = settings.search.hh_acquisition.max_returned_ids
    pending = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT external_id FROM hh_detail_queue
            WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND state = 'pending'
            """,
            (run_id, stream_key),
        ).fetchall()
    }
    items = [
        item
        for item in reconciliation.get("items", [])
        if item.get("detail_enqueued") is True
        and str(item.get("external_id", "")) in pending
    ]
    return {
        "counts": dict(reconciliation.get("counts", {})),
        "new_or_changed": items[:limit],
        "truncated": max(len(items) - limit, 0),
    }


def _record_result(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    normalized: Mapping[str, Any],
    verified: bool,
    reconciliation: Mapping[str, Any],
    idempotent: bool,
) -> dict[str, Any]:
    stream = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    return {
        "run_id": run_id,
        "stream_key": stream_key,
        "capture_hash": normalized["capture_hash"],
        "idempotent": idempotent,
        "verified": verified,
        "capture_warnings": normalized.get("warnings", []),
        "capture_blockers": normalized.get("blockers", []),
        "reconciliation": _bounded_reconciliation(
            conn,
            reconciliation,
            settings,
            run_id=run_id,
            stream_key=stream_key,
        ),
        "run_totals": run_reconciliation_totals(conn, run_id=run_id),
        "stream_state": stream["state"],
        "effective_mode": stream["effective_mode"],
        "next_safe_action": _next_action_for_stream(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            limit=settings.search.hh_acquisition.max_returned_ids,
        ),
    }


def record_detail_capture(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    external_id_hint = payload.get("vacancy_id") if isinstance(payload, Mapping) else None
    _, expected_external_id = canonical_external_id(external_id_hint)
    queue = conn.execute(
        """
        SELECT * FROM hh_detail_queue
        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND external_id = ?
        """,
        (run_id, stream_key, expected_external_id),
    ).fetchone()
    if queue is None:
        raise ValueError("Вакансия отсутствует в ограниченной очереди подробностей этого потока.")
    normalized = validate_detail_capture(payload, expected_external_id=expected_external_id)
    if str(queue["state"]) == "captured":
        if str(queue["detail_capture_hash"] or "") == normalized["capture_hash"]:
            return {
                "run_id": run_id,
                "stream_key": stream_key,
                "external_id": expected_external_id,
                "capture_hash": normalized["capture_hash"],
                "idempotent": True,
                "next_safe_action": _next_action_for_stream(
                    conn,
                    run_id=run_id,
                    stream_key=stream_key,
                    limit=settings.search.hh_acquisition.max_returned_ids,
                ),
            }
        raise ValueError(
            "Для этой вакансии уже записан другой снимок; требуется явная инвалидация."
        )
    artifact_path, artifact_sha = _write_gzip_artifact(
        settings.workspace_root,
        run_id=run_id,
        stream_key=stream_key,
        filename=f"detail-{normalized['vacancy_id']}-{normalized['capture_hash'][:16]}.json.gz",
        payload=normalized,
    )
    conn.execute(
        """
        UPDATE hh_detail_queue SET state = 'captured', detail_capture_hash = ?,
            detail_artifact_path = ?, detail_artifact_sha256 = ?,
            detail_payload_json = ?, updated_at = ? WHERE id = ?
        """,
        (
            normalized["capture_hash"],
            artifact_path,
            artifact_sha,
            canonical_json(normalized),
            now_iso(),
            int(queue["id"]),
        ),
    )
    if "fields" in normalized:
        conn.execute(
            """
            UPDATE hh_vacancy_snapshots SET detail_material_fingerprint = ?,
                evidence_level = 'detail', last_capture_hash = ?, updated_at = ?
            WHERE external_id = ?
            """,
            (
                normalized["material_fingerprint"],
                normalized["capture_hash"],
                now_iso(),
                expected_external_id,
            ),
        )
    pending = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM hh_detail_queue
            WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND state = 'pending'
            """,
            (run_id, stream_key),
        ).fetchone()[0]
    )
    stream = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if pending == 0 and stream is not None and str(stream["state"]) == "details_pending":
        ready = bool(stream["source_exhausted"] or stream["boundary_proven_page"] is not None)
        if ready:
            conn.execute(
                """
                UPDATE hh_stream_runs SET state = 'ready_to_finalize', updated_at = ?
                WHERE run_id = ? AND source = 'hh' AND stream_key = ?
                """,
                (now_iso(), run_id, stream_key),
            )
    result = {
        "run_id": run_id,
        "stream_key": stream_key,
        "external_id": expected_external_id,
        "capture_hash": normalized["capture_hash"],
        "idempotent": False,
        "artifact": {"path": artifact_path, "sha256": artifact_sha},
        "next_safe_action": _next_action_for_stream(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            limit=settings.search.hh_acquisition.max_returned_ids,
        ),
    }
    if "fields" in normalized:
        result["fields"] = normalized["fields"]
    else:
        result["availability"] = normalized["availability"]
    return result


def _page_manifest_rows(
    conn: sqlite3.Connection, *, run_id: str, stream_key: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT page_index, recapture_no, capture_hash, canonical_id_set_hash,
               source_reported_count, source_expected_page_count,
               raw_card_count, canonical_unique_count, stability_json,
               navigation_json, ordering_json, session_id_state,
               session_fingerprint, warnings_json, blockers_json,
               count_drift_state, verified, artifact_path, artifact_sha256,
               reconciliation_json
        FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        ORDER BY page_index, recapture_no
        """,
        (run_id, stream_key),
    ).fetchall()
    result = []
    for row in rows:
        reconciliation = json.loads(str(row["reconciliation_json"] or "{}"))
        result.append(
            {
                "page_index": int(row["page_index"]),
                "recapture_no": int(row["recapture_no"]),
                "capture_hash": row["capture_hash"],
                "canonical_id_set_hash": row["canonical_id_set_hash"],
                "source_reported_count": row["source_reported_count"],
                "source_expected_page_count": row["source_expected_page_count"],
                "raw_card_count": int(row["raw_card_count"]),
                "canonical_unique_count": int(row["canonical_unique_count"]),
                "stability": json.loads(str(row["stability_json"])),
                "navigation": json.loads(str(row["navigation_json"])),
                "ordering": json.loads(str(row["ordering_json"])),
                "session": {
                    "session_id_state": row["session_id_state"],
                    "session_fingerprint": row["session_fingerprint"],
                },
                "warnings": json.loads(str(row["warnings_json"])),
                "blockers": json.loads(str(row["blockers_json"])),
                "count_drift_state": row["count_drift_state"],
                "verified": bool(row["verified"]),
                "artifact": {
                    "path": row["artifact_path"],
                    "sha256": row["artifact_sha256"],
                },
                "reconciliation_counts": dict(reconciliation.get("counts", {})),
            }
        )
    return result


def _stream_counts(row: sqlite3.Row) -> dict[str, int]:
    return {
        "raw": int(row["raw_count"]),
        "unique": int(row["unique_count"]),
        "known_unchanged": int(row["known_unchanged_count"]),
        "known_changed": int(row["known_changed_count"]),
        "new": int(row["new_count"]),
        "duplicate_on_page": int(row["duplicate_on_page_count"]),
        "duplicate_across_pages": int(row["duplicate_across_pages_count"]),
        "duplicate_across_streams": int(row["duplicate_across_streams_count"]),
        "processed": int(row["raw_count"]),
        "reconciled": int(row["unique_count"]),
        "blocked": 1 if str(row["state"]) == "blocked" else 0,
    }


def build_p1_checkpoint_manifest(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        raise ValueError("Поток сбора не найден.")
    pages = _page_manifest_rows(conn, run_id=run_id, stream_key=stream_key)
    artifact_payload = {
        "manifest_version": MANIFEST_VERSION,
        "run_id": run_id,
        "stream_key": stream_key,
        "source_kind": row["source_kind"],
        "acquisition_mode": row["effective_mode"],
        "query_fingerprint": row["query_fingerprint"],
        "configuration_fingerprint": row["configuration_fingerprint"],
        "adapter_version": row["adapter_version"],
        "pages": pages,
    }
    digest = payload_hash(artifact_payload)
    artifact_path, artifact_sha = _write_gzip_artifact(
        settings.workspace_root,
        run_id=run_id,
        stream_key=stream_key,
        filename=f"checkpoint-{digest[:20]}.manifest-v2.json.gz",
        payload=artifact_payload,
    )
    return {
        "manifest_version": MANIFEST_VERSION,
        "kind": row["p1_step_key"] == "hh_coverage" and "hh_stream" or "source_gate",
        "run_id": run_id,
        "step_key": row["p1_step_key"],
        "item_key": row["p1_item_key"],
        "observed_at": now_iso(),
        "captured_scope": {
            "stream_key": stream_key,
            "source_kind": row["source_kind"],
            "query_fingerprint": row["query_fingerprint"],
            "configuration_fingerprint": row["configuration_fingerprint"],
        },
        "acquisition_mode": row["effective_mode"],
        "adapter_version": row["adapter_version"],
        "session_capability": {
            "session_id_state": row["session_id_state"] or "",
            "session_fingerprint": row["session_fingerprint"] or "",
        },
        "counts": _stream_counts(row),
        "pages": [
            {
                "page_index": item["page_index"],
                "recapture_no": item["recapture_no"],
                "capture_hash": item["capture_hash"],
                "canonical_id_set_hash": item["canonical_id_set_hash"],
                "verified": item["verified"],
                "count_drift_state": item["count_drift_state"],
                "stability": {
                    "sample_count": item["stability"].get("sample_count", 0),
                    "stable_scroll_height": item["stability"].get("stable_scroll_height", False),
                    "end_of_list_evidence": item["stability"].get("end_of_list_evidence", False),
                },
            }
            for item in pages
        ],
        "count_drift_evidence": [
            {
                "page_index": item["page_index"],
                "state": item["count_drift_state"],
                "capture_hash": item["capture_hash"],
            }
            for item in pages
            if item["count_drift_state"] != "none"
        ],
        "completion_boundary": {
            "last_verified_page": int(row["last_verified_page"]),
            "next_page": int(row["next_page"]),
            "boundary_candidate_page": row["boundary_candidate_page"],
            "boundary_proven_page": row["boundary_proven_page"],
            "source_exhausted": bool(row["source_exhausted"]),
        },
        "boundary_proof": _boundary_proof(row, settings),
        "remote_boundary_verified": False,
        "cursor_candidate": {},
        "blockers": (
            [
                {
                    "code": row["blocker_code"],
                    "reason": row["blocker_reason"],
                    "retryable": True,
                }
            ]
            if row["state"] == "blocked"
            else []
        ),
        "warnings": _stream_warnings(conn, run_id=run_id, stream_key=stream_key),
        "artifact": {"path": artifact_path, "sha256": artifact_sha},
    }


def _boundary_proof(row: sqlite3.Row, settings: Settings) -> dict[str, Any]:
    cfg = settings.search.hh_acquisition
    return {
        "minimum_overlap_pages_required": cfg.minimum_overlap_pages,
        "consecutive_known_pages_required": (
            cfg.personal_consecutive_known_pages
            if row["source_kind"] == "personal_recommendations"
            else cfg.consecutive_known_boundary_pages
        ),
        "known_page_streak": int(row["known_page_streak"]),
        "guard_page_required": cfg.guard_page_required
        if row["source_kind"] == "ordinary_search"
        else False,
        "guard_pages_verified": int(row["guard_pages_verified"]),
        "boundary_candidate_page": row["boundary_candidate_page"],
        "boundary_proven_page": row["boundary_proven_page"],
        "source_exhausted": bool(row["source_exhausted"]),
        "fallback_reason": row["fallback_reason"] or "",
    }


def _stream_warnings(
    conn: sqlite3.Connection, *, run_id: str, stream_key: str
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    rows = conn.execute(
        """
        SELECT page_index, warnings_json, count_drift_state
        FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        ORDER BY page_index, recapture_no
        """,
        (run_id, stream_key),
    ).fetchall()
    for row in rows:
        for warning in json.loads(str(row["warnings_json"])):
            warnings.append(
                warning
                if isinstance(warning, dict)
                else {"code": "adapter_warning", "message": str(warning)}
            )
    return warnings[:100]


def _validated_page_indexes(
    conn: sqlite3.Connection, *, run_id: str, stream_key: str
) -> list[int]:
    rows = conn.execute(
        """
        SELECT page_index, COUNT(*) AS verified_count
        FROM hh_page_captures
        WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND verified = 1
        GROUP BY page_index ORDER BY page_index
        """,
        (run_id, stream_key),
    ).fetchall()
    indexes = [int(row["page_index"]) for row in rows]
    if any(int(row["verified_count"]) != 1 for row in rows):
        raise ValueError("Для страницы должен существовать ровно один проверенный снимок.")
    if indexes != list(range(len(indexes))):
        raise ValueError("Проверенные страницы должны образовывать непрерывный диапазон от страницы 0.")
    return indexes


def _items_beyond_boundary(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    boundary_page: int | None,
) -> list[str]:
    if boundary_page is None:
        return []
    return [
        str(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT item.external_id
            FROM hh_page_items item
            JOIN hh_page_captures capture ON capture.id = item.capture_id
            WHERE item.run_id = ? AND item.source = 'hh' AND item.stream_key = ?
              AND item.page_index > ? AND capture.verified = 1
              AND item.base_classification IN ('new','known_changed')
            ORDER BY item.external_id
            """,
            (run_id, stream_key, boundary_page),
        ).fetchall()
    ]


def _boundary_sample(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    boundary_page: int,
) -> list[str]:
    start = max(boundary_page - 2, 0)
    rows = conn.execute(
        """
        SELECT DISTINCT item.external_id
        FROM hh_page_items item
        JOIN hh_page_captures capture ON capture.id = item.capture_id
        WHERE item.run_id = ? AND item.source = 'hh' AND item.stream_key = ?
          AND item.page_index BETWEEN ? AND ? AND capture.verified = 1
        ORDER BY item.external_id LIMIT ?
        """,
        (run_id, stream_key, start, boundary_page, BOUNDARY_SAMPLE_LIMIT),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _checkpoint_snapshot(
    row: sqlite3.Row,
    *,
    settings: Settings,
    run_date: str,
    stop_reason: str,
    shadow_result: Mapping[str, Any],
    audit_result: Mapping[str, Any],
    shadow_clean_runs: int,
    anomaly_state: str,
    boundary_sample: Sequence[str],
) -> dict[str, Any]:
    cfg = settings.search.hh_acquisition
    rule_mode = _effective_rule_mode(row)
    full_like = rule_mode in {"full", "shadow", "audit"} or bool(row["source_exhausted"])
    # Eligibility is evidence, not activation. Shadow mode may accumulate the
    # required clean runs; the planner separately refuses early stopping until
    # incremental_mode is explicitly enabled.
    eligible = shadow_clean_runs >= cfg.shadow_runs_required and not anomaly_state
    timestamp = now_iso()
    cursor = {
        "valid_completion_boundary": True,
        "stop_reason": stop_reason,
        "last_verified_page": int(row["last_verified_page"]),
        "boundary_proven_page": row["boundary_proven_page"],
        "source_exhausted": bool(row["source_exhausted"]),
        "ordering_compatible": not anomaly_state,
        "adapter_version": row["adapter_version"],
    }
    return {
        "source": "hh",
        "stream_key": row["stream_key"],
        "source_kind": row["source_kind"],
        "last_successful_run_id": row["run_id"],
        "last_successful_date": run_date,
        "acquisition_mode": row["effective_mode"],
        "query_fingerprint": row["query_fingerprint"],
        "configuration_fingerprint": row["configuration_fingerprint"],
        "adapter_version": row["adapter_version"],
        "newest_publication": row["newest_publication"] or "",
        "oldest_publication": row["oldest_publication"] or "",
        "covered_range": {
            "newest_publication": row["newest_publication"] or "",
            "oldest_publication": row["oldest_publication"] or "",
            "pages": int(row["last_verified_page"]) + 1,
            "full_like": full_like,
        },
        "boundary_id_hash": payload_hash(list(boundary_sample)),
        "boundary_sample": list(boundary_sample),
        "last_full_scan_at": timestamp if full_like else "",
        "last_audit_scan_at": timestamp if rule_mode == "audit" else "",
        "shadow_clean_runs": shadow_clean_runs,
        "shadow_runs_required": cfg.shadow_runs_required,
        "last_shadow_result": dict(shadow_result),
        "last_audit_result": dict(audit_result),
        "session_id_state": row["session_id_state"],
        "session_fingerprint": row["session_fingerprint"],
        "anomaly_state": anomaly_state,
        "eligibility_state": "eligible" if eligible else "shadow_required",
        "cursor": cursor,
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def _persist_checkpoint(
    conn: sqlite3.Connection,
    *,
    snapshot: Mapping[str, Any],
    event_type: str,
) -> int:
    existing = conn.execute(
        "SELECT checkpoint_version, created_at FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
        (snapshot["stream_key"],),
    ).fetchone()
    version = int(existing["checkpoint_version"]) + 1 if existing else 1
    created_at = str(existing["created_at"]) if existing else str(snapshot["created_at"])
    conn.execute(
        """
        INSERT INTO hh_stream_checkpoints (
            source, stream_key, source_kind, checkpoint_version,
            last_successful_run_id, last_successful_date, acquisition_mode,
            query_fingerprint, configuration_fingerprint, adapter_version,
            newest_publication, oldest_publication, covered_range_json,
            boundary_id_hash, boundary_sample_json, last_full_scan_at,
            last_audit_scan_at, shadow_clean_runs, shadow_runs_required,
            last_shadow_result_json, last_audit_result_json, session_id_state,
            session_fingerprint, anomaly_state, eligibility_state, cursor_json,
            invalidated_at, invalidation_reason, created_at, updated_at
        ) VALUES ('hh', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
        ON CONFLICT(source, stream_key) DO UPDATE SET
            source_kind = excluded.source_kind,
            checkpoint_version = excluded.checkpoint_version,
            last_successful_run_id = excluded.last_successful_run_id,
            last_successful_date = excluded.last_successful_date,
            acquisition_mode = excluded.acquisition_mode,
            query_fingerprint = excluded.query_fingerprint,
            configuration_fingerprint = excluded.configuration_fingerprint,
            adapter_version = excluded.adapter_version,
            newest_publication = excluded.newest_publication,
            oldest_publication = excluded.oldest_publication,
            covered_range_json = excluded.covered_range_json,
            boundary_id_hash = excluded.boundary_id_hash,
            boundary_sample_json = excluded.boundary_sample_json,
            last_full_scan_at = CASE WHEN excluded.last_full_scan_at <> '' THEN excluded.last_full_scan_at ELSE hh_stream_checkpoints.last_full_scan_at END,
            last_audit_scan_at = CASE WHEN excluded.last_audit_scan_at <> '' THEN excluded.last_audit_scan_at ELSE hh_stream_checkpoints.last_audit_scan_at END,
            shadow_clean_runs = excluded.shadow_clean_runs,
            shadow_runs_required = excluded.shadow_runs_required,
            last_shadow_result_json = excluded.last_shadow_result_json,
            last_audit_result_json = excluded.last_audit_result_json,
            session_id_state = excluded.session_id_state,
            session_fingerprint = excluded.session_fingerprint,
            anomaly_state = excluded.anomaly_state,
            eligibility_state = excluded.eligibility_state,
            cursor_json = excluded.cursor_json,
            invalidated_at = NULL,
            invalidation_reason = NULL,
            updated_at = excluded.updated_at
        """,
        (
            snapshot["stream_key"],
            snapshot["source_kind"],
            version,
            snapshot["last_successful_run_id"],
            snapshot["last_successful_date"],
            snapshot["acquisition_mode"],
            snapshot["query_fingerprint"],
            snapshot["configuration_fingerprint"],
            snapshot["adapter_version"],
            snapshot["newest_publication"],
            snapshot["oldest_publication"],
            canonical_json(snapshot["covered_range"]),
            snapshot["boundary_id_hash"],
            canonical_json(snapshot["boundary_sample"]),
            snapshot["last_full_scan_at"],
            snapshot["last_audit_scan_at"],
            snapshot["shadow_clean_runs"],
            snapshot["shadow_runs_required"],
            canonical_json(snapshot["last_shadow_result"]),
            canonical_json(snapshot["last_audit_result"]),
            snapshot["session_id_state"],
            snapshot["session_fingerprint"],
            snapshot["anomaly_state"],
            snapshot["eligibility_state"],
            canonical_json(snapshot["cursor"]),
            created_at,
            snapshot["updated_at"],
        ),
    )
    history_snapshot = {**dict(snapshot), "checkpoint_version": version}
    digest = payload_hash(history_snapshot)
    conn.execute(
        """
        INSERT OR IGNORE INTO hh_stream_checkpoint_history (
            source, stream_key, run_id, checkpoint_version, event_type,
            snapshot_json, snapshot_hash, created_at
        ) VALUES ('hh', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["stream_key"],
            snapshot["last_successful_run_id"],
            version,
            event_type,
            canonical_json(history_snapshot),
            digest,
            now_iso(),
        ),
    )
    return version


PERSONAL_CONFIGURED_BOUNDARY_POLICY_KEYS = frozenset(
    {
        "personal_initial_depth_pages",
        "personal_max_pages",
        "personal_max_is_completion_boundary",
    }
)


def _acquisition_payload_from_plan_scope(
    scope: Mapping[str, Any],
    *,
    source_kind: str,
    stream_key: str,
    query_fingerprint: str,
    adapter_version: str,
) -> dict[str, Any] | None:
    configuration = scope.get("configuration")
    if not isinstance(configuration, Mapping):
        return None
    hh_scope = configuration.get("hh_acquisition")
    if not isinstance(hh_scope, Mapping):
        return None
    required = {
        "minimum_overlap_pages",
        "consecutive_known_boundary_pages",
        "guard_page_required",
        "checkpoint_staleness_days",
        "shadow_runs_required",
        "full_audit_interval_days",
        "page_stability_samples",
        "page_stability_delay_ms",
        "page_stability_timeout_ms",
        "count_drift_recaptures",
        "max_pages_per_stream",
        "personal_initial_depth_pages",
        "personal_minimum_stable_pages",
        "personal_consecutive_known_pages",
        "personal_max_pages",
        "personal_max_is_completion_boundary",
    }
    if not required.issubset(hh_scope):
        return None
    if (
        "search_period_days" not in configuration
        or "search_items_per_page" not in configuration
    ):
        return None
    return {
        "source_kind": source_kind,
        "stream_key": stream_key.casefold(),
        "query_fingerprint": query_fingerprint,
        "adapter_version": adapter_version,
        "search_period_days": configuration["search_period_days"],
        "items_per_page": configuration["search_items_per_page"],
        **{key: hh_scope[key] for key in sorted(required)},
    }


def _reconcile_refreshed_personal_configured_boundary(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    row: sqlite3.Row,
    verified_indexes: list[int],
) -> sqlite3.Row:
    """Accept a user-refreshed lower personal cap using existing verified pages.

    This is intentionally narrower than general configuration recovery.  It only
    permits an explicit plan refresh that lowers the personal page cap, marks the
    cap as a completion boundary, and changes no unrelated acquisition setting.
    """

    cfg = settings.search.hh_acquisition
    if (
        str(row["source_kind"]) != "personal_recommendations"
        or bool(row["source_exhausted"])
        or row["boundary_proven_page"] is not None
        or not cfg.personal_max_is_completion_boundary
    ):
        return row
    current_max = int(cfg.personal_max_pages)
    if (
        current_max < 1
        or verified_indexes != list(range(current_max))
        or int(row["last_verified_page"]) != current_max - 1
        or int(row["next_page"]) != current_max
    ):
        return row

    daily_run = conn.execute(
        "SELECT plan_revision FROM daily_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if daily_run is None:
        return row
    current_revision = int(daily_run["plan_revision"])
    revision_rows = conn.execute(
        """
        SELECT revision, scope_json FROM daily_run_plan_revisions
        WHERE run_id = ? AND revision <= ?
        ORDER BY revision DESC
        """,
        (run_id, current_revision),
    ).fetchall()
    current_payload = acquisition_config_payload(
        settings,
        source_kind="personal_recommendations",
        stream_key=stream_key,
        query_fingerprint=str(row["query_fingerprint"]),
    )
    current_plan_payload: dict[str, Any] | None = None
    previous_payload: dict[str, Any] | None = None
    previous_revision: int | None = None
    for revision_row in revision_rows:
        try:
            revision_scope = json.loads(str(revision_row["scope_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(revision_scope, dict):
            continue
        payload = _acquisition_payload_from_plan_scope(
            revision_scope,
            source_kind="personal_recommendations",
            stream_key=stream_key,
            query_fingerprint=str(row["query_fingerprint"]),
            adapter_version=str(row["adapter_version"]),
        )
        if payload is None:
            continue
        revision = int(revision_row["revision"])
        if revision == current_revision:
            current_plan_payload = payload
        if payload_hash(payload) == str(row["configuration_fingerprint"]):
            previous_payload = payload
            previous_revision = revision
            break
    if current_plan_payload != current_payload or previous_payload is None:
        return row

    changed_keys = {
        key
        for key in current_payload
        if current_payload.get(key) != previous_payload.get(key)
    }
    previous_max = int(previous_payload["personal_max_pages"])
    if (
        not changed_keys
        or not changed_keys.issubset(PERSONAL_CONFIGURED_BOUNDARY_POLICY_KEYS)
        or previous_payload["personal_max_is_completion_boundary"] is not False
        or current_payload["personal_max_is_completion_boundary"] is not True
        or current_max >= previous_max
        or int(current_payload["personal_initial_depth_pages"]) > previous_max
    ):
        return row

    timestamp = now_iso()
    current_fingerprint = payload_hash(current_payload)
    boundary_page = current_max - 1
    cursor = conn.execute(
        """
        UPDATE hh_stream_runs
        SET configuration_fingerprint = ?, boundary_candidate_page = ?,
            predicted_boundary_page = ?, boundary_proven_page = ?,
            state = 'ready_to_finalize', updated_at = ?
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
          AND state <> 'completed' AND source_exhausted = 0
          AND boundary_proven_page IS NULL AND next_page = ?
        """,
        (
            current_fingerprint,
            boundary_page,
            boundary_page,
            boundary_page,
            timestamp,
            run_id,
            stream_key,
            current_max,
        ),
    )
    if cursor.rowcount != 1:
        return row
    _append_event(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        event_type="personal_configured_boundary_reconciled",
        severity="warning",
        details={
            "previous_plan_revision": previous_revision,
            "current_plan_revision": current_revision,
            "previous_personal_max_pages": previous_max,
            "current_personal_max_pages": current_max,
            "boundary_proven_page": boundary_page,
            "verified_page_count": len(verified_indexes),
            "changed_policy_keys": sorted(changed_keys),
            "source_exhausted": False,
        },
    )
    refreshed = conn.execute(
        """
        SELECT * FROM hh_stream_runs
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (run_id, stream_key),
    ).fetchone()
    return refreshed if refreshed is not None else row


def finalize_stream(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        raise ValueError("Поток сбора не найден.")
    if str(row["state"]) == "completed":
        history = conn.execute(
            """
            SELECT checkpoint_version FROM hh_stream_checkpoint_history
            WHERE source = 'hh' AND stream_key = ? AND run_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (stream_key, run_id),
        ).fetchone()
        stored_manifest = json.loads(str(row["completion_manifest_json"] or "{}"))
        if not stored_manifest:
            stored_manifest = build_completion_manifest(
                conn, settings, run_id=run_id, stream_key=stream_key
            )
        return {
            "run_id": run_id,
            "stream_key": stream_key,
            "idempotent": True,
            "checkpoint_version": int(history[0]) if history else None,
            "manifest": stored_manifest,
        }
    if str(row["state"]) == "blocked":
        raise ValueError("Заблокированный поток нельзя завершить.")
    if row["unresolved_drift_page"] is not None:
        raise ValueError("Расхождение счётчика не подтверждено двумя независимыми устойчивыми снимками.")
    pending = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM hh_detail_queue
            WHERE run_id = ? AND source = 'hh' AND stream_key = ? AND state <> 'captured'
            """,
            (run_id, stream_key),
        ).fetchone()[0]
    )
    if pending:
        raise ValueError("Сначала запишите снимки вакансий для всех возвращённых новых и изменённых ID.")
    indexes = _validated_page_indexes(conn, run_id=run_id, stream_key=stream_key)
    if not indexes:
        raise ValueError("В потоке нет ни одного проверенного снимка страницы.")
    row = _reconcile_refreshed_personal_configured_boundary(
        conn,
        settings,
        run_id=run_id,
        stream_key=stream_key,
        row=row,
        verified_indexes=indexes,
    )
    rule_mode = _effective_rule_mode(row)
    source_kind = str(row["source_kind"])
    if source_kind == "ordinary_search":
        if rule_mode in {"full", "shadow", "audit"} and not bool(row["source_exhausted"]):
            raise ValueError("Режимы full, shadow и audit требуют доказанного исчерпания источника.")
        if rule_mode == "delta" and row["boundary_proven_page"] is None:
            raise ValueError("Режим delta требует полного доказательства границы v2.")
    else:
        if not bool(row["source_exhausted"]) and row["boundary_proven_page"] is None:
            raise ValueError("Персональные рекомендации не достигли настроенной границы остановки.")
    predicted = row["predicted_boundary_page"]
    missed = _items_beyond_boundary(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        boundary_page=int(predicted) if predicted is not None else None,
    )
    previous = conn.execute(
        "SELECT * FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
        (stream_key,),
    ).fetchone()
    previous_clean = int(previous["shadow_clean_runs"]) if previous else 0
    same_config = bool(
        previous
        and str(previous["query_fingerprint"]) == str(row["query_fingerprint"])
        and str(previous["configuration_fingerprint"])
        == str(row["configuration_fingerprint"])
    )
    shadow_result: dict[str, Any] = {}
    audit_result: dict[str, Any] = {}
    anomaly_state = str(row["fallback_reason"] or "")
    shadow_clean_runs = previous_clean if same_config and not anomaly_state else 0
    if rule_mode == "shadow":
        shadow_result = {
            "predicted_boundary_page": predicted,
            "missed_count": len(missed),
            "missed_id_hash": payload_hash(missed),
            "clean": predicted is not None and not missed,
        }
        if shadow_result["clean"]:
            shadow_clean_runs += 1
        else:
            shadow_clean_runs = 0
            anomaly_state = "incremental_shadow_miss" if missed else "shadow_boundary_not_proven"
        _append_event(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            event_type="shadow_comparison",
            severity="info" if shadow_result["clean"] else "warning",
            details=shadow_result,
        )
    if rule_mode == "audit":
        audit_result = {
            "predicted_boundary_page": predicted,
            "missed_count": len(missed),
            "missed_id_hash": payload_hash(missed),
            "clean": predicted is not None and not missed,
        }
        if missed:
            timestamp = now_iso()
            if previous is not None:
                conn.execute(
                    """
                    UPDATE hh_stream_checkpoints SET anomaly_state = 'audit_discrepancy',
                        eligibility_state = 'invalidated', invalidated_at = ?,
                        invalidation_reason = ?, updated_at = ?
                    WHERE source = 'hh' AND stream_key = ?
                    """,
                    (
                        timestamp,
                        "Периодический полный аудит обнаружил новые или изменённые ID "
                        "за предсказанной границей.",
                        timestamp,
                        stream_key,
                    ),
                )
                invalidated = conn.execute(
                    "SELECT * FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
                    (stream_key,),
                ).fetchone()
                snapshot = dict(invalidated) if invalidated is not None else {}
                digest = payload_hash(snapshot)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO hh_stream_checkpoint_history (
                        source, stream_key, run_id, checkpoint_version,
                        event_type, snapshot_json, snapshot_hash, created_at
                    ) VALUES ('hh', ?, ?, ?, 'audit_invalidated', ?, ?, ?)
                    """,
                    (
                        stream_key,
                        run_id,
                        int(previous["checkpoint_version"]),
                        canonical_json(snapshot),
                        digest,
                        timestamp,
                    ),
                )
            _append_event(
                conn,
                run_id=run_id,
                stream_key=stream_key,
                event_type="incremental_safety_failure",
                severity="failure",
                details=audit_result,
            )
            _set_stream_blocked(
                conn,
                run_id=run_id,
                stream_key=stream_key,
                code="audit_incremental_discrepancy",
                reason=(
                    "Полный аудит обнаружил новую или изменённую вакансию за "
                    "предсказанной границей delta; контрольная точка инвалидирована, "
                    "требуется возврат в shadow или full."
                ),
            )
            return {
                "run_id": run_id,
                "stream_key": stream_key,
                "completed": False,
                "audit_failure": True,
                "audit_result": audit_result,
                "blocker": {
                    "code": "audit_incremental_discrepancy",
                    "reason": "Полный аудит обнаружил расхождение инкрементальной безопасности.",
                },
            }
        _append_event(
            conn,
            run_id=run_id,
            stream_key=stream_key,
            event_type="audit_comparison",
            severity="info",
            details=audit_result,
        )
    stop_reason = "source_exhausted"
    if rule_mode == "delta":
        stop_reason = "proven_known_boundary"
    elif source_kind == "personal_recommendations" and not bool(row["source_exhausted"]):
        stop_reason = "personal_novelty_or_configured_boundary"
    elif row["fallback_reason"]:
        stop_reason = "full_fallback_after_unproven_boundary"
    boundary_page = (
        int(row["boundary_proven_page"])
        if row["boundary_proven_page"] is not None
        else int(row["last_verified_page"])
    )
    sample = _boundary_sample(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        boundary_page=boundary_page,
    )
    run_date = str(
        conn.execute("SELECT run_date FROM daily_runs WHERE run_id = ?", (run_id,)).fetchone()[0]
    )
    snapshot = _checkpoint_snapshot(
        row,
        settings=settings,
        run_date=run_date,
        stop_reason=stop_reason,
        shadow_result=shadow_result,
        audit_result=audit_result,
        shadow_clean_runs=shadow_clean_runs,
        anomaly_state=anomaly_state,
        boundary_sample=sample,
    )
    version = _persist_checkpoint(
        conn,
        snapshot=snapshot,
        event_type="successful_checkpoint",
    )
    conn.execute(
        """
        UPDATE hh_stream_runs SET state = 'completed', completed_at = ?, updated_at = ?
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (now_iso(), now_iso(), run_id, stream_key),
    )
    manifest = build_completion_manifest(
        conn,
        settings,
        run_id=run_id,
        stream_key=stream_key,
        stop_reason=stop_reason,
        shadow_result=shadow_result,
        audit_result=audit_result,
        cursor_candidate={**snapshot["cursor"], "checkpoint_version": version},
    )
    conn.execute(
        """
        UPDATE hh_stream_runs SET completion_manifest_json = ?, updated_at = ?
        WHERE run_id = ? AND source = 'hh' AND stream_key = ?
        """,
        (canonical_json(manifest), now_iso(), run_id, stream_key),
    )
    return {
        "run_id": run_id,
        "stream_key": stream_key,
        "completed": True,
        "idempotent": False,
        "checkpoint_version": version,
        "shadow_clean_runs": shadow_clean_runs,
        "delta_eligible": snapshot["eligibility_state"] == "eligible",
        "manifest": manifest,
    }


def build_completion_manifest(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str,
    stop_reason: str = "",
    shadow_result: Mapping[str, Any] | None = None,
    audit_result: Mapping[str, Any] | None = None,
    cursor_candidate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = build_p1_checkpoint_manifest(
        conn, settings, run_id=run_id, stream_key=stream_key
    )
    row = conn.execute(
        "SELECT * FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if not stop_reason:
        rule_mode = _effective_rule_mode(row)
        stop_reason = (
            "proven_known_boundary"
            if rule_mode == "delta" and row["boundary_proven_page"] is not None
            else "source_exhausted"
            if row["source_exhausted"]
            else "personal_novelty_or_configured_boundary"
        )
    manifest.update(
        {
            "stop_reason": stop_reason,
            "shadow_audit_comparison": {
                "shadow": dict(shadow_result or {}),
                "audit": dict(audit_result or {}),
            },
            "cursor_candidate": dict(cursor_candidate or {}),
            "remote_boundary_verified": True,
            "blockers": [],
        }
    )
    return manifest


def validate_manifest_v2(
    manifest: Mapping[str, Any],
    *,
    expected_run_id: str,
    expected_step_key: str,
    expected_item_key: str,
    expected_kind: str,
    completion: bool,
) -> dict[str, Any]:
    """Validate P2 coverage manifest v2 while legacy v1 stays in P1."""

    if not isinstance(manifest, Mapping):
        raise ValueError("Манифест HH v2 должен быть объектом JSON.")
    normalized = dict(manifest)
    if normalized.get("manifest_version") != MANIFEST_VERSION:
        raise ValueError("Манифест HH v2 требует manifest_version = 2.")
    for field, expected in (
        ("run_id", expected_run_id),
        ("step_key", expected_step_key),
        ("item_key", expected_item_key),
        ("kind", expected_kind),
    ):
        if normalized.get(field, "") != expected:
            raise ValueError(f"Поле {field} должно быть равно {expected!r}.")
    _parse_iso(normalized.get("observed_at"), "observed_at")
    captured_scope = _json_object(normalized.get("captured_scope"), "captured_scope")
    _nonempty_string(captured_scope.get("stream_key"), "captured_scope.stream_key", 256)
    source_kind = _nonempty_string(
        captured_scope.get("source_kind"), "captured_scope.source_kind", 64
    )
    if source_kind not in SOURCE_KINDS:
        raise ValueError("captured_scope.source_kind неподдерживаем.")
    for field in ("query_fingerprint", "configuration_fingerprint"):
        value = _nonempty_string(captured_scope.get(field), f"captured_scope.{field}", 64)
        if not HEX_64_RE.fullmatch(value):
            raise ValueError(f"captured_scope.{field} должен быть SHA-256.")
    if normalized.get("adapter_version") != ADAPTER_VERSION:
        raise ValueError(f"Манифест HH v2 требует adapter_version={ADAPTER_VERSION}.")
    mode = _nonempty_string(normalized.get("acquisition_mode"), "acquisition_mode", 32)
    if mode not in ACQUISITION_MODES:
        raise ValueError("Неподдерживаемый acquisition_mode.")
    session = _json_object(normalized.get("session_capability"), "session_capability")
    state = _nonempty_string(
        session.get("session_id_state"), "session_capability.session_id_state", 32
    )
    fingerprint = _nonempty_string(
        session.get("session_fingerprint"), "session_capability.session_fingerprint", 64
    )
    if state not in SESSION_STATES or not HEX_64_RE.fullmatch(fingerprint):
        raise ValueError("Данные о доступности ID сессии в манифесте v2 неполны или необъяснимо пусты.")
    counts = _json_object(normalized.get("counts"), "counts")
    count_fields = (
        "raw",
        "unique",
        "known_unchanged",
        "known_changed",
        "new",
        "duplicate_on_page",
        "duplicate_across_pages",
        "duplicate_across_streams",
        "processed",
        "reconciled",
        "blocked",
    )
    for field in count_fields:
        _nonnegative_int(counts.get(field), f"counts.{field}")
    if counts["known_unchanged"] + counts["known_changed"] + counts["new"] != counts["unique"]:
        raise ValueError(
            "known_unchanged + known_changed + new должно равняться canonical unique."
        )
    pages = _json_list(normalized.get("pages"), "pages")
    if len(pages) > 1_000:
        raise ValueError("Манифест v2 содержит слишком много страниц.")
    page_indexes: set[int] = set()
    for index, raw_page in enumerate(pages):
        page = _json_object(raw_page, f"pages[{index}]")
        page_index = _nonnegative_int(page.get("page_index"), f"pages[{index}].page_index")
        recapture = _nonnegative_int(page.get("recapture_no"), f"pages[{index}].recapture_no")
        if recapture < 1:
            raise ValueError("recapture_no должен начинаться с 1.")
        for field in ("capture_hash", "canonical_id_set_hash"):
            value = _nonempty_string(page.get(field), f"pages[{index}].{field}", 64)
            if not HEX_64_RE.fullmatch(value):
                raise ValueError(f"pages[{index}].{field} должен быть SHA-256.")
        drift_state = _nonempty_string(
            page.get("count_drift_state"), f"pages[{index}].count_drift_state", 32
        )
        if drift_state not in {"none", "awaiting_recapture", "verified", "conflict"}:
            raise ValueError("Неподдерживаемый count_drift_state.")
        if page.get("verified") is True:
            if page_index in page_indexes:
                raise ValueError("Манифест содержит несколько проверенных снимков одной страницы.")
            page_indexes.add(page_index)
    boundary = _json_object(normalized.get("completion_boundary"), "completion_boundary")
    blockers = _json_list(normalized.get("blockers", []), "blockers")
    if completion:
        if normalized.get("remote_boundary_verified") is not True:
            raise ValueError("Завершающий манифест v2 требует remote_boundary_verified=true.")
        if blockers:
            raise ValueError("Завершающий манифест v2 не может содержать блокировки.")
        unresolved_conflict_pages = {
            page.get("page_index")
            for page in pages
            if page.get("count_drift_state") == "conflict"
            and page.get("page_index") not in page_indexes
        }
        unresolved_drift_pages = {
            page.get("page_index")
            for page in pages
            if page.get("count_drift_state") == "awaiting_recapture"
            and page.get("page_index") not in page_indexes
        }
        if unresolved_conflict_pages or unresolved_drift_pages:
            raise ValueError("Непроверенное расхождение счётчика блокирует завершающий манифест v2.")
        if page_indexes and page_indexes != set(range(max(page_indexes) + 1)):
            raise ValueError("Проверенные страницы в завершающем манифесте должны быть непрерывными.")
        stop_reason = _nonempty_string(normalized.get("stop_reason"), "stop_reason", 128)
        proof = _json_object(normalized.get("boundary_proof"), "boundary_proof")
        source_exhausted = boundary.get("source_exhausted") is True
        boundary_proven = boundary.get("boundary_proven_page") is not None
        personal_boundary = bool(
            source_kind == "personal_recommendations" and boundary_proven
        )
        if (
            mode in {"full", "shadow", "audit"}
            and not source_exhausted
            and not personal_boundary
        ):
            raise ValueError("Манифест v2 для full, shadow и audit требует полного обхода пагинации.")
        if mode == "delta" and not boundary_proven:
            raise ValueError("Манифест v2 для delta требует доказанной границы известных результатов.")
        if mode == "resume" and not (source_exhausted or boundary_proven):
            raise ValueError("Манифест v2 для resume требует исчерпания источника или доказанной границы.")
        if source_kind == "ordinary_search" and stop_reason.startswith("personal_"):
            raise ValueError("Причину остановки персональных рекомендаций нельзя использовать для обычного потока HH.")
        if mode == "delta":
            required_known = _nonnegative_int(
                proof.get("consecutive_known_pages_required"),
                "boundary_proof.consecutive_known_pages_required",
            )
            streak = _nonnegative_int(
                proof.get("known_page_streak"), "boundary_proof.known_page_streak"
            )
            if streak < required_known:
                raise ValueError("Доказательство границы delta не содержит требуемую последовательность известных страниц.")
            if proof.get("guard_page_required") is True and _nonnegative_int(
                proof.get("guard_pages_verified"), "boundary_proof.guard_pages_verified"
            ) < 1:
                raise ValueError("Доказательство границы delta не содержит обязательную защитную страницу.")
    return normalized


def p1_target(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    source_kind: str,
) -> dict[str, str]:
    step_key, item_key, kind = _p1_target(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        source_kind=source_kind,
    )
    return {"step_key": step_key, "item_key": item_key, "kind": kind}


def record_validation_failure(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    code: str,
    reason: str,
) -> dict[str, Any]:
    row = conn.execute(
        "SELECT source_kind FROM hh_stream_runs WHERE run_id = ? AND source = 'hh' AND stream_key = ?",
        (run_id, stream_key),
    ).fetchone()
    if row is None:
        raise ValueError("Поток сбора не найден для фиксации блокировки.")
    _set_stream_blocked(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        code=code,
        reason=reason,
    )
    return p1_target(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        source_kind=str(row["source_kind"]),
    )


def inspect_checkpoint_state(
    conn: sqlite3.Connection,
    *,
    stream_key: str | None = None,
    run_id: str | None = None,
    history_limit: int = 20,
) -> dict[str, Any]:
    params: list[Any] = []
    checkpoint_where = ""
    if stream_key:
        checkpoint_where = " WHERE stream_key = ?"
        params.append(stream_key)
    checkpoints = [
        {
            **dict(row),
            "covered_range": json.loads(str(row["covered_range_json"])),
            "boundary_sample": json.loads(str(row["boundary_sample_json"])),
            "last_shadow_result": json.loads(str(row["last_shadow_result_json"])),
            "last_audit_result": json.loads(str(row["last_audit_result_json"])),
            "cursor": json.loads(str(row["cursor_json"])),
        }
        for row in conn.execute(
            "SELECT * FROM hh_stream_checkpoints" + checkpoint_where + " ORDER BY stream_key",
            tuple(params),
        ).fetchall()
    ]
    run_params: list[Any] = []
    clauses: list[str] = []
    if run_id:
        clauses.append("run_id = ?")
        run_params.append(run_id)
    if stream_key:
        clauses.append("stream_key = ?")
        run_params.append(stream_key)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    runs = [
        {
            key: row[key]
            for key in (
                "run_id",
                "stream_key",
                "source_kind",
                "requested_mode",
                "effective_mode",
                "resume_from_mode",
                "state",
                "next_page",
                "last_verified_page",
                "predicted_boundary_page",
                "boundary_proven_page",
                "known_page_streak",
                "source_exhausted",
                "unresolved_drift_page",
                "fallback_reason",
                "blocker_code",
                "blocker_reason",
            )
        }
        for row in conn.execute(
            "SELECT * FROM hh_stream_runs" + where + " ORDER BY run_id, stream_key",
            tuple(run_params),
        ).fetchall()
    ]
    history_params: list[Any] = []
    history_where = ""
    if stream_key:
        history_where = " WHERE stream_key = ?"
        history_params.append(stream_key)
    history = [
        {
            "id": row["id"],
            "stream_key": row["stream_key"],
            "run_id": row["run_id"],
            "checkpoint_version": row["checkpoint_version"],
            "event_type": row["event_type"],
            "snapshot_hash": row["snapshot_hash"],
            "created_at": row["created_at"],
        }
        for row in conn.execute(
            "SELECT * FROM hh_stream_checkpoint_history"
            + history_where
            + " ORDER BY id DESC LIMIT ?",
            (*history_params, max(1, min(history_limit, 200))),
        ).fetchall()
    ]
    events = [
        {
            "id": row["id"],
            "run_id": row["run_id"],
            "stream_key": row["stream_key"],
            "event_type": row["event_type"],
            "severity": row["severity"],
            "details": json.loads(str(row["details_json"])),
            "created_at": row["created_at"],
        }
        for row in conn.execute(
            "SELECT * FROM hh_incremental_events"
            + (" WHERE stream_key = ?" if stream_key else "")
            + " ORDER BY id DESC LIMIT ?",
            ((stream_key, max(1, min(history_limit, 200))) if stream_key else (max(1, min(history_limit, 200)),)),
        ).fetchall()
    ]
    result = {"checkpoints": checkpoints, "runs": runs, "history": history, "events": events}
    if run_id:
        result["run_totals"] = run_reconciliation_totals(conn, run_id=run_id)
    return result


def invalidate_checkpoint(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    stream_key: str,
    reason: str,
) -> dict[str, Any]:
    reason = _nonempty_string(reason, "reason", 1000)
    row = conn.execute(
        "SELECT * FROM hh_stream_checkpoints WHERE source = 'hh' AND stream_key = ?",
        (stream_key,),
    ).fetchone()
    if row is None:
        raise ValueError("Контрольная точка потока не найдена.")
    _p1_target(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        source_kind=str(row["source_kind"]),
    )
    timestamp = now_iso()
    conn.execute(
        """
        UPDATE hh_stream_checkpoints SET eligibility_state = 'invalidated',
            anomaly_state = 'manual_invalidation', invalidated_at = ?,
            invalidation_reason = ?, updated_at = ?
        WHERE source = 'hh' AND stream_key = ?
        """,
        (timestamp, reason, timestamp, stream_key),
    )
    snapshot = {
        **dict(row),
        "eligibility_state": "invalidated",
        "anomaly_state": "manual_invalidation",
        "invalidated_at": timestamp,
        "invalidation_reason": reason,
        "updated_at": timestamp,
    }
    digest = payload_hash(snapshot)
    conn.execute(
        """
        INSERT OR IGNORE INTO hh_stream_checkpoint_history (
            source, stream_key, run_id, checkpoint_version, event_type,
            snapshot_json, snapshot_hash, created_at
        ) VALUES ('hh', ?, ?, ?, 'invalidated', ?, ?, ?)
        """,
        (
            stream_key,
            run_id,
            int(row["checkpoint_version"]),
            canonical_json(snapshot),
            digest,
            timestamp,
        ),
    )
    _append_event(
        conn,
        run_id=run_id,
        stream_key=stream_key,
        event_type="checkpoint_invalidated",
        severity="warning",
        details={"reason": reason, "checkpoint_version": int(row["checkpoint_version"])},
    )
    return {
        "run_id": run_id,
        "stream_key": stream_key,
        "invalidated": True,
        "checkpoint_version": int(row["checkpoint_version"]),
        "reason": reason,
    }


def next_acquisition_work(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    run_id: str,
    stream_key: str | None = None,
) -> dict[str, Any]:
    run = conn.execute("SELECT status FROM daily_runs WHERE run_id = ?", (run_id,)).fetchone()
    if run is None:
        raise ValueError("Указанный ежедневный запуск P1 не найден.")
    params: list[Any] = [run_id]
    where = "run_id = ? AND source = 'hh' AND state <> 'completed'"
    if stream_key:
        where += " AND stream_key = ?"
        params.append(stream_key)
    rows = conn.execute(
        "SELECT stream_key FROM hh_stream_runs WHERE " + where + " ORDER BY stream_key",
        tuple(params),
    ).fetchall()
    items = [
        {
            "stream_key": str(row["stream_key"]),
            "next_safe_action": _next_action_for_stream(
                conn,
                run_id=run_id,
                stream_key=str(row["stream_key"]),
                limit=settings.search.hh_acquisition.max_returned_ids,
            ),
        }
        for row in rows
    ]
    return {
        "run_id": run_id,
        "work": items[:20],
        "truncated": max(len(items) - 20, 0),
        "run_totals": run_reconciliation_totals(conn, run_id=run_id),
    }


def query_fingerprint_from_payload(payload: Any) -> str:
    if not isinstance(payload, (dict, list)):
        raise ValueError("План запроса должен быть объектом или массивом JSON.")
    return payload_hash(payload)
