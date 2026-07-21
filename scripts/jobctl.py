#!/usr/bin/env python3
"""Local job-search control plane.

SQLite is the source of truth. The script ingests structured vacancy data into
the database and regenerates reader-friendly views plus a static dashboard.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from jobsearch_config import Settings, display_path, load_settings
from search_coverage import (
    build_coverage_plan,
    semantic_vacancy_fingerprint,
    validate_coverage_manifest,
)


CODE_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 2

SETTINGS: Settings
ROOT: Path
DB_PATH: Path
DASHBOARD_PATH: Path
VIEWS_DIR: Path
REPORTS_DIR: Path
ARCHIVE_DIR: Path
PROJECT_TITLE: str
PROJECT_LOCALE: str
FOLLOW_UP_LIMIT: int
FOLLOW_UP_INTERVAL_BUSINESS_DAYS: int
PRIMARY_OUTREACH_CHANNEL: str
DIRECT_OUTREACH_CHANNELS: tuple[str, ...]
FOLLOW_UP_CHANNELS: tuple[str, ...]
MAX_DIRECT_MESSAGES_PER_ROUND: int
REQUIRED_SEARCH_STREAMS: tuple[str, ...]
DEFAULT_SEARCH_PERIOD_DAYS: int
SEARCH_ITEMS_PER_PAGE: int
CHANNEL_LABELS: dict[str, str]


def configure_runtime(config_path: Path | None = None) -> None:
    """Load local settings and update runtime paths and policy constants."""

    global SETTINGS, ROOT, DB_PATH, DASHBOARD_PATH, VIEWS_DIR, REPORTS_DIR
    global ARCHIVE_DIR, PROJECT_TITLE, PROJECT_LOCALE, FOLLOW_UP_LIMIT
    global FOLLOW_UP_INTERVAL_BUSINESS_DAYS, PRIMARY_OUTREACH_CHANNEL
    global DIRECT_OUTREACH_CHANNELS, FOLLOW_UP_CHANNELS
    global MAX_DIRECT_MESSAGES_PER_ROUND, REQUIRED_SEARCH_STREAMS
    global DEFAULT_SEARCH_PERIOD_DAYS, SEARCH_ITEMS_PER_PAGE, CHANNEL_LABELS

    SETTINGS = load_settings(CODE_ROOT, config_path)
    ROOT = SETTINGS.workspace_root
    DB_PATH = SETTINGS.paths.database
    DASHBOARD_PATH = SETTINGS.paths.dashboard
    VIEWS_DIR = SETTINGS.paths.views
    REPORTS_DIR = SETTINGS.paths.reports
    ARCHIVE_DIR = SETTINGS.paths.archive
    PROJECT_TITLE = SETTINGS.project.title
    PROJECT_LOCALE = SETTINGS.project.locale
    FOLLOW_UP_LIMIT = SETTINGS.follow_up.limit
    FOLLOW_UP_INTERVAL_BUSINESS_DAYS = SETTINGS.follow_up.interval_business_days
    PRIMARY_OUTREACH_CHANNEL = SETTINGS.follow_up.primary_channel
    DIRECT_OUTREACH_CHANNELS = SETTINGS.follow_up.direct_channels
    FOLLOW_UP_CHANNELS = (PRIMARY_OUTREACH_CHANNEL,) + DIRECT_OUTREACH_CHANNELS
    MAX_DIRECT_MESSAGES_PER_ROUND = SETTINGS.follow_up.max_direct_messages_per_round
    REQUIRED_SEARCH_STREAMS = SETTINGS.search.required_streams
    DEFAULT_SEARCH_PERIOD_DAYS = SETTINGS.search.default_period_days
    SEARCH_ITEMS_PER_PAGE = SETTINGS.search.items_per_page
    CHANNEL_LABELS = dict(SETTINGS.channel_labels)


# Load safe built-in defaults at import time. main() reloads the selected local
# config after it has parsed --config, including when the default config is bad.
configure_runtime(CODE_ROOT / "config" / ".defaults-only.toml")

VACANCY_ID_RE = re.compile(r"/vacancy/(\d+)")
FOLLOW_UP_STATUS_RE = re.compile(r"^FOLLOW_UP_(\d+)_")
DIRECT_SEND_CONFIDENCE = {"confirmed", "strong"}
CONTACT_CONFIDENCE = {"confirmed", "strong", "weak"}
CONTACT_RELATIONSHIPS = {
    "hiring_manager",
    "recruiter",
    "talent_partner",
    "founder",
    "other",
}
CONTACT_SEARCH_STATUSES = {
    "found",
    "reused_verified_contact",
    "not_found",
    "ambiguous",
    "unreachable",
}
OUTREACH_DELIVERY_STATUSES = {"sent", "failed", "not_sent"}
EXPECTED_TABLES = {
    "applications",
    "contact_searches",
    "employer_contacts",
    "evaluations",
    "followup_rounds",
    "import_issues",
    "interview_summaries",
    "outreach_messages",
    "source_hits",
    "search_coverage",
    "search_runs",
    "stage_events",
    "vacancies",
    "vacancy_fingerprints",
}

STAGE_ALIASES = {
    "": "seen",
    "discovered": "seen",
    "quick_screened": "seen",
    "deep_reviewed": "seen",
    "needs_review": "seen",
    "shortlisted": "seen",
    "track": "seen",
    "archived": "seen",
    "skipped": "seen",
    "duplicate": "seen",
    "needs_user_answers": "needs_input",
    "needs_followup": "follow_up",
    "hh_chat_started": "applied",
    "ai_screen_done": "applied",
}

TERMINAL_STAGES = {"rejected"}
ACTIVE_REVIEW_STAGES = {
    "needs_input",
    "follow_up",
}

STAGE_PRIORITY = {
    "seen": 10,
    "needs_input": 20,
    "follow_up": 30,
    "applied": 40,
    "interview_1": 50,
    "interview_2": 60,
    "interview_3": 70,
    "offer": 80,
    "rejected": 90,
}

STAGE_LABELS = {
    "seen": "Seen",
    "needs_input": "Needs Input",
    "follow_up": "Follow-up",
    "applied": "Applied",
    "interview_1": "Interview 1",
    "interview_2": "Interview 2",
    "interview_3": "Interview 3",
    "offer": "Offer",
    "rejected": "Rejected",
}

FUNNEL_STAGES = [
    "seen",
    "needs_input",
    "follow_up",
    "applied",
    "interview_1",
    "interview_2",
    "interview_3",
    "rejected",
    "offer",
]

def now_iso() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def add_business_days(value: str, business_days: int) -> str:
    if business_days < 1:
        raise ValueError("business_days must be positive")
    current = dt.date.fromisoformat(value)
    added = 0
    while added < business_days:
        current += dt.timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current.isoformat()


def follow_up_number_from_status(status: str | None) -> int:
    normalized = clean_cell(status)
    if normalized == "FOLLOW_UP_SENT_WAITING_EMPLOYER":
        return 1
    match = FOLLOW_UP_STATUS_RE.match(normalized)
    if not match:
        return 0
    return int(match.group(1))


def normalize_outreach_channel(value: str | None) -> str:
    channel = clean_cell(value).lower()
    aliases = {
        "head hunter": "hh",
        "headhunter": "hh",
        "hh.ru": "hh",
        "e-mail": "email",
        "mail": "email",
        "telegram": "telegram",
        "телеграм": "telegram",
        "max": "max",
        "макс": "max",
        "linkedin": "linkedin",
        "linked in": "linkedin",
        "signal messenger": "signal",
        "whats app": "whatsapp",
    }
    return aliases.get(channel, channel)


def unique_outreach_channels(
    values: list[str] | tuple[str, ...],
    allowed_channels: tuple[str, ...] | None = None,
) -> list[str]:
    allowed_channels = allowed_channels or DIRECT_OUTREACH_CHANNELS
    result: list[str] = []
    for value in values:
        channel = normalize_outreach_channel(value)
        if not channel:
            continue
        if channel not in allowed_channels:
            allowed = ", ".join(allowed_channels)
            raise ValueError(f"Unsupported outreach channel '{value}'. Allowed: {allowed}")
        if channel not in result:
            result.append(channel)
    return result


def canonical_stage(stage: str | None) -> str:
    key = (stage or "").strip().lower()
    return STAGE_ALIASES.get(key, key if key in STAGE_PRIORITY else "seen")


def clean_cell(value: str | None) -> str:
    if not value:
        return ""
    value = value.replace("\\|", "|")
    value = value.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def project_relative_file_path(path: Path) -> str:
    candidate = path if path.is_absolute() else ROOT / path
    resolved = candidate.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"File not found: {candidate}")
    if not resolved.is_file():
        raise ValueError(f"Expected a file path: {candidate}")
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError as exc:
        raise ValueError(f"Interview summary must be inside the project folder: {resolved}") from exc


def parse_interview_no(stage: str | None) -> int | None:
    match = re.search(r"interview[_\s-]*(\d+)", (stage or "").strip().lower())
    if not match:
        return None
    return int(match.group(1))


def normalize_url(url: str) -> str:
    url = clean_cell(url)
    if not url:
        return ""
    parts = urlsplit(url)
    if not parts.scheme:
        return url
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", parts.fragment))


def vacancy_external_id(channel: str, url: str, title: str, company: str) -> str:
    match = VACANCY_ID_RE.search(url or "")
    if match:
        return f"hh:{match.group(1)}"
    basis = url or f"{channel}:{title}:{company}"
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]
    return f"{channel}:{digest}"


def to_int(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"-?\d+", str(value))
    return int(match.group(0)) if match else None


def detect_stage(status: str, source_kind: str = "") -> str:
    raw = (status or "").strip()
    text = raw.lower()
    text_ru = text.replace("ё", "е")

    if not raw:
        return "seen"
    if "offer" in text or "офер" in text_ru:
        return "offer"
    if "interview_3" in text or "собеседование 3" in text_ru:
        return "interview_3"
    if "interview_2" in text or "собеседование 2" in text_ru:
        return "interview_2"
    if "interview_1" in text or "собеседование 1" in text_ru or "интервью" in text_ru:
        return "interview_1"
    if "rejected" in text or "отказ" in text_ru:
        return "rejected"
    if "ai_screen_done" in text or "ai-screen" in text or "ai screen" in text:
        return "applied"
    if "hh_chat" in text or "чат" in text_ru:
        return "applied"
    if "needs_input" in text or "needs_user_answers" in text or "нужны ответы" in text_ru or "анкета" in text_ru:
        return "needs_input"
    if "applied_needs_followup" in text or "follow-up" in text or "follow_up" in text:
        return "follow_up"
    if (
        "applied" in text
        or "отклик отправлен" in text_ru
        or "вы откликнулись" in text_ru
        or "спасибо за отклик" in text_ru
        or "otklik otpravlen" in text
        or "spasibo" in text
    ):
        return "applied"
    if "shortlisted" in text or "apply_now" in text:
        return "seen"
    if "manual_review" in text or "needs_review" in text or text == "review" or "review" in text:
        return "seen"
    if text == "track" or "track_only" in text or "отслеж" in text_ru:
        return "seen"
    if "duplicate" in text or "дуб" in text_ru:
        return "seen"
    if "low_fit" in text or "skip" in text or "skipped" in text or "не откликаться" in text_ru:
        return "seen"
    if source_kind == "review":
        return "seen"
    if source_kind == "shortlist":
        return "seen"
    if source_kind == "application":
        return "applied"
    return "seen"


def better_stage(current: str | None, new: str | None) -> str:
    current = canonical_stage(current)
    new = canonical_stage(new)
    if not current:
        return new or "seen"
    if not new:
        return current
    return new if STAGE_PRIORITY.get(new, 0) >= STAGE_PRIORITY.get(current, 0) else current


def connect_db(path: Path | None = None) -> sqlite3.Connection:
    path = path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def missing_schema_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    present = {str(row[0]) for row in rows}
    return sorted(EXPECTED_TABLES - present)


def ensure_schema(conn: sqlite3.Connection) -> None:
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version {version} is newer than supported version "
            f"{SCHEMA_VERSION}"
        )
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'vacancies'"
    ).fetchone()
    if not exists:
        reset_schema(conn)
    else:
        if version < SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema version {version} requires an explicit migration to "
                f"{SCHEMA_VERSION}. Run: jobctl migrate-schema"
            )
        ensure_auxiliary_schema(conn)
        missing = missing_schema_tables(conn)
        if missing:
            raise RuntimeError(
                "Database is missing required tables: " + ", ".join(missing)
            )


def reset_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS outreach_messages;
        DROP TABLE IF EXISTS followup_rounds;
        DROP TABLE IF EXISTS contact_searches;
        DROP TABLE IF EXISTS employer_contacts;
        DROP TABLE IF EXISTS search_coverage;
        DROP TABLE IF EXISTS search_runs;
        DROP TABLE IF EXISTS source_hits;
        DROP TABLE IF EXISTS evaluations;
        DROP TABLE IF EXISTS applications;
        DROP TABLE IF EXISTS stage_events;
        DROP TABLE IF EXISTS interview_summaries;
        DROP TABLE IF EXISTS import_issues;
        DROP TABLE IF EXISTS vacancy_fingerprints;
        DROP TABLE IF EXISTS vacancies;

        CREATE TABLE vacancies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            source TEXT,
            external_id TEXT NOT NULL UNIQUE,
            url TEXT,
            title TEXT,
            company TEXT,
            first_seen_date TEXT,
            last_seen_date TEXT,
            latest_status TEXT,
            latest_stage TEXT,
            score INTEGER,
            role_type TEXT,
            reason TEXT,
            risks TEXT,
            open_questions TEXT,
            next_action TEXT,
            follow_up_date TEXT,
            resume_version TEXT,
            cover_letter TEXT,
            origin_files TEXT,
            updated_at TEXT
        );

        CREATE TABLE source_hits (
            id INTEGER PRIMARY KEY,
            vacancy_id INTEGER NOT NULL,
            seen_date TEXT,
            source_name TEXT,
            source_stream TEXT,
            raw_status TEXT,
            quick_score INTEGER,
            reason TEXT,
            next_action TEXT,
            origin_file TEXT,
            line_no INTEGER,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        );

        CREATE TABLE vacancy_fingerprints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            fingerprint TEXT NOT NULL UNIQUE,
            created_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE
        );

        CREATE INDEX idx_vacancy_fingerprints_vacancy
            ON vacancy_fingerprints(vacancy_id);

        CREATE TABLE search_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            required_streams TEXT NOT NULL,
            total_unique INTEGER NOT NULL DEFAULT 0,
            known_count INTEGER NOT NULL DEFAULT 0,
            new_count INTEGER NOT NULL DEFAULT 0,
            issue_count INTEGER NOT NULL DEFAULT 0,
            manifest_file TEXT,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(run_date, source)
        );

        CREATE TABLE search_coverage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_run_id INTEGER NOT NULL,
            stream_key TEXT NOT NULL,
            status TEXT NOT NULL,
            query_url TEXT,
            query_text TEXT,
            search_period_days INTEGER,
            page_size INTEGER,
            found INTEGER,
            pages_expected INTEGER,
            pages_visited INTEGER,
            extracted INTEGER,
            unique_count INTEGER,
            known_count INTEGER,
            new_count INTEGER,
            error TEXT,
            issues TEXT,
            FOREIGN KEY (search_run_id) REFERENCES search_runs(id) ON DELETE CASCADE,
            UNIQUE(search_run_id, stream_key)
        );

        CREATE INDEX idx_search_coverage_run
            ON search_coverage(search_run_id, stream_key);

        CREATE TABLE evaluations (
            id INTEGER PRIMARY KEY,
            vacancy_id INTEGER NOT NULL,
            evaluation_date TEXT,
            kind TEXT,
            score INTEGER,
            status TEXT,
            stage TEXT,
            role_type TEXT,
            reason TEXT,
            risks TEXT,
            open_questions TEXT,
            recommendation TEXT,
            origin_file TEXT,
            line_no INTEGER,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        );

        CREATE TABLE applications (
            id INTEGER PRIMARY KEY,
            vacancy_id INTEGER NOT NULL,
            applied_date TEXT,
            status TEXT,
            stage TEXT,
            score INTEGER,
            resume_version TEXT,
            cover_letter TEXT,
            why_applied TEXT,
            risks TEXT,
            follow_up_date TEXT,
            origin_file TEXT,
            line_no INTEGER,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        );

        CREATE TABLE stage_events (
            id INTEGER PRIMARY KEY,
            vacancy_id INTEGER NOT NULL,
            event_date TEXT,
            stage TEXT,
            status TEXT,
            note TEXT,
            origin_file TEXT,
            line_no INTEGER,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        );

        CREATE TABLE interview_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            interview_no INTEGER NOT NULL,
            stage TEXT NOT NULL,
            summary_date TEXT,
            title TEXT,
            file_path TEXT NOT NULL,
            note TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            UNIQUE(vacancy_id, interview_no, file_path)
        );

        CREATE INDEX idx_interview_summaries_vacancy
            ON interview_summaries(vacancy_id, interview_no);

        CREATE TABLE employer_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            person_name TEXT NOT NULL,
            person_role TEXT,
            relationship TEXT NOT NULL,
            confidence TEXT NOT NULL,
            channel TEXT NOT NULL,
            contact_address TEXT NOT NULL,
            profile_url TEXT,
            evidence_url TEXT,
            evidence_note TEXT,
            verified_date TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            UNIQUE(vacancy_id, channel, contact_address)
        );

        CREATE INDEX idx_employer_contacts_vacancy
            ON employer_contacts(vacancy_id, is_active, confidence, relationship);

        CREATE TABLE contact_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            search_date TEXT NOT NULL,
            status TEXT NOT NULL,
            channels_checked TEXT NOT NULL,
            note TEXT,
            created_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        );

        CREATE INDEX idx_contact_searches_vacancy
            ON contact_searches(vacancy_id, search_date, id);

        CREATE TABLE followup_rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            sent_date TEXT NOT NULL,
            next_follow_up_date TEXT,
            status TEXT NOT NULL,
            contact_search_status TEXT NOT NULL,
            channels_checked TEXT NOT NULL,
            research_note TEXT,
            created_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            UNIQUE(vacancy_id, round_number)
        );

        CREATE INDEX idx_followup_rounds_vacancy
            ON followup_rounds(vacancy_id, round_number);

        CREATE TABLE outreach_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            followup_round_id INTEGER NOT NULL,
            vacancy_id INTEGER NOT NULL,
            contact_id INTEGER,
            channel TEXT NOT NULL,
            recipient_name TEXT,
            recipient_address TEXT,
            message_text TEXT NOT NULL,
            delivery_status TEXT NOT NULL,
            evidence_note TEXT,
            sent_at TEXT,
            created_at TEXT,
            FOREIGN KEY (followup_round_id) REFERENCES followup_rounds(id),
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            FOREIGN KEY (contact_id) REFERENCES employer_contacts(id),
            UNIQUE(followup_round_id, channel)
        );

        CREATE INDEX idx_outreach_messages_vacancy
            ON outreach_messages(vacancy_id, followup_round_id, channel);

        CREATE TABLE import_issues (
            id INTEGER PRIMARY KEY,
            origin_file TEXT,
            line_no INTEGER,
            issue TEXT,
            raw_text TEXT
        );
        """
    )
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()


def ensure_auxiliary_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS vacancy_fingerprints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            fingerprint TEXT NOT NULL UNIQUE,
            created_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_vacancy_fingerprints_vacancy
            ON vacancy_fingerprints(vacancy_id);

        CREATE TABLE IF NOT EXISTS search_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            required_streams TEXT NOT NULL,
            total_unique INTEGER NOT NULL DEFAULT 0,
            known_count INTEGER NOT NULL DEFAULT 0,
            new_count INTEGER NOT NULL DEFAULT 0,
            issue_count INTEGER NOT NULL DEFAULT 0,
            manifest_file TEXT,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(run_date, source)
        );

        CREATE TABLE IF NOT EXISTS search_coverage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_run_id INTEGER NOT NULL,
            stream_key TEXT NOT NULL,
            status TEXT NOT NULL,
            query_url TEXT,
            query_text TEXT,
            search_period_days INTEGER,
            page_size INTEGER,
            found INTEGER,
            pages_expected INTEGER,
            pages_visited INTEGER,
            extracted INTEGER,
            unique_count INTEGER,
            known_count INTEGER,
            new_count INTEGER,
            error TEXT,
            issues TEXT,
            FOREIGN KEY (search_run_id) REFERENCES search_runs(id) ON DELETE CASCADE,
            UNIQUE(search_run_id, stream_key)
        );

        CREATE INDEX IF NOT EXISTS idx_search_coverage_run
            ON search_coverage(search_run_id, stream_key);

        CREATE TABLE IF NOT EXISTS interview_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            interview_no INTEGER NOT NULL,
            stage TEXT NOT NULL,
            summary_date TEXT,
            title TEXT,
            file_path TEXT NOT NULL,
            note TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            UNIQUE(vacancy_id, interview_no, file_path)
        );

        CREATE INDEX IF NOT EXISTS idx_interview_summaries_vacancy
            ON interview_summaries(vacancy_id, interview_no);

        CREATE TABLE IF NOT EXISTS employer_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            person_name TEXT NOT NULL,
            person_role TEXT,
            relationship TEXT NOT NULL,
            confidence TEXT NOT NULL,
            channel TEXT NOT NULL,
            contact_address TEXT NOT NULL,
            profile_url TEXT,
            evidence_url TEXT,
            evidence_note TEXT,
            verified_date TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            UNIQUE(vacancy_id, channel, contact_address)
        );

        CREATE INDEX IF NOT EXISTS idx_employer_contacts_vacancy
            ON employer_contacts(vacancy_id, is_active, confidence, relationship);

        CREATE TABLE IF NOT EXISTS contact_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            search_date TEXT NOT NULL,
            status TEXT NOT NULL,
            channels_checked TEXT NOT NULL,
            note TEXT,
            created_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id)
        );

        CREATE INDEX IF NOT EXISTS idx_contact_searches_vacancy
            ON contact_searches(vacancy_id, search_date, id);

        CREATE TABLE IF NOT EXISTS followup_rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            sent_date TEXT NOT NULL,
            next_follow_up_date TEXT,
            status TEXT NOT NULL,
            contact_search_status TEXT NOT NULL,
            channels_checked TEXT NOT NULL,
            research_note TEXT,
            created_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            UNIQUE(vacancy_id, round_number)
        );

        CREATE INDEX IF NOT EXISTS idx_followup_rounds_vacancy
            ON followup_rounds(vacancy_id, round_number);

        CREATE TABLE IF NOT EXISTS outreach_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            followup_round_id INTEGER NOT NULL,
            vacancy_id INTEGER NOT NULL,
            contact_id INTEGER,
            channel TEXT NOT NULL,
            recipient_name TEXT,
            recipient_address TEXT,
            message_text TEXT NOT NULL,
            delivery_status TEXT NOT NULL,
            evidence_note TEXT,
            sent_at TEXT,
            created_at TEXT,
            FOREIGN KEY (followup_round_id) REFERENCES followup_rounds(id),
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id),
            FOREIGN KEY (contact_id) REFERENCES employer_contacts(id),
            UNIQUE(followup_round_id, channel)
        );

        CREATE INDEX IF NOT EXISTS idx_outreach_messages_vacancy
            ON outreach_messages(vacancy_id, followup_round_id, channel);
        """
    )
    conn.commit()


def merge_origin(existing: str | None, new_origin: str) -> str:
    origins = [x for x in (existing or "").split(",") if x]
    if new_origin not in origins:
        origins.append(new_origin)
    return ",".join(origins)


def upsert_vacancy(
    conn: sqlite3.Connection,
    *,
    channel: str,
    source: str,
    title: str,
    company: str,
    description: str,
    url: str,
    seen_date: str,
    status: str,
    stage: str,
    score: int | None = None,
    role_type: str = "",
    reason: str = "",
    risks: str = "",
    open_questions: str = "",
    next_action: str = "",
    follow_up_date: str = "",
    resume_version: str = "",
    cover_letter: str = "",
    origin_file: str = "",
) -> int:
    norm_url = normalize_url(url)
    stage = canonical_stage(stage)
    external_id = vacancy_external_id(channel, norm_url, title, company)
    semantic_fingerprint = semantic_vacancy_fingerprint(company, title, description)
    row = conn.execute("SELECT * FROM vacancies WHERE external_id = ?", (external_id,)).fetchone()
    if not row and semantic_fingerprint:
        row = conn.execute(
            """
            SELECT v.*
            FROM vacancy_fingerprints f
            JOIN vacancies v ON v.id = f.vacancy_id
            WHERE f.fingerprint = ?
            """,
            (semantic_fingerprint,),
        ).fetchone()
    now = now_iso()
    if not row:
        cur = conn.execute(
            """
            INSERT INTO vacancies (
                channel, source, external_id, url, title, company,
                first_seen_date, last_seen_date, latest_status, latest_stage,
                score, role_type, reason, risks, open_questions, next_action,
                follow_up_date, resume_version, cover_letter, origin_files, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                channel,
                source,
                external_id,
                norm_url,
                title,
                company,
                seen_date,
                seen_date,
                status,
                stage,
                score,
                role_type,
                reason,
                risks,
                open_questions,
                next_action,
                follow_up_date,
                resume_version,
                cover_letter,
                origin_file,
                now,
            ),
        )
        vacancy_id = int(cur.lastrowid)
        if semantic_fingerprint:
            conn.execute(
                """
                INSERT OR IGNORE INTO vacancy_fingerprints
                    (vacancy_id, fingerprint, created_at)
                VALUES (?, ?, ?)
                """,
                (vacancy_id, semantic_fingerprint, now),
            )
        return vacancy_id

    vacancy_id = int(row["id"])
    first_seen = min(filter(None, [row["first_seen_date"], seen_date]), default=seen_date)
    last_seen = max(filter(None, [row["last_seen_date"], seen_date]), default=seen_date)
    best_score = row["score"]
    if score is not None and (best_score is None or score > int(best_score)):
        best_score = score
    latest_stage = better_stage(row["latest_stage"], stage)

    def choose(field: str, new_value: str) -> str:
        return new_value or row[field] or ""

    conn.execute(
        """
        UPDATE vacancies
        SET source = ?, url = ?, title = ?, company = ?, first_seen_date = ?,
            last_seen_date = ?, latest_status = ?, latest_stage = ?, score = ?,
            role_type = ?, reason = ?, risks = ?, open_questions = ?,
            next_action = ?, follow_up_date = ?, resume_version = ?,
            cover_letter = ?, origin_files = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            choose("source", source),
            choose("url", norm_url),
            choose("title", title),
            choose("company", company),
            first_seen,
            last_seen,
            status or row["latest_status"],
            latest_stage,
            best_score,
            choose("role_type", role_type),
            choose("reason", reason),
            choose("risks", risks),
            choose("open_questions", open_questions),
            choose("next_action", next_action),
            choose("follow_up_date", follow_up_date),
            choose("resume_version", resume_version),
            choose("cover_letter", cover_letter),
            merge_origin(row["origin_files"], origin_file),
            now,
            vacancy_id,
        ),
    )
    if semantic_fingerprint:
        conn.execute(
            """
            INSERT OR IGNORE INTO vacancy_fingerprints
                (vacancy_id, fingerprint, created_at)
            VALUES (?, ?, ?)
            """,
            (vacancy_id, semantic_fingerprint, now),
        )
    return vacancy_id


def insert_stage_event(
    conn: sqlite3.Connection,
    vacancy_id: int,
    date: str,
    stage: str,
    status: str,
    note: str,
    origin_file: str,
    line_no: int,
) -> None:
    stage = canonical_stage(stage)
    existing = conn.execute(
        """
        SELECT 1
        FROM stage_events
        WHERE vacancy_id = ?
          AND COALESCE(event_date, '') = COALESCE(?, '')
          AND COALESCE(stage, '') = COALESCE(?, '')
          AND COALESCE(status, '') = COALESCE(?, '')
          AND COALESCE(note, '') = COALESCE(?, '')
          AND COALESCE(origin_file, '') = COALESCE(?, '')
        LIMIT 1
        """,
        (vacancy_id, date, stage, status, note, origin_file),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO stage_events (vacancy_id, event_date, stage, status, note, origin_file, line_no)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (vacancy_id, date, stage, status, note, origin_file, line_no),
    )


def source_files() -> list[Path]:
    return [DB_PATH]


def origin_for(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("vacancies", "items", "jobs", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def infer_channel(item: dict[str, Any], default_channel: str = "") -> str:
    channel = clean_cell(str(item.get("channel") or default_channel or ""))
    if channel:
        return channel
    url = clean_cell(str(item.get("url") or ""))
    if "hh.ru/" in url:
        return "hh"
    return "other"


def infer_kind(item: dict[str, Any], stage: str) -> str:
    stage = canonical_stage(stage)
    kind = clean_cell(str(item.get("kind") or item.get("type") or "")).lower()
    if kind:
        return kind
    if stage in {"follow_up", "applied", "interview_1", "interview_2", "interview_3", "offer"}:
        return "application"
    if stage in {"needs_input"}:
        return "review"
    return "screening"


def insert_source_hit_once(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    seen_date: str,
    source_name: str,
    source_stream: str,
    raw_status: str,
    quick_score: int | None,
    reason: str,
    next_action: str,
    origin_file: str,
    line_no: int,
) -> None:
    existing = conn.execute(
        """
        SELECT 1
        FROM source_hits
        WHERE vacancy_id = ?
          AND COALESCE(seen_date, '') = COALESCE(?, '')
          AND COALESCE(source_stream, '') = COALESCE(?, '')
          AND COALESCE(raw_status, '') = COALESCE(?, '')
          AND COALESCE(origin_file, '') = COALESCE(?, '')
        LIMIT 1
        """,
        (vacancy_id, seen_date, source_stream, raw_status, origin_file),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO source_hits (
            vacancy_id, seen_date, source_name, source_stream, raw_status,
            quick_score, reason, next_action, origin_file, line_no
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            seen_date,
            source_name,
            source_stream,
            raw_status,
            quick_score,
            reason,
            next_action,
            origin_file,
            line_no,
        ),
    )


def insert_evaluation_once(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    evaluation_date: str,
    kind: str,
    score: int | None,
    status: str,
    stage: str,
    role_type: str,
    reason: str,
    risks: str,
    open_questions: str,
    recommendation: str,
    origin_file: str,
    line_no: int,
) -> None:
    existing = conn.execute(
        """
        SELECT 1
        FROM evaluations
        WHERE vacancy_id = ?
          AND COALESCE(evaluation_date, '') = COALESCE(?, '')
          AND COALESCE(kind, '') = COALESCE(?, '')
          AND COALESCE(status, '') = COALESCE(?, '')
          AND COALESCE(origin_file, '') = COALESCE(?, '')
        LIMIT 1
        """,
        (vacancy_id, evaluation_date, kind, status, origin_file),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO evaluations (
            vacancy_id, evaluation_date, kind, score, status, stage, role_type,
            reason, risks, open_questions, recommendation, origin_file, line_no
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            evaluation_date,
            kind,
            score,
            status,
            stage,
            role_type,
            reason,
            risks,
            open_questions,
            recommendation,
            origin_file,
            line_no,
        ),
    )


def insert_application_once(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    applied_date: str,
    status: str,
    stage: str,
    score: int | None,
    resume_version: str,
    cover_letter: str,
    why_applied: str,
    risks: str,
    follow_up_date: str,
    origin_file: str,
    line_no: int,
) -> None:
    existing = conn.execute(
        """
        SELECT 1
        FROM applications
        WHERE vacancy_id = ?
          AND COALESCE(applied_date, '') = COALESCE(?, '')
          AND COALESCE(status, '') = COALESCE(?, '')
          AND COALESCE(origin_file, '') = COALESCE(?, '')
        LIMIT 1
        """,
        (vacancy_id, applied_date, status, origin_file),
    ).fetchone()
    if existing:
        return
    conn.execute(
        """
        INSERT INTO applications (
            vacancy_id, applied_date, status, stage, score, resume_version,
            cover_letter, why_applied, risks, follow_up_date, origin_file, line_no
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            applied_date,
            status,
            stage,
            score,
            resume_version,
            cover_letter,
            why_applied,
            risks,
            follow_up_date,
            origin_file,
            line_no,
        ),
    )


def ingest_item(
    conn: sqlite3.Connection,
    item: dict[str, Any],
    *,
    default_channel: str = "",
    default_source: str = "",
    origin_file: str = "",
    line_no: int = 0,
) -> int | None:
    channel = infer_channel(item, default_channel)
    title = clean_cell(str(item.get("title") or item.get("vacancy_title") or item.get("name") or ""))
    company = clean_cell(str(item.get("company") or item.get("employer") or ""))
    description = str(
        item.get("description")
        or item.get("description_html")
        or item.get("snippet")
        or item.get("summary")
        or ""
    )
    url = clean_cell(str(item.get("url") or item.get("href") or ""))
    if not title and not url:
        return None

    date = clean_cell(
        str(
            item.get("date")
            or item.get("seen_date")
            or item.get("evaluation_date")
            or item.get("applied_date")
            or dt.date.today().isoformat()
        )
    )
    source = clean_cell(str(item.get("source") or default_source or "manual_json"))
    source_stream = clean_cell(str(item.get("source_stream") or item.get("stream") or source))
    status = clean_cell(str(item.get("status") or item.get("latest_status") or "DISCOVERED"))
    stage = clean_cell(str(item.get("stage") or item.get("latest_stage") or ""))
    if not stage:
        stage = detect_stage(status)
    stage = canonical_stage(stage)
    kind = infer_kind(item, stage)
    score = to_int(str(item.get("score") or item.get("quick_score") or ""))
    reason = clean_cell(str(item.get("reason") or item.get("why_fit") or item.get("note") or ""))
    risks = clean_cell(str(item.get("risks") or ""))
    open_questions = clean_cell(str(item.get("open_questions") or item.get("questions") or ""))
    next_action = clean_cell(str(item.get("next_action") or item.get("recommendation") or ""))
    follow_up_date = clean_cell(str(item.get("follow_up_date") or ""))
    role_type = clean_cell(str(item.get("role_type") or ""))
    resume_version = clean_cell(str(item.get("resume_version") or ""))
    cover_letter = clean_cell(str(item.get("cover_letter") or ""))

    vacancy_id = upsert_vacancy(
        conn,
        channel=channel,
        source=source,
        title=title,
        company=company,
        description=description,
        url=url,
        seen_date=date,
        status=status,
        stage=stage,
        score=score,
        role_type=role_type,
        reason=reason,
        risks=risks,
        open_questions=open_questions,
        next_action=next_action,
        follow_up_date=follow_up_date,
        resume_version=resume_version,
        cover_letter=cover_letter,
        origin_file=origin_file,
    )

    insert_source_hit_once(
        conn,
        vacancy_id=vacancy_id,
        seen_date=date,
        source_name=channel,
        source_stream=source_stream,
        raw_status=status,
        quick_score=score,
        reason=reason,
        next_action=next_action,
        origin_file=origin_file,
        line_no=line_no,
    )

    if kind in {"review", "shortlist", "evaluation"} or stage in ACTIVE_REVIEW_STAGES:
        insert_evaluation_once(
            conn,
            vacancy_id=vacancy_id,
            evaluation_date=date,
            kind=kind,
            score=score,
            status=status,
            stage=stage,
            role_type=role_type,
            reason=reason,
            risks=risks,
            open_questions=open_questions,
            recommendation=next_action or status,
            origin_file=origin_file,
            line_no=line_no,
        )

    if kind == "application" or stage in {
        "follow_up",
        "applied",
        "interview_1",
        "interview_2",
        "interview_3",
        "offer",
    }:
        insert_application_once(
            conn,
            vacancy_id=vacancy_id,
            applied_date=date,
            status=status,
            stage=stage,
            score=score,
            resume_version=resume_version,
            cover_letter=cover_letter,
            why_applied=reason,
            risks=risks,
            follow_up_date=follow_up_date,
            origin_file=origin_file,
            line_no=line_no,
        )

    insert_stage_event(
        conn,
        vacancy_id,
        date,
        stage,
        status,
        reason or open_questions or next_action or source,
        origin_file,
        line_no,
    )
    return vacancy_id


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [{key: row[key] for key in row.keys()} for row in rows]


def build_snapshot(
    conn: sqlite3.Connection, db_path: Path | None = None
) -> dict[str, Any]:
    db_path = db_path or DB_PATH
    vacancies = rows_to_dicts(
        conn.execute(
            """
            SELECT * FROM vacancies
            ORDER BY COALESCE(last_seen_date, first_seen_date, '') DESC,
                     COALESCE(score, -1) DESC,
                     company,
                     title
            """
        ).fetchall()
    )
    for vacancy in vacancies:
        vacancy["latest_stage"] = canonical_stage(vacancy.get("latest_stage"))

    interview_summaries = rows_to_dicts(
        conn.execute(
            """
            SELECT id, vacancy_id, interview_no, stage, summary_date, title,
                   file_path, note, created_at, updated_at
            FROM interview_summaries
            ORDER BY interview_no ASC, COALESCE(summary_date, '') ASC, id ASC
            """
        ).fetchall()
    )
    summaries_by_vacancy: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for summary in interview_summaries:
        summaries_by_vacancy[int(summary["vacancy_id"])].append(summary)
    for vacancy in vacancies:
        vacancy["interview_summaries"] = summaries_by_vacancy.get(int(vacancy["id"]), [])

    employer_contacts = rows_to_dicts(
        conn.execute(
            """
            SELECT c.*, v.company, v.title, v.url
            FROM employer_contacts c
            JOIN vacancies v ON v.id = c.vacancy_id
            ORDER BY c.is_active DESC,
                     CASE c.relationship
                       WHEN 'hiring_manager' THEN 1
                       WHEN 'recruiter' THEN 2
                       WHEN 'talent_partner' THEN 3
                       WHEN 'founder' THEN 4
                       ELSE 5
                     END,
                     CASE c.confidence
                       WHEN 'confirmed' THEN 1
                       WHEN 'strong' THEN 2
                       ELSE 3
                     END,
                     c.person_name
            """
        ).fetchall()
    )
    channel_rank = {
        channel: index for index, channel in enumerate(DIRECT_OUTREACH_CHANNELS)
    }
    relationship_rank = {
        "hiring_manager": 1,
        "recruiter": 2,
        "talent_partner": 3,
        "founder": 4,
    }
    confidence_rank = {"confirmed": 1, "strong": 2, "weak": 3}
    employer_contacts.sort(
        key=lambda contact: (
            -int(contact.get("is_active") or 0),
            relationship_rank.get(str(contact.get("relationship") or ""), 5),
            confidence_rank.get(str(contact.get("confidence") or ""), 4),
            channel_rank.get(
                str(contact.get("channel") or ""), len(channel_rank) + 1
            ),
            str(contact.get("person_name") or "").casefold(),
        )
    )
    contacts_by_vacancy: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for contact in employer_contacts:
        contacts_by_vacancy[int(contact["vacancy_id"])].append(contact)
    for vacancy in vacancies:
        vacancy["employer_contacts"] = contacts_by_vacancy.get(int(vacancy["id"]), [])

    contact_searches = rows_to_dicts(
        conn.execute(
            """
            SELECT id, vacancy_id, search_date, status, channels_checked, note
            FROM contact_searches
            ORDER BY search_date ASC, id ASC
            """
        ).fetchall()
    )
    latest_contact_search_by_vacancy: dict[int, dict[str, Any]] = {}
    for search in contact_searches:
        latest_contact_search_by_vacancy[int(search["vacancy_id"])] = search

    followup_rounds = rows_to_dicts(
        conn.execute(
            """
            SELECT id, vacancy_id, round_number, sent_date, next_follow_up_date,
                   status, contact_search_status, channels_checked, research_note
            FROM followup_rounds
            ORDER BY round_number ASC, id ASC
            """
        ).fetchall()
    )
    outreach_messages = rows_to_dicts(
        conn.execute(
            """
            SELECT followup_round_id, vacancy_id, channel, recipient_name,
                   delivery_status, sent_at
            FROM outreach_messages
            ORDER BY id ASC
            """
        ).fetchall()
    )
    messages_by_round: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for message in outreach_messages:
        messages_by_round[int(message["followup_round_id"])].append(message)
    latest_round_by_vacancy: dict[int, dict[str, Any]] = {}
    for round_item in followup_rounds:
        round_item["touchpoints"] = messages_by_round.get(int(round_item["id"]), [])
        latest_round_by_vacancy[int(round_item["vacancy_id"])] = round_item

    channel_counts = Counter(v["channel"] for v in vacancies)
    stage_counts = Counter(v["latest_stage"] or "seen" for v in vacancies)
    status_counts = Counter(v["latest_status"] or "" for v in vacancies)
    applied_ids = {
        int(row["vacancy_id"])
        for row in conn.execute(
            """
            SELECT DISTINCT vacancy_id
            FROM applications
            WHERE stage IN ('applied', 'follow_up', 'interview_1', 'interview_2', 'interview_3', 'offer', 'rejected')
              AND LOWER(COALESCE(status, '')) NOT LIKE '%не отправлен%'
            """
        ).fetchall()
    }
    applied_ids.update(
        int(v["id"])
        for v in vacancies
        if v["latest_stage"] in {"applied", "follow_up", "interview_1", "interview_2", "interview_3", "offer"}
    )

    funnel_by_channel: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for vacancy in vacancies:
        channel = vacancy["channel"] or "other"
        stage = vacancy["latest_stage"] or "seen"
        funnel_by_channel[channel]["seen"] += 1
        if int(vacancy["id"]) in applied_ids:
            funnel_by_channel[channel]["applied"] += 1
        if stage not in {"seen", "applied"}:
            funnel_by_channel[channel][stage] += 1

    def review_candidate(vacancy: dict[str, Any]) -> bool:
        stage = vacancy.get("latest_stage") or "seen"
        if stage in ACTIVE_REVIEW_STAGES:
            return True
        status_text = " ".join(
            str(vacancy.get(field) or "").lower()
            for field in ("latest_status", "next_action", "open_questions")
        )
        review_markers = (
            "needs_review",
            "review",
            "apply_now",
            "await_user",
            "track",
            "manual",
        )
        terminal_markers = ("low_fit", "skip", "duplicate", "reject")
        if any(marker in status_text for marker in terminal_markers):
            return False
        if any(marker in status_text for marker in review_markers):
            return True
        return False

    active_review = [
        v
        for v in vacancies
        if review_candidate(v)
        and (v["score"] is None or int(v["score"]) >= 65 or v["latest_stage"] in ACTIVE_REVIEW_STAGES)
    ]
    active_review.sort(
        key=lambda v: (
            int(v["score"] or 0),
            v.get("last_seen_date") or "",
            v.get("company") or "",
        ),
        reverse=True,
    )

    recent = vacancies[:120]

    followups = rows_to_dicts(
        conn.execute(
            """
            SELECT v.id, v.channel, v.company, v.title, v.url, v.score, a.status,
                   a.follow_up_date, a.why_applied, a.risks
            FROM applications a
            JOIN vacancies v ON v.id = a.vacancy_id
            WHERE COALESCE(a.follow_up_date, '') NOT IN ('', '-', '—')
              AND LOWER(a.status) NOT LIKE '%reject%'
              AND COALESCE(v.latest_stage, '') != 'rejected'
            ORDER BY a.follow_up_date ASC, v.company ASC
            """
        ).fetchall()
    )
    for followup in followups:
        vacancy_id = int(followup["id"])
        followup["interview_summaries"] = summaries_by_vacancy.get(vacancy_id, [])
        direct_contacts = [
            contact
            for contact in contacts_by_vacancy.get(vacancy_id, [])
            if int(contact.get("is_active") or 0)
            and contact.get("confidence") in DIRECT_SEND_CONFIDENCE
        ]
        best_contact = direct_contacts[0] if direct_contacts else None
        followup["direct_contact"] = (
            " — ".join(
                part
                for part in (
                    best_contact.get("person_name") if best_contact else "",
                    best_contact.get("person_role") if best_contact else "",
                )
                if part
            )
            if best_contact
            else ""
        )
        followup["direct_channels"] = ", ".join(
            dict.fromkeys(contact["channel"] for contact in direct_contacts)
        )
        latest_search = latest_contact_search_by_vacancy.get(vacancy_id, {})
        followup["contact_search_status"] = latest_search.get("status") or ""
        followup["contact_search_date"] = latest_search.get("search_date") or ""
        followup["contact_search_note"] = latest_search.get("note") or ""
        latest_round = latest_round_by_vacancy.get(vacancy_id)
        if latest_round:
            sent_round_channels = [
                message["channel"]
                for message in latest_round.get("touchpoints", [])
                if message.get("delivery_status") == "sent"
            ]
            followup["last_outreach"] = (
                f"#{latest_round['round_number']} {latest_round['sent_date']}: "
                + ", ".join(sent_round_channels)
            )
        else:
            followup["last_outreach"] = ""

    needs_input = [
        v
        for v in vacancies
        if v["latest_stage"] == "needs_input"
    ]
    needs_action = [
        v
        for v in vacancies
        if v["latest_stage"] in {"needs_input", "follow_up"}
    ]

    best_sources = rows_to_dicts(
        conn.execute(
            """
            SELECT source_stream, COUNT(*) AS seen,
                   SUM(CASE WHEN raw_status IN ('POTENTIAL', 'NEEDS_REVIEW', 'SHORTLISTED', 'APPLIED') THEN 1 ELSE 0 END) AS signal
            FROM source_hits
            GROUP BY source_stream
            ORDER BY signal DESC, seen DESC
            LIMIT 40
            """
        ).fetchall()
    )

    issue_count = conn.execute("SELECT COUNT(*) AS n FROM import_issues").fetchone()["n"]

    return {
        "generated_at": now_iso(),
        "db_path": display_path(db_path, ROOT),
        "kpis": {
            "vacancies": len(vacancies),
            "channels": len(channel_counts),
            "active_review": len(active_review),
            "needs_user": len(needs_input),
            "needs_action": len(needs_action),
            "followups": len(followups),
            "direct_contacts": sum(
                1 for contact in employer_contacts if int(contact.get("is_active") or 0)
            ),
            "applied": len(applied_ids),
            "interviews": sum(1 for v in vacancies if v["latest_stage"] in {"interview_1", "interview_2", "interview_3"}),
            "offers": stage_counts["offer"],
            "rejected": stage_counts["rejected"],
            "import_issues": issue_count,
        },
        "channels": dict(channel_counts),
        "stages": dict(stage_counts),
        "statuses": dict(status_counts.most_common(30)),
        "funnel_by_channel": {k: dict(v) for k, v in funnel_by_channel.items()},
        "active_review": active_review[:200],
        "recent": recent,
        "followups": followups,
        "needs_user": needs_input,
        "needs_action": needs_action,
        "best_sources": best_sources,
        "interview_summaries": interview_summaries,
        "employer_contacts": employer_contacts,
        "vacancies": vacancies,
    }


def md_escape(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def safe_external_url(value: str) -> str:
    url = clean_cell(value)
    if not url:
        return ""
    parts = urlsplit(url)
    if parts.scheme.lower() not in {"http", "https"} or not parts.netloc:
        return ""
    return url


def md_link(title: str, url: str) -> str:
    title = md_escape(title or "link")
    safe_url = safe_external_url(url)
    if safe_url:
        return f"[{title}]({safe_url})"
    return title


def write_markdown_table(path: Path, title: str, headers: list[str], rows: list[list[Any]], intro: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"# {title}", ""]
    if intro:
        lines.extend([intro, ""])
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(md_escape(cell) for cell in row) + " |")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def generate_views(
    snapshot: dict[str, Any], db_path: Path | None = None
) -> None:
    db_path = db_path or DB_PATH
    VIEWS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    active_rows = []
    for v in snapshot["active_review"]:
        active_rows.append(
            [
                v.get("id") or "",
                v["last_seen_date"] or v["first_seen_date"] or "",
                CHANNEL_LABELS.get(v["channel"], v["channel"]),
                v.get("score") or "",
                STAGE_LABELS.get(v["latest_stage"], v["latest_stage"]),
                v.get("company") or "",
                md_link(v.get("title") or "", v.get("url") or ""),
                v.get("reason") or "",
                v.get("open_questions") or v.get("next_action") or "",
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "review_active.md",
        "Active Review Inbox",
        ["id", "date", "channel", "score", "stage", "company", "vacancy", "why", "questions/action"],
        active_rows,
        f"Generated: {snapshot['generated_at']}. Sorted by score and recency.",
    )

    today = dt.date.today().isoformat()
    latest_dates = sorted({v["last_seen_date"] for v in snapshot["vacancies"] if v["last_seen_date"]}, reverse=True)
    report_date = latest_dates[0] if latest_dates else today
    today_rows = []
    for v in snapshot["vacancies"]:
        if v["last_seen_date"] != report_date:
            continue
        today_rows.append(
            [
                v["last_seen_date"],
                CHANNEL_LABELS.get(v["channel"], v["channel"]),
                v.get("score") or "",
                STAGE_LABELS.get(v["latest_stage"], v["latest_stage"]),
                v.get("company") or "",
                md_link(v.get("title") or "", v.get("url") or ""),
                v.get("reason") or "",
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "today.md",
        f"Today View ({report_date})",
        ["date", "channel", "score", "stage", "company", "vacancy", "note"],
        today_rows,
        "Freshest imported date from the current data set.",
    )

    funnel_rows = []
    for channel, stages in sorted(snapshot["funnel_by_channel"].items()):
        row = [CHANNEL_LABELS.get(channel, channel)]
        row.extend(stages.get(stage, 0) for stage in FUNNEL_STAGES)
        funnel_rows.append(row)
    write_markdown_table(
        VIEWS_DIR / "funnel.md",
        "Funnel By Channel",
        ["channel"] + [STAGE_LABELS[s] for s in FUNNEL_STAGES],
        funnel_rows,
        f"Generated: {snapshot['generated_at']}. Seen is total unique vacancies per channel; later columns use the current compact stage.",
    )

    followup_rows = []
    for item in snapshot["followups"]:
        followup_rows.append(
            [
                item.get("follow_up_date") or "",
                CHANNEL_LABELS.get(item.get("channel"), item.get("channel")),
                item.get("score") or "",
                item.get("company") or "",
                md_link(item.get("title") or "", item.get("url") or ""),
                item.get("status") or "",
                item.get("direct_contact") or "",
                item.get("direct_channels") or "",
                item.get("last_outreach") or "",
                " ".join(
                    part
                    for part in (
                        item.get("contact_search_date") or "",
                        item.get("contact_search_status") or "",
                        item.get("contact_search_note") or "",
                    )
                    if part
                ),
                item.get("why_applied") or item.get("risks") or "",
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "followups.md",
        "Follow-ups",
        [
            "follow_up_date",
            "source_channel",
            "score",
            "company",
            "vacancy",
            "status",
            "direct_contact",
            "direct_channels",
            "last_outreach",
            "contact_search",
            "note",
        ],
        followup_rows,
        "Due follow-ups with verified direct-contact research and multi-channel outreach history.",
    )

    contact_rows = []
    for contact in snapshot["employer_contacts"]:
        address = contact.get("contact_address") or ""
        profile_url = contact.get("profile_url") or ""
        evidence = contact.get("evidence_note") or ""
        if contact.get("evidence_url"):
            evidence = md_link(evidence or "source", contact["evidence_url"])
        contact_rows.append(
            [
                contact.get("vacancy_id") or "",
                contact.get("company") or "",
                md_link(contact.get("title") or "", contact.get("url") or ""),
                contact.get("person_name") or "",
                contact.get("person_role") or "",
                contact.get("relationship") or "",
                contact.get("confidence") or "",
                contact.get("channel") or "",
                md_link(address, profile_url) if profile_url else address,
                contact.get("verified_date") or "",
                "active" if int(contact.get("is_active") or 0) else "inactive",
                evidence,
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "outreach_contacts.md",
        "Employer Outreach Contacts",
        [
            "vacancy_id",
            "company",
            "vacancy",
            "person",
            "role",
            "relationship",
            "confidence",
            "channel",
            "contact",
            "verified_date",
            "state",
            "evidence",
        ],
        contact_rows,
        "Verified recruiter and hiring-manager contacts. Only confirmed/strong active contacts may be messaged automatically.",
    )

    sources_rows = []
    for item in snapshot["best_sources"]:
        seen = int(item.get("seen") or 0)
        signal = int(item.get("signal") or 0)
        rate = f"{(signal / seen * 100):.1f}%" if seen else "0.0%"
        sources_rows.append([item.get("source_stream") or "", seen, signal, rate])
    write_markdown_table(
        REPORTS_DIR / "source_quality.md",
        "Source Quality",
        ["source_stream", "seen", "signals", "signal_rate"],
        sources_rows,
        "Signal means POTENTIAL, NEEDS_REVIEW, SHORTLISTED, or APPLIED in screening rows.",
    )

    coverage_rows: list[list[Any]] = []
    coverage_intro = "No search coverage manifest has been recorded."
    with connect_db(db_path) as conn:
        latest_run = conn.execute(
            """
            SELECT * FROM search_runs
            ORDER BY run_date DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if latest_run:
            coverage_intro = (
                f"Run {latest_run['run_date']} / {latest_run['source']}: "
                f"{latest_run['status']}; unique={latest_run['total_unique']}, "
                f"known={latest_run['known_count']}, new={latest_run['new_count']}, "
                f"issues={latest_run['issue_count']}."
            )
            checkpoints = conn.execute(
                """
                SELECT stream_key, status, query_url, found, page_size,
                       pages_visited, pages_expected, extracted, unique_count,
                       known_count, new_count, error, issues
                FROM search_coverage
                WHERE search_run_id = ?
                ORDER BY id
                """,
                (latest_run["id"],),
            ).fetchall()
            for item in checkpoints:
                coverage_rows.append(
                    [
                        item["stream_key"],
                        item["status"],
                        md_link("query", item["query_url"] or ""),
                        item["found"],
                        item["page_size"],
                        f"{item['pages_visited']}/{item['pages_expected']}",
                        item["extracted"],
                        item["unique_count"],
                        item["known_count"],
                        item["new_count"],
                        item["error"] or item["issues"] or "",
                    ]
                )
    write_markdown_table(
        REPORTS_DIR / "search_coverage.md",
        "Search Coverage",
        [
            "stream",
            "status",
            "query",
            "found",
            "page_size",
            "pages",
            "extracted",
            "unique",
            "known",
            "new",
            "error/issues",
        ],
        coverage_rows,
        coverage_intro,
    )

    issue_rows = []
    # `snapshot` only contains the count; load issue details lazily if possible.
    with connect_db(db_path) as conn:
        issues = conn.execute(
            """
            SELECT origin_file, line_no, issue, raw_text
            FROM import_issues
            ORDER BY origin_file, line_no
            LIMIT 100
            """
        ).fetchall()
    for issue in issues:
        issue_rows.append([issue["origin_file"], issue["line_no"], issue["issue"], issue["raw_text"][:160]])
    write_markdown_table(
        REPORTS_DIR / "data_quality.md",
        "Data Quality",
        ["file", "line", "issue", "raw"],
        issue_rows,
        f"Import issues: {snapshot['kpis']['import_issues']}. Most unrecognized rows are usually notes or non-vacancy tables.",
    )


def json_for_script(value: Any) -> str:
    """Serialize JSON safely for an inline script element."""

    return (
        json.dumps(value, ensure_ascii=False)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def render_dashboard(snapshot: dict[str, Any]) -> str:
    data_json = json_for_script(snapshot)
    stage_labels = json_for_script(STAGE_LABELS)
    funnel_stages = json_for_script(FUNNEL_STAGES)
    channel_labels = json_for_script(CHANNEL_LABELS)
    dashboard_title = html.escape(PROJECT_TITLE, quote=True)
    dashboard_locale = html.escape(PROJECT_LOCALE, quote=True)
    return f"""<!doctype html>
<html lang="{dashboard_locale}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{dashboard_title}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f7f4;
      --panel: #ffffff;
      --ink: #202124;
      --muted: #666b73;
      --line: #d8d9d6;
      --blue: #315f9f;
      --green: #2f7d5f;
      --amber: #a46b12;
      --red: #a33a3a;
      --violet: #6e5798;
      --chip: #eef1f4;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.4;
    }}
    header {{
      padding: 20px 24px 12px;
      border-bottom: 1px solid var(--line);
      background: #fcfcfa;
      position: sticky;
      top: 0;
      z-index: 10;
    }}
    h1 {{
      margin: 0;
      font-size: 22px;
      letter-spacing: 0;
    }}
    .sub {{
      color: var(--muted);
      margin-top: 4px;
      font-size: 13px;
    }}
    main {{
      padding: 18px 24px 28px;
      max-width: 1480px;
      margin: 0 auto;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(7, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }}
    .kpi {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px 12px;
      min-height: 72px;
    }}
    .kpi .label {{
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }}
    .kpi .value {{
      font-size: 26px;
      font-weight: 700;
      margin-top: 6px;
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: minmax(220px, 1fr) 220px 260px 170px;
      gap: 10px;
      margin: 14px 0;
      align-items: center;
    }}
    input, select, button {{
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
      color: var(--ink);
      min-height: 36px;
      padding: 7px 10px;
      font: inherit;
    }}
    button {{
      cursor: pointer;
      background: #f1f3f2;
    }}
    button.active {{
      background: var(--blue);
      color: #fff;
      border-color: var(--blue);
    }}
    .multi-filter {{
      position: relative;
      min-width: 0;
    }}
    .multi-trigger {{
      width: 100%;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      text-align: left;
      background: var(--panel);
    }}
    .multi-trigger-label {{
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .multi-trigger-mark {{
      color: var(--muted);
      font-size: 12px;
      flex: 0 0 auto;
    }}
    .multi-menu {{
      position: absolute;
      z-index: 30;
      top: calc(100% + 5px);
      left: 0;
      right: 0;
      max-height: 280px;
      overflow: auto;
      padding: 6px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
      box-shadow: 0 12px 30px rgba(32, 33, 36, 0.14);
    }}
    .multi-option {{
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 32px;
      padding: 6px 7px;
      border-radius: 4px;
      font-size: 13px;
      cursor: pointer;
    }}
    .multi-option:hover {{
      background: #f7faf9;
    }}
    .multi-option input {{
      width: 15px;
      height: 15px;
      min-height: 0;
      margin: 0;
      padding: 0;
      flex: 0 0 auto;
    }}
    .multi-option-all {{
      border-bottom: 1px solid #ececea;
      border-radius: 4px 4px 0 0;
      margin-bottom: 4px;
      font-weight: 600;
    }}
    .tabs {{
      display: flex;
      gap: 8px;
      margin: 10px 0 14px;
      flex-wrap: wrap;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 14px;
      align-items: start;
    }}
    section {{
      margin-bottom: 14px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: hidden;
    }}
    .panel h2 {{
      margin: 0;
      padding: 11px 12px;
      font-size: 15px;
      border-bottom: 1px solid var(--line);
      background: #fbfbf9;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }}
    .table-wrap {{
      max-height: 68vh;
      overflow: auto;
      border-top: 1px solid #ececea;
    }}
    .table-meta {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      padding: 8px 12px;
      color: var(--muted);
      font-size: 12px;
      background: #fbfbf9;
    }}
    th, td {{
      text-align: left;
      padding: 8px 9px;
      border-bottom: 1px solid #ececea;
      vertical-align: top;
      overflow-wrap: anywhere;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
      background: #fbfbf9;
      position: sticky;
      top: 0;
      z-index: 5;
    }}
    tr:hover td {{
      background: #f7faf9;
    }}
    a {{ color: var(--blue); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .score {{
      font-weight: 700;
      color: var(--blue);
    }}
    .id-cell {{
      color: var(--muted);
      font-weight: 700;
      white-space: nowrap;
    }}
    .stage {{
      display: inline-block;
      border-radius: 6px;
      padding: 2px 6px;
      background: var(--chip);
      font-size: 12px;
      white-space: nowrap;
    }}
    .stage.applied, .stage.follow_up {{ background: #e7f2ec; color: var(--green); }}
    .stage.needs_input {{ background: #fff3df; color: var(--amber); }}
    .stage.rejected {{ background: #f7e6e6; color: var(--red); }}
    .stage.offer {{ background: #ece8f6; color: var(--violet); }}
    .bars {{
      padding: 10px 12px 12px;
    }}
    .funnel {{
      overflow-x: auto;
    }}
    .funnel-visual {{
      padding: 12px;
      border-top: 1px solid #ececea;
      background: #fcfcfa;
    }}
    .funnel-visual-head {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: baseline;
      margin-bottom: 10px;
    }}
    .funnel-visual-title {{
      font-weight: 700;
      font-size: 14px;
    }}
    .funnel-visual-sub {{
      color: var(--muted);
      font-size: 12px;
      text-align: right;
    }}
    .funnel-chart {{
      display: grid;
      gap: 10px;
      margin-top: 10px;
    }}
    .funnel-stage-row {{
      display: grid;
      grid-template-columns: 128px minmax(0, 1fr);
      gap: 12px;
      align-items: center;
    }}
    .funnel-stage-label {{
      display: grid;
      gap: 1px;
      min-width: 0;
    }}
    .funnel-stage-name {{
      font-size: 12px;
      font-weight: 700;
      color: var(--ink);
    }}
    .funnel-stage-meta {{
      color: var(--muted);
      font-size: 12px;
    }}
    .funnel-band-area {{
      height: 30px;
      border-radius: 6px;
      background: #ececea;
      overflow: hidden;
      position: relative;
    }}
    .funnel-band {{
      width: var(--bar-width);
      min-width: 2px;
      height: 100%;
      display: flex;
      overflow: hidden;
      border-radius: 6px;
      box-shadow: inset 0 0 0 1px rgba(0,0,0,0.04);
    }}
    .funnel-band-segment {{
      height: 100%;
      min-width: 2px;
    }}
    .funnel-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px 12px;
      margin-top: 12px;
      color: var(--muted);
      font-size: 12px;
    }}
    .legend-chip {{
      display: inline-flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
    }}
    .legend-dot {{
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--dot);
    }}
    .interview-links {{
      display: flex;
      flex-wrap: wrap;
      gap: 5px;
      margin-top: 6px;
    }}
    .interview-link {{
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 7px;
      border: 1px solid #d9e3ef;
      border-radius: 6px;
      background: #f4f7fb;
      font-size: 12px;
      font-weight: 600;
      line-height: 1.2;
      white-space: nowrap;
    }}
    .funnel-vacancies {{
      border-top: 1px solid #ececea;
      background: var(--panel);
    }}
    .funnel-vacancies-head {{
      padding: 11px 12px;
      border-bottom: 1px solid #ececea;
      background: #fbfbf9;
      font-weight: 700;
      font-size: 14px;
    }}
    .hidden {{ display: none; }}
    .muted {{ color: var(--muted); }}
    .note {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 6px;
    }}
    .detail-cell {{
      min-width: 240px;
    }}
    .vacancy-cell {{
      min-width: 180px;
      font-weight: 600;
    }}
    .company-cell {{
      min-width: 130px;
    }}
    #reviewTable th:first-child,
    #reviewTable td:first-child {{
      width: 72px;
    }}
    @media (max-width: 980px) {{
      .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .toolbar {{ grid-template-columns: 1fr; }}
      .grid {{ grid-template-columns: 1fr; }}
      th {{ top: 0; }}
      header {{ position: static; }}
    }}
    @media (max-width: 700px) {{
      main {{ padding: 18px 24px 24px; }}
      .tabs button {{ flex: 1 1 calc(50% - 8px); }}
      .table-wrap {{
        max-height: none;
        overflow: visible;
      }}
      table, thead, tbody, tr, td {{
        display: block;
        width: 100%;
      }}
      thead {{ display: none; }}
      tr {{
        padding: 10px 12px;
        border-bottom: 1px solid #ececea;
      }}
      td {{
        display: grid;
        grid-template-columns: 96px minmax(0, 1fr);
        gap: 8px;
        border-bottom: 0;
        padding: 4px 0;
      }}
      td::before {{
        content: attr(data-label);
        color: var(--muted);
        font-size: 12px;
        font-weight: 600;
      }}
      .stage {{ white-space: normal; width: fit-content; }}
      .detail-cell, .vacancy-cell, .company-cell {{ min-width: 0; }}
      .funnel-visual-head {{
        display: block;
      }}
      .funnel-visual-sub {{
        text-align: left;
        margin-top: 2px;
      }}
      .multi-menu {{
        position: static;
        margin-top: 5px;
        max-height: 220px;
        box-shadow: none;
      }}
      .funnel-stage-row {{
        grid-template-columns: 1fr;
        gap: 5px;
      }}
      .funnel-band-area {{
        height: 24px;
      }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>{dashboard_title}</h1>
    <div class="sub">Generated <span id="generated"></span> from <span id="dbPath"></span></div>
  </header>
  <main>
    <div class="kpis" id="kpis"></div>
    <div class="tabs">
      <button data-tab="review" class="active">Review Inbox</button>
      <button data-tab="funnel">Funnel</button>
      <button data-tab="today">Freshest</button>
      <button data-tab="followups">Follow-ups</button>
    </div>
    <div class="toolbar">
      <input id="search" placeholder="Search company, role, reason">
      <div class="multi-filter" id="channelPicker">
        <button id="channelTrigger" class="multi-trigger" type="button" aria-expanded="false" aria-controls="channelMenu">
          <span id="channelSummary" class="multi-trigger-label">All channels</span>
          <span class="multi-trigger-mark">v</span>
        </button>
        <div id="channelMenu" class="multi-menu hidden"></div>
      </div>
      <div class="multi-filter" id="stagePicker">
        <button id="stageTrigger" class="multi-trigger" type="button" aria-expanded="false" aria-controls="stageMenu">
          <span id="stageSummary" class="multi-trigger-label">All stages</span>
          <span class="multi-trigger-mark">v</span>
        </button>
        <div id="stageMenu" class="multi-menu hidden"></div>
      </div>
      <button id="reset">Reset filters</button>
    </div>

    <section id="tab-review">
      <div class="panel">
        <h2>Active Review Inbox</h2>
        <div class="note" style="padding: 0 12px 10px;">Open questions, high-score review items, and roles that need user answers or follow-up.</div>
        <div id="reviewTable"></div>
      </div>
    </section>

    <section id="tab-funnel" class="hidden">
      <div class="panel">
        <h2>Funnel By Channel</h2>
        <div id="funnelTable" class="funnel"></div>
        <div id="funnelVisual" class="funnel-visual"></div>
        <div id="funnelVacancies" class="funnel-vacancies"></div>
      </div>
    </section>

    <section id="tab-today" class="hidden">
      <div class="panel">
        <h2>Freshest Imported Vacancies</h2>
        <div id="recentTable"></div>
      </div>
    </section>

    <section id="tab-followups" class="hidden">
      <div class="panel">
        <h2>Follow-ups</h2>
        <div id="followupTable"></div>
      </div>
    </section>

  </main>

  <script>
    const DATA = {data_json};
    const STAGE_LABELS = {stage_labels};
    const FUNNEL_STAGES = {funnel_stages};
    const CHANNEL_LABELS = {channel_labels};
    const CHANNEL_PALETTE = ['#315f9f', '#2f7d5f', '#a46b12', '#6e5798', '#8b4b5b', '#4f747d', '#7a6a2d', '#53656f'];
    const ALL_CHANNELS = Object.keys(DATA.channels).sort();
    const ALL_STAGES = FUNNEL_STAGES.slice();

    const $ = (id) => document.getElementById(id);
    const state = {{
      tab: 'review',
      search: '',
      channels: new Set(ALL_CHANNELS),
      stages: new Set(ALL_STAGES),
    }};

    function text(v) {{ return (v === null || v === undefined) ? '' : String(v); }}
    function esc(v) {{
      return text(v).replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}}[ch]));
    }}
    function link(title, url) {{
      const label = esc(title || 'link');
      const href = text(url).trim();
      const lowerHref = href.toLowerCase();
      return (lowerHref.startsWith('http://') || lowerHref.startsWith('https://'))
        ? `<a href="${{esc(href)}}" target="_blank" rel="noreferrer">${{label}}</a>`
        : label;
    }}
    function fileHref(path) {{
      const safeParts = text(path).split('/').filter(part => part && part !== '.' && part !== '..');
      return '../' + safeParts.map(part => encodeURIComponent(part)).join('/');
    }}
    function stageBadge(stage) {{
      return `<span class="stage ${{esc(stage)}}">${{esc(STAGE_LABELS[stage] || stage || 'unknown')}}</span>`;
    }}
    function interviewStageLabel(summary) {{
      return STAGE_LABELS[summary.stage] || `Interview ${{summary.interview_no || ''}}`.trim();
    }}
    function interviewLinks(row) {{
      const summaries = row.interview_summaries || [];
      if (!summaries.length) return '';
      return `<div class="interview-links">${{summaries.map(summary => {{
        const label = summary.title || interviewStageLabel(summary);
        const title = [interviewStageLabel(summary), summary.summary_date, summary.note].filter(Boolean).join(' · ');
        return `<a class="interview-link" href="${{esc(fileHref(summary.file_path))}}" target="_blank" rel="noreferrer" title="${{esc(title)}}">${{esc(label)}}</a>`;
      }}).join('')}}</div>`;
    }}
    function detailWithInterviews(value, row) {{
      const base = esc(value || '');
      const links = interviewLinks(row);
      return base || links ? `${{base}}${{links}}` : '';
    }}
    function channelLabel(channel) {{ return CHANNEL_LABELS[channel] || channel || 'other'; }}
    function channelColor(index) {{ return CHANNEL_PALETTE[index % CHANNEL_PALETTE.length]; }}
    function cell(label, content, cls = '') {{
      const classAttr = cls ? ` class="${{esc(cls)}}"` : '';
      return `<td data-label="${{esc(label)}}"${{classAttr}}>${{content}}</td>`;
    }}
    function isAllSelected(selected, allValues) {{
      return selected.size === allValues.length;
    }}
    function selectedSummary(selected, allValues, allLabel, noneLabel, countLabel, labelFor) {{
      if (selected.size === 0) return noneLabel;
      if (isAllSelected(selected, allValues)) return allLabel;
      const labels = allValues.filter(value => selected.has(value)).map(labelFor);
      if (labels.length <= 2) return labels.join(', ');
      return `${{labels.length}} ${{countLabel}}`;
    }}
    function renderMultiMenu(menuId, kind, allValues, selected, allLabel, labelFor) {{
      const menu = $(menuId);
      menu.innerHTML = `<label class="multi-option multi-option-all">
        <input type="checkbox" data-kind="${{esc(kind)}}" data-all="1" ${{isAllSelected(selected, allValues) ? 'checked' : ''}}>
        <span>${{esc(allLabel)}}</span>
      </label>` + allValues.map(value => `<label class="multi-option">
        <input type="checkbox" data-kind="${{esc(kind)}}" value="${{esc(value)}}" ${{selected.has(value) ? 'checked' : ''}}>
        <span>${{esc(labelFor(value))}}</span>
      </label>`).join('');
      const allInput = menu.querySelector('input[data-all="1"]');
      if (allInput) allInput.indeterminate = selected.size > 0 && selected.size < allValues.length;
    }}

    function filtered(rows) {{
      const q = state.search.toLowerCase();
      return rows.filter(row => {{
        if (!state.channels.has(row.channel)) return false;
        if (!state.stages.has(row.latest_stage)) return false;
        if (!q) return true;
        const summaries = (row.interview_summaries || [])
          .flatMap(summary => [summary.title, summary.stage, summary.summary_date, summary.file_path, summary.note]);
        return [row.id, row.company, row.title, row.reason, row.risks, row.open_questions, row.next_action, ...summaries]
          .some(v => text(v).toLowerCase().includes(q));
      }});
    }}
    function rowMatchesSearch(row) {{
      const q = state.search.toLowerCase();
      if (!q) return true;
      const summaries = (row.interview_summaries || [])
        .flatMap(summary => [summary.title, summary.stage, summary.summary_date, summary.file_path, summary.note]);
      return [row.id, row.company, row.title, row.reason, row.risks, row.open_questions, row.next_action, ...summaries]
        .some(v => text(v).toLowerCase().includes(q));
    }}
    function filteredFollowups(rows) {{
      const q = state.search.toLowerCase();
      return rows.filter(row => {{
        if (!state.channels.has(row.channel)) return false;
        if (!q) return true;
        return [
          row.company, row.title, row.status, row.why_applied, row.risks,
          row.direct_contact, row.direct_channels, row.last_outreach,
          row.contact_search_status, row.contact_search_note,
        ]
          .some(v => text(v).toLowerCase().includes(q));
      }});
    }}
    function filteredFunnelChannels(channels) {{
      const q = state.search.toLowerCase();
      return channels.filter(ch => {{
        if (!state.channels.has(ch)) return false;
        if (!q) return true;
        if (channelLabel(ch).toLowerCase().includes(q) || ch.toLowerCase().includes(q)) return true;
        return DATA.vacancies.some(row => row.channel === ch && rowMatchesFunnelStage(row) && rowMatchesSearch(row));
      }});
    }}
    function rowMatchesFunnelStage(row) {{
      if (!state.stages.size) return false;
      if (state.stages.has('seen')) return true;
      return state.stages.has(row.latest_stage || 'seen');
    }}
    function currentFunnelStages() {{
      return FUNNEL_STAGES.filter(stage => state.stages.has(stage));
    }}
    function currentFunnelChannels() {{
      return filteredFunnelChannels(Object.keys(DATA.funnel_by_channel).sort());
    }}
    function stageTotal(channels, stage) {{
      return channels.reduce((sum, ch) => sum + Number((DATA.funnel_by_channel[ch] || {{}})[stage] || 0), 0);
    }}
    function pct(value, total) {{
      return total ? Math.round((Number(value) / Number(total)) * 100) : 0;
    }}

    function renderKpis() {{
      const items = [
        ['Vacancies', DATA.kpis.vacancies],
        ['Channels', DATA.kpis.channels],
        ['Open items', DATA.kpis.active_review],
        ['Needs input', DATA.kpis.needs_user],
        ['Follow-ups', DATA.kpis.followups],
        ['Direct contacts', DATA.kpis.direct_contacts],
        ['Applied', DATA.kpis.applied],
        ['Rejected', DATA.kpis.rejected],
      ];
      $('kpis').innerHTML = items.map(([label, value]) => `
        <div class="kpi"><div class="label">${{esc(label)}}</div><div class="value">${{esc(value)}}</div></div>
      `).join('');
    }}

    function renderFilters() {{
      renderMultiMenu('channelMenu', 'channel', ALL_CHANNELS, state.channels, 'All channels', channelLabel);
      renderMultiMenu('stageMenu', 'stage', ALL_STAGES, state.stages, 'All stages', stage => STAGE_LABELS[stage] || stage);
      $('channelSummary').textContent = selectedSummary(state.channels, ALL_CHANNELS, 'All channels', 'No channels', 'channels', channelLabel);
      $('stageSummary').textContent = selectedSummary(state.stages, ALL_STAGES, 'All stages', 'No stages', 'stages', stage => STAGE_LABELS[stage] || stage);
    }}

    function table(headers, rows, renderRow, options = {{}}) {{
      if (!rows.length) return '<div class="bars muted">No rows for current filters.</div>';
      const limit = options.limit || rows.length;
      const visible = rows.slice(0, limit);
      const more = rows.length > visible.length ? `<span>Showing ${{visible.length}} of ${{rows.length}}. Refine filters to narrow the list.</span>` : '<span></span>';
      return `<div class="table-meta"><span>${{rows.length}} rows</span>${{more}}</div><div class="table-wrap"><table><thead><tr>${{headers.map(h => `<th>${{esc(h)}}</th>`).join('')}}</tr></thead><tbody>${{visible.map(renderRow).join('')}}</tbody></table></div>`;
    }}

    function renderReview() {{
      const rows = filtered(DATA.active_review);
      const headers = ['ID','Score','Stage','Date','Channel','Company','Vacancy','Why / Questions'];
      $('reviewTable').innerHTML = table(headers, rows, r => `
        <tr>
          ${{cell(headers[0], esc(r.id || ''), 'id-cell')}}
          ${{cell(headers[1], esc(r.score || ''), 'score')}}
          ${{cell(headers[2], stageBadge(r.latest_stage))}}
          ${{cell(headers[3], esc(r.last_seen_date || r.first_seen_date || ''))}}
          ${{cell(headers[4], esc(channelLabel(r.channel)))}}
          ${{cell(headers[5], esc(r.company || ''), 'company-cell')}}
          ${{cell(headers[6], link(r.title, r.url), 'vacancy-cell')}}
          ${{cell(headers[7], detailWithInterviews(r.open_questions || r.reason || r.next_action || '', r), 'detail-cell')}}
        </tr>
      `, {{ limit: 120 }});
    }}

    function renderRecent() {{
      const rows = filtered(DATA.recent);
      const headers = ['Score','Stage','Date','Channel','Company','Vacancy','Note'];
      $('recentTable').innerHTML = table(headers, rows, r => `
        <tr>
          ${{cell(headers[0], esc(r.score || ''), 'score')}}
          ${{cell(headers[1], stageBadge(r.latest_stage))}}
          ${{cell(headers[2], esc(r.last_seen_date || r.first_seen_date || ''))}}
          ${{cell(headers[3], esc(channelLabel(r.channel)))}}
          ${{cell(headers[4], esc(r.company || ''), 'company-cell')}}
          ${{cell(headers[5], link(r.title, r.url), 'vacancy-cell')}}
          ${{cell(headers[6], detailWithInterviews(r.reason || r.next_action || '', r), 'detail-cell')}}
        </tr>
      `, {{ limit: 120 }});
    }}

    function renderFollowups() {{
      const rows = filteredFollowups(DATA.followups);
      const headers = ['Date','Source','Score','Company','Vacancy','Status','Direct contact','Last outreach','Contact search','Note'];
      $('followupTable').innerHTML = table(headers, rows, r => `
        <tr>
          ${{cell(headers[0], esc(r.follow_up_date || ''))}}
          ${{cell(headers[1], esc(channelLabel(r.channel)))}}
          ${{cell(headers[2], esc(r.score || ''), 'score')}}
          ${{cell(headers[3], esc(r.company || ''), 'company-cell')}}
          ${{cell(headers[4], link(r.title, r.url), 'vacancy-cell')}}
          ${{cell(headers[5], esc(r.status || ''))}}
          ${{cell(headers[6], esc([r.direct_contact, r.direct_channels].filter(Boolean).join(' · ')), 'detail-cell')}}
          ${{cell(headers[7], esc(r.last_outreach || ''), 'detail-cell')}}
          ${{cell(headers[8], esc([r.contact_search_date, r.contact_search_status, r.contact_search_note].filter(Boolean).join(' · ')), 'detail-cell')}}
          ${{cell(headers[9], detailWithInterviews(r.why_applied || r.risks || '', r), 'detail-cell')}}
        </tr>
      `);
    }}

    function renderFunnel() {{
      const visibleStages = currentFunnelStages();
      const channels = currentFunnelChannels();
      const headers = ['Channel', ...visibleStages.map(st => STAGE_LABELS[st] || st)];
      $('funnelTable').innerHTML = table(headers, channels, ch => {{
        const stages = DATA.funnel_by_channel[ch] || {{}};
        return `<tr>${{cell(headers[0], esc(channelLabel(ch)))}}` +
          visibleStages.map((st, idx) => cell(headers[idx + 1], esc(stages[st] || 0))).join('') +
          '</tr>';
      }});
      renderFunnelVisual(channels, visibleStages);
      renderFunnelVacancies(channels, visibleStages);
    }}

    function renderFunnelVisual(channels, visibleStages) {{
      if (!channels.length || !visibleStages.length) {{
        const reason = !channels.length ? 'No channels selected.' : 'No stages selected.';
        $('funnelVisual').innerHTML = `<div class="funnel-visual-head">
          <div class="funnel-visual-title">Visual Funnel</div>
          <div class="funnel-visual-sub">0 selected</div>
        </div><div class="bars muted">${{esc(reason)}}</div>`;
        return;
      }}
      const totalSeen = stageTotal(channels, 'seen');
      const maxTotal = Math.max(1, totalSeen, ...visibleStages.map(stage => stageTotal(channels, stage)));
      const legend = channels.map((ch, idx) => `<span class="legend-chip"><span class="legend-dot" style="--dot:${{channelColor(idx)}}"></span>${{esc(channelLabel(ch))}}</span>`).join('');
      const stageRows = visibleStages.map(stage => {{
        const value = stageTotal(channels, stage);
        const rate = pct(value, totalSeen);
        const width = Math.max(value ? 2 : 0, value / maxTotal * 100);
        const segments = channels.map((ch, idx) => {{
          const channelValue = Number((DATA.funnel_by_channel[ch] || {{}})[stage] || 0);
          if (!channelValue) return '';
          const segmentWidth = channelValue / value * 100;
          const title = `${{channelLabel(ch)}}: ${{channelValue}} ${{STAGE_LABELS[stage] || stage}}`;
          return `<span class="funnel-band-segment" title="${{esc(title)}}" style="width:${{segmentWidth}}%; background:${{channelColor(idx)}}"></span>`;
        }}).join('');
        const meta = stage === 'seen' ? '100% of selected' : `${{rate}}% of Seen`;
        return `<div class="funnel-stage-row">
          <div class="funnel-stage-label">
            <div class="funnel-stage-name">${{esc(STAGE_LABELS[stage] || stage)}}</div>
            <div class="funnel-stage-meta">${{esc(value)}} · ${{esc(meta)}}</div>
          </div>
          <div class="funnel-band-area" aria-label="${{esc((STAGE_LABELS[stage] || stage) + ': ' + value)}}">
            <div class="funnel-band" style="--bar-width:${{width}}%">${{segments}}</div>
          </div>
        </div>`;
      }}).join('');
      $('funnelVisual').innerHTML = `<div class="funnel-visual-head">
        <div class="funnel-visual-title">Visual Funnel</div>
        <div class="funnel-visual-sub">${{esc(channels.length)}} channel(s), ${{esc(totalSeen)}} seen vacancies</div>
      </div>
      <div class="funnel-chart">${{stageRows}}</div>
      <div class="funnel-legend">${{legend}}</div>`;
    }}

    function renderFunnelVacancies(channels, visibleStages) {{
      const channelSet = new Set(channels);
      const rows = DATA.vacancies.filter(row => {{
        if (!channelSet.has(row.channel)) return false;
        if (!rowMatchesSearch(row)) return false;
        return rowMatchesFunnelStage(row);
      }});
      const headers = ['ID','Score','Stage','Date','Channel','Company','Vacancy','Note'];
      const body = table(headers, rows, r => `
        <tr>
          ${{cell(headers[0], esc(r.id || ''), 'id-cell')}}
          ${{cell(headers[1], esc(r.score || ''), 'score')}}
          ${{cell(headers[2], stageBadge(r.latest_stage))}}
          ${{cell(headers[3], esc(r.last_seen_date || r.first_seen_date || ''))}}
          ${{cell(headers[4], esc(channelLabel(r.channel)))}}
          ${{cell(headers[5], esc(r.company || ''), 'company-cell')}}
          ${{cell(headers[6], link(r.title, r.url), 'vacancy-cell')}}
          ${{cell(headers[7], detailWithInterviews(r.reason || r.open_questions || r.next_action || r.risks || r.latest_status || '', r), 'detail-cell')}}
        </tr>
      `, {{ limit: 120 }});
      $('funnelVacancies').innerHTML = '<div class="funnel-vacancies-head">Filtered Vacancies</div>' + body;
    }}

    function renderTab() {{
      document.querySelectorAll('section[id^="tab-"]').forEach(el => el.classList.add('hidden'));
      $(`tab-${{state.tab}}`).classList.remove('hidden');
      document.querySelectorAll('.tabs button').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === state.tab));
      renderReview();
      renderRecent();
      renderFollowups();
      renderFunnel();
    }}
    function setPickerOpen(kind, open) {{
      $(`${{kind}}Menu`).classList.toggle('hidden', !open);
      $(`${{kind}}Trigger`).setAttribute('aria-expanded', open ? 'true' : 'false');
    }}
    function closePickers(exceptKind = '') {{
      ['channel', 'stage'].forEach(kind => {{
        if (kind !== exceptKind) setPickerOpen(kind, false);
      }});
    }}
    function handleMultiChange(event, kind) {{
      const input = event.target;
      if (!input.matches('input[type="checkbox"]')) return;
      const allValues = kind === 'channel' ? ALL_CHANNELS : ALL_STAGES;
      const current = kind === 'channel' ? state.channels : state.stages;
      let next = new Set(current);
      if (input.dataset.all === '1') {{
        next = input.checked ? new Set(allValues) : new Set();
      }} else if (input.checked) {{
        next.add(input.value);
      }} else {{
        next.delete(input.value);
      }}
      if (kind === 'channel') state.channels = next;
      if (kind === 'stage') state.stages = next;
      renderFilters();
      renderTab();
    }}
    function bindPicker(kind) {{
      const trigger = $(`${{kind}}Trigger`);
      const menu = $(`${{kind}}Menu`);
      trigger.addEventListener('click', (event) => {{
        event.stopPropagation();
        const nextOpen = menu.classList.contains('hidden');
        closePickers(kind);
        setPickerOpen(kind, nextOpen);
      }});
      menu.addEventListener('click', event => event.stopPropagation());
      menu.addEventListener('change', event => handleMultiChange(event, kind));
    }}

    $('generated').textContent = DATA.generated_at;
    $('dbPath').textContent = DATA.db_path;
    renderKpis();
    renderFilters();
    renderTab();
    bindPicker('channel');
    bindPicker('stage');

    $('search').addEventListener('input', (e) => {{ state.search = e.target.value; renderTab(); }});
    $('reset').addEventListener('click', () => {{
      state.search = '';
      state.channels = new Set(ALL_CHANNELS);
      state.stages = new Set(ALL_STAGES);
      $('search').value = '';
      closePickers();
      renderFilters();
      renderTab();
    }});
    document.querySelectorAll('.tabs button').forEach(btn => btn.addEventListener('click', () => {{
      state.tab = btn.dataset.tab;
      renderTab();
    }}));
    document.addEventListener('click', () => closePickers());
  </script>
</body>
</html>
"""


def generate_dashboard(snapshot: dict[str, Any]) -> None:
    DASHBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    DASHBOARD_PATH.write_text(render_dashboard(snapshot), encoding="utf-8")


def render_outputs(db_path: Path) -> dict[str, Any]:
    with connect_db(db_path) as conn:
        ensure_schema(conn)
        snapshot = build_snapshot(conn, db_path)
    generate_views(snapshot, db_path)
    generate_dashboard(snapshot)
    return snapshot


def db_label(path: Path) -> str:
    return display_path(path, ROOT)


def print_render_summary(action: str, db_path: Path, snapshot: dict[str, Any]) -> None:
    print(action)
    print(f"  database: {db_label(db_path)}")
    print(f"  dashboard: {display_path(DASHBOARD_PATH, ROOT)}")
    print(f"  vacancies: {snapshot['kpis']['vacancies']}")
    print(f"  open items: {snapshot['kpis']['active_review']}")
    print(f"  needs input: {snapshot['kpis']['needs_user']}")
    print(f"  follow-ups: {snapshot['kpis']['followups']}")
    print(f"  direct contacts: {snapshot['kpis']['direct_contacts']}")
    print(f"  applied: {snapshot['kpis']['applied']}")
    print(f"  import issues: {snapshot['kpis']['import_issues']}")


def rebuild(args: argparse.Namespace) -> None:
    snapshot = render_outputs(args.db)
    if args.json:
        print(json.dumps({"kpis": snapshot["kpis"]}, ensure_ascii=False, indent=2))
    else:
        print_render_summary("Regenerated job-search views from SQLite", args.db, snapshot)


def file_signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    signature: list[tuple[str, int, int]] = []
    for path in paths:
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        signature.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(sorted(signature))


def watch(args: argparse.Namespace) -> None:
    print("Watching SQLite database. Press Ctrl-C to stop.")
    rebuild(argparse.Namespace(db=args.db, json=False))
    previous = file_signature([args.db])
    try:
        while True:
            time.sleep(args.interval)
            current = file_signature([args.db])
            if current != previous:
                print(f"\nChange detected at {now_iso()}")
                rebuild(argparse.Namespace(db=args.db, json=False))
                previous = current
    except KeyboardInterrupt:
        print("\nStopped watcher.")


def print_stats(args: argparse.Namespace) -> None:
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        snapshot = build_snapshot(conn, args.db)
    print(json.dumps(snapshot["kpis"], ensure_ascii=False, indent=2))


def migrate_schema(args: argparse.Namespace) -> None:
    """Explicitly migrate an existing database with a recoverable backup."""

    if not args.db.exists():
        raise FileNotFoundError(f"Database not found: {args.db}")
    with sqlite3.connect(args.db) as probe:
        current_version = int(probe.execute("PRAGMA user_version").fetchone()[0])
    if current_version == SCHEMA_VERSION:
        print(f"Database schema is already at version {SCHEMA_VERSION}")
        return
    if current_version != 1:
        raise RuntimeError(
            f"Unsupported migration path: {current_version} -> {SCHEMA_VERSION}"
        )

    backup_path: Path | None = None
    if not args.no_backup:
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = args.db.with_name(f"{args.db.name}.bak-schema-v{current_version}-{stamp}")
        with sqlite3.connect(args.db) as source_conn, sqlite3.connect(backup_path) as backup_conn:
            source_conn.backup(backup_conn)

    with connect_db(args.db) as conn:
        ensure_auxiliary_schema(conn)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.commit()

    snapshot = render_outputs(args.db)
    result = {
        "from_version": current_version,
        "to_version": SCHEMA_VERSION,
        "backup": db_label(backup_path) if backup_path else "",
        "kpis": snapshot["kpis"],
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"Migrated database schema {current_version} -> {SCHEMA_VERSION}")
        if backup_path:
            print(f"  backup: {db_label(backup_path)}")


def build_coverage_plan_command(args: argparse.Namespace) -> None:
    plan_path = args.file if args.file.is_absolute() else ROOT / args.file
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    plan = build_coverage_plan(
        payload,
        REQUIRED_SEARCH_STREAMS,
        default_period_days=DEFAULT_SEARCH_PERIOD_DAYS,
        default_items_per_page=SEARCH_ITEMS_PER_PAGE,
    )
    rendered = json.dumps(plan, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        output_path = args.output if args.output.is_absolute() else ROOT / args.output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(f"Built coverage plan: {display_path(output_path.resolve(), ROOT)}")
    else:
        print(rendered, end="")


def persist_coverage_result(
    conn: sqlite3.Connection,
    result: dict[str, Any],
    manifest_file: str,
) -> int:
    now = now_iso()
    status = "completed" if result["ok"] else "incomplete"
    totals = result["totals"]
    conn.execute(
        """
        INSERT INTO search_runs (
            run_date, source, status, required_streams, total_unique,
            known_count, new_count, issue_count, manifest_file, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_date, source) DO UPDATE SET
            status = excluded.status,
            required_streams = excluded.required_streams,
            total_unique = excluded.total_unique,
            known_count = excluded.known_count,
            new_count = excluded.new_count,
            issue_count = excluded.issue_count,
            manifest_file = excluded.manifest_file,
            updated_at = excluded.updated_at
        """,
        (
            result["run_date"],
            result["source"],
            status,
            json.dumps(result["required_streams"], ensure_ascii=False),
            totals["unique"],
            totals["known"],
            totals["new"],
            len(result["issues"]),
            manifest_file,
            now,
            now,
        ),
    )
    run_id = int(
        conn.execute(
            "SELECT id FROM search_runs WHERE run_date = ? AND source = ?",
            (result["run_date"], result["source"]),
        ).fetchone()[0]
    )
    conn.execute("DELETE FROM search_coverage WHERE search_run_id = ?", (run_id,))
    for stream in result["streams"]:
        conn.execute(
            """
            INSERT INTO search_coverage (
                search_run_id, stream_key, status, query_url, query_text,
                search_period_days, page_size, found, pages_expected,
                pages_visited, extracted, unique_count, known_count, new_count,
                error, issues
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                stream["key"],
                stream["status"],
                stream["query_url"],
                stream["query_text"],
                stream["search_period_days"],
                stream["page_size"],
                stream["found"],
                stream["pages_expected"],
                stream["pages_visited"],
                stream["extracted"],
                stream["unique"],
                stream["known"],
                stream["new"],
                stream["error"],
                json.dumps(stream["issues"], ensure_ascii=False),
            ),
        )
    return run_id


def check_coverage(args: argparse.Namespace) -> None:
    manifest_path = args.file if args.file.is_absolute() else ROOT / args.file
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    result = validate_coverage_manifest(payload, REQUIRED_SEARCH_STREAMS)
    if result.get("run_date") and result.get("source"):
        with connect_db(args.db) as conn:
            ensure_schema(conn)
            run_id = persist_coverage_result(conn, result, origin_for(manifest_path))
            conn.commit()
        result["search_run_id"] = run_id
        render_outputs(args.db)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["ok"]:
        raise SystemExit(1)


def migrate_stage_column(conn: sqlite3.Connection, table: str, column: str) -> int:
    changed = 0
    rows = conn.execute(f"SELECT rowid AS rid, {column} AS stage FROM {table}").fetchall()
    for row in rows:
        old_stage = row["stage"]
        new_stage = canonical_stage(old_stage)
        if (old_stage or "") != new_stage:
            conn.execute(f"UPDATE {table} SET {column} = ? WHERE rowid = ?", (new_stage, row["rid"]))
            changed += 1
    return changed


def migrate_stages(args: argparse.Namespace) -> None:
    if not args.no_backup and args.db.exists():
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = args.db.with_name(f"{args.db.name}.bak-{stamp}")
        shutil.copy2(args.db, backup_path)
        print(f"Backup: {db_label(backup_path)}")

    with connect_db(args.db) as conn:
        ensure_schema(conn)
        changed = {
            "vacancies": migrate_stage_column(conn, "vacancies", "latest_stage"),
            "stage_events": migrate_stage_column(conn, "stage_events", "stage"),
            "evaluations": migrate_stage_column(conn, "evaluations", "stage"),
            "applications": migrate_stage_column(conn, "applications", "stage"),
        }

        followup_applications = conn.execute(
            """
            UPDATE applications
            SET stage = 'follow_up'
            WHERE COALESCE(follow_up_date, '') NOT IN ('', '-', '—')
              AND LOWER(COALESCE(status, '')) NOT LIKE '%reject%'
              AND LOWER(COALESCE(status, '')) NOT LIKE '%отказ%'
              AND stage IN ('applied', 'follow_up')
            """
        ).rowcount
        followup_vacancies = conn.execute(
            """
            UPDATE vacancies
            SET latest_stage = 'follow_up', updated_at = ?
            WHERE latest_stage = 'applied'
              AND id IN (
                SELECT vacancy_id
                FROM applications
                WHERE COALESCE(follow_up_date, '') NOT IN ('', '-', '—')
                  AND LOWER(COALESCE(status, '')) NOT LIKE '%reject%'
                  AND LOWER(COALESCE(status, '')) NOT LIKE '%отказ%'
              )
            """,
            (now_iso(),),
        ).rowcount

        conn.commit()

    snapshot = render_outputs(args.db)
    print("Migrated stages to compact funnel model")
    print(f"  changed stage values: {changed}")
    print(f"  follow-up application rows: {followup_applications}")
    print(f"  follow-up vacancies: {followup_vacancies}")
    print(f"  kpis: {snapshot['kpis']}")


def ingest_json(args: argparse.Namespace) -> None:
    payload = json.loads(args.file.read_text(encoding="utf-8"))
    items = payload_items(payload)
    if not items:
        raise SystemExit("Expected a JSON array or an object with vacancies/items/jobs/data array")
    origin_file = origin_for(args.file)
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        count = 0
        for line_no, item in enumerate(items, start=1):
            if ingest_item(
                conn,
                item,
                default_channel=args.channel or "",
                default_source=args.source or "",
                origin_file=origin_file,
                line_no=line_no,
            ):
                count += 1
        conn.commit()
    snapshot = render_outputs(args.db)
    print(
        f"Ingested {count} vacancy rows into SQLite and regenerated "
        f"{display_path(DASHBOARD_PATH, ROOT)}"
    )
    if args.json:
        print(json.dumps({"ingested": count, "kpis": snapshot["kpis"]}, ensure_ascii=False, indent=2))


def ingest_gmail_json(args: argparse.Namespace) -> None:
    payload = json.loads(args.file.read_text(encoding="utf-8"))
    items = payload_items(payload)
    if not items:
        raise SystemExit("Expected a JSON array or an object with a vacancies array")
    origin_file = origin_for(args.file)
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        count = 0
        for line_no, item in enumerate(items, start=1):
            item = dict(item)
            item.setdefault("channel", "gmail_hh")
            item.setdefault("source", "hh_gmail_digest")
            item.setdefault("status", "DISCOVERED_FROM_GMAIL")
            item.setdefault("stage", "seen")
            if ingest_item(
                conn,
                item,
                default_channel="gmail_hh",
                default_source="hh_gmail_digest",
                origin_file=origin_file,
                line_no=line_no,
            ):
                count += 1
        conn.commit()
    render_outputs(args.db)
    print(
        f"Ingested {count} Gmail HH rows and regenerated "
        f"{display_path(DASHBOARD_PATH, ROOT)}"
    )


def update_vacancy(args: argparse.Namespace) -> None:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Use --id, --url, or --external-id to identify the vacancy")
    status = args.status or args.stage or "UPDATED"
    stage = args.stage or detect_stage(status)
    date = args.date or dt.date.today().isoformat()
    norm_url = normalize_url(args.url or "")
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        row = None
        if args.id is not None:
            row = conn.execute("SELECT * FROM vacancies WHERE id = ?", (args.id,)).fetchone()
            if not row:
                raise SystemExit(f"Vacancy id {args.id} not found")
        if not row and args.external_id:
            row = conn.execute("SELECT * FROM vacancies WHERE external_id = ?", (args.external_id,)).fetchone()
        if not row and norm_url:
            row = conn.execute("SELECT * FROM vacancies WHERE url = ?", (norm_url,)).fetchone()
        if not row and not (args.title or norm_url):
            raise SystemExit("Vacancy not found. Provide --title/--company with --url to create it.")

        if row:
            vacancy_id = int(row["id"])

            def value(field: str, new_value: Any) -> Any:
                if new_value is None:
                    return row[field]
                return new_value

            score = args.score if args.score is not None else row["score"]
            conn.execute(
                """
                UPDATE vacancies
                SET source = ?, url = ?, title = ?, company = ?,
                    last_seen_date = ?, latest_status = ?, latest_stage = ?,
                    score = ?, role_type = ?, reason = ?, risks = ?,
                    open_questions = ?, next_action = ?, follow_up_date = ?,
                    resume_version = ?, cover_letter = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    value("source", args.source),
                    value("url", norm_url or None),
                    value("title", args.title),
                    value("company", args.company),
                    date,
                    status,
                    stage,
                    score,
                    value("role_type", args.role_type),
                    value("reason", args.reason),
                    value("risks", args.risks),
                    value("open_questions", args.open_questions),
                    value("next_action", args.next_action),
                    value("follow_up_date", args.follow_up_date),
                    value("resume_version", args.resume_version),
                    value("cover_letter", args.cover_letter),
                    now_iso(),
                    vacancy_id,
                ),
            )
        else:
            vacancy_id = upsert_vacancy(
                conn,
                channel=args.channel or infer_channel({"url": norm_url}),
                source=args.source or "manual_update",
                title=args.title or "",
                company=args.company or "",
                description="",
                url=norm_url,
                seen_date=date,
                status=status,
                stage=stage,
                score=args.score,
                role_type=args.role_type or "",
                reason=args.reason or "",
                risks=args.risks or "",
                open_questions=args.open_questions or "",
                next_action=args.next_action or "",
                follow_up_date=args.follow_up_date or "",
                resume_version=args.resume_version or "",
                cover_letter=args.cover_letter or "",
                origin_file="cli:update-vacancy",
            )

        synced_application_id = None
        if args.sync_application:
            application = conn.execute(
                """
                SELECT id, follow_up_date, resume_version, cover_letter
                FROM applications
                WHERE vacancy_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (vacancy_id,),
            ).fetchone()
            if application:
                application_follow_up_date = (
                    args.follow_up_date
                    if args.follow_up_date is not None
                    else application["follow_up_date"]
                )
                application_resume_version = (
                    args.resume_version
                    if args.resume_version is not None
                    else application["resume_version"]
                )
                application_cover_letter = (
                    args.cover_letter
                    if args.cover_letter is not None
                    else application["cover_letter"]
                )
                conn.execute(
                    """
                    UPDATE applications
                    SET status = ?, stage = ?, follow_up_date = ?,
                        resume_version = ?, cover_letter = ?
                    WHERE id = ?
                    """,
                    (
                        status,
                        stage,
                        application_follow_up_date,
                        application_resume_version,
                        application_cover_letter,
                        int(application["id"]),
                    ),
                )
                synced_application_id = int(application["id"])

        insert_stage_event(
            conn,
            vacancy_id,
            date,
            stage,
            status,
            args.note or args.reason or args.open_questions or args.next_action or "",
            "cli:update-vacancy",
            0,
        )
        conn.commit()
    render_outputs(args.db)
    sync_note = (
        f"; synced application {synced_application_id}"
        if synced_application_id is not None
        else ""
    )
    print(
        f"Updated vacancy {vacancy_id}{sync_note} and regenerated "
        f"{display_path(DASHBOARD_PATH, ROOT)}"
    )


def resolve_vacancy_row(conn: sqlite3.Connection, args: argparse.Namespace) -> sqlite3.Row:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Use --id, --url, or --external-id to identify the vacancy")

    norm_url = normalize_url(args.url or "")
    row = None
    if args.id is not None:
        row = conn.execute("SELECT * FROM vacancies WHERE id = ?", (args.id,)).fetchone()
    if not row and args.external_id:
        row = conn.execute(
            "SELECT * FROM vacancies WHERE external_id = ?", (args.external_id,)
        ).fetchone()
    if not row and norm_url:
        row = conn.execute("SELECT * FROM vacancies WHERE url = ?", (norm_url,)).fetchone()
    if not row:
        raise SystemExit("Vacancy not found")
    return row


def upsert_employer_contact(args: argparse.Namespace) -> None:
    channel = normalize_outreach_channel(args.contact_channel)
    if channel not in DIRECT_OUTREACH_CHANNELS:
        allowed = ", ".join(DIRECT_OUTREACH_CHANNELS)
        raise SystemExit(f"Contact channel must be one of: {allowed}")
    if args.relationship not in CONTACT_RELATIONSHIPS:
        raise SystemExit("Unsupported contact relationship")
    if args.confidence not in CONTACT_CONFIDENCE:
        raise SystemExit("Unsupported contact confidence")
    if not clean_cell(args.evidence_url) and not clean_cell(args.evidence_note):
        raise SystemExit("Use --evidence-url or --evidence-note to prove the contact identity")

    verified_date = args.verified_date or dt.date.today().isoformat()
    now = now_iso()
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy = resolve_vacancy_row(conn, args)
        vacancy_id = int(vacancy["id"])
        conn.execute(
            """
            INSERT INTO employer_contacts (
                vacancy_id, person_name, person_role, relationship, confidence,
                channel, contact_address, profile_url, evidence_url, evidence_note,
                verified_date, is_active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(vacancy_id, channel, contact_address) DO UPDATE SET
                person_name = excluded.person_name,
                person_role = excluded.person_role,
                relationship = excluded.relationship,
                confidence = excluded.confidence,
                profile_url = excluded.profile_url,
                evidence_url = excluded.evidence_url,
                evidence_note = excluded.evidence_note,
                verified_date = excluded.verified_date,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
            """,
            (
                vacancy_id,
                clean_cell(args.person_name),
                clean_cell(args.person_role),
                args.relationship,
                args.confidence,
                channel,
                clean_cell(args.contact_address),
                clean_cell(args.profile_url),
                clean_cell(args.evidence_url),
                clean_cell(args.evidence_note),
                verified_date,
                0 if args.inactive else 1,
                now,
                now,
            ),
        )
        contact = conn.execute(
            """
            SELECT id FROM employer_contacts
            WHERE vacancy_id = ? AND channel = ? AND contact_address = ?
            """,
            (vacancy_id, channel, clean_cell(args.contact_address)),
        ).fetchone()
        conn.commit()

    render_outputs(args.db)
    print(
        f"Stored employer contact {contact['id']} for vacancy {vacancy_id} "
        f"({channel}, {args.confidence}) and regenerated "
        f"{display_path(DASHBOARD_PATH, ROOT)}"
    )


def record_contact_search(args: argparse.Namespace) -> None:
    channels_checked = unique_outreach_channels(args.channels_checked.split(","))
    if not channels_checked:
        allowed = ", ".join(DIRECT_OUTREACH_CHANNELS)
        raise SystemExit(f"--channels-checked must include one of: {allowed}")
    if args.search_status not in CONTACT_SEARCH_STATUSES:
        raise SystemExit("Unsupported contact search status")

    search_date = args.date or dt.date.today().isoformat()
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy = resolve_vacancy_row(conn, args)
        vacancy_id = int(vacancy["id"])
        cur = conn.execute(
            """
            INSERT INTO contact_searches (
                vacancy_id, search_date, status, channels_checked, note, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                vacancy_id,
                search_date,
                args.search_status,
                ",".join(channels_checked),
                clean_cell(args.note),
                now_iso(),
            ),
        )
        insert_stage_event(
            conn,
            vacancy_id,
            search_date,
            vacancy["latest_stage"],
            f"DIRECT_CONTACT_SEARCH_{args.search_status.upper()}",
            clean_cell(args.note) or f"Проверены каналы: {', '.join(channels_checked)}",
            "cli:record-contact-search",
            0,
        )
        conn.commit()

    render_outputs(args.db)
    print(
        f"Recorded contact search {cur.lastrowid} for vacancy {vacancy_id} "
        f"and regenerated {display_path(DASHBOARD_PATH, ROOT)}"
    )


def load_outreach_payload(path: Path) -> dict[str, Any]:
    candidate = path if path.is_absolute() else ROOT / path
    if not candidate.exists():
        raise FileNotFoundError(f"Outreach JSON not found: {candidate}")
    payload = json.loads(candidate.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Outreach JSON must be an object")
    if not isinstance(payload.get("contact_search"), dict):
        raise ValueError("Outreach JSON must contain a contact_search object")
    if not isinstance(payload.get("touchpoints"), list) or not payload["touchpoints"]:
        raise ValueError("Outreach JSON must contain a non-empty touchpoints array")
    return payload


def record_followup(args: argparse.Namespace) -> None:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Use --id, --url, or --external-id to identify the vacancy")
    if not args.outreach_json:
        raise SystemExit(
            "--outreach-json is required so contact research, channels, exact messages, "
            "and visible delivery evidence are stored"
        )

    payload = load_outreach_payload(args.outreach_json)
    contact_search = payload["contact_search"]
    search_status = clean_cell(str(contact_search.get("status") or "")).lower()
    if search_status not in CONTACT_SEARCH_STATUSES:
        allowed = ", ".join(sorted(CONTACT_SEARCH_STATUSES))
        raise SystemExit(f"contact_search.status must be one of: {allowed}")
    raw_checked = contact_search.get("channels_checked") or []
    if not isinstance(raw_checked, list):
        raise SystemExit("contact_search.channels_checked must be an array")
    channels_checked = unique_outreach_channels(
        [str(value) for value in raw_checked], FOLLOW_UP_CHANNELS
    )
    if not channels_checked:
        raise SystemExit(
            "contact_search.channels_checked must include at least one configured "
            "outreach channel"
        )
    research_note = clean_cell(str(contact_search.get("note") or ""))
    if not research_note:
        raise SystemExit("contact_search.note is required")

    event_date = args.date or dt.date.today().isoformat()
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        row = resolve_vacancy_row(conn, args)

        vacancy_id = int(row["id"])
        current_stage = canonical_stage(row["latest_stage"])
        if current_stage not in {"applied", "follow_up"}:
            raise SystemExit(
                f"Vacancy {vacancy_id} is in stage {current_stage}; "
                "record-followup is allowed only for applied/follow_up"
            )
        events = conn.execute(
            """
            SELECT event_date, status
            FROM stage_events
            WHERE vacancy_id = ?
            ORDER BY id ASC
            """,
            (vacancy_id,),
        ).fetchall()
        numbered_events = [
            (event["event_date"], follow_up_number_from_status(event["status"]))
            for event in events
        ]
        round_row = conn.execute(
            "SELECT MAX(round_number) AS n FROM followup_rounds WHERE vacancy_id = ?",
            (vacancy_id,),
        ).fetchone()
        previous_number = max(
            max((number for _, number in numbered_events), default=0),
            int(round_row["n"] or 0),
        )
        if previous_number >= FOLLOW_UP_LIMIT:
            raise SystemExit(
                f"Vacancy {vacancy_id} already reached the {FOLLOW_UP_LIMIT}-follow-up limit"
            )
        if any(date == event_date and number > 0 for date, number in numbered_events):
            raise SystemExit(
                f"Vacancy {vacancy_id} already has a follow-up recorded on {event_date}"
            )
        duplicate_round_date = conn.execute(
            "SELECT 1 FROM followup_rounds WHERE vacancy_id = ? AND sent_date = ? LIMIT 1",
            (vacancy_id, event_date),
        ).fetchone()
        if duplicate_round_date:
            raise SystemExit(
                f"Vacancy {vacancy_id} already has a structured follow-up round on {event_date}"
            )

        prepared_touchpoints: list[dict[str, Any]] = []
        seen_channels: set[str] = set()
        sent_channels: list[str] = []
        sent_direct_count = 0
        for index, raw_touchpoint in enumerate(payload["touchpoints"], start=1):
            if not isinstance(raw_touchpoint, dict):
                raise SystemExit(f"touchpoints[{index}] must be an object")
            channel = normalize_outreach_channel(str(raw_touchpoint.get("channel") or ""))
            if channel not in FOLLOW_UP_CHANNELS:
                allowed = ", ".join(FOLLOW_UP_CHANNELS)
                raise SystemExit(f"touchpoints[{index}].channel must be one of: {allowed}")
            if channel in seen_channels:
                raise SystemExit(f"Only one touchpoint per channel is allowed in a follow-up round: {channel}")
            seen_channels.add(channel)

            delivery_status = clean_cell(
                str(raw_touchpoint.get("delivery_status") or "sent")
            ).lower()
            if delivery_status not in OUTREACH_DELIVERY_STATUSES:
                raise SystemExit(f"Unsupported delivery status: {delivery_status}")
            message_text = str(
                raw_touchpoint.get("message_text") or raw_touchpoint.get("message") or ""
            ).strip()
            evidence_note = clean_cell(str(raw_touchpoint.get("evidence_note") or ""))
            if delivery_status == "sent" and (not message_text or not evidence_note):
                raise SystemExit(
                    f"Sent touchpoint {channel} requires exact message_text and evidence_note"
                )

            contact_id: int | None = None
            recipient_name = clean_cell(str(raw_touchpoint.get("recipient_name") or ""))
            recipient_address = clean_cell(str(raw_touchpoint.get("recipient_address") or ""))
            if channel in DIRECT_OUTREACH_CHANNELS:
                try:
                    contact_id = int(raw_touchpoint.get("contact_id"))
                except (TypeError, ValueError):
                    raise SystemExit(
                        f"Direct touchpoint {channel} requires contact_id from upsert-contact"
                    )
                contact = conn.execute(
                    "SELECT * FROM employer_contacts WHERE id = ?",
                    (contact_id,),
                ).fetchone()
                if not contact or int(contact["vacancy_id"]) != vacancy_id:
                    raise SystemExit(f"Contact {contact_id} does not belong to vacancy {vacancy_id}")
                if not int(contact["is_active"]):
                    raise SystemExit(f"Contact {contact_id} is inactive")
                if contact["channel"] != channel:
                    raise SystemExit(
                        f"Contact {contact_id} is stored for {contact['channel']}, not {channel}"
                    )
                if delivery_status == "sent" and contact["confidence"] not in DIRECT_SEND_CONFIDENCE:
                    raise SystemExit(
                        f"Contact {contact_id} confidence is {contact['confidence']}; "
                        "automatic send requires confirmed or strong"
                    )
                recipient_name = contact["person_name"]
                recipient_address = contact["contact_address"]
                if delivery_status == "sent":
                    sent_direct_count += 1
            else:
                primary_label = CHANNEL_LABELS.get(
                    PRIMARY_OUTREACH_CHANNEL, PRIMARY_OUTREACH_CHANNEL.title()
                )
                recipient_name = recipient_name or f"{primary_label} vacancy thread"
                recipient_address = recipient_address or row["url"] or row["external_id"]

            if delivery_status == "sent":
                sent_channels.append(channel)
            prepared_touchpoints.append(
                {
                    "contact_id": contact_id,
                    "channel": channel,
                    "recipient_name": recipient_name,
                    "recipient_address": recipient_address,
                    "message_text": message_text,
                    "delivery_status": delivery_status,
                    "evidence_note": evidence_note,
                    "sent_at": clean_cell(str(raw_touchpoint.get("sent_at") or event_date)),
                }
            )

        if not sent_channels:
            raise SystemExit("A follow-up round must contain at least one sent touchpoint")
        if sent_direct_count > MAX_DIRECT_MESSAGES_PER_ROUND:
            preferred = ", then ".join(
                CHANNEL_LABELS.get(channel, channel.title())
                for channel in DIRECT_OUTREACH_CHANNELS
            )
            raise SystemExit(
                f"Use at most {MAX_DIRECT_MESSAGES_PER_ROUND} sent direct channel(s) "
                f"per follow-up round; configured order: {preferred}"
            )

        follow_up_number = previous_number + 1
        if follow_up_number < FOLLOW_UP_LIMIT:
            next_date = add_business_days(event_date, args.business_days)
            stage = "follow_up"
            status = f"FOLLOW_UP_{follow_up_number}_SENT_WAITING_EMPLOYER"
            next_action = (
                f"Если работодатель не ответит, отправить follow-up №"
                f"{follow_up_number + 1} {next_date}"
            )
        else:
            next_date = ""
            stage = "applied"
            status = (
                f"FOLLOW_UP_{FOLLOW_UP_LIMIT}_SENT_LIMIT_REACHED_WAITING_EMPLOYER"
            )
            next_action = (
                "Ждать ответа работодателя; лимит из "
                f"{FOLLOW_UP_LIMIT} follow-up исчерпан"
            )

        now = now_iso()
        contact_search_cur = conn.execute(
            """
            INSERT INTO contact_searches (
                vacancy_id, search_date, status, channels_checked, note, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                vacancy_id,
                event_date,
                search_status,
                ",".join(channels_checked),
                research_note,
                now,
            ),
        )
        round_cur = conn.execute(
            """
            INSERT INTO followup_rounds (
                vacancy_id, round_number, sent_date, next_follow_up_date, status,
                contact_search_status, channels_checked, research_note, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vacancy_id,
                follow_up_number,
                event_date,
                next_date,
                status,
                search_status,
                ",".join(channels_checked),
                research_note,
                now,
            ),
        )
        followup_round_id = int(round_cur.lastrowid)
        for touchpoint in prepared_touchpoints:
            conn.execute(
                """
                INSERT INTO outreach_messages (
                    followup_round_id, vacancy_id, contact_id, channel,
                    recipient_name, recipient_address, message_text,
                    delivery_status, evidence_note, sent_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    followup_round_id,
                    vacancy_id,
                    touchpoint["contact_id"],
                    touchpoint["channel"],
                    touchpoint["recipient_name"],
                    touchpoint["recipient_address"],
                    touchpoint["message_text"],
                    touchpoint["delivery_status"],
                    touchpoint["evidence_note"],
                    touchpoint["sent_at"],
                    now,
                ),
            )

        conn.execute(
            """
            UPDATE vacancies
            SET latest_status = ?, latest_stage = ?, next_action = ?,
                follow_up_date = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, stage, next_action, next_date, now, vacancy_id),
        )
        application = conn.execute(
            """
            SELECT id FROM applications
            WHERE vacancy_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (vacancy_id,),
        ).fetchone()
        if application:
            conn.execute(
                """
                UPDATE applications
                SET status = ?, stage = ?, follow_up_date = ?
                WHERE id = ?
                """,
                (status, stage, next_date, int(application["id"])),
            )

        sent_label = ", ".join(
            CHANNEL_LABELS.get(channel, channel.title()) for channel in sent_channels
        )
        note = f"Follow-up №{follow_up_number} отправлен. Каналы: {sent_label}."
        if args.note:
            note = f"{note} {clean_cell(args.note)}"
        insert_stage_event(
            conn,
            vacancy_id,
            event_date,
            stage,
            status,
            note,
            "cli:record-followup",
            0,
        )
        conn.commit()

    render_outputs(args.db)
    print(
        f"Recorded follow-up round {follow_up_number} for vacancy {vacancy_id} "
        f"via {', '.join(sent_channels)}; contact search {contact_search_cur.lastrowid}; "
        f"regenerated {display_path(DASHBOARD_PATH, ROOT)}"
    )


def attach_interview_summary(args: argparse.Namespace) -> None:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Use --id, --url, or --external-id to identify the vacancy")

    file_path = project_relative_file_path(args.file)
    norm_url = normalize_url(args.url or "")
    date = args.date or dt.date.today().isoformat()

    with connect_db(args.db) as conn:
        ensure_schema(conn)
        row = None
        if args.id is not None:
            row = conn.execute("SELECT * FROM vacancies WHERE id = ?", (args.id,)).fetchone()
            if not row:
                raise SystemExit(f"Vacancy id {args.id} not found")
        if not row and args.external_id:
            row = conn.execute("SELECT * FROM vacancies WHERE external_id = ?", (args.external_id,)).fetchone()
        if not row and norm_url:
            row = conn.execute("SELECT * FROM vacancies WHERE url = ?", (norm_url,)).fetchone()
        if not row:
            raise SystemExit("Vacancy not found")

        vacancy_id = int(row["id"])
        parsed_no = parse_interview_no(args.stage)
        if args.interview_no is not None:
            interview_no = args.interview_no
        elif parsed_no is not None:
            interview_no = parsed_no
        else:
            max_row = conn.execute(
                "SELECT MAX(interview_no) AS max_no FROM interview_summaries WHERE vacancy_id = ?",
                (vacancy_id,),
            ).fetchone()
            interview_no = int(max_row["max_no"] or 0) + 1
        if interview_no < 1:
            raise SystemExit("--interview-no must be a positive integer")

        stage = args.stage or f"interview_{interview_no}"
        title = args.title or f"Interview {interview_no}"
        now = now_iso()
        conn.execute(
            """
            INSERT INTO interview_summaries (
                vacancy_id, interview_no, stage, summary_date, title, file_path,
                note, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(vacancy_id, interview_no, file_path) DO UPDATE SET
                stage = excluded.stage,
                summary_date = excluded.summary_date,
                title = excluded.title,
                note = excluded.note,
                updated_at = excluded.updated_at
            """,
            (
                vacancy_id,
                interview_no,
                stage,
                date,
                title,
                file_path,
                args.note or "",
                now,
                now,
            ),
        )
        summary_row = conn.execute(
            """
            SELECT id FROM interview_summaries
            WHERE vacancy_id = ? AND interview_no = ? AND file_path = ?
            """,
            (vacancy_id, interview_no, file_path),
        ).fetchone()
        conn.commit()

    render_outputs(args.db)
    print(
        "Attached interview summary "
        f"{summary_row['id']} to vacancy {vacancy_id} "
        f"({stage}, {file_path}) and regenerated {display_path(DASHBOARD_PATH, ROOT)}"
    )


def initialize_workspace(args: argparse.Namespace) -> None:
    """Create local-only settings, profile templates, and an empty database."""

    created: list[str] = []
    kept: list[str] = []
    settings_template = CODE_ROOT / "config" / "settings.example.toml"
    if not settings_template.exists():
        raise FileNotFoundError(f"Missing settings template: {settings_template}")

    if SETTINGS.config_path.exists():
        kept.append(display_path(SETTINGS.config_path, ROOT))
    else:
        SETTINGS.config_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(settings_template, SETTINGS.config_path)
        created.append(display_path(SETTINGS.config_path, ROOT))
        configure_runtime(SETTINGS.config_path)

    templates: list[tuple[Path, Path]] = []
    if SETTINGS.profile.files:
        templates.append((CODE_ROOT / "examples" / "profile.md", SETTINGS.profile.files[0]))
    templates.extend(
        [
            (
                CODE_ROOT / "examples" / "preferences.md",
                SETTINGS.profile.preferences_file,
            ),
            (CODE_ROOT / "examples" / "scoring.md", SETTINGS.profile.scoring_file),
            (
                CODE_ROOT / "examples" / "questions_and_answers.md",
                SETTINGS.profile.answers_file,
            ),
        ]
    )
    for source, target in templates:
        if target.exists():
            kept.append(display_path(target, ROOT))
            continue
        if not source.exists():
            raise FileNotFoundError(f"Missing workspace template: {source}")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        created.append(display_path(target, ROOT))

    for directory in (
        DB_PATH.parent,
        DASHBOARD_PATH.parent,
        VIEWS_DIR,
        REPORTS_DIR,
        ARCHIVE_DIR,
        ROOT / "tmp",
        ROOT / "output",
    ):
        directory.mkdir(parents=True, exist_ok=True)

    database_existed = DB_PATH.exists()
    snapshot = render_outputs(DB_PATH)
    (created if not database_existed else kept).append(display_path(DB_PATH, ROOT))

    result = {
        "workspace": str(ROOT),
        "created": created,
        "kept": kept,
        "kpis": snapshot["kpis"],
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print(f"Initialized workspace: {ROOT}")
    if created:
        print("  created: " + ", ".join(created))
    if kept:
        print("  kept existing: " + ", ".join(kept))
    print(f"  dashboard: {display_path(DASHBOARD_PATH, ROOT)}")


def doctor(args: argparse.Namespace) -> None:
    """Check configuration, private profile files, and database health."""

    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str, required: bool = True) -> None:
        checks.append(
            {"name": name, "ok": ok, "required": required, "detail": detail}
        )

    add(
        "settings",
        SETTINGS.config_loaded,
        display_path(SETTINGS.config_path, ROOT)
        if SETTINGS.config_loaded
        else "local settings are absent; built-in safe defaults are active",
    )
    add("workspace", ROOT.exists() and ROOT.is_dir(), str(ROOT))
    add(
        "workspace_writable",
        ROOT.exists() and os.access(ROOT, os.W_OK),
        str(ROOT),
    )

    for profile_path in SETTINGS.profile.all_files():
        add(
            f"profile:{display_path(profile_path, ROOT)}",
            profile_path.is_file(),
            "present" if profile_path.is_file() else "missing",
        )

    if DB_PATH.exists():
        try:
            uri = f"file:{DB_PATH.as_posix()}?mode=ro"
            with sqlite3.connect(uri, uri=True) as conn:
                quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
                schema_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
                missing_tables = missing_schema_tables(conn)
                foreign_key_issues = len(conn.execute("PRAGMA foreign_key_check").fetchall())
            add("database_integrity", quick_check == "ok", quick_check)
            add(
                "database_schema",
                schema_version == SCHEMA_VERSION and not missing_tables,
                (
                    f"database={schema_version}, supported={SCHEMA_VERSION}, "
                    f"missing_tables={','.join(missing_tables) or 'none'}"
                ),
            )
            add(
                "database_foreign_keys",
                foreign_key_issues == 0,
                f"issues={foreign_key_issues}",
            )
        except sqlite3.Error as exc:
            add("database_integrity", False, str(exc))
    else:
        add("database", False, f"missing: {display_path(DB_PATH, ROOT)}")

    add(
        "automation_confirmation",
        SETTINGS.automation.require_visible_confirmation,
        "visible confirmation required"
        if SETTINGS.automation.require_visible_confirmation
        else "visible confirmation disabled",
    )
    add(
        "search_streams",
        bool(REQUIRED_SEARCH_STREAMS),
        ", ".join(REQUIRED_SEARCH_STREAMS) or "none configured",
    )

    failed = [check for check in checks if check["required"] and not check["ok"]]
    result = {
        "ok": not failed,
        "workspace": str(ROOT),
        "config": display_path(SETTINGS.config_path, ROOT),
        "auto_apply": SETTINGS.automation.auto_apply,
        "apply_threshold": SETTINGS.automation.apply_threshold,
        "checks": checks,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        for check in checks:
            marker = "OK" if check["ok"] else "FAIL"
            print(f"[{marker}] {check['name']}: {check['detail']}")
        print("Workspace is ready." if not failed else "Workspace needs attention.")
    if args.strict and failed:
        raise SystemExit(1)


def extract_config_argument(argv: list[str]) -> tuple[list[str], Path | None]:
    """Allow --config before or after the subcommand."""

    normalized: list[str] = []
    selected: Path | None = None
    index = 0
    while index < len(argv):
        value = argv[index]
        if value == "--config":
            if index + 1 >= len(argv):
                raise SystemExit("--config requires a path")
            selected = Path(argv[index + 1])
            index += 2
            continue
        if value.startswith("--config="):
            selected = Path(value.split("=", 1)[1])
            index += 1
            continue
        normalized.append(value)
        index += 1
    if selected is not None:
        normalized = ["--config", str(selected), *normalized]
    return normalized, selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Job-search database, reports, and dashboard"
    )
    parser.set_defaults(func=rebuild)
    parser.add_argument(
        "--config",
        type=Path,
        default=SETTINGS.config_path,
        help="Local TOML settings path (also supports JOB_SEARCH_CONFIG)",
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="SQLite database path")
    parser.add_argument("--json", action="store_true", help="Print machine-readable rebuild summary")

    sub = parser.add_subparsers()

    init_parser = sub.add_parser(
        "init", help="Create local settings, private profile templates, and database"
    )
    init_parser.add_argument("--json", action="store_true")
    init_parser.set_defaults(func=initialize_workspace)

    doctor_parser = sub.add_parser(
        "doctor", help="Check local configuration, profile files, and database"
    )
    doctor_parser.add_argument("--json", action="store_true")
    doctor_parser.add_argument(
        "--strict", action="store_true", help="Exit non-zero when a required check fails"
    )
    doctor_parser.set_defaults(func=doctor)

    rebuild_parser = sub.add_parser("rebuild", help="Regenerate views and dashboard from SQLite")
    rebuild_parser.add_argument("--db", type=Path, default=DB_PATH)
    rebuild_parser.add_argument("--json", action="store_true")
    rebuild_parser.set_defaults(func=rebuild)

    stats_parser = sub.add_parser("stats", help="Print current database KPI summary")
    stats_parser.add_argument("--db", type=Path, default=DB_PATH)
    stats_parser.set_defaults(func=print_stats)

    migrate_parser = sub.add_parser("migrate-stages", help="Migrate existing DB rows to the compact funnel stage model")
    migrate_parser.add_argument("--db", type=Path, default=DB_PATH)
    migrate_parser.add_argument("--no-backup", action="store_true", help="Do not create a .bak copy before migration")
    migrate_parser.set_defaults(func=migrate_stages)

    schema_parser = sub.add_parser(
        "migrate-schema",
        help="Back up and migrate an existing SQLite database to the current schema",
    )
    schema_parser.add_argument("--db", type=Path, default=DB_PATH)
    schema_parser.add_argument("--no-backup", action="store_true")
    schema_parser.add_argument("--json", action="store_true")
    schema_parser.set_defaults(func=migrate_schema)

    plan_parser = sub.add_parser(
        "build-coverage-plan",
        help="Build deterministic HH URLs and a coverage-manifest skeleton",
    )
    plan_parser.add_argument("file", type=Path, help="JSON plan with stream query specifications")
    plan_parser.add_argument("--output", type=Path, default=None)
    plan_parser.set_defaults(func=build_coverage_plan_command)

    coverage_parser = sub.add_parser(
        "check-coverage",
        help="Persist and fail-closed validate a completed search coverage manifest",
    )
    coverage_parser.add_argument("file", type=Path, help="Completed coverage manifest JSON")
    coverage_parser.add_argument("--db", type=Path, default=DB_PATH)
    coverage_parser.set_defaults(func=check_coverage)

    watch_parser = sub.add_parser("watch", help="Regenerate views automatically when the SQLite database changes")
    watch_parser.add_argument("--db", type=Path, default=DB_PATH)
    watch_parser.add_argument("--interval", type=float, default=2.0, help="Polling interval in seconds")
    watch_parser.set_defaults(func=watch)

    ingest_parser = sub.add_parser("ingest-json", help="Ingest structured vacancy rows into SQLite")
    ingest_parser.add_argument("file", type=Path, help="JSON array or object with vacancies/items/jobs/data")
    ingest_parser.add_argument("--db", type=Path, default=DB_PATH)
    ingest_parser.add_argument("--channel", default="", help="Default channel for rows without a channel field")
    ingest_parser.add_argument("--source", default="", help="Default source/source_stream for rows without source fields")
    ingest_parser.add_argument("--json", action="store_true")
    ingest_parser.set_defaults(func=ingest_json)

    gmail_parser = sub.add_parser("ingest-gmail-json", help="Ingest HH vacancy links extracted from Gmail")
    gmail_parser.add_argument("file", type=Path, help="JSON file with vacancies")
    gmail_parser.add_argument("--db", type=Path, default=DB_PATH)
    gmail_parser.set_defaults(func=ingest_gmail_json)

    update_parser = sub.add_parser("update-vacancy", help="Update one vacancy status/stage in SQLite")
    update_parser.add_argument("--db", type=Path, default=DB_PATH)
    update_parser.add_argument("--id", type=int, default=None, help="Internal vacancy id from Review Inbox")
    update_parser.add_argument("--url", default="")
    update_parser.add_argument("--external-id", default="")
    update_parser.add_argument("--date", default="")
    update_parser.add_argument("--channel", default="")
    update_parser.add_argument("--source", default=None)
    update_parser.add_argument("--title", default=None)
    update_parser.add_argument("--company", default=None)
    update_parser.add_argument("--status", default="")
    update_parser.add_argument("--stage", default="")
    update_parser.add_argument("--score", type=int, default=None)
    update_parser.add_argument("--role-type", default=None)
    update_parser.add_argument("--reason", default=None)
    update_parser.add_argument("--risks", default=None)
    update_parser.add_argument("--open-questions", default=None)
    update_parser.add_argument("--next-action", default=None)
    update_parser.add_argument("--follow-up-date", default=None)
    update_parser.add_argument("--resume-version", default=None)
    update_parser.add_argument("--cover-letter", default=None)
    update_parser.add_argument("--note", default="")
    update_parser.add_argument(
        "--sync-application",
        action="store_true",
        help="Also sync status, stage, and follow-up date to the latest application row",
    )
    update_parser.set_defaults(func=update_vacancy)

    contact_parser = sub.add_parser(
        "upsert-contact",
        help="Store or update a verified recruiter/hiring-manager contact for one vacancy",
    )
    contact_parser.add_argument("--db", type=Path, default=DB_PATH)
    contact_parser.add_argument("--id", type=int, default=None)
    contact_parser.add_argument("--url", default="")
    contact_parser.add_argument("--external-id", default="")
    contact_parser.add_argument("--person-name", required=True)
    contact_parser.add_argument("--person-role", default="")
    contact_parser.add_argument(
        "--relationship",
        choices=sorted(CONTACT_RELATIONSHIPS),
        required=True,
    )
    contact_parser.add_argument(
        "--confidence",
        choices=sorted(CONTACT_CONFIDENCE),
        required=True,
    )
    contact_parser.add_argument(
        "--contact-channel",
        choices=list(DIRECT_OUTREACH_CHANNELS),
        required=True,
    )
    contact_parser.add_argument("--contact-address", required=True)
    contact_parser.add_argument("--profile-url", default="")
    contact_parser.add_argument("--evidence-url", default="")
    contact_parser.add_argument("--evidence-note", default="")
    contact_parser.add_argument("--verified-date", default="")
    contact_parser.add_argument("--inactive", action="store_true")
    contact_parser.set_defaults(func=upsert_employer_contact)

    contact_search_parser = sub.add_parser(
        "record-contact-search",
        help="Record direct-contact research when no follow-up message is being recorded",
    )
    contact_search_parser.add_argument("--db", type=Path, default=DB_PATH)
    contact_search_parser.add_argument("--id", type=int, default=None)
    contact_search_parser.add_argument("--url", default="")
    contact_search_parser.add_argument("--external-id", default="")
    contact_search_parser.add_argument("--date", default="")
    contact_search_parser.add_argument(
        "--status",
        dest="search_status",
        choices=sorted(CONTACT_SEARCH_STATUSES),
        required=True,
    )
    contact_search_parser.add_argument(
        "--channels-checked",
        required=True,
        help=(
            "Comma-separated subset of configured direct channels: "
            + ",".join(DIRECT_OUTREACH_CHANNELS)
        ),
    )
    contact_search_parser.add_argument("--note", required=True)
    contact_search_parser.set_defaults(func=record_contact_search)

    followup_parser = sub.add_parser(
        "record-followup",
        help="Record one multi-channel follow-up round and schedule the next one",
    )
    followup_parser.add_argument("--db", type=Path, default=DB_PATH)
    followup_parser.add_argument("--id", type=int, default=None)
    followup_parser.add_argument("--url", default="")
    followup_parser.add_argument("--external-id", default="")
    followup_parser.add_argument("--date", default="")
    followup_parser.add_argument(
        "--business-days",
        type=int,
        default=FOLLOW_UP_INTERVAL_BUSINESS_DAYS,
    )
    followup_parser.add_argument(
        "--outreach-json",
        type=Path,
        required=True,
        help="JSON with contact_search and exact channel touchpoints plus delivery evidence",
    )
    followup_parser.add_argument("--note", default="")
    followup_parser.set_defaults(func=record_followup)

    interview_parser = sub.add_parser("attach-interview-summary", help="Attach a Markdown interview summary to one vacancy")
    interview_parser.add_argument("--db", type=Path, default=DB_PATH)
    interview_parser.add_argument("--id", type=int, default=None, help="Internal vacancy id from dashboard")
    interview_parser.add_argument("--url", default="")
    interview_parser.add_argument("--external-id", default="")
    interview_parser.add_argument("--file", type=Path, required=True, help="Markdown summary file inside the project folder")
    interview_parser.add_argument("--interview-no", type=int, default=None, help="Interview sequence number, supports 1, 2, 3, 4, ...")
    interview_parser.add_argument("--stage", default="", help="Interview stage label, defaults to interview_<number>")
    interview_parser.add_argument("--date", default="", help="Interview or summary date, defaults to today")
    interview_parser.add_argument("--title", default="", help="Short dashboard link label, defaults to Interview <number>")
    interview_parser.add_argument("--note", default="")
    interview_parser.set_defaults(func=attach_interview_summary)

    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    raw_argv, config_path = extract_config_argument(raw_argv)
    try:
        configure_runtime(config_path)
    except (OSError, ValueError) as exc:
        print(f"jobctl config error: {exc}", file=sys.stderr)
        return 2
    parser = build_parser()
    args = parser.parse_args(raw_argv)
    if hasattr(args, "db"):
        args.db = args.db.resolve() if not args.db.is_absolute() else args.db
    try:
        args.func(args)
    except Exception as exc:  # Keep CLI errors readable for daily runs.
        print(f"jobctl error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
