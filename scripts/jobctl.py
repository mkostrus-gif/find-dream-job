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
import statistics
import sys
import time
import unicodedata
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
from telegram_source import build_telegram_plan, validate_telegram_manifest


CODE_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 7

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
SOURCE_STREAM_ALIASES: dict[str, tuple[str, ...]]
TELEGRAM_ENABLED: bool
TELEGRAM_INITIAL_LOOKBACK_DAYS: int
TELEGRAM_CHANNELS: tuple[Any, ...]
DECISION_CAMPAIGN_IDS: tuple[str, ...]
DECISION_ROLE_FAMILIES: tuple[str, ...]
DECISION_RESUME_IDS: tuple[str, ...]
DECISION_MESSAGE_VARIANTS: tuple[str, ...]
WIP_BUCKETS: tuple[Any, ...]
WIP_PAGE_SIZE: int
PERSONAL_RECOMMENDATIONS_ENABLED: bool
PERSONAL_RECOMMENDATION_STREAM: str
ACTIVE_POLICY_VERSION: str
ACTIVE_POLICY_EFFECTIVE_DATE: str
ACCOUNT_ACTIVE_PORTFOLIO_LIMIT: int


def configure_runtime(config_path: Path | None = None) -> None:
    """Load local settings and update runtime paths and policy constants."""

    global SETTINGS, ROOT, DB_PATH, DASHBOARD_PATH, VIEWS_DIR, REPORTS_DIR
    global ARCHIVE_DIR, PROJECT_TITLE, PROJECT_LOCALE, FOLLOW_UP_LIMIT
    global FOLLOW_UP_INTERVAL_BUSINESS_DAYS, PRIMARY_OUTREACH_CHANNEL
    global DIRECT_OUTREACH_CHANNELS, FOLLOW_UP_CHANNELS
    global MAX_DIRECT_MESSAGES_PER_ROUND, REQUIRED_SEARCH_STREAMS
    global DEFAULT_SEARCH_PERIOD_DAYS, SEARCH_ITEMS_PER_PAGE, CHANNEL_LABELS
    global SOURCE_STREAM_ALIASES, TELEGRAM_ENABLED
    global TELEGRAM_INITIAL_LOOKBACK_DAYS, TELEGRAM_CHANNELS
    global DECISION_CAMPAIGN_IDS, DECISION_ROLE_FAMILIES, DECISION_RESUME_IDS
    global DECISION_MESSAGE_VARIANTS, WIP_BUCKETS, WIP_PAGE_SIZE
    global PERSONAL_RECOMMENDATIONS_ENABLED, PERSONAL_RECOMMENDATION_STREAM
    global ACTIVE_POLICY_VERSION, ACTIVE_POLICY_EFFECTIVE_DATE
    global ACCOUNT_ACTIVE_PORTFOLIO_LIMIT

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
    SOURCE_STREAM_ALIASES = dict(SETTINGS.search.stream_aliases)
    TELEGRAM_ENABLED = SETTINGS.telegram.enabled
    TELEGRAM_INITIAL_LOOKBACK_DAYS = SETTINGS.telegram.initial_lookback_days
    TELEGRAM_CHANNELS = SETTINGS.telegram.channels
    DECISION_CAMPAIGN_IDS = SETTINGS.decision.campaign_ids
    DECISION_ROLE_FAMILIES = SETTINGS.decision.role_families
    DECISION_RESUME_IDS = SETTINGS.decision.resume_ids
    DECISION_MESSAGE_VARIANTS = SETTINGS.decision.message_variants
    WIP_BUCKETS = SETTINGS.wip.buckets
    WIP_PAGE_SIZE = SETTINGS.wip.page_size
    PERSONAL_RECOMMENDATIONS_ENABLED = (
        SETTINGS.search.personal_recommendations_enabled
    )
    PERSONAL_RECOMMENDATION_STREAM = (
        SETTINGS.search.personal_recommendation_stream
    )
    ACTIVE_POLICY_VERSION = SETTINGS.policy.active_version
    ACTIVE_POLICY_EFFECTIVE_DATE = SETTINGS.policy.effective_date
    ACCOUNT_ACTIVE_PORTFOLIO_LIMIT = SETTINGS.account.active_portfolio_limit


# Load safe built-in defaults at import time. main() reloads the selected local
# config after it has parsed --config, including when the default config is bad.
configure_runtime(CODE_ROOT / "config" / ".defaults-only.toml")

VACANCY_ID_RE = re.compile(r"/vacancy/(\d+)")
LINKEDIN_JOB_ID_RE = re.compile(r"/jobs/view/(?:[^/?#]*-)?(\d+)(?:[/?#]|$)")
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
EMPLOYER_INTERACTION_TYPES = {
    "human_reply",
    "automated_ack",
    "screening_request",
    "interview_invite",
    "rejection",
    "other",
}
LIFECYCLE_EVENT_TYPES = {
    "application_confirmed",
    "rejected",
    "interview_invited",
    "interview_scheduled",
    "interview_completed",
    "interview_cancelled",
    "interview_no_show_candidate",
    "interview_no_show_employer",
    "offer_received",
}
INTERVIEW_EVENT_TYPES = {
    "interview_invited",
    "interview_scheduled",
    "interview_completed",
    "interview_cancelled",
    "interview_no_show_candidate",
    "interview_no_show_employer",
}
EXTERNAL_ACTION_TYPES = {
    "application",
    "message",
    "follow_up",
    "mailbox_mutation",
    "publication",
    "other",
}
EXTERNAL_ACTION_STATES = {
    "drafted",
    "authorized",
    "attempted",
    "visibly_confirmed",
    "blocked",
    "failed",
}
ACTION_STATES = {
    "review",
    "needs_input",
    "employer_reply",
    "follow_up",
    "account_research",
    "waiting",
    "none",
}
WIP_BUCKET_KEYS = {
    "urgent",
    "due_follow_up",
    "deep_review",
    "account_research",
    "backlog",
}
HUMAN_PATH_STATUSES = {
    "unknown",
    "not_searched",
    "researching",
    "verified",
    "not_found",
    "blocked",
}
HARD_GATE_RESULTS = {"pass", "fail", "unknown"}
QUARANTINE_CLASSIFICATIONS = {
    "captcha",
    "logged_out",
    "access_error",
    "malformed",
    "missing_required_fields",
    "non_vacancy",
    "unknown",
}
QUARANTINE_STATUSES = {"pending", "reprocessed", "dismissed"}
EMPLOYER_ACTOR_TYPES = {
    "recruiter",
    "hiring_manager",
    "founder",
    "system",
    "unknown",
}
EMPLOYER_SIGNAL_TYPES = {
    "technology_adoption",
    "ai_adoption",
    "hiring_growth",
    "restructuring",
    "leadership_change",
    "culture",
    "other",
}
EVIDENCE_CONFIDENCE = {"low", "medium", "high", "confirmed"}
CORE_VACANCY_FACTORS = {
    "technology_adoption_maturity",
    "work_content_risk",
    "hiring_reality",
    "human_access",
}
FACTOR_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{1,63}$")
EXPECTED_TABLES = {
    "action_events",
    "applications",
    "contact_searches",
    "employer_contacts",
    "employer_accounts",
    "employer_account_signals",
    "employer_interactions",
    "employer_interaction_invalidations",
    "evaluations",
    "followup_rounds",
    "import_issues",
    "interview_summaries",
    "lifecycle_events",
    "external_actions",
    "migration_log",
    "outreach_messages",
    "policy_versions",
    "quarantine_records",
    "screening_decisions",
    "source_hits",
    "source_hit_labels",
    "source_labels",
    "search_coverage",
    "search_runs",
    "source_checkpoints",
    "stage_events",
    "vacancies",
    "vacancy_employer_accounts",
    "vacancy_external_aliases",
    "vacancy_factors",
    "vacancy_fingerprints",
    "vacancy_decision_metadata",
}
EXPECTED_INDEXES = {
    "idx_action_events_vacancy",
    "idx_employer_account_signals_account",
    "idx_employer_interaction_invalidations_vacancy",
    "idx_employer_interactions_identity",
    "idx_employer_interactions_vacancy",
    "idx_external_actions_vacancy",
    "idx_lifecycle_events_vacancy",
    "idx_quarantine_status",
    "idx_screening_decisions_policy",
    "idx_source_checkpoints_source",
    "idx_source_hit_labels_label",
    "idx_vacancy_external_aliases_external_id",
    "idx_vacancy_external_aliases_vacancy",
    "idx_vacancy_employer_accounts_account",
    "idx_vacancy_factors_vacancy",
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
    "seen": "Новая",
    "needs_input": "Нужны данные",
    "follow_up": "Повторное обращение",
    "applied": "Отклик подтверждён",
    "interview_1": "Интервью 1",
    "interview_2": "Интервью 2",
    "interview_3": "Интервью 3",
    "offer": "Предложение",
    "rejected": "Отказ",
}

ACTION_STATE_LABELS = {
    "review": "проверка",
    "needs_input": "нужны данные",
    "employer_reply": "ответ работодателя",
    "follow_up": "повторное обращение",
    "account_research": "исследование работодателя",
    "waiting": "ожидание",
    "none": "действий нет",
}

HUMAN_PATH_LABELS = {
    "unknown": "неизвестно",
    "not_searched": "поиск не проводился",
    "researching": "идёт поиск",
    "verified": "проверен",
    "not_found": "не найден",
    "blocked": "заблокирован",
}

QUARANTINE_LABELS = {
    "captcha": "CAPTCHA",
    "logged_out": "сеанс не авторизован",
    "access_error": "ошибка доступа",
    "malformed": "некорректная запись",
    "missing_required_fields": "нет обязательных полей",
    "non_vacancy": "не вакансия",
    "unknown": "неизвестно",
}

QUARANTINE_STATUS_LABELS = {
    "pending": "ожидает обработки",
    "reprocessed": "обработана повторно",
    "dismissed": "исключена после проверки",
}

ACCOUNT_PRIORITY_LABELS = {
    "critical": "критический",
    "high": "высокий",
    "medium": "средний",
    "low": "низкий",
}

ACCOUNT_STATUS_LABELS = {
    "target": "целевой",
    "active": "активный",
    "watch": "под наблюдением",
    "paused": "приостановлен",
    "inactive": "неактивный",
    "archived": "архивный",
}

EMPLOYER_SIGNAL_TYPE_LABELS = {
    "technology_adoption": "внедрение технологий",
    "ai_adoption": "внедрение ИИ",
    "hiring_growth": "рост найма",
    "restructuring": "реструктуризация",
    "leadership_change": "смена руководства",
    "culture": "культура",
    "other": "другое",
}

EVIDENCE_CONFIDENCE_LABELS = {
    "unknown": "неизвестна",
    "low": "низкая",
    "medium": "средняя",
    "high": "высокая",
    "confirmed": "подтверждена",
}

CONTACT_CONFIDENCE_LABELS = {
    "confirmed": "подтверждена",
    "strong": "высокая",
    "weak": "низкая",
}

CONTACT_RELATIONSHIP_LABELS = {
    "hiring_manager": "нанимающий руководитель",
    "recruiter": "рекрутер",
    "talent_partner": "партнёр по подбору",
    "founder": "основатель",
    "other": "другая",
}

CONTACT_SEARCH_STATUS_LABELS = {
    "found": "найден",
    "reused_verified_contact": "использован проверенный контакт",
    "not_found": "не найден",
    "ambiguous": "неоднозначный результат",
    "unreachable": "недоступен",
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
        raise ValueError("Число рабочих дней должно быть положительным.")
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
            raise ValueError(f"Неподдерживаемый канал {value!r}. Допустимо: {allowed}.")
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


def canonical_source_stream(value: str | None) -> tuple[str, bool]:
    """Return the deterministic primary label for compatibility readers."""

    raw = clean_cell(value)
    if not raw:
        return "unknown", False
    mapped = SOURCE_STREAM_ALIASES.get(raw.casefold())
    labels = mapped or ()
    return (labels[0] if labels else raw), bool(labels)


def canonical_source_labels(value: str | None) -> tuple[tuple[str, ...], str]:
    """Resolve explicit aliases conservatively and return labels plus mapping kind.

    Raw text is never split on punctuation.  A multi-label result therefore
    exists only when the local configuration explicitly maps the whole raw key
    to several canonical labels.
    """

    raw = clean_cell(value)
    if not raw:
        return (("unknown",), "unknown")
    mapped = SOURCE_STREAM_ALIASES.get(raw.casefold())
    if mapped:
        return (mapped, "configured_alias")
    for configured in REQUIRED_SEARCH_STREAMS:
        if configured.casefold() == raw.casefold():
            return ((configured,), "configured_identity")
    return ((raw,), "raw_identity")


def configured_value(
    value: str | None,
    allowed: tuple[str, ...],
    *,
    label: str,
    allow_unknown: bool = True,
) -> str | None:
    candidate = clean_cell(value)
    if not candidate:
        return None if allow_unknown else ""
    matches = {item.casefold(): item for item in allowed}
    if candidate.casefold() not in matches:
        allowed_label = ", ".join(allowed) if allowed else "нет настроенных значений"
        raise ValueError(
            f"{label}: значение {candidate!r} отсутствует в локальной конфигурации "
            f"({allowed_label})"
        )
    return matches[candidate.casefold()]


def validate_hard_gates(value: Any) -> list[dict[str, str]]:
    if value in (None, ""):
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("hard_gates должен быть массивом объектов.")
    result: list[dict[str, str]] = []
    for index, item in enumerate(value, start=1):
        gate = clean_cell(str(item.get("gate") or item.get("key") or ""))
        status = clean_cell(str(item.get("result") or item.get("status") or "")).lower()
        if not gate:
            raise ValueError(f"Для hard_gates[{index}] требуется поле gate.")
        if status not in HARD_GATE_RESULTS:
            raise ValueError(
                f"Поле hard_gates[{index}].result должно иметь значение pass, fail или unknown."
            )
        result.append(
            {
                "gate": gate,
                "result": status,
                "evidence_note": clean_cell(str(item.get("evidence_note") or "")),
            }
        )
    return result


def validate_unresolved_questions(value: Any) -> list[dict[str, str]]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [{"question": clean_cell(value), "status": "open"}] if clean_cell(value) else []
    if not isinstance(value, list):
        raise ValueError("unresolved_questions должен быть строкой или массивом.")
    result: list[dict[str, str]] = []
    for index, item in enumerate(value, start=1):
        if isinstance(item, str):
            question = clean_cell(item)
            status = "open"
        elif isinstance(item, dict):
            question = clean_cell(str(item.get("question") or item.get("text") or ""))
            status = clean_cell(str(item.get("status") or "open")).lower()
        else:
            raise ValueError(f"unresolved_questions[{index}] должен быть строкой или объектом.")
        if not question:
            raise ValueError(f"Для unresolved_questions[{index}] требуется текст вопроса.")
        if status not in {"open", "resolved"}:
            raise ValueError(f"Статус unresolved_questions[{index}] должен быть open или resolved.")
        result.append({"question": question, "status": status})
    return result


def normalized_account_name(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKC", clean_cell(value)).casefold()
    return " ".join(normalized.split())


def parse_iso_date(value: str, *, label: str) -> dt.date:
    candidate = clean_cell(value)
    try:
        return dt.date.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"{label}: требуется дата в формате ГГГГ-ММ-ДД.") from exc


def parse_iso_datetime(value: str, *, label: str) -> dt.datetime:
    candidate = clean_cell(value)
    if not candidate:
        raise ValueError(f"Требуется значение {label}.")
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(candidate)
    except ValueError:
        try:
            parsed = dt.datetime.combine(dt.date.fromisoformat(candidate), dt.time())
        except ValueError as exc:
            raise ValueError(
                f"{label}: требуется дата или отметка времени в формате ISO."
            ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def project_relative_file_path(path: Path) -> str:
    candidate = path if path.is_absolute() else ROOT / path
    resolved = candidate.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Файл не найден: {candidate}")
    if not resolved.is_file():
        raise ValueError(f"Ожидался путь к файлу: {candidate}")
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError as exc:
        raise ValueError(f"Резюме интервью должно находиться внутри рабочей области: {resolved}") from exc


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
    host = (urlsplit(url).hostname or "").lower()
    match = LINKEDIN_JOB_ID_RE.search(url or "")
    if match and (host == "linkedin.com" or host.endswith(".linkedin.com")):
        return f"linkedin:{match.group(1)}"
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


def missing_schema_indexes(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index'"
    ).fetchall()
    present = {str(row[0]) for row in rows}
    return sorted(EXPECTED_INDEXES - present)


def vacancy_external_alias_schema_issues(conn: sqlite3.Connection) -> list[str]:
    table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'vacancy_external_aliases'
        """
    ).fetchone()
    if not table_exists:
        return ["таблица отсутствует"]

    required_columns = {
        "id",
        "vacancy_id",
        "channel",
        "external_id",
        "url",
        "first_seen_date",
        "last_seen_date",
        "created_at",
        "updated_at",
    }
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(vacancy_external_aliases)").fetchall()
    }
    issues: list[str] = []
    missing_columns = required_columns - columns
    if missing_columns:
        issues.append(
            f"отсутствуют столбцы: {','.join(sorted(missing_columns))}"
        )

    unique_channel_external_id = False
    alias_indexes = conn.execute(
        "PRAGMA index_list(vacancy_external_aliases)"
    ).fetchall()
    for index_row in alias_indexes:
        if not int(index_row[2]):
            continue
        index_name = str(index_row[1]).replace("'", "''")
        indexed_columns = [
            str(row[2])
            for row in conn.execute(f"PRAGMA index_info('{index_name}')").fetchall()
        ]
        if indexed_columns == ["channel", "external_id"]:
            unique_channel_external_id = True
            break
    if not unique_channel_external_id:
        issues.append("отсутствует ограничение UNIQUE(channel, external_id)")

    foreign_keys = conn.execute(
        "PRAGMA foreign_key_list(vacancy_external_aliases)"
    ).fetchall()
    cascade_fk = any(
        str(row[2]) == "vacancies"
        and str(row[3]) == "vacancy_id"
        and str(row[4]) == "id"
        and str(row[6]).upper() == "CASCADE"
        for row in foreign_keys
    )
    if not cascade_fk:
        issues.append(
            "отсутствует внешний ключ vacancy_id -> vacancies(id) ON DELETE CASCADE"
        )
    return issues


def schema_v4_issues(conn: sqlite3.Connection) -> list[str]:
    required_columns = {
        "source_hits": {"source_stream", "canonical_source_stream"},
        "employer_interactions": {
            "vacancy_id",
            "event_at",
            "direction",
            "event_type",
            "channel",
            "actor_type",
            "is_human",
            "evidence_note",
            "dedupe_key",
        },
        "employer_accounts": {"canonical_name", "normalized_name", "updated_at"},
        "employer_account_signals": {
            "account_id",
            "signal_type",
            "observed_date",
            "confidence",
            "evidence_note",
        },
        "vacancy_employer_accounts": {"vacancy_id", "account_id", "link_method"},
        "vacancy_factors": {
            "vacancy_id",
            "factor_key",
            "factor_value",
            "observed_date",
            "evidence_note",
            "confidence",
        },
    }
    issues: list[str] = []
    present_tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    for table, columns in required_columns.items():
        if table not in present_tables:
            issues.append(f"{table}: таблица отсутствует")
            continue
        present_columns = {
            str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        missing = columns - present_columns
        if missing:
            issues.append(
                f"{table}: отсутствуют столбцы {','.join(sorted(missing))}"
            )
    return issues


def schema_v5_issues(conn: sqlite3.Connection) -> list[str]:
    table_exists = conn.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'source_checkpoints'
        """
    ).fetchone()
    if not table_exists:
        return ["source_checkpoints: таблица отсутствует"]
    required_columns = {
        "source",
        "stream_key",
        "cursor_value",
        "cursor_date",
        "initialized_at",
        "last_completed_run_date",
        "last_manifest_file",
        "created_at",
        "updated_at",
    }
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(source_checkpoints)").fetchall()
    }
    missing = required_columns - columns
    return (
        ["source_checkpoints: отсутствуют столбцы " + ",".join(sorted(missing))]
        if missing
        else []
    )


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }


def add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, declaration: str
) -> None:
    if column not in table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")


def ensure_v6_schema(conn: sqlite3.Connection) -> None:
    """Create additive v6 structures without rewriting legacy evidence."""

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS source_labels (
            label_key TEXT PRIMARY KEY,
            display_label TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS source_hit_labels (
            source_hit_id INTEGER NOT NULL,
            label_key TEXT NOT NULL,
            mapping_kind TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (source_hit_id, label_key),
            FOREIGN KEY (source_hit_id) REFERENCES source_hits(id) ON DELETE CASCADE,
            FOREIGN KEY (label_key) REFERENCES source_labels(label_key) ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_source_hit_labels_label
            ON source_hit_labels(label_key, source_hit_id);

        CREATE TABLE IF NOT EXISTS external_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER,
            action_key TEXT NOT NULL,
            action_type TEXT NOT NULL,
            state TEXT NOT NULL,
            event_at TEXT NOT NULL,
            authorization_note TEXT,
            evidence_note TEXT,
            evidence_url TEXT,
            source TEXT NOT NULL,
            external_reference TEXT,
            metadata_json TEXT,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_external_actions_vacancy
            ON external_actions(vacancy_id, action_key, event_at, id);

        CREATE TABLE IF NOT EXISTS lifecycle_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_at TEXT NOT NULL,
            evidence_at TEXT NOT NULL,
            evidence_note TEXT NOT NULL,
            evidence_url TEXT,
            evidence_source TEXT NOT NULL,
            origin TEXT NOT NULL,
            external_reference TEXT,
            round_no INTEGER,
            scheduled_at TEXT,
            external_action_id INTEGER,
            application_source_hit_id INTEGER,
            campaign_id TEXT,
            role_family TEXT,
            confidence TEXT,
            master_resume_id TEXT,
            planned_resume_id TEXT,
            actual_resume_id TEXT,
            message_variant TEXT,
            hard_gate_results_json TEXT,
            unresolved_questions_json TEXT,
            human_path_status TEXT,
            history_complete INTEGER NOT NULL DEFAULT 1,
            authorization_status TEXT NOT NULL DEFAULT 'not_applicable',
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            FOREIGN KEY (external_action_id) REFERENCES external_actions(id) ON DELETE SET NULL,
            FOREIGN KEY (application_source_hit_id) REFERENCES source_hits(id) ON DELETE SET NULL,
            CHECK(history_complete IN (0, 1))
        );

        CREATE INDEX IF NOT EXISTS idx_lifecycle_events_vacancy
            ON lifecycle_events(vacancy_id, event_at, id);

        CREATE TABLE IF NOT EXISTS action_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            event_at TEXT NOT NULL,
            action_state TEXT NOT NULL,
            bucket TEXT NOT NULL,
            due_date TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            evidence_note TEXT,
            source TEXT NOT NULL,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_action_events_vacancy
            ON action_events(vacancy_id, event_at, id);

        CREATE TABLE IF NOT EXISTS vacancy_decision_metadata (
            vacancy_id INTEGER PRIMARY KEY,
            campaign_id TEXT,
            role_family TEXT,
            confidence TEXT,
            hard_gate_results_json TEXT,
            unresolved_questions_json TEXT,
            master_resume_id TEXT,
            planned_resume_id TEXT,
            actual_resume_id TEXT,
            message_variant TEXT,
            application_source_hit_id INTEGER,
            human_path_status TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            FOREIGN KEY (application_source_hit_id) REFERENCES source_hits(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS quarantine_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_file TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            source_name TEXT,
            source_stream TEXT,
            classification TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            evidence_note TEXT,
            raw_payload_json TEXT NOT NULL,
            retry_context_json TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            reprocessed_vacancy_id INTEGER,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (reprocessed_vacancy_id) REFERENCES vacancies(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_quarantine_status
            ON quarantine_records(status, classification, id);

        CREATE TABLE IF NOT EXISTS policy_versions (
            version TEXT PRIMARY KEY,
            effective_date TEXT NOT NULL,
            is_active INTEGER NOT NULL,
            source TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK(is_active IN (0, 1))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_versions_one_active
            ON policy_versions(is_active) WHERE is_active = 1;

        CREATE TABLE IF NOT EXISTS screening_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            evaluated_at TEXT NOT NULL,
            decision TEXT NOT NULL,
            score INTEGER,
            priority TEXT,
            policy_version TEXT NOT NULL,
            policy_effective_date TEXT NOT NULL,
            rule_results_json TEXT NOT NULL,
            evidence_note TEXT,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            FOREIGN KEY (policy_version) REFERENCES policy_versions(version) ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_screening_decisions_policy
            ON screening_decisions(policy_version, decision, vacancy_id);

        CREATE TABLE IF NOT EXISTS migration_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_version INTEGER NOT NULL,
            to_version INTEGER NOT NULL,
            applied_at TEXT NOT NULL,
            backup_path TEXT,
            row_counts_json TEXT NOT NULL,
            notes TEXT,
            UNIQUE(from_version, to_version)
        );
        """
    )

    for column, declaration in (
        ("lifecycle_event_id", "INTEGER REFERENCES lifecycle_events(id)"),
        ("application_source_hit_id", "INTEGER REFERENCES source_hits(id)"),
        ("campaign_id", "TEXT"),
        ("role_family", "TEXT"),
        ("actual_resume_version", "TEXT"),
        ("message_variant", "TEXT"),
    ):
        add_column_if_missing(conn, "applications", column, declaration)

    for column, declaration in (
        ("confirms_completion", "INTEGER NOT NULL DEFAULT 0"),
        ("completion_lifecycle_event_id", "INTEGER REFERENCES lifecycle_events(id)"),
    ):
        add_column_if_missing(conn, "interview_summaries", column, declaration)

    for column, declaration in (
        ("portfolio_limit", "INTEGER"),
        ("review_cadence_days", "INTEGER"),
        ("next_review_date", "TEXT"),
        ("website_checked_date", "TEXT"),
        ("careers_checked_date", "TEXT"),
        ("target_campaigns_json", "TEXT"),
        ("target_role_families_json", "TEXT"),
        ("owner_evidence", "TEXT"),
        ("sponsor_evidence", "TEXT"),
        ("governance_evidence", "TEXT"),
        ("human_path_status", "TEXT"),
    ):
        add_column_if_missing(conn, "employer_accounts", column, declaration)

    add_column_if_missing(
        conn,
        "outreach_messages",
        "external_action_id",
        "INTEGER REFERENCES external_actions(id)",
    )
    add_column_if_missing(
        conn,
        "employer_interactions",
        "external_action_id",
        "INTEGER REFERENCES external_actions(id)",
    )


def ensure_v7_schema(conn: sqlite3.Connection) -> None:
    """Create append-only evidence corrections and deterministic read models."""

    conn.executescript(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_employer_interactions_identity
            ON employer_interactions(id, vacancy_id);

        CREATE TABLE IF NOT EXISTS employer_interaction_invalidations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            interaction_id INTEGER NOT NULL,
            vacancy_id INTEGER NOT NULL,
            corrected_at TEXT NOT NULL,
            reason TEXT NOT NULL,
            evidence_note TEXT NOT NULL,
            source TEXT NOT NULL,
            operator_context TEXT NOT NULL,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (interaction_id, vacancy_id)
                REFERENCES employer_interactions(id, vacancy_id) ON DELETE CASCADE,
            UNIQUE(interaction_id)
        );

        CREATE INDEX IF NOT EXISTS idx_employer_interaction_invalidations_vacancy
            ON employer_interaction_invalidations(vacancy_id, corrected_at, id);

        CREATE VIEW IF NOT EXISTS effective_employer_interactions AS
        SELECT interaction.*
        FROM employer_interactions interaction
        WHERE NOT EXISTS (
            SELECT 1
            FROM employer_interaction_invalidations invalidation
            WHERE invalidation.interaction_id = interaction.id
        );

        CREATE VIEW IF NOT EXISTS effective_applications AS
        SELECT application.*
        FROM applications application
        WHERE application.id = (
            SELECT MAX(candidate.id)
            FROM applications candidate
            WHERE candidate.vacancy_id = application.vacancy_id
        );
        """
    )


def schema_v6_issues(conn: sqlite3.Connection) -> list[str]:
    required_columns = {
        "lifecycle_events": {
            "vacancy_id",
            "event_type",
            "event_at",
            "evidence_at",
            "evidence_note",
            "evidence_source",
            "dedupe_key",
            "history_complete",
            "authorization_status",
        },
        "action_events": {
            "vacancy_id",
            "event_at",
            "action_state",
            "bucket",
            "priority",
            "dedupe_key",
        },
        "external_actions": {
            "action_key",
            "action_type",
            "state",
            "event_at",
            "dedupe_key",
        },
        "source_hit_labels": {"source_hit_id", "label_key", "mapping_kind"},
        "quarantine_records": {
            "classification",
            "status",
            "raw_payload_json",
            "retry_context_json",
            "dedupe_key",
        },
        "vacancy_decision_metadata": {
            "campaign_id",
            "role_family",
            "confidence",
            "hard_gate_results_json",
            "unresolved_questions_json",
            "master_resume_id",
            "planned_resume_id",
            "actual_resume_id",
            "message_variant",
            "human_path_status",
        },
    }
    present_tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    issues: list[str] = []
    for table, expected in required_columns.items():
        if table not in present_tables:
            issues.append(f"{table}: таблица отсутствует")
            continue
        missing = expected - table_columns(conn, table)
        if missing:
            issues.append(
                f"{table}: отсутствуют столбцы {','.join(sorted(missing))}"
            )
    return issues


def schema_v7_issues(conn: sqlite3.Connection) -> list[str]:
    required_columns = {
        "interaction_id",
        "vacancy_id",
        "corrected_at",
        "reason",
        "evidence_note",
        "source",
        "operator_context",
        "dedupe_key",
        "created_at",
    }
    present = table_columns(conn, "employer_interaction_invalidations")
    issues: list[str] = []
    missing = required_columns - present
    if missing:
        issues.append(
            "employer_interaction_invalidations: отсутствуют столбцы "
            + ",".join(sorted(missing))
        )
    views = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'view'"
        ).fetchall()
    }
    for view in ("effective_employer_interactions", "effective_applications"):
        if view not in views:
            issues.append(f"{view}: представление отсутствует")
    return issues


def ensure_active_policy(conn: sqlite3.Connection) -> None:
    """Persist one unambiguous active policy version from local configuration."""

    timestamp = now_iso()
    current = conn.execute(
        "SELECT version, effective_date FROM policy_versions WHERE is_active = 1"
    ).fetchone()
    if (
        current
        and current["version"] == ACTIVE_POLICY_VERSION
        and current["effective_date"] == ACTIVE_POLICY_EFFECTIVE_DATE
    ):
        return
    conn.execute("UPDATE policy_versions SET is_active = 0, updated_at = ?", (timestamp,))
    conn.execute(
        """
        INSERT INTO policy_versions (
            version, effective_date, is_active, source, created_at, updated_at
        )
        VALUES (?, ?, 1, 'local_config', ?, ?)
        ON CONFLICT(version) DO UPDATE SET
            effective_date = excluded.effective_date,
            is_active = 1,
            source = excluded.source,
            updated_at = excluded.updated_at
        """,
        (
            ACTIVE_POLICY_VERSION,
            ACTIVE_POLICY_EFFECTIVE_DATE,
            timestamp,
            timestamp,
        ),
    )


def refresh_source_hit_labels(conn: sqlite3.Connection) -> int:
    """Rebuild derived many-to-many labels from preserved raw stream keys."""

    changed = 0
    timestamp = now_iso()
    rows = conn.execute(
        "SELECT id, source_stream, canonical_source_stream FROM source_hits ORDER BY id"
    ).fetchall()
    for row in rows:
        hit_id = int(row["id"])
        labels, mapping_kind = canonical_source_labels(row["source_stream"])
        primary = labels[0]
        if clean_cell(row["canonical_source_stream"]) != primary:
            conn.execute(
                "UPDATE source_hits SET canonical_source_stream = ? WHERE id = ?",
                (primary, hit_id),
            )
            changed += 1
        existing = {
            (str(item["label_key"]), str(item["mapping_kind"]))
            for item in conn.execute(
                "SELECT label_key, mapping_kind FROM source_hit_labels WHERE source_hit_id = ?",
                (hit_id,),
            ).fetchall()
        }
        desired = {(label, mapping_kind) for label in labels}
        if existing == desired:
            continue
        conn.execute("DELETE FROM source_hit_labels WHERE source_hit_id = ?", (hit_id,))
        for label in labels:
            conn.execute(
                """
                INSERT INTO source_labels (label_key, display_label, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(label_key) DO UPDATE SET
                    display_label = excluded.display_label,
                    updated_at = excluded.updated_at
                """,
                (label, label, timestamp, timestamp),
            )
            conn.execute(
                """
                INSERT INTO source_hit_labels (
                    source_hit_id, label_key, mapping_kind, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (hit_id, label, mapping_kind, timestamp),
            )
        changed += 1
    return changed


def ensure_schema(conn: sqlite3.Connection) -> None:
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"Версия схемы базы данных {version} новее поддерживаемой версии "
            f"{SCHEMA_VERSION}."
        )
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'vacancies'"
    ).fetchone()
    if not exists:
        reset_schema(conn)
    else:
        if version < SCHEMA_VERSION:
            raise RuntimeError(
                f"Схему базы данных версии {version} требуется явно перенести на "
                f"версию {SCHEMA_VERSION}. Выполните: jobctl migrate-schema"
            )
        ensure_auxiliary_schema(conn)
        missing = missing_schema_tables(conn)
        if missing:
            raise RuntimeError(
                "В базе данных отсутствуют обязательные таблицы: " + ", ".join(missing)
            )
        missing_indexes = missing_schema_indexes(conn)
        if missing_indexes:
            raise RuntimeError(
                "В базе данных отсутствуют обязательные индексы: " + ", ".join(missing_indexes)
            )
        alias_schema_issues = vacancy_external_alias_schema_issues(conn)
        if alias_schema_issues:
            raise RuntimeError(
                "Некорректна схема внешних псевдонимов вакансий: "
                + "; ".join(alias_schema_issues)
            )
        v4_issues = schema_v4_issues(conn)
        if v4_issues:
            raise RuntimeError(
                "Нарушен контракт схемы базы данных v4: " + "; ".join(v4_issues)
            )
        v5_issues = schema_v5_issues(conn)
        if v5_issues:
            raise RuntimeError(
                "Нарушен контракт схемы базы данных v5: " + "; ".join(v5_issues)
            )
        v6_issues = schema_v6_issues(conn)
        if v6_issues:
            raise RuntimeError(
                "Нарушен контракт схемы базы данных v6: " + "; ".join(v6_issues)
            )
        v7_issues = schema_v7_issues(conn)
        if v7_issues:
            raise RuntimeError(
                "Нарушен контракт схемы базы данных v7: " + "; ".join(v7_issues)
            )


def reset_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP VIEW IF EXISTS effective_applications;
        DROP VIEW IF EXISTS effective_employer_interactions;
        DROP TABLE IF EXISTS migration_log;
        DROP TABLE IF EXISTS screening_decisions;
        DROP TABLE IF EXISTS policy_versions;
        DROP TABLE IF EXISTS quarantine_records;
        DROP TABLE IF EXISTS vacancy_decision_metadata;
        DROP TABLE IF EXISTS action_events;
        DROP TABLE IF EXISTS lifecycle_events;
        DROP TABLE IF EXISTS external_actions;
        DROP TABLE IF EXISTS source_hit_labels;
        DROP TABLE IF EXISTS source_labels;
        DROP TABLE IF EXISTS outreach_messages;
        DROP TABLE IF EXISTS followup_rounds;
        DROP TABLE IF EXISTS contact_searches;
        DROP TABLE IF EXISTS employer_contacts;
        DROP TABLE IF EXISTS employer_account_signals;
        DROP TABLE IF EXISTS vacancy_employer_accounts;
        DROP TABLE IF EXISTS employer_accounts;
        DROP TABLE IF EXISTS employer_interaction_invalidations;
        DROP TABLE IF EXISTS employer_interactions;
        DROP TABLE IF EXISTS vacancy_factors;
        DROP TABLE IF EXISTS source_checkpoints;
        DROP TABLE IF EXISTS search_coverage;
        DROP TABLE IF EXISTS search_runs;
        DROP TABLE IF EXISTS source_hits;
        DROP TABLE IF EXISTS evaluations;
        DROP TABLE IF EXISTS applications;
        DROP TABLE IF EXISTS stage_events;
        DROP TABLE IF EXISTS interview_summaries;
        DROP TABLE IF EXISTS import_issues;
        DROP TABLE IF EXISTS vacancy_external_aliases;
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
            canonical_source_stream TEXT,
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

        CREATE TABLE vacancy_external_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            external_id TEXT NOT NULL,
            url TEXT,
            first_seen_date TEXT,
            last_seen_date TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            UNIQUE(channel, external_id)
        );

        CREATE INDEX idx_vacancy_external_aliases_vacancy
            ON vacancy_external_aliases(vacancy_id);

        CREATE INDEX idx_vacancy_external_aliases_external_id
            ON vacancy_external_aliases(external_id);

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

        CREATE TABLE source_checkpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            cursor_value TEXT,
            cursor_date TEXT,
            initialized_at TEXT NOT NULL,
            last_completed_run_date TEXT NOT NULL,
            last_manifest_file TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source, stream_key)
        );

        CREATE INDEX idx_source_checkpoints_source
            ON source_checkpoints(source, stream_key);

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

        CREATE TABLE employer_interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            event_at TEXT NOT NULL,
            direction TEXT NOT NULL,
            event_type TEXT NOT NULL,
            channel TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            is_human INTEGER NOT NULL,
            evidence_note TEXT,
            evidence_url TEXT,
            external_reference TEXT,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            CHECK(direction IN ('inbound', 'outbound')),
            CHECK(is_human IN (0, 1))
        );

        CREATE INDEX idx_employer_interactions_vacancy
            ON employer_interactions(vacancy_id, event_at, id);

        CREATE TABLE employer_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            website TEXT,
            careers_url TEXT,
            country_market TEXT,
            priority TEXT,
            status TEXT,
            last_checked_date TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE employer_account_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            observed_date TEXT NOT NULL,
            confidence TEXT NOT NULL,
            evidence_url TEXT,
            evidence_note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (account_id) REFERENCES employer_accounts(id) ON DELETE CASCADE,
            UNIQUE(account_id, signal_type, observed_date, evidence_url, evidence_note)
        );

        CREATE INDEX idx_employer_account_signals_account
            ON employer_account_signals(account_id, observed_date, id);

        CREATE TABLE vacancy_employer_accounts (
            vacancy_id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL,
            link_method TEXT NOT NULL,
            evidence_note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            FOREIGN KEY (account_id) REFERENCES employer_accounts(id) ON DELETE RESTRICT
        );

        CREATE INDEX idx_vacancy_employer_accounts_account
            ON vacancy_employer_accounts(account_id, vacancy_id);

        CREATE TABLE vacancy_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            factor_key TEXT NOT NULL,
            factor_value TEXT NOT NULL,
            observed_date TEXT NOT NULL,
            evidence_note TEXT NOT NULL,
            evidence_url TEXT,
            confidence TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            UNIQUE(vacancy_id, factor_key, factor_value, observed_date, evidence_note)
        );

        CREATE INDEX idx_vacancy_factors_vacancy
            ON vacancy_factors(vacancy_id, factor_key, observed_date, id);

        CREATE TABLE import_issues (
            id INTEGER PRIMARY KEY,
            origin_file TEXT,
            line_no INTEGER,
            issue TEXT,
            raw_text TEXT
        );
        """
    )
    ensure_v6_schema(conn)
    ensure_v7_schema(conn)
    ensure_active_policy(conn)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()


def ensure_auxiliary_schema(conn: sqlite3.Connection) -> None:
    source_hit_columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(source_hits)").fetchall()
    }
    if "canonical_source_stream" not in source_hit_columns:
        conn.execute("ALTER TABLE source_hits ADD COLUMN canonical_source_stream TEXT")
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

        CREATE TABLE IF NOT EXISTS vacancy_external_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            external_id TEXT NOT NULL,
            url TEXT,
            first_seen_date TEXT,
            last_seen_date TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            UNIQUE(channel, external_id)
        );

        CREATE INDEX IF NOT EXISTS idx_vacancy_external_aliases_vacancy
            ON vacancy_external_aliases(vacancy_id);

        CREATE INDEX IF NOT EXISTS idx_vacancy_external_aliases_external_id
            ON vacancy_external_aliases(external_id);

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

        CREATE TABLE IF NOT EXISTS source_checkpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            stream_key TEXT NOT NULL,
            cursor_value TEXT,
            cursor_date TEXT,
            initialized_at TEXT NOT NULL,
            last_completed_run_date TEXT NOT NULL,
            last_manifest_file TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source, stream_key)
        );

        CREATE INDEX IF NOT EXISTS idx_source_checkpoints_source
            ON source_checkpoints(source, stream_key);

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

        CREATE TABLE IF NOT EXISTS employer_interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            event_at TEXT NOT NULL,
            direction TEXT NOT NULL,
            event_type TEXT NOT NULL,
            channel TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            is_human INTEGER NOT NULL,
            evidence_note TEXT,
            evidence_url TEXT,
            external_reference TEXT,
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            CHECK(direction IN ('inbound', 'outbound')),
            CHECK(is_human IN (0, 1))
        );

        CREATE INDEX IF NOT EXISTS idx_employer_interactions_vacancy
            ON employer_interactions(vacancy_id, event_at, id);

        CREATE TABLE IF NOT EXISTS employer_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            website TEXT,
            careers_url TEXT,
            country_market TEXT,
            priority TEXT,
            status TEXT,
            last_checked_date TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS employer_account_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            observed_date TEXT NOT NULL,
            confidence TEXT NOT NULL,
            evidence_url TEXT,
            evidence_note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (account_id) REFERENCES employer_accounts(id) ON DELETE CASCADE,
            UNIQUE(account_id, signal_type, observed_date, evidence_url, evidence_note)
        );

        CREATE INDEX IF NOT EXISTS idx_employer_account_signals_account
            ON employer_account_signals(account_id, observed_date, id);

        CREATE TABLE IF NOT EXISTS vacancy_employer_accounts (
            vacancy_id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL,
            link_method TEXT NOT NULL,
            evidence_note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            FOREIGN KEY (account_id) REFERENCES employer_accounts(id) ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_vacancy_employer_accounts_account
            ON vacancy_employer_accounts(account_id, vacancy_id);

        CREATE TABLE IF NOT EXISTS vacancy_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL,
            factor_key TEXT NOT NULL,
            factor_value TEXT NOT NULL,
            observed_date TEXT NOT NULL,
            evidence_note TEXT NOT NULL,
            evidence_url TEXT,
            confidence TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (vacancy_id) REFERENCES vacancies(id) ON DELETE CASCADE,
            UNIQUE(vacancy_id, factor_key, factor_value, observed_date, evidence_note)
        );

        CREATE INDEX IF NOT EXISTS idx_vacancy_factors_vacancy
            ON vacancy_factors(vacancy_id, factor_key, observed_date, id);
        """
    )
    ensure_v6_schema(conn)
    ensure_v7_schema(conn)
    ensure_active_policy(conn)
    conn.commit()


def merge_origin(existing: str | None, new_origin: str) -> str:
    origins = [x for x in (existing or "").split(",") if x]
    if new_origin not in origins:
        origins.append(new_origin)
    return ",".join(origins)


def store_vacancy_external_alias(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    channel: str,
    external_id: str,
    url: str,
    seen_date: str,
    timestamp: str | None = None,
) -> bool:
    """Persist one source identity without changing the canonical vacancy identity."""

    channel = clean_cell(channel)
    external_id = clean_cell(external_id)
    if not channel or not external_id:
        raise ValueError("Для псевдонима вакансии требуются channel и external_id.")
    norm_url = normalize_url(url)
    timestamp = timestamp or now_iso()
    conflicting = conn.execute(
        """
        SELECT DISTINCT vacancy_id
        FROM vacancy_external_aliases
        WHERE external_id = ? AND vacancy_id != ?
        """,
        (external_id, vacancy_id),
    ).fetchone()
    if conflicting:
        raise RuntimeError(
            f"Внешний идентификатор {external_id!r} уже относится к вакансии "
            f"№{conflicting['vacancy_id']} в другом канале."
        )
    existing = conn.execute(
        """
        SELECT *
        FROM vacancy_external_aliases
        WHERE channel = ? AND external_id = ?
        """,
        (channel, external_id),
    ).fetchone()
    if existing:
        if int(existing["vacancy_id"]) != vacancy_id:
            raise RuntimeError(
                f"Внешний идентификатор {external_id!r} в канале {channel!r} уже "
                f"относится к вакансии №{existing['vacancy_id']}."
            )
        observed_dates = [
            value
            for value in (
                existing["first_seen_date"],
                existing["last_seen_date"],
                seen_date,
            )
            if value
        ]
        first_seen_date = min(observed_dates) if observed_dates else ""
        last_seen_date = max(observed_dates) if observed_dates else ""
        conn.execute(
            """
            UPDATE vacancy_external_aliases
            SET url = ?, first_seen_date = ?, last_seen_date = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                norm_url or existing["url"] or "",
                first_seen_date,
                last_seen_date,
                timestamp,
                int(existing["id"]),
            ),
        )
        return False

    conn.execute(
        """
        INSERT INTO vacancy_external_aliases (
            vacancy_id, channel, external_id, url, first_seen_date,
            last_seen_date, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            channel,
            external_id,
            norm_url,
            seen_date,
            seen_date,
            timestamp,
            timestamp,
        ),
    )
    return True


def backfill_canonical_external_aliases(conn: sqlite3.Connection) -> int:
    inserted = 0
    for row in conn.execute(
        """
        SELECT id, channel, external_id, url, first_seen_date, last_seen_date, updated_at
        FROM vacancies
        ORDER BY id
        """
    ).fetchall():
        first_seen_date = row["first_seen_date"] or row["last_seen_date"] or ""
        last_seen_date = row["last_seen_date"] or first_seen_date
        was_inserted = store_vacancy_external_alias(
            conn,
            vacancy_id=int(row["id"]),
            channel=row["channel"],
            external_id=row["external_id"],
            url=row["url"] or "",
            seen_date=first_seen_date,
            timestamp=row["updated_at"] or now_iso(),
        )
        if last_seen_date != first_seen_date:
            store_vacancy_external_alias(
                conn,
                vacancy_id=int(row["id"]),
                channel=row["channel"],
                external_id=row["external_id"],
                url=row["url"] or "",
                seen_date=last_seen_date,
                timestamp=row["updated_at"] or now_iso(),
            )
        if was_inserted:
            inserted += 1
    return inserted


def backfill_canonical_source_streams(conn: sqlite3.Connection) -> int:
    updated = 0
    for row in conn.execute(
        """
        SELECT id, source_stream, canonical_source_stream
        FROM source_hits
        ORDER BY id
        """
    ).fetchall():
        if clean_cell(row["canonical_source_stream"]):
            continue
        canonical, _ = canonical_source_stream(row["source_stream"])
        conn.execute(
            "UPDATE source_hits SET canonical_source_stream = ? WHERE id = ?",
            (canonical, int(row["id"])),
        )
        updated += 1
    return updated


def dedupe_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def append_lifecycle_event(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    event_type: str,
    event_at: str,
    evidence_at: str,
    evidence_note: str,
    evidence_source: str,
    origin: str,
    evidence_url: str = "",
    external_reference: str = "",
    round_no: int | None = None,
    scheduled_at: str = "",
    external_action_id: int | None = None,
    application_source_hit_id: int | None = None,
    metadata: dict[str, Any] | None = None,
    history_complete: bool = True,
    authorization_status: str = "not_applicable",
) -> tuple[int, bool]:
    event_type = clean_cell(event_type).lower()
    if event_type not in LIFECYCLE_EVENT_TYPES:
        raise ValueError("Неподдерживаемый тип события жизненного цикла.")
    event_at_value = parse_iso_datetime(event_at, label="event_at").isoformat()
    evidence_at_value = parse_iso_datetime(evidence_at, label="evidence_at").isoformat()
    evidence_note = clean_cell(evidence_note)
    evidence_source = clean_cell(evidence_source)
    if not evidence_note or not evidence_source:
        raise ValueError("Для события жизненного цикла требуются примечание и источник доказательства.")
    evidence_url = validate_optional_external_url(
        evidence_url, label="evidence_url"
    )
    if event_type in INTERVIEW_EVENT_TYPES:
        if round_no is None or round_no < 1:
            raise ValueError("Для события интервью требуется положительный номер раунда.")
    elif round_no is not None and round_no < 1:
        raise ValueError("Значение round_no должно быть положительным.")
    if scheduled_at:
        scheduled_at = parse_iso_datetime(
            scheduled_at, label="scheduled_at"
        ).isoformat()
    if event_type == "interview_scheduled" and not scheduled_at:
        raise ValueError("Для interview_scheduled требуется scheduled_at.")
    if event_type == "application_confirmed":
        if authorization_status not in {"explicit", "legacy_unknown"}:
            raise ValueError(
                "Подтверждённый отклик требует доказательства явного разрешения."
            )
        if authorization_status == "explicit" and external_action_id is None:
            raise ValueError(
                "Для явно подтверждённого отклика требуется запись внешнего действия."
            )
    rejection = conn.execute(
        """
        SELECT event_at FROM lifecycle_events
        WHERE vacancy_id = ? AND event_type = 'rejected'
        ORDER BY event_at, id LIMIT 1
        """,
        (vacancy_id,),
    ).fetchone()
    positive_after_rejection = {
        "application_confirmed",
        "interview_invited",
        "interview_scheduled",
        "interview_completed",
        "offer_received",
    }
    if (
        rejection
        and event_type in positive_after_rejection
        and event_at_value >= str(rejection["event_at"])
    ):
        raise ValueError(
            "Отклонённую каноническую вакансию нельзя вернуть к положительному состоянию жизненного цикла."
        )

    metadata = metadata or {}
    external_reference = clean_cell(external_reference)
    if external_reference:
        dedupe_material = {
            "vacancy_id": vacancy_id,
            "event_type": event_type,
            "external_reference": external_reference,
        }
    else:
        dedupe_material = {
            "vacancy_id": vacancy_id,
            "event_type": event_type,
            "event_at": event_at_value,
            "round_no": round_no,
            "scheduled_at": scheduled_at,
            "evidence_note": evidence_note,
            "origin": origin,
        }
    dedupe_key = dedupe_hash(dedupe_material)
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO lifecycle_events (
            vacancy_id, event_type, event_at, evidence_at, evidence_note,
            evidence_url, evidence_source, origin, external_reference,
            round_no, scheduled_at, external_action_id,
            application_source_hit_id, campaign_id, role_family, confidence,
            master_resume_id, planned_resume_id, actual_resume_id,
            message_variant, hard_gate_results_json,
            unresolved_questions_json, human_path_status, history_complete,
            authorization_status, dedupe_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            event_type,
            event_at_value,
            evidence_at_value,
            evidence_note,
            evidence_url,
            evidence_source,
            clean_cell(origin),
            external_reference,
            round_no,
            scheduled_at,
            external_action_id,
            application_source_hit_id,
            metadata.get("campaign_id"),
            metadata.get("role_family"),
            metadata.get("confidence"),
            metadata.get("master_resume_id"),
            metadata.get("planned_resume_id"),
            metadata.get("actual_resume_id"),
            metadata.get("message_variant"),
            json.dumps(metadata.get("hard_gates"), ensure_ascii=False)
            if "hard_gates" in metadata
            else None,
            json.dumps(metadata.get("unresolved_questions"), ensure_ascii=False)
            if "unresolved_questions" in metadata
            else None,
            metadata.get("human_path_status"),
            1 if history_complete else 0,
            authorization_status,
            dedupe_key,
            now_iso(),
        ),
    )
    created = conn.total_changes > before
    row = conn.execute(
        "SELECT id FROM lifecycle_events WHERE dedupe_key = ?", (dedupe_key,)
    ).fetchone()
    return int(row["id"]), created


def append_action_event(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    action_state: str,
    bucket: str,
    event_at: str,
    due_date: str = "",
    priority: int = 0,
    reason: str = "",
    evidence_note: str = "",
    source: str,
) -> tuple[int, bool]:
    action_state = clean_cell(action_state).lower()
    bucket = clean_cell(bucket).lower()
    if action_state not in ACTION_STATES:
        raise ValueError("Неподдерживаемое текущее рабочее состояние.")
    if bucket not in WIP_BUCKET_KEYS:
        raise ValueError("Неподдерживаемая группа незавершённой работы.")
    event_at_value = parse_iso_datetime(event_at, label="event_at").isoformat()
    if due_date:
        parse_iso_date(due_date, label="due_date")
    if priority < 0 or priority > 100:
        raise ValueError("Значение priority должно быть от 0 до 100.")
    payload = {
        "vacancy_id": vacancy_id,
        "action_state": action_state,
        "bucket": bucket,
        "event_at": event_at_value,
        "due_date": due_date,
        "priority": priority,
        "reason": clean_cell(reason),
        "source": clean_cell(source),
    }
    dedupe_key = dedupe_hash(payload)
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO action_events (
            vacancy_id, event_at, action_state, bucket, due_date, priority,
            reason, evidence_note, source, dedupe_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            event_at_value,
            action_state,
            bucket,
            due_date,
            priority,
            clean_cell(reason),
            clean_cell(evidence_note),
            clean_cell(source),
            dedupe_key,
            now_iso(),
        ),
    )
    created = conn.total_changes > before
    row = conn.execute(
        "SELECT id FROM action_events WHERE dedupe_key = ?", (dedupe_key,)
    ).fetchone()
    return int(row["id"]), created


def action_from_legacy_row(row: sqlite3.Row) -> tuple[str, str, str, int]:
    stage = canonical_stage(row["latest_stage"])
    if stage == "needs_input":
        return ("needs_input", "urgent", clean_cell(row["follow_up_date"]), 100)
    if stage == "follow_up":
        return ("follow_up", "due_follow_up", clean_cell(row["follow_up_date"]), 90)
    status_text = " ".join(
        clean_cell(row[key]).casefold()
        for key in ("latest_status", "next_action", "open_questions")
    )
    if stage == "seen" and any(
        marker in status_text for marker in ("review", "провер", "уточн")
    ):
        return ("review", "deep_review", "", min(int(row["score"] or 0), 100))
    if stage in {"applied", "interview_1", "interview_2", "interview_3", "offer"}:
        return ("waiting", "backlog", clean_cell(row["follow_up_date"]), 20)
    return ("none", "backlog", "", 0)


def backfill_v6_evidence(conn: sqlite3.Connection) -> dict[str, int]:
    """Translate only evidence already represented by legacy supported rows."""

    counts = {
        "lifecycle_applications": 0,
        "lifecycle_rejections": 0,
        "action_events": 0,
    }
    application_columns = table_columns(conn, "applications")
    rows = conn.execute(
        "SELECT * FROM applications ORDER BY id"
    ).fetchall()
    for row in rows:
        if not application_row_is_confirmed(row):
            continue
        applied_date = clean_cell(row["applied_date"])
        try:
            event_at = parse_iso_datetime(applied_date, label="applied_date").isoformat()
        except ValueError:
            continue
        event_id, created = append_lifecycle_event(
            conn,
            vacancy_id=int(row["vacancy_id"]),
            event_type="application_confirmed",
            event_at=event_at,
            evidence_at=event_at,
            evidence_note=f"Подтверждённая историческая запись отклика №{row['id']}.",
            evidence_source="legacy_applications",
            origin="migration:v6",
            external_reference=f"legacy-application:{row['id']}",
            history_complete=False,
            authorization_status="legacy_unknown",
            metadata={
                "actual_resume_id": clean_cell(row["resume_version"]),
            },
        )
        if created:
            counts["lifecycle_applications"] += 1
        if "lifecycle_event_id" in application_columns:
            conn.execute(
                "UPDATE applications SET lifecycle_event_id = ? WHERE id = ?",
                (event_id, int(row["id"])),
            )

    for row in conn.execute(
        """
        SELECT id, vacancy_id, event_date, note, origin_file
        FROM stage_events
        WHERE stage = 'rejected' AND TRIM(COALESCE(note, '')) != ''
        ORDER BY id
        """
    ).fetchall():
        date = clean_cell(row["event_date"])
        try:
            event_at = parse_iso_datetime(date, label="event_date").isoformat()
        except ValueError:
            continue
        _, created = append_lifecycle_event(
            conn,
            vacancy_id=int(row["vacancy_id"]),
            event_type="rejected",
            event_at=event_at,
            evidence_at=event_at,
            evidence_note=clean_cell(row["note"]),
            evidence_source="legacy_stage_events",
            origin="migration:v6",
            external_reference=f"legacy-stage-event:{row['id']}",
            history_complete=False,
            authorization_status="not_applicable",
        )
        if created:
            counts["lifecycle_rejections"] += 1

    for row in conn.execute("SELECT * FROM vacancies ORDER BY id").fetchall():
        action_state, bucket, due_date, priority = action_from_legacy_row(row)
        raw_event_at = clean_cell(row["updated_at"]) or clean_cell(row["last_seen_date"])
        try:
            event_at = parse_iso_datetime(raw_event_at, label="updated_at").isoformat()
        except ValueError:
            event_at = now_iso()
        _, created = append_action_event(
            conn,
            vacancy_id=int(row["id"]),
            action_state=action_state,
            bucket=bucket,
            event_at=event_at,
            due_date=due_date,
            priority=priority,
            reason=clean_cell(row["next_action"]) or clean_cell(row["open_questions"]),
            evidence_note="Текущее рабочее состояние перенесено без изменения фактов жизненного цикла.",
            source="migration:v6",
        )
        if created:
            counts["action_events"] += 1
    return counts


def upsert_vacancy(
    conn: sqlite3.Connection,
    *,
    channel: str,
    source: str,
    title: str,
    company: str,
    description: str,
    url: str,
    external_id: str = "",
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
    external_id = clean_cell(external_id) or vacancy_external_id(
        channel, norm_url, title, company
    )
    semantic_fingerprint = semantic_vacancy_fingerprint(company, title, description)
    row = conn.execute("SELECT * FROM vacancies WHERE external_id = ?", (external_id,)).fetchone()
    if not row:
        row = conn.execute(
            """
            SELECT v.*
            FROM vacancy_external_aliases a
            JOIN vacancies v ON v.id = a.vacancy_id
            WHERE a.channel = ? AND a.external_id = ?
            """,
            (channel, external_id),
        ).fetchone()
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
        store_vacancy_external_alias(
            conn,
            vacancy_id=vacancy_id,
            channel=channel,
            external_id=external_id,
            url=norm_url,
            seen_date=seen_date,
            timestamp=now,
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

    vacancy_id = int(row["id"])
    first_seen = min(filter(None, [row["first_seen_date"], seen_date]), default=seen_date)
    last_seen = max(filter(None, [row["last_seen_date"], seen_date]), default=seen_date)
    best_score = row["score"]
    if score is not None and (best_score is None or score > int(best_score)):
        best_score = score
    latest_stage = better_stage(row["latest_stage"], stage)

    def choose(field: str, new_value: str) -> str:
        return new_value or row[field] or ""

    canonical_url = row["url"] or ""
    if (
        external_id == row["external_id"]
        and channel == row["channel"]
        and norm_url
    ):
        canonical_url = norm_url

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
            canonical_url,
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
    store_vacancy_external_alias(
        conn,
        vacancy_id=vacancy_id,
        channel=channel,
        external_id=external_id,
        url=norm_url,
        seen_date=seen_date,
        timestamp=now,
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


def payload_items(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("vacancies", "items", "jobs", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
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
) -> int:
    canonical_stream, _ = canonical_source_stream(source_stream)
    existing = conn.execute(
        """
        SELECT id
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
        return int(existing["id"])
    cur = conn.execute(
        """
        INSERT INTO source_hits (
            vacancy_id, seen_date, source_name, source_stream,
            canonical_source_stream, raw_status,
            quick_score, reason, next_action, origin_file, line_no
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            seen_date,
            source_name,
            source_stream,
            canonical_stream,
            raw_status,
            quick_score,
            reason,
            next_action,
            origin_file,
            line_no,
        ),
    )
    return int(cur.lastrowid)


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


def insert_vacancy_factor_once(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    factor: dict[str, Any],
    default_date: str,
) -> bool:
    factor_key = clean_cell(str(factor.get("factor_key") or factor.get("key") or "")).lower()
    if not FACTOR_KEY_RE.fullmatch(factor_key):
        raise ValueError(
            "Ключ фактора должен быть записан в нижнем регистре snake_case и содержать от 2 до 64 знаков."
        )
    raw_value = factor.get("value", factor.get("level"))
    if raw_value is None or isinstance(raw_value, (dict, list)):
        raise ValueError(f"Для фактора {factor_key!r} требуется скалярное поле value или level.")
    factor_value = clean_cell(str(raw_value))
    if not factor_value:
        raise ValueError(f"Для фактора {factor_key!r} требуется непустое значение.")
    observed_date = clean_cell(
        str(factor.get("observed_date") or factor.get("evaluation_date") or default_date)
    )
    parse_iso_date(observed_date, label=f"factor {factor_key} observed_date")
    evidence_note = clean_cell(str(factor.get("evidence_note") or ""))
    if not evidence_note:
        raise ValueError(f"Для фактора {factor_key!r} требуется evidence_note.")
    evidence_url = normalize_url(str(factor.get("evidence_url") or ""))
    if evidence_url and not safe_external_url(evidence_url):
        raise ValueError(f"Для фактора {factor_key!r} адрес evidence_url должен использовать http или https.")
    confidence = clean_cell(str(factor.get("confidence") or "")).lower()
    if confidence not in EVIDENCE_CONFIDENCE:
        raise ValueError(
            f"Для фактора {factor_key!r} поле confidence должно иметь одно из значений: "
            + ", ".join(sorted(EVIDENCE_CONFIDENCE))
        )
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO vacancy_factors (
            vacancy_id, factor_key, factor_value, observed_date,
            evidence_note, evidence_url, confidence, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            factor_key,
            factor_value,
            observed_date,
            evidence_note,
            evidence_url,
            confidence,
            now_iso(),
        ),
    )
    return conn.total_changes > before


def extract_decision_metadata(item: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    field_specs = (
        ("campaign_id", DECISION_CAMPAIGN_IDS, "campaign_id"),
        ("role_family", DECISION_ROLE_FAMILIES, "role_family"),
        ("master_resume_id", DECISION_RESUME_IDS, "master_resume_id"),
        ("planned_resume_id", DECISION_RESUME_IDS, "planned_resume_id"),
        ("actual_resume_id", DECISION_RESUME_IDS, "actual_resume_id"),
        ("message_variant", DECISION_MESSAGE_VARIANTS, "message_variant"),
    )
    for key, allowed, label in field_specs:
        if key in item:
            metadata[key] = configured_value(item.get(key), allowed, label=label)
    if "actual_submitted_resume_version" in item and "actual_resume_id" not in metadata:
        metadata["actual_resume_id"] = configured_value(
            item.get("actual_submitted_resume_version"),
            DECISION_RESUME_IDS,
            label="actual_submitted_resume_version",
        )
    if "confidence" in item:
        confidence = clean_cell(str(item.get("confidence") or "")).lower()
        if confidence and confidence not in EVIDENCE_CONFIDENCE | {"unknown"}:
            raise ValueError(
                "Поле confidence должно иметь одно из значений: "
                + ", ".join(sorted(EVIDENCE_CONFIDENCE | {"unknown"}))
            )
        metadata["confidence"] = confidence or None
    if "hard_gates" in item:
        metadata["hard_gates"] = validate_hard_gates(item.get("hard_gates"))
    if "unresolved_questions" in item:
        metadata["unresolved_questions"] = validate_unresolved_questions(
            item.get("unresolved_questions")
        )
    if "human_path_status" in item:
        human_path_status = clean_cell(
            str(item.get("human_path_status") or "")
        ).lower()
        if human_path_status and human_path_status not in HUMAN_PATH_STATUSES:
            raise ValueError(
                "Поле human_path_status должно иметь одно из значений: "
                + ", ".join(sorted(HUMAN_PATH_STATUSES))
            )
        metadata["human_path_status"] = human_path_status or None
    return metadata


def upsert_decision_metadata(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    metadata: dict[str, Any],
    application_source_hit_id: int | None = None,
) -> None:
    if not metadata and application_source_hit_id is None:
        return
    row = conn.execute(
        "SELECT * FROM vacancy_decision_metadata WHERE vacancy_id = ?",
        (vacancy_id,),
    ).fetchone()

    def selected(key: str) -> Any:
        if key in metadata:
            return metadata[key]
        return row[key] if row else None

    hard_gates_json = (
        json.dumps(metadata["hard_gates"], ensure_ascii=False)
        if "hard_gates" in metadata
        else (row["hard_gate_results_json"] if row else None)
    )
    questions_json = (
        json.dumps(metadata["unresolved_questions"], ensure_ascii=False)
        if "unresolved_questions" in metadata
        else (row["unresolved_questions_json"] if row else None)
    )
    conn.execute(
        """
        INSERT INTO vacancy_decision_metadata (
            vacancy_id, campaign_id, role_family, confidence,
            hard_gate_results_json, unresolved_questions_json,
            master_resume_id, planned_resume_id, actual_resume_id,
            message_variant, application_source_hit_id, human_path_status,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(vacancy_id) DO UPDATE SET
            campaign_id = excluded.campaign_id,
            role_family = excluded.role_family,
            confidence = excluded.confidence,
            hard_gate_results_json = excluded.hard_gate_results_json,
            unresolved_questions_json = excluded.unresolved_questions_json,
            master_resume_id = excluded.master_resume_id,
            planned_resume_id = excluded.planned_resume_id,
            actual_resume_id = excluded.actual_resume_id,
            message_variant = excluded.message_variant,
            application_source_hit_id = excluded.application_source_hit_id,
            human_path_status = excluded.human_path_status,
            updated_at = excluded.updated_at
        """,
        (
            vacancy_id,
            selected("campaign_id"),
            selected("role_family"),
            selected("confidence"),
            hard_gates_json,
            questions_json,
            selected("master_resume_id"),
            selected("planned_resume_id"),
            selected("actual_resume_id"),
            selected("message_variant"),
            application_source_hit_id
            if application_source_hit_id is not None
            else (row["application_source_hit_id"] if row else None),
            selected("human_path_status"),
            now_iso(),
        ),
    )


def append_external_action(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int | None,
    action_key: str,
    action_type: str,
    state: str,
    event_at: str,
    authorization_note: str,
    evidence_note: str,
    evidence_url: str,
    source: str,
    external_reference: str = "",
    metadata: dict[str, Any] | None = None,
) -> tuple[int, bool]:
    action_key = clean_cell(action_key)
    action_type = clean_cell(action_type).lower()
    state = clean_cell(state).lower()
    if not action_key:
        raise ValueError("Требуется action_key.")
    if action_type not in EXTERNAL_ACTION_TYPES:
        raise ValueError("Неподдерживаемый тип внешнего действия.")
    if state not in EXTERNAL_ACTION_STATES:
        raise ValueError("Неподдерживаемое состояние внешнего действия.")
    event_at_value = parse_iso_datetime(event_at, label="event_at").isoformat()
    authorization_note = clean_cell(authorization_note)
    evidence_note = clean_cell(evidence_note)
    existing_context = conn.execute(
        """
        SELECT vacancy_id, action_type FROM external_actions
        WHERE action_key = ? ORDER BY id LIMIT 1
        """,
        (action_key,),
    ).fetchone()
    if existing_context and (
        existing_context["vacancy_id"] != vacancy_id
        or str(existing_context["action_type"]) != action_type
    ):
        raise ValueError(
            "action_key уже относится к другой вакансии или другому типу действия."
        )
    prior_authorization = conn.execute(
        """
        SELECT 1 FROM external_actions
        WHERE action_key = ? AND action_type = ?
          AND vacancy_id IS ? AND state = 'authorized'
        LIMIT 1
        """,
        (action_key, action_type, vacancy_id),
    ).fetchone()
    if state == "authorized" and not authorization_note:
        raise ValueError("Для состояния authorized требуется пояснение разрешения.")
    if state in {"attempted", "visibly_confirmed"} and not prior_authorization:
        raise ValueError(
            "Перед попыткой или подтверждением требуется сохранённое состояние authorized."
        )
    if state in {"attempted", "visibly_confirmed", "blocked", "failed"} and not evidence_note:
        raise ValueError("Для этого состояния внешнего действия требуется доказательное примечание.")
    if state == "visibly_confirmed" and not external_reference:
        raise ValueError("Для видимого подтверждения требуется внешняя ссылка или идентификатор.")
    evidence_url = validate_optional_external_url(
        evidence_url, label="evidence_url"
    )
    if clean_cell(external_reference):
        payload = {
            "vacancy_id": vacancy_id,
            "action_key": action_key,
            "action_type": action_type,
            "state": state,
            "external_reference": clean_cell(external_reference),
        }
    else:
        payload = {
            "vacancy_id": vacancy_id,
            "action_key": action_key,
            "action_type": action_type,
            "state": state,
            "event_at": event_at_value,
        }
    dedupe_key = dedupe_hash(payload)
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO external_actions (
            vacancy_id, action_key, action_type, state, event_at,
            authorization_note, evidence_note, evidence_url, source,
            external_reference, metadata_json, dedupe_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            action_key,
            action_type,
            state,
            event_at_value,
            authorization_note,
            evidence_note,
            evidence_url,
            clean_cell(source),
            clean_cell(external_reference),
            json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
            dedupe_key,
            now_iso(),
        ),
    )
    created = conn.total_changes > before
    row = conn.execute(
        "SELECT id FROM external_actions WHERE dedupe_key = ?", (dedupe_key,)
    ).fetchone()
    return int(row["id"]), created


def classify_ingestion_record(
    item: dict[str, Any], *, channel: str
) -> tuple[str, list[str]]:
    explicit = clean_cell(
        str(item.get("record_classification") or item.get("classification") or "")
    ).lower()
    if explicit in QUARANTINE_CLASSIFICATIONS:
        return explicit, []
    record_type = clean_cell(
        str(item.get("record_type") or item.get("kind") or "")
    ).lower()
    if record_type in {"non_vacancy", "technical", "error"} or item.get("is_vacancy") is False:
        return "non_vacancy", []
    technical_text = " ".join(
        str(item.get(key) or "")
        for key in ("title", "status", "error", "message", "body", "description")
    ).casefold()
    if any(marker in technical_text for marker in ("captcha", "капча", "robot check")):
        return "captcha", []
    if any(
        marker in technical_text
        for marker in ("logged out", "sign in to continue", "login required", "требуется вход")
    ):
        return "logged_out", []
    if any(
        marker in technical_text
        for marker in ("access denied", "forbidden", "429 too many", "доступ запрещён")
    ):
        return "access_error", []

    title = clean_cell(str(item.get("title") or item.get("vacancy_title") or item.get("name") or ""))
    company = clean_cell(str(item.get("company") or item.get("employer") or ""))
    url = clean_cell(str(item.get("url") or item.get("href") or ""))
    missing: list[str] = []
    if not title:
        missing.append("title")
    if not url:
        missing.append("url")
    if channel in {"hh", "linkedin", "telegram", "company_site", "gmail_hh"} and not company:
        missing.append("company")
    if missing:
        return "missing_required_fields", missing
    if not safe_external_url(normalize_url(url)):
        return "malformed", ["url"]
    return "", []


def store_quarantine_record(
    conn: sqlite3.Connection,
    *,
    item: Any,
    origin_file: str,
    line_no: int,
    source_name: str,
    source_stream: str,
    classification: str,
    evidence_note: str,
    retry_context: dict[str, Any] | None = None,
) -> tuple[int, bool]:
    if classification not in QUARANTINE_CLASSIFICATIONS:
        raise ValueError("Неподдерживаемая классификация записи карантина.")
    raw_payload = json.dumps(item, ensure_ascii=False, sort_keys=True)
    dedupe_key = dedupe_hash(
        {
            "origin_file": origin_file,
            "line_no": line_no,
            "raw_payload": raw_payload,
            "classification": classification,
        }
    )
    timestamp = now_iso()
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO quarantine_records (
            origin_file, line_no, source_name, source_stream, classification,
            status, evidence_note, raw_payload_json, retry_context_json,
            retry_count, dedupe_key, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, 0, ?, ?, ?)
        """,
        (
            origin_file,
            line_no,
            source_name,
            source_stream,
            classification,
            clean_cell(evidence_note),
            raw_payload,
            json.dumps(retry_context or {}, ensure_ascii=False, sort_keys=True),
            dedupe_key,
            timestamp,
            timestamp,
        ),
    )
    created = conn.total_changes > before
    row = conn.execute(
        "SELECT id FROM quarantine_records WHERE dedupe_key = ?", (dedupe_key,)
    ).fetchone()
    return int(row["id"]), created


def validate_rule_results(value: Any) -> list[dict[str, str]]:
    if value in (None, ""):
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("Поле rule_results должно быть массивом объектов.")
    result: list[dict[str, str]] = []
    for index, item in enumerate(value, start=1):
        key = clean_cell(str(item.get("rule") or item.get("rule_key") or ""))
        outcome = clean_cell(str(item.get("result") or "unknown")).lower()
        if not key:
            raise ValueError(f"Для rule_results[{index}] требуется rule_key.")
        if outcome not in {"matched", "not_matched", "unknown"}:
            raise ValueError(
                f"Поле rule_results[{index}].result должно иметь значение matched, not_matched или unknown."
            )
        result.append(
            {
                "rule_key": key,
                "result": outcome,
                "note": clean_cell(str(item.get("note") or "")),
            }
        )
    return result


def insert_screening_decision(
    conn: sqlite3.Connection,
    *,
    vacancy_id: int,
    item: dict[str, Any],
    date: str,
    score: int | None,
    stage: str,
    status: str,
) -> None:
    explicit_decision = clean_cell(str(item.get("screening_decision") or "")).lower()
    if explicit_decision:
        decision = explicit_decision
    elif stage == "rejected" or any(
        marker in status.casefold() for marker in ("low_fit", "skip", "reject")
    ):
        decision = "rejected"
    elif clean_cell(str(item.get("priority") or "")).lower() == "low":
        decision = "low_priority"
    else:
        return
    if decision not in {"rejected", "low_priority", "review"}:
        raise ValueError("Поле screening_decision должно иметь значение rejected, low_priority или review.")
    rule_results = validate_rule_results(item.get("rule_results"))
    payload = {
        "vacancy_id": vacancy_id,
        "evaluated_at": date,
        "decision": decision,
        "policy_version": ACTIVE_POLICY_VERSION,
        "rule_results": rule_results,
    }
    conn.execute(
        """
        INSERT OR IGNORE INTO screening_decisions (
            vacancy_id, evaluated_at, decision, score, priority,
            policy_version, policy_effective_date, rule_results_json,
            evidence_note, dedupe_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vacancy_id,
            parse_iso_datetime(date, label="evaluation date").isoformat(),
            decision,
            score,
            clean_cell(str(item.get("priority") or "")),
            ACTIVE_POLICY_VERSION,
            ACTIVE_POLICY_EFFECTIVE_DATE,
            json.dumps(rule_results, ensure_ascii=False),
            clean_cell(str(item.get("decision_evidence_note") or item.get("reason") or "")),
            dedupe_hash(payload),
            now_iso(),
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
    ingestion_stats: dict[str, int] | None = None,
) -> int | None:
    ingestion_stats = ingestion_stats if ingestion_stats is not None else {}
    channel = infer_channel(item, default_channel)
    raw_source_stream_value = item.get("source_stream", item.get("stream"))
    source_hint = clean_cell(str(item.get("source") or default_source or "manual_json"))
    source_stream = (
        str(raw_source_stream_value)
        if raw_source_stream_value is not None
        else source_hint
    )
    classification, classification_details = classify_ingestion_record(
        item, channel=channel
    )
    if classification:
        note = clean_cell(str(item.get("quarantine_note") or ""))
        if not note and classification_details:
            note = "Не пройдена проверка полей: " + ", ".join(classification_details)
        if not note:
            note = "Запись исключена из показателей вакансий до явной повторной обработки."
        store_quarantine_record(
            conn,
            item=item,
            origin_file=origin_file,
            line_no=line_no,
            source_name=channel,
                source_stream=source_stream,
            classification=classification,
            evidence_note=note,
            retry_context=item.get("retry_context")
            if isinstance(item.get("retry_context"), dict)
            else {},
        )
        ingestion_stats["quarantined"] = ingestion_stats.get("quarantined", 0) + 1
        return None

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
    external_id = clean_cell(str(item.get("external_id") or ""))
    date = clean_cell(
        str(
            item.get("date")
            or item.get("seen_date")
            or item.get("evaluation_date")
            or item.get("applied_date")
            or dt.date.today().isoformat()
        )
    )
    try:
        parse_iso_datetime(date, label="record date")
    except ValueError:
        store_quarantine_record(
            conn,
            item=item,
            origin_file=origin_file,
            line_no=line_no,
            source_name=channel,
            source_stream=source_stream,
            classification="malformed",
            evidence_note="Дата записи не соответствует ISO 8601.",
            retry_context={},
        )
        ingestion_stats["quarantined"] = ingestion_stats.get("quarantined", 0) + 1
        return None
    source = clean_cell(str(item.get("source") or default_source or "manual_json"))
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
    decision_metadata = extract_decision_metadata(item)

    vacancy_id = upsert_vacancy(
        conn,
        channel=channel,
        source=source,
        title=title,
        company=company,
        description=description,
        url=url,
        external_id=external_id,
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

    source_hit_id = insert_source_hit_once(
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
    upsert_decision_metadata(
        conn,
        vacancy_id=vacancy_id,
        metadata=decision_metadata,
        application_source_hit_id=(
            source_hit_id if item.get("application_source") is True else None
        ),
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

    factors = item.get("factors", [])
    if factors is None:
        factors = []
    if not isinstance(factors, list) or not all(
        isinstance(factor, dict) for factor in factors
    ):
        raise ValueError("Поле factors должно быть массивом объектов.")
    for factor in factors:
        insert_vacancy_factor_once(
            conn,
            vacancy_id=vacancy_id,
            factor=factor,
            default_date=date,
        )

    insert_screening_decision(
        conn,
        vacancy_id=vacancy_id,
        item=item,
        date=date,
        score=score,
        stage=stage,
        status=status,
    )

    external_action_id: int | None = None
    external_action = item.get("external_action")
    if external_action is not None:
        if not isinstance(external_action, dict):
            raise ValueError("Поле external_action должно быть объектом.")
        action_metadata = dict(decision_metadata)
        action_type = clean_cell(
            str(external_action.get("action_type") or "application")
        ).lower()
        action_state = clean_cell(str(external_action.get("state") or "")).lower()
        external_action_id, _ = append_external_action(
            conn,
            vacancy_id=vacancy_id,
            action_key=clean_cell(str(external_action.get("action_key") or "")),
            action_type=action_type,
            state=action_state,
            event_at=clean_cell(str(external_action.get("event_at") or date)),
            authorization_note=clean_cell(
                str(external_action.get("authorization_note") or "")
            ),
            evidence_note=clean_cell(
                str(external_action.get("evidence_note") or "")
            ),
            evidence_url=clean_cell(str(external_action.get("evidence_url") or "")),
            source=clean_cell(str(external_action.get("source") or source)),
            external_reference=clean_cell(
                str(external_action.get("external_reference") or "")
            ),
            metadata=action_metadata,
        )
        if action_type == "application" and action_state == "visibly_confirmed":
            lifecycle_id, _ = append_lifecycle_event(
                conn,
                vacancy_id=vacancy_id,
                event_type="application_confirmed",
                event_at=clean_cell(str(external_action.get("event_at") or date)),
                evidence_at=clean_cell(
                    str(external_action.get("evidence_at") or now_iso())
                ),
                evidence_note=clean_cell(
                    str(external_action.get("evidence_note") or "")
                ),
                evidence_source=clean_cell(
                    str(external_action.get("source") or source)
                ),
                origin=origin_file or "ingest-json",
                evidence_url=clean_cell(
                    str(external_action.get("evidence_url") or "")
                ),
                external_reference=clean_cell(
                    str(external_action.get("external_reference") or "")
                ),
                external_action_id=external_action_id,
                application_source_hit_id=(
                    source_hit_id if item.get("application_source") is True else None
                ),
                metadata=action_metadata,
                history_complete=True,
                authorization_status="explicit",
            )
            application = conn.execute(
                "SELECT id FROM applications WHERE vacancy_id = ? ORDER BY id DESC LIMIT 1",
                (vacancy_id,),
            ).fetchone()
            if application:
                conn.execute(
                    """
                    UPDATE applications
                    SET lifecycle_event_id = ?, application_source_hit_id = ?,
                        campaign_id = ?, role_family = ?, actual_resume_version = ?,
                        message_variant = ?
                    WHERE id = ?
                    """,
                    (
                        lifecycle_id,
                        source_hit_id if item.get("application_source") is True else None,
                        decision_metadata.get("campaign_id"),
                        decision_metadata.get("role_family"),
                        decision_metadata.get("actual_resume_id"),
                        decision_metadata.get("message_variant"),
                        int(application["id"]),
                    ),
                )

    lifecycle_items = item.get("lifecycle_events", [])
    if lifecycle_items is None:
        lifecycle_items = []
    if not isinstance(lifecycle_items, list) or not all(
        isinstance(event, dict) for event in lifecycle_items
    ):
        raise ValueError("Поле lifecycle_events должно быть массивом объектов.")
    for event in lifecycle_items:
        event_type = clean_cell(str(event.get("event_type") or event.get("type") or ""))
        if event_type == "application_confirmed":
            raise ValueError(
                "Событие application_confirmed можно записать только через видимо подтверждённое external_action."
            )
        append_lifecycle_event(
            conn,
            vacancy_id=vacancy_id,
            event_type=event_type,
            event_at=clean_cell(str(event.get("event_at") or date)),
            evidence_at=clean_cell(str(event.get("evidence_at") or now_iso())),
            evidence_note=clean_cell(str(event.get("evidence_note") or "")),
            evidence_source=clean_cell(str(event.get("source") or source)),
            origin=origin_file or "ingest-json",
            evidence_url=clean_cell(str(event.get("evidence_url") or "")),
            external_reference=clean_cell(
                str(event.get("external_reference") or "")
            ),
            round_no=to_int(str(event.get("round_no") or event.get("interview_no") or "")),
            scheduled_at=clean_cell(str(event.get("scheduled_at") or "")),
            metadata=decision_metadata,
            history_complete=True,
            authorization_status="not_applicable",
        )

    action_state = clean_cell(str(item.get("action_state") or "")).lower()
    action_bucket = clean_cell(str(item.get("action_bucket") or "")).lower()
    if not action_state or not action_bucket:
        legacy_action = {
            "latest_stage": stage,
            "latest_status": status,
            "next_action": next_action,
            "open_questions": open_questions,
            "follow_up_date": follow_up_date,
            "score": score,
        }
        action_state, action_bucket, derived_due, derived_priority = action_from_legacy_row(
            legacy_action  # type: ignore[arg-type]
        )
    else:
        derived_due = follow_up_date
        derived_priority = int(score or 0)
    append_action_event(
        conn,
        vacancy_id=vacancy_id,
        action_state=action_state,
        bucket=action_bucket,
        event_at=clean_cell(str(item.get("action_at") or date)),
        due_date=clean_cell(str(item.get("action_due_date") or derived_due)),
        priority=to_int(str(item.get("action_priority") or derived_priority)) or 0,
        reason=clean_cell(str(item.get("priority_reason") or next_action or open_questions or reason)),
        evidence_note=clean_cell(str(item.get("action_evidence_note") or "")),
        source=origin_file or source,
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
    ingestion_stats["ingested"] = ingestion_stats.get("ingested", 0) + 1
    return vacancy_id


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [{key: row[key] for key in row.keys()} for row in rows]


APPLICATION_STAGES = {
    "applied",
    "follow_up",
    "interview_1",
    "interview_2",
    "interview_3",
    "offer",
    "rejected",
}
UNCONFIRMED_APPLICATION_MARKERS = (
    "not_sent",
    "not sent",
    "не отправлен",
    "draft",
    "attempted",
    "unconfirmed",
    "failed",
)


def application_row_is_confirmed(row: sqlite3.Row) -> bool:
    if canonical_stage(row["stage"]) not in APPLICATION_STAGES:
        return False
    status = clean_cell(row["status"]).casefold()
    return not any(marker in status for marker in UNCONFIRMED_APPLICATION_MARKERS)


def first_application_cohorts(
    conn: sqlite3.Connection, as_of: dt.date
) -> tuple[list[dict[str, Any]], int]:
    first_by_vacancy: dict[int, dict[str, Any]] = {}
    invalid_dates = 0
    rows = conn.execute(
        """
        SELECT a.id, a.vacancy_id, a.applied_date, a.status, a.stage,
               v.channel, v.company, v.title
        FROM applications a
        JOIN vacancies v ON v.id = a.vacancy_id
        ORDER BY a.id
        """
    ).fetchall()
    for row in rows:
        if not application_row_is_confirmed(row):
            continue
        try:
            applied_date = dt.date.fromisoformat(clean_cell(row["applied_date"])[:10])
        except ValueError:
            invalid_dates += 1
            continue
        if applied_date > as_of:
            continue
        vacancy_id = int(row["vacancy_id"])
        candidate = {
            "vacancy_id": vacancy_id,
            "application_id": int(row["id"]),
            "application_date": applied_date,
            "source_channel": clean_cell(row["channel"]) or "unknown",
            "company": row["company"] or "",
            "title": row["title"] or "",
        }
        existing = first_by_vacancy.get(vacancy_id)
        if existing is None or (
            applied_date,
            int(row["id"]),
        ) < (
            existing["application_date"],
            existing["application_id"],
        ):
            first_by_vacancy[vacancy_id] = candidate

    source_hits_by_vacancy: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT id, vacancy_id, seen_date, source_stream, canonical_source_stream
        FROM source_hits
        ORDER BY COALESCE(seen_date, ''), id
        """
    ).fetchall():
        source_hits_by_vacancy[int(row["vacancy_id"])].append(row)

    for cohort in first_by_vacancy.values():
        first_touch = None
        for hit in source_hits_by_vacancy.get(cohort["vacancy_id"], []):
            try:
                hit_date = dt.date.fromisoformat(clean_cell(hit["seen_date"])[:10])
            except ValueError:
                continue
            if hit_date <= cohort["application_date"]:
                first_touch = hit
                break
        if first_touch is None:
            cohort["source_stream"] = "unknown"
        else:
            cohort["source_stream"] = canonical_source_stream(
                first_touch["source_stream"]
            )[0]
        cohort["application_month"] = cohort["application_date"].strftime("%Y-%m")
        cohort["age_days"] = (as_of - cohort["application_date"]).days
    return sorted(first_by_vacancy.values(), key=lambda row: row["vacancy_id"]), invalid_dates


def aggregate_conversion_group(
    rows: list[dict[str, Any]], *, interaction_history_available: bool
) -> dict[str, Any]:
    applications_unique = len(rows)
    matured_14 = [row for row in rows if row["age_days"] >= 14]
    matured_30 = [row for row in rows if row["age_days"] >= 30]
    human_reply_rows = [row for row in matured_14 if row["first_human_reply_at"]]
    interview_rows = [row for row in matured_30 if row["interview_1_ever"]]
    reply_times = [
        float(row["time_to_first_human_reply_days"])
        for row in rows
        if row["time_to_first_human_reply_days"] is not None
    ]
    verified_contacts = sum(1 for row in rows if row["verified_contact"])
    contact_searches = sum(1 for row in rows if row["contact_search_completed"])

    def percent(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator * 100, 1) if denominator else None

    return {
        "applications_unique": applications_unique,
        "matured_applications_14d": len(matured_14),
        "human_replies": len(human_reply_rows)
        if interaction_history_available
        else None,
        "human_reply_rate_14d": percent(len(human_reply_rows), len(matured_14))
        if interaction_history_available
        else None,
        "matured_applications_30d": len(matured_30),
        "interview_1_ever": len(interview_rows),
        "interview_1_rate_30d": percent(len(interview_rows), len(matured_30)),
        "first_human_reply_sample": len(reply_times),
        "median_time_to_first_human_reply_days": round(statistics.median(reply_times), 2)
        if reply_times
        else None,
        "average_time_to_first_human_reply_days": round(statistics.mean(reply_times), 2)
        if reply_times
        else None,
        "verified_contacts": verified_contacts,
        "verified_contact_coverage": percent(verified_contacts, applications_unique),
        "contact_search_completed": contact_searches,
        "contact_search_coverage": percent(contact_searches, applications_unique),
    }


def build_legacy_conversion_report_data(
    conn: sqlite3.Connection, as_of: dt.date
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cohorts, invalid_application_dates = first_application_cohorts(conn, as_of)
    cohort_by_vacancy = {row["vacancy_id"]: row for row in cohorts}

    interactions = conn.execute(
        """
        SELECT vacancy_id, event_at
        FROM effective_employer_interactions
        WHERE direction = 'inbound' AND is_human = 1
        ORDER BY event_at, id
        """
    ).fetchall()
    interaction_history_available = bool(
        conn.execute(
            "SELECT 1 FROM effective_employer_interactions WHERE event_at <= ? LIMIT 1",
            (as_of.isoformat() + "T23:59:59",),
        ).fetchone()
    )
    for row in cohorts:
        row["first_human_reply_at"] = None
        row["time_to_first_human_reply_days"] = None
        row["interview_1_ever"] = False
        row["verified_contact"] = False
        row["contact_search_completed"] = False

    for interaction in interactions:
        cohort = cohort_by_vacancy.get(int(interaction["vacancy_id"]))
        if not cohort or cohort["first_human_reply_at"]:
            continue
        try:
            event_at = parse_iso_datetime(interaction["event_at"], label="event_at")
        except ValueError:
            continue
        applied_at = dt.datetime.combine(cohort["application_date"], dt.time())
        if applied_at <= event_at < dt.datetime.combine(
            as_of + dt.timedelta(days=1), dt.time()
        ):
            cohort["first_human_reply_at"] = event_at.isoformat()
            cohort["time_to_first_human_reply_days"] = (
                event_at - applied_at
            ).total_seconds() / 86400

    for row in conn.execute(
        """
        SELECT DISTINCT vacancy_id
        FROM stage_events
        WHERE stage = 'interview_1'
          AND TRIM(COALESCE(note, '')) != ''
          AND COALESCE(event_date, '') <= ?
        """,
        (as_of.isoformat(),),
    ).fetchall():
        cohort = cohort_by_vacancy.get(int(row["vacancy_id"]))
        if cohort:
            cohort["interview_1_ever"] = True

    for row in conn.execute(
        """
        SELECT DISTINCT vacancy_id
        FROM employer_contacts
        WHERE is_active = 1
          AND confidence IN ('confirmed', 'strong')
          AND (COALESCE(verified_date, '') = '' OR verified_date <= ?)
        """,
        (as_of.isoformat(),),
    ).fetchall():
        cohort = cohort_by_vacancy.get(int(row["vacancy_id"]))
        if cohort:
            cohort["verified_contact"] = True

    for row in conn.execute(
        """
        SELECT DISTINCT vacancy_id
        FROM contact_searches
        WHERE search_date <= ?
        """,
        (as_of.isoformat(),),
    ).fetchall():
        cohort = cohort_by_vacancy.get(int(row["vacancy_id"]))
        if cohort:
            cohort["contact_search_completed"] = True

    overall = aggregate_conversion_group(
        cohorts, interaction_history_available=interaction_history_available
    )
    breakdowns: dict[str, list[dict[str, Any]]] = {}
    for grouping in ("source_channel", "source_stream", "application_month"):
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in cohorts:
            grouped[str(row[grouping] or "unknown")].append(row)
        breakdowns[grouping] = [
            {
                grouping: key,
                **aggregate_conversion_group(
                    grouped[key],
                    interaction_history_available=interaction_history_available,
                ),
            }
            for key in sorted(grouped)
        ]

    caveats = [
        "Единица когорты — самая ранняя подтверждённая строка отклика для одной уникальной вакансии.",
        "Канонический поток определяется по самому раннему касанию до отклика; равные даты разрешаются по source_hits.id.",
        "Ответы людей — входящие взаимодействия после отклика с признаком человека; автоматические подтверждения исключены.",
        "Для конверсии интервью требуется этап interview_1 с непустым доказательным примечанием.",
    ]
    if not interaction_history_available:
        caveats.append(
            "История взаимодействий с работодателями отсутствует, поэтому ответы и доли равны n/a, а не нулю."
        )
    else:
        caveats.append(
            "Показатели ответов учитывают только сохранённые взаимодействия; система не восстанавливает историю из текста состояния."
        )
    if invalid_application_dates:
        caveats.append(
            f"Исключено строк откликов с некорректной датой: {invalid_application_dates}."
        )
    return (
        {
            "as_of": as_of.isoformat(),
            "interaction_history_available": interaction_history_available,
            "methodology": {
                "grain": "одна уникальная вакансия и её самый ранний подтверждённый отклик",
                "reply_maturity_days": 14,
                "interview_maturity_days": 30,
                "stream_attribution": "самое раннее касание до даты отклика; source_hits.id разрешает равные даты; отсутствие означает неизвестный источник",
            },
            "overall": overall,
            "breakdowns": breakdowns,
            "caveats": caveats,
        },
        cohorts,
    )


def first_confirmed_application_cohorts(
    conn: sqlite3.Connection, as_of: dt.date
) -> tuple[list[dict[str, Any]], int]:
    """Reduce append-only evidence to one earliest confirmed event per vacancy."""

    end_at = dt.datetime.combine(as_of + dt.timedelta(days=1), dt.time())
    first_by_vacancy: dict[int, dict[str, Any]] = {}
    invalid_dates = 0
    rows = conn.execute(
        """
        SELECT le.*, v.channel AS vacancy_channel, v.company, v.title
        FROM lifecycle_events le
        JOIN vacancies v ON v.id = le.vacancy_id
        WHERE le.event_type = 'application_confirmed'
        ORDER BY le.event_at, le.id
        """
    ).fetchall()
    for row in rows:
        try:
            application_at = parse_iso_datetime(row["event_at"], label="event_at")
        except ValueError:
            invalid_dates += 1
            continue
        if application_at >= end_at:
            continue
        vacancy_id = int(row["vacancy_id"])
        if vacancy_id in first_by_vacancy:
            continue
        first_by_vacancy[vacancy_id] = {
            "vacancy_id": vacancy_id,
            "application_event_id": int(row["id"]),
            "application_at": application_at,
            "application_date": application_at.date(),
            "application_month": application_at.strftime("%Y-%m"),
            "age_days": (as_of - application_at.date()).days,
            "company": clean_cell(row["company"]),
            "title": clean_cell(row["title"]),
            "vacancy_channel": clean_cell(row["vacancy_channel"]) or "unknown",
            "history_complete": bool(row["history_complete"]),
            "authorization_status": clean_cell(row["authorization_status"]),
            "campaign_id": clean_cell(row["campaign_id"]) or "unknown",
            "role_family": clean_cell(row["role_family"]) or "unknown",
            "confidence": clean_cell(row["confidence"]) or "unknown",
            "master_resume_id": clean_cell(row["master_resume_id"]) or "unknown",
            "planned_resume_id": clean_cell(row["planned_resume_id"]) or "unknown",
            "actual_resume_version": clean_cell(row["actual_resume_id"]) or "unknown",
            "message_variant": clean_cell(row["message_variant"]) or "unknown",
            "hard_gate_results_known": row["hard_gate_results_json"] is not None,
            "unresolved_questions_known": row["unresolved_questions_json"] is not None,
            "human_path_status": clean_cell(row["human_path_status"]) or "unknown",
            "explicit_application_source_hit_id": row["application_source_hit_id"],
        }

    hits_by_vacancy: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for hit in conn.execute(
        """
        SELECT id, vacancy_id, seen_date, source_name, source_stream
        FROM source_hits ORDER BY COALESCE(seen_date, ''), id
        """
    ).fetchall():
        hits_by_vacancy[int(hit["vacancy_id"])].append(hit)

    account_by_vacancy = {
        int(row["vacancy_id"]): clean_cell(row["canonical_name"]) or "unknown"
        for row in conn.execute(
            """
            SELECT l.vacancy_id, a.canonical_name
            FROM vacancy_employer_accounts l
            JOIN employer_accounts a ON a.id = l.account_id
            """
        ).fetchall()
    }
    for cohort in first_by_vacancy.values():
        selected_hit: sqlite3.Row | None = None
        explicit_id = cohort["explicit_application_source_hit_id"]
        if explicit_id is not None:
            selected_hit = conn.execute(
                """
                SELECT id, vacancy_id, seen_date, source_name, source_stream
                FROM source_hits WHERE id = ? AND vacancy_id = ?
                """,
                (int(explicit_id), cohort["vacancy_id"]),
            ).fetchone()
        if selected_hit is None:
            for hit in hits_by_vacancy.get(cohort["vacancy_id"], []):
                try:
                    hit_at = parse_iso_datetime(hit["seen_date"], label="seen_date")
                except ValueError:
                    continue
                if hit_at.date() <= cohort["application_date"]:
                    selected_hit = hit
                    break
        if selected_hit is None:
            cohort.update(
                {
                    "source_channel": "unknown",
                    "source_stream": "unknown",
                    "source_labels": ["unknown"],
                    "raw_source_stream": "",
                    "source_attribution": "unknown",
                }
            )
        else:
            labels, _ = canonical_source_labels(selected_hit["source_stream"])
            cohort.update(
                {
                    "source_channel": clean_cell(selected_hit["source_name"]) or cohort["vacancy_channel"],
                    "source_stream": labels[0],
                    "source_labels": list(labels),
                    "raw_source_stream": selected_hit["source_stream"] or "",
                    "source_attribution": "explicit_application_source"
                    if explicit_id is not None
                    else "deterministic_first_touch",
                }
            )
        cohort["employer_account"] = account_by_vacancy.get(
            cohort["vacancy_id"], "unknown"
        )
        cohort["first_human_reply_at"] = None
        cohort["screening_request"] = False
        cohort["automated_acknowledgments"] = 0
        cohort["interview_invited"] = False
        cohort["interview_scheduled"] = False
        cohort["completed_first_interview"] = False
        cohort["later_interview_round"] = False
        cohort["offer_received"] = False
        cohort["interview_cancelled"] = False
        cohort["candidate_no_show"] = False
        cohort["employer_no_show"] = False
        cohort["contact_search_completed"] = False
        cohort["verified_human_path"] = cohort["human_path_status"] == "verified"

    return (
        sorted(first_by_vacancy.values(), key=lambda item: item["application_event_id"]),
        invalid_dates,
    )


def rate_payload(
    numerator: int | None, denominator: int, *, available: bool = True
) -> dict[str, Any]:
    percent_value = None
    if available and numerator is not None and denominator:
        percent_value = round(numerator / denominator * 100, 1)
    return {
        "numerator": numerator if available else None,
        "denominator": denominator,
        "percent": percent_value,
        "available": bool(available and numerator is not None),
    }


def aggregate_outcome_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    confirmed = len(rows)
    matured_14 = [row for row in rows if row["age_days"] >= 14]
    matured_30 = [row for row in rows if row["age_days"] >= 30]
    reply_complete = all(row["history_complete"] for row in matured_14)
    interview_complete = all(row["history_complete"] for row in matured_30)
    all_complete = all(row["history_complete"] for row in rows)

    human_replies_count = sum(bool(row["first_human_reply_at"]) for row in matured_14)
    screening_request_count = sum(bool(row["screening_request"]) for row in rows)
    reply_times = [
        (
            parse_iso_datetime(row["first_human_reply_at"], label="event_at")
            - row["application_at"]
        ).total_seconds()
        / 86400
        for row in rows
        if row["first_human_reply_at"]
    ]
    invitation_count = sum(bool(row["interview_invited"]) for row in rows)
    scheduled_count = sum(bool(row["interview_scheduled"]) for row in rows)
    completed_first_count = sum(
        bool(row["completed_first_interview"]) for row in matured_30
    )
    later_round_count = sum(bool(row["later_interview_round"]) for row in rows)
    offer_count = sum(bool(row["offer_received"]) for row in rows)
    contact_search_count = sum(bool(row["contact_search_completed"]) for row in rows)
    human_path_count = sum(bool(row["verified_human_path"]) for row in rows)

    def conservative_count(count: int, complete: bool) -> int | None:
        return count if complete or count > 0 else None

    completeness_fields = (
        "campaign_id",
        "role_family",
        "confidence",
        "master_resume_id",
        "planned_resume_id",
        "actual_resume_version",
        "message_variant",
        "human_path_status",
        "employer_account",
    )
    filled = 0
    for row in rows:
        filled += sum(
            1 for field in completeness_fields if row.get(field) not in {None, "", "unknown"}
        )
        filled += int(row["hard_gate_results_known"])
        filled += int(row["unresolved_questions_known"])
    possible = confirmed * (len(completeness_fields) + 2)

    human_replies = conservative_count(human_replies_count, reply_complete)
    screening_requests = conservative_count(screening_request_count, all_complete)
    completed_first = conservative_count(completed_first_count, interview_complete)
    invitations = conservative_count(invitation_count, all_complete)
    scheduled = conservative_count(scheduled_count, all_complete)
    later_rounds = conservative_count(later_round_count, all_complete)
    offers = conservative_count(offer_count, all_complete)
    contact_searches = conservative_count(contact_search_count, all_complete)
    verified_paths = conservative_count(human_path_count, all_complete)
    return {
        "confirmed_applications": confirmed,
        "matured_for_human_reply_14d": len(matured_14),
        "recorded_inbound_human_replies": human_replies,
        "screening_requests": screening_requests,
        "first_human_reply_sample": len(reply_times),
        "median_time_to_first_human_reply_days": (
            round(statistics.median(reply_times), 2) if reply_times else None
        ),
        "average_time_to_first_human_reply_days": (
            round(statistics.mean(reply_times), 2) if reply_times else None
        ),
        "human_reply_rate_14d": rate_payload(
            human_replies,
            len(matured_14),
            available=reply_complete,
        ),
        "matured_for_interview_outcomes_30d": len(matured_30),
        "interview_invitations": invitations,
        "scheduled_interviews": scheduled,
        "completed_first_interviews": completed_first,
        "completed_first_interview_rate_30d": rate_payload(
            completed_first,
            len(matured_30),
            available=interview_complete,
        ),
        "later_interview_rounds": later_rounds,
        "offers": offers,
        "contact_searches_completed": contact_searches,
        "contact_search_coverage": rate_payload(
            contact_searches,
            confirmed,
            available=all_complete,
        ),
        "verified_human_paths": verified_paths,
        "verified_human_path_coverage": rate_payload(
            verified_paths,
            confirmed,
            available=all_complete,
        ),
        "field_values_present": filled,
        "field_values_expected": possible,
        "field_completeness": rate_payload(filled, possible, available=True),
        "history_complete": all_complete,
        "small_sample": 0 < confirmed < 10,
    }


def build_outcome_scorecard_data(
    conn: sqlite3.Connection, as_of: dt.date
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cohorts, invalid_application_dates = first_confirmed_application_cohorts(
        conn, as_of
    )
    cohort_by_vacancy = {row["vacancy_id"]: row for row in cohorts}
    end_at = dt.datetime.combine(as_of + dt.timedelta(days=1), dt.time())

    for interaction in conn.execute(
        """
        SELECT vacancy_id, event_at, direction, event_type, is_human
        FROM effective_employer_interactions
        ORDER BY event_at, id
        """
    ).fetchall():
        cohort = cohort_by_vacancy.get(int(interaction["vacancy_id"]))
        if not cohort:
            continue
        try:
            event_at = parse_iso_datetime(interaction["event_at"], label="event_at")
        except ValueError:
            continue
        if not (cohort["application_at"] <= event_at < end_at):
            continue
        if interaction["event_type"] == "automated_ack" or not int(interaction["is_human"]):
            cohort["automated_acknowledgments"] += 1
        elif interaction["direction"] == "inbound" and cohort["first_human_reply_at"] is None:
            cohort["first_human_reply_at"] = event_at.isoformat()
            if interaction["event_type"] == "screening_request":
                cohort["screening_request"] = True
        elif (
            interaction["direction"] == "inbound"
            and interaction["event_type"] == "screening_request"
        ):
            cohort["screening_request"] = True

    for event in conn.execute(
        """
        SELECT vacancy_id, event_type, event_at, round_no
        FROM lifecycle_events
        WHERE event_type != 'application_confirmed'
        ORDER BY event_at, id
        """
    ).fetchall():
        cohort = cohort_by_vacancy.get(int(event["vacancy_id"]))
        if not cohort:
            continue
        try:
            event_at = parse_iso_datetime(event["event_at"], label="event_at")
        except ValueError:
            continue
        if not (cohort["application_at"] <= event_at < end_at):
            continue
        event_type = str(event["event_type"])
        round_no = int(event["round_no"] or 0)
        if round_no > 1 and event_type in {
            "interview_invited",
            "interview_scheduled",
            "interview_completed",
        }:
            cohort["later_interview_round"] = True
        if event_type == "interview_invited":
            cohort["interview_invited"] = True
        elif event_type == "interview_scheduled":
            cohort["interview_scheduled"] = True
        elif event_type == "interview_completed":
            if round_no == 1:
                cohort["completed_first_interview"] = True
        elif event_type == "offer_received":
            cohort["offer_received"] = True
        elif event_type == "interview_cancelled":
            cohort["interview_cancelled"] = True
        elif event_type == "interview_no_show_candidate":
            cohort["candidate_no_show"] = True
        elif event_type == "interview_no_show_employer":
            cohort["employer_no_show"] = True

    for row in conn.execute(
        "SELECT vacancy_id, search_date FROM contact_searches ORDER BY search_date, id"
    ).fetchall():
        cohort = cohort_by_vacancy.get(int(row["vacancy_id"]))
        if not cohort:
            continue
        try:
            searched_at = parse_iso_datetime(row["search_date"], label="search_date")
        except ValueError:
            continue
        if cohort["application_at"].date() <= searched_at.date() <= as_of:
            cohort["contact_search_completed"] = True

    for row in conn.execute(
        """
        SELECT vacancy_id, verified_date FROM employer_contacts
        WHERE is_active = 1 AND confidence IN ('confirmed', 'strong')
        """
    ).fetchall():
        cohort = cohort_by_vacancy.get(int(row["vacancy_id"]))
        if not cohort:
            continue
        verified_date = clean_cell(row["verified_date"])
        if not verified_date or verified_date[:10] <= as_of.isoformat():
            cohort["verified_human_path"] = True

    overall = aggregate_outcome_group(cohorts)
    groupings = (
        "campaign_id",
        "role_family",
        "source_stream",
        "source_channel",
        "employer_account",
        "actual_resume_version",
        "message_variant",
        "application_month",
    )
    breakdowns: dict[str, list[dict[str, Any]]] = {}
    for grouping in groupings:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in cohorts:
            grouped[str(row.get(grouping) or "unknown")].append(row)
        breakdowns[grouping] = [
            {grouping: key, **aggregate_outcome_group(grouped[key])}
            for key in sorted(grouped, key=str.casefold)
        ]

    caveats = [
        "Единица анализа — одна каноническая вакансия и самое раннее подтверждённое событие отклика.",
        "Автоматические подтверждения отделены от ответов людей и не входят в показатель ответов.",
        "Приглашение или назначенное время не считаются завершённым интервью: требуется отдельное событие завершения или проверенное резюме интервью.",
        "Если исходная история неполна, отсутствие события показывается как «н/д», а не как ноль.",
        "Источник отклика используется только при явной ссылке; иначе применяется первое касание до отклика с развязкой по идентификатору записи.",
        "Небольшие выборки не используются как доказательство превосходства кампании или источника.",
    ]
    if invalid_application_dates:
        caveats.append(
            f"Исключено записей отклика с некорректной датой: {invalid_application_dates}."
        )
    return (
        {
            "as_of": as_of.isoformat(),
            "methodology": {
                "grain": "одна каноническая вакансия и самое раннее подтверждённое событие отклика",
                "human_reply_maturity_days": 14,
                "interview_outcome_maturity_days": 30,
                "source_attribution": "явно указанный источник отклика; иначе самое раннее касание до отклика с развязкой по source_hits.id",
                "completed_interview_evidence": "явное событие interview_completed или приложенное резюме интервью, прошедшее правило доказательности",
            },
            "overall": overall,
            "breakdowns": breakdowns,
            "caveats": caveats,
            "small_sample_warning": (
                "Выборка меньше 10 подтверждённых откликов; сравнения носят описательный характер."
                if overall["small_sample"]
                else ""
            ),
        },
        cohorts,
    )


def build_conversion_report_data(
    conn: sqlite3.Connection, as_of: dt.date
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Backward-compatible projection of the v6 outcome scorecard."""

    scorecard, cohorts = build_outcome_scorecard_data(conn, as_of)

    def project(item: dict[str, Any]) -> dict[str, Any]:
        reply = item["human_reply_rate_14d"]
        interview = item["completed_first_interview_rate_30d"]
        human_path = item["verified_human_path_coverage"]
        contact = item["contact_search_coverage"]
        return {
            "applications_unique": item["confirmed_applications"],
            "matured_applications_14d": item["matured_for_human_reply_14d"],
            "human_replies": item["recorded_inbound_human_replies"],
            "screening_requests": item["screening_requests"],
            "human_reply_rate_14d": reply["percent"],
            "matured_applications_30d": item["matured_for_interview_outcomes_30d"],
            "interview_1_ever": item["completed_first_interviews"],
            "interview_1_rate_30d": interview["percent"],
            "first_human_reply_sample": item["first_human_reply_sample"],
            "median_time_to_first_human_reply_days": item[
                "median_time_to_first_human_reply_days"
            ],
            "average_time_to_first_human_reply_days": item[
                "average_time_to_first_human_reply_days"
            ],
            "verified_contacts": item["verified_human_paths"],
            "verified_contact_coverage": human_path["percent"],
            "contact_search_completed": item["contact_searches_completed"],
            "contact_search_coverage": contact["percent"],
        }

    breakdowns: dict[str, list[dict[str, Any]]] = {}
    for grouping in ("source_channel", "source_stream", "application_month"):
        breakdowns[grouping] = [
            {grouping: row[grouping], **project(row)}
            for row in scorecard["breakdowns"][grouping]
        ]
    return (
        {
            "as_of": scorecard["as_of"],
            "interaction_history_available": scorecard["overall"]["history_complete"],
            "methodology": scorecard["methodology"],
            "overall": project(scorecard["overall"]),
            "breakdowns": breakdowns,
            "caveats": scorecard["caveats"],
        },
        cohorts,
    )


def build_source_quality_data(
    conn: sqlite3.Connection,
    conversion_report: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    screening: dict[str, dict[str, set[int]]] = defaultdict(
        lambda: {"seen": set(), "signals": set()}
    )
    raw_streams: dict[str, dict[str, Any]] = {}
    signal_statuses = {"POTENTIAL", "NEEDS_REVIEW", "SHORTLISTED", "APPLIED"}
    for row in conn.execute(
        """
        SELECT sh.id, sh.vacancy_id, sh.source_stream, sh.raw_status,
               labels.label_key, labels.mapping_kind
        FROM source_hits sh
        JOIN source_hit_labels labels ON labels.source_hit_id = sh.id
        ORDER BY sh.id, labels.label_key COLLATE NOCASE
        """
    ).fetchall():
        raw = row["source_stream"] if row["source_stream"] not in (None, "") else "unknown"
        canonical = clean_cell(row["label_key"]) or "unknown"
        mapped = row["mapping_kind"] == "configured_alias"
        vacancy_id = int(row["vacancy_id"])
        screening[canonical]["seen"].add(vacancy_id)
        if clean_cell(row["raw_status"]).upper() in signal_statuses:
            screening[canonical]["signals"].add(vacancy_id)
        diagnostic = raw_streams.setdefault(
            raw,
            {
                "raw_stream": raw,
                "canonical_stream": canonical,
                "canonical_labels": [],
                "mapped": mapped,
                "hit_ids": set(),
                "hits": 0,
            },
        )
        if canonical not in diagnostic["canonical_labels"]:
            diagnostic["canonical_labels"].append(canonical)
        diagnostic["mapped"] = diagnostic["mapped"] or mapped
        diagnostic["hit_ids"].add(int(row["id"]))
        diagnostic["hits"] = len(diagnostic["hit_ids"])

    downstream = {
        row["source_stream"]: row
        for row in conversion_report["breakdowns"]["source_stream"]
    }
    keys = sorted(set(screening) | set(downstream))
    result: list[dict[str, Any]] = []
    for key in keys:
        seen = len(screening[key]["seen"])
        signals = len(screening[key]["signals"])
        outcomes = downstream.get(key, {})
        result.append(
            {
                "canonical_source_stream": key,
                "seen": seen,
                "signals": signals,
                "signal_rate": round(signals / seen * 100, 1) if seen else None,
                "applied": outcomes.get("applications_unique", 0),
                "matured_for_reply": outcomes.get("matured_applications_14d", 0),
                "human_replies": outcomes.get("human_replies"),
                "human_reply_rate": outcomes.get("human_reply_rate_14d"),
                "matured_for_interview": outcomes.get("matured_applications_30d", 0),
                "interview_1": outcomes.get("interview_1_ever", 0),
                "interview_rate": outcomes.get("interview_1_rate_30d"),
            }
        )
    diagnostics = []
    for diagnostic in raw_streams.values():
        diagnostic = dict(diagnostic)
        diagnostic.pop("hit_ids", None)
        diagnostic["canonical_labels"].sort(key=str.casefold)
        diagnostic["canonical_stream"] = diagnostic["canonical_labels"][0]
        diagnostics.append(diagnostic)
    diagnostics.sort(key=lambda row: (row["canonical_stream"], row["raw_stream"]))
    return result, diagnostics


def durable_lifecycle_by_vacancy(
    conn: sqlite3.Connection,
) -> dict[int, dict[str, Any]]:
    events: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT id, vacancy_id, event_type, event_at, round_no,
               history_complete, evidence_note
        FROM lifecycle_events ORDER BY event_at, id
        """
    ).fetchall():
        events[int(row["vacancy_id"])].append(row)
    result: dict[int, dict[str, Any]] = {}
    for vacancy_id, items in events.items():
        types = {str(item["event_type"]) for item in items}
        if "rejected" in types:
            state = "rejected"
        elif "offer_received" in types:
            state = "offer"
        else:
            completed_rounds = [
                int(item["round_no"] or 0)
                for item in items
                if item["event_type"] == "interview_completed"
            ]
            if completed_rounds:
                state = f"interview_{min(max(completed_rounds), 3)}"
            elif "application_confirmed" in types:
                state = "applied"
            else:
                state = "seen"
        result[vacancy_id] = {
            "state": state,
            "known": True,
            "events": len(items),
            "history_complete": all(bool(item["history_complete"]) for item in items),
        }
    return result


def latest_actions_by_vacancy(
    conn: sqlite3.Connection,
) -> dict[int, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT current.*
        FROM action_events current
        WHERE current.id = (
            SELECT candidate.id FROM action_events candidate
            WHERE candidate.vacancy_id = current.vacancy_id
            ORDER BY candidate.event_at DESC, candidate.id DESC LIMIT 1
        )
        """
    ).fetchall()
    return {int(row["vacancy_id"]): dict(row) for row in rows}


def build_wip_queue_data(
    conn: sqlite3.Connection,
    *,
    as_of: dt.date,
    page: int = 1,
    page_size: int | None = None,
    bucket_filter: str = "",
) -> dict[str, Any]:
    page_size = page_size or WIP_PAGE_SIZE
    if page < 1:
        raise ValueError("Номер страницы должен быть не меньше 1.")
    if page_size < 1 or page_size > 500:
        raise ValueError("Размер страницы должен быть от 1 до 500.")
    if bucket_filter and bucket_filter not in WIP_BUCKET_KEYS:
        raise ValueError("Неподдерживаемый фильтр группы незавершённой работы.")

    bucket_config = {bucket.key: bucket for bucket in WIP_BUCKETS}
    bucket_order = {bucket.key: index for index, bucket in enumerate(WIP_BUCKETS)}
    latest = latest_actions_by_vacancy(conn)
    vacancies = {
        int(row["id"]): row
        for row in conn.execute(
            """
            SELECT id, channel, company, title, url, score, first_seen_date,
                   last_seen_date, updated_at
            FROM vacancies
            """
        ).fetchall()
    }
    lifecycle = durable_lifecycle_by_vacancy(conn)
    items: list[dict[str, Any]] = []
    for vacancy_id, action in latest.items():
        vacancy = vacancies.get(vacancy_id)
        if vacancy is None:
            continue
        bucket_key = clean_cell(action.get("bucket")) or "backlog"
        if bucket_filter and bucket_key != bucket_filter:
            continue
        bucket = bucket_config[bucket_key]
        try:
            action_at = parse_iso_datetime(action["event_at"], label="event_at")
        except ValueError:
            action_at = dt.datetime.combine(as_of, dt.time())
        age_days = max((as_of - action_at.date()).days, 0)
        explicit_due = clean_cell(action.get("due_date"))
        if explicit_due:
            try:
                due = parse_iso_date(explicit_due, label="due_date")
            except ValueError:
                due = action_at.date() + dt.timedelta(days=bucket.sla_days)
        else:
            due = action_at.date() + dt.timedelta(days=bucket.sla_days)
        overdue_days = max((as_of - due).days, 0)
        state = lifecycle.get(vacancy_id, {}).get("state", "seen")
        terminal = state in {"rejected", "offer"}
        items.append(
            {
                "vacancy_id": vacancy_id,
                "action_event_id": int(action["id"]),
                "action_state": action["action_state"],
                "bucket": bucket_key,
                "bucket_label": bucket.label,
                "company": vacancy["company"] or "",
                "title": vacancy["title"] or "",
                "url": vacancy["url"] or "",
                "channel": vacancy["channel"] or "",
                "score": vacancy["score"],
                "priority": int(action.get("priority") or 0),
                "priority_reason": action.get("reason") or "",
                "action_at": action_at.isoformat(),
                "age_days": age_days,
                "due_date": due.isoformat(),
                "overdue": as_of > due,
                "overdue_days": overdue_days,
                "sla_days": bucket.sla_days,
                "terminal_lifecycle": terminal,
                "active_bucket": bucket.active,
            }
        )

    items.sort(
        key=lambda item: (
            bucket_order[item["bucket"]],
            0 if item["overdue"] else 1,
            -item["overdue_days"],
            -item["priority"],
            item["due_date"],
            item["action_at"],
            item["vacancy_id"],
        )
    )
    per_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        per_bucket[item["bucket"]].append(item)
    bucket_summaries: list[dict[str, Any]] = []
    for bucket in WIP_BUCKETS:
        bucket_items = per_bucket.get(bucket.key, [])
        for index, item in enumerate(bucket_items, start=1):
            item["bucket_rank"] = index
            item["in_active_wip"] = bool(
                bucket.active and bucket.limit > 0 and index <= bucket.limit
            )
            item["wip_overflow"] = bool(
                bucket.active and (bucket.limit == 0 or index > bucket.limit)
            )
        bucket_summaries.append(
            {
                "bucket": bucket.key,
                "label": bucket.label,
                "total": len(bucket_items),
                "limit": bucket.limit,
                "active": min(len(bucket_items), bucket.limit) if bucket.active else 0,
                "overflow": max(len(bucket_items) - bucket.limit, 0)
                if bucket.active
                else 0,
                "overdue": sum(bool(item["overdue"]) for item in bucket_items),
                "sla_days": bucket.sla_days,
                "is_active_bucket": bucket.active,
            }
        )

    total = len(items)
    pages = max((total + page_size - 1) // page_size, 1)
    offset = (page - 1) * page_size
    page_items = items[offset : offset + page_size] if page <= pages else []
    return {
        "as_of": as_of.isoformat(),
        "items": page_items,
        "buckets": bucket_summaries,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": total,
            "total_pages": pages,
            "has_previous": page > 1,
            "has_next": page < pages,
        },
        "overflow_total": sum(item["overflow"] for item in bucket_summaries),
        "overdue_total": sum(item["overdue"] for item in bucket_summaries),
        "active_wip_total": sum(item["active"] for item in bucket_summaries),
        "ordering": [
            "bucket_order",
            "overdue_first",
            "overdue_days_desc",
            "priority_desc",
            "due_date",
            "action_at",
            "vacancy_id",
        ],
    }


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
    lifecycle_states = durable_lifecycle_by_vacancy(conn)
    current_actions = latest_actions_by_vacancy(conn)
    for vacancy in vacancies:
        vacancy_id = int(vacancy["id"])
        legacy_stage = canonical_stage(vacancy.get("latest_stage"))
        lifecycle = lifecycle_states.get(vacancy_id)
        vacancy["legacy_stage"] = legacy_stage
        vacancy["durable_lifecycle_known"] = bool(lifecycle)
        vacancy["durable_lifecycle_state"] = (
            lifecycle["state"] if lifecycle else "seen"
        )
        vacancy["latest_stage"] = vacancy["durable_lifecycle_state"]
        action = current_actions.get(vacancy_id, {})
        vacancy["current_action_state"] = action.get("action_state") or "none"
        vacancy["action_bucket"] = action.get("bucket") or "backlog"
        vacancy["action_due_date"] = action.get("due_date") or ""
        vacancy["action_priority"] = int(action.get("priority") or 0)
        vacancy["action_reason"] = action.get("reason") or ""

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
            FROM lifecycle_events
            WHERE event_type = 'application_confirmed'
            """
        ).fetchall()
    }

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
        action_state = vacancy.get("current_action_state") or "none"
        if action_state in {"needs_input", "follow_up", "employer_reply", "review"}:
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
        and (
            v["score"] is None
            or int(v["score"]) >= 65
            or v["current_action_state"] in {"needs_input", "follow_up", "employer_reply"}
        )
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
            FROM effective_applications a
            JOIN vacancies v ON v.id = a.vacancy_id
            WHERE COALESCE(a.follow_up_date, '') NOT IN ('', '-', '—')
              AND LOWER(a.status) NOT LIKE '%reject%'
              AND EXISTS (
                  SELECT 1 FROM lifecycle_events le
                  WHERE le.vacancy_id = v.id
                    AND le.event_type = 'application_confirmed'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM lifecycle_events le
                  WHERE le.vacancy_id = v.id
                    AND le.event_type = 'rejected'
              )
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
        if v["current_action_state"] == "needs_input"
    ]
    needs_action = [
        v
        for v in vacancies
        if v["current_action_state"] in {
            "needs_input",
            "follow_up",
            "employer_reply",
            "review",
            "account_research",
        }
    ]

    outcome_scorecard, _ = build_outcome_scorecard_data(conn, dt.date.today())
    conversion_report, _ = build_conversion_report_data(conn, dt.date.today())
    wip_queue = build_wip_queue_data(
        conn, as_of=dt.date.today(), page=1, page_size=WIP_PAGE_SIZE
    )
    source_quality, source_stream_diagnostics = build_source_quality_data(
        conn, conversion_report
    )

    vacancy_factors = rows_to_dicts(
        conn.execute(
            """
            SELECT f.*, v.company, v.title, v.url
            FROM vacancy_factors f
            JOIN vacancies v ON v.id = f.vacancy_id
            ORDER BY f.observed_date DESC, f.id DESC
            """
        ).fetchall()
    )
    factors_by_vacancy: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for factor in vacancy_factors:
        factors_by_vacancy[int(factor["vacancy_id"])].append(factor)
    for vacancy in vacancies:
        vacancy["factors"] = factors_by_vacancy.get(int(vacancy["id"]), [])

    account_signals = rows_to_dicts(
        conn.execute(
            """
            SELECT *
            FROM employer_account_signals
            ORDER BY observed_date DESC, id DESC
            """
        ).fetchall()
    )
    signals_by_account: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for signal in account_signals:
        signals_by_account[int(signal["account_id"])].append(signal)
    employer_accounts = rows_to_dicts(
        conn.execute(
            """
            SELECT a.*,
                   COUNT(l.vacancy_id) AS linked_vacancies
            FROM employer_accounts a
            LEFT JOIN vacancy_employer_accounts l ON l.account_id = a.id
            GROUP BY a.id
            ORDER BY COALESCE(a.priority, ''), a.canonical_name
            """
        ).fetchall()
    )
    linked_vacancies_by_account: dict[int, list[int]] = defaultdict(list)
    for link in conn.execute(
        "SELECT account_id, vacancy_id FROM vacancy_employer_accounts"
    ).fetchall():
        linked_vacancies_by_account[int(link["account_id"])].append(
            int(link["vacancy_id"])
        )
    for account in employer_accounts:
        signals = signals_by_account.get(int(account["id"]), [])
        account["latest_signal"] = signals[0] if signals else None
        account["signals"] = signals
        account["active_target_vacancies"] = sum(
            lifecycle_states.get(vacancy_id, {}).get("state", "seen") != "rejected"
            for vacancy_id in linked_vacancies_by_account.get(int(account["id"]), [])
        )
        for json_field, output_field in (
            ("target_campaigns_json", "target_campaigns"),
            ("target_role_families_json", "target_role_families"),
        ):
            try:
                account[output_field] = json.loads(account.get(json_field) or "[]")
            except json.JSONDecodeError:
                account[output_field] = []
        limit = int(account.get("portfolio_limit") or ACCOUNT_ACTIVE_PORTFOLIO_LIMIT)
        account["portfolio_limit_effective"] = limit
        account["portfolio_overflow"] = max(
            int(account.get("active_target_vacancies") or 0) - limit, 0
        )
        account["review_overdue"] = bool(
            account.get("next_review_date")
            and account["next_review_date"] < dt.date.today().isoformat()
        )

    issue_count = conn.execute("SELECT COUNT(*) AS n FROM import_issues").fetchone()["n"]
    quarantine_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM quarantine_records WHERE status = 'pending'"
        ).fetchone()[0]
    )
    conversion_overall = conversion_report["overall"]
    outcome_overall = outcome_scorecard["overall"]
    current_interview_stage_count = sum(
        1
        for vacancy in vacancies
        if vacancy["latest_stage"] in {"interview_1", "interview_2", "interview_3"}
    )

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
            "applications_unique": conversion_overall["applications_unique"],
            "matured_applications_14d": conversion_overall[
                "matured_applications_14d"
            ],
            "human_replies": conversion_overall["human_replies"],
            "screening_requests": conversion_overall["screening_requests"],
            "human_reply_rate_14d": conversion_overall["human_reply_rate_14d"],
            "interview_1_rate_30d": conversion_overall[
                "interview_1_rate_30d"
            ],
            "verified_contact_coverage": conversion_overall[
                "verified_contact_coverage"
            ],
            "employer_accounts": len(employer_accounts),
            "employer_account_signals": len(account_signals),
            "active_account_targets": sum(
                int(account.get("active_target_vacancies") or 0)
                for account in employer_accounts
            ),
            "active_account_portfolio": sum(
                1
                for account in employer_accounts
                if clean_cell(account.get("status")).lower() in {"target", "active"}
            ),
            "account_portfolio_limit": ACCOUNT_ACTIVE_PORTFOLIO_LIMIT,
            "account_portfolio_overflow": max(
                sum(
                    1
                    for account in employer_accounts
                    if clean_cell(account.get("status")).lower() in {"target", "active"}
                )
                - ACCOUNT_ACTIVE_PORTFOLIO_LIMIT,
                0,
            ),
            "interviews": outcome_overall["completed_first_interviews"],
            "interview_invitations": outcome_overall["interview_invitations"],
            "scheduled_interviews": outcome_overall["scheduled_interviews"],
            "completed_first_interviews": outcome_overall[
                "completed_first_interviews"
            ],
            "later_interview_rounds": outcome_overall["later_interview_rounds"],
            "interview_current_stage": current_interview_stage_count,
            "offers": outcome_overall["offers"],
            "rejected": sum(
                1
                for state in lifecycle_states.values()
                if state["state"] == "rejected"
            ),
            "import_issues": issue_count,
            "quarantine_pending": quarantine_count,
            "wip_active": wip_queue["active_wip_total"],
            "wip_overflow": wip_queue["overflow_total"],
            "sla_overdue": wip_queue["overdue_total"],
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
        "best_sources": source_quality,
        "source_quality": source_quality,
        "source_stream_diagnostics": source_stream_diagnostics,
        "conversion_report": conversion_report,
        "outcome_scorecard": outcome_scorecard,
        "wip_queue": wip_queue,
        "employer_accounts": employer_accounts,
        "account_signals": account_signals,
        "vacancy_factors": vacancy_factors,
        "interview_summaries": interview_summaries,
        "employer_contacts": employer_contacts,
        "vacancies": vacancies,
    }


def md_escape(value: Any) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("|", "\\|")
        .replace("\n", " ")
        .strip()
    )


def safe_external_url(value: str) -> str:
    url = clean_cell(value)
    if not url:
        return ""
    parts = urlsplit(url)
    if parts.scheme.lower() not in {"http", "https"} or not parts.netloc:
        return ""
    return url


def md_link(title: str, url: str) -> str:
    title = md_escape(title or "ссылка")
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


def scorecard_rate_label(metric: dict[str, Any]) -> str:
    if not metric.get("available") or metric.get("numerator") is None:
        return "n/a"
    percent_value = metric.get("percent")
    suffix = "n/a" if percent_value is None else f"{float(percent_value):.1f}%"
    return f"{metric['numerator']}/{metric['denominator']} ({suffix})"


def report_group_value(grouping: str, value: Any) -> Any:
    """Translate controlled presentation values without changing JSON identifiers."""

    if value == "unknown":
        return "неизвестно"
    if grouping == "source_channel":
        return CHANNEL_LABELS.get(str(value), value)
    return value


def render_outcome_scorecard_markdown(scorecard: dict[str, Any]) -> str:
    overall = scorecard["overall"]
    lines = [
        "# Карта исходов поиска работы",
        "",
        f"Состояние на {md_escape(scorecard['as_of'])}.",
        "",
    ]
    if scorecard.get("small_sample_warning"):
        lines.extend(
            [
                f"> Предупреждение: {md_escape(scorecard['small_sample_warning'])}",
                "",
            ]
        )
    lines.extend(
        [
            "## Общие результаты",
            "",
            "| Показатель | Значение |",
            "|---|---|",
            f"| Уникальные подтверждённые отклики | {overall['confirmed_applications']} |",
            f"| Созревшие к оценке ответа человека (14 дней) | {overall['matured_for_human_reply_14d']} |",
            "| Ответы людей | "
            + (
                str(overall["recorded_inbound_human_replies"])
                if overall["recorded_inbound_human_replies"] is not None
                else "n/a"
            )
            + " |",
            "| Запросы на скрининг | "
            + (
                str(overall["screening_requests"])
                if overall["screening_requests"] is not None
                else "n/a"
            )
            + " |",
            f"| Доля ответов людей | {scorecard_rate_label(overall['human_reply_rate_14d'])} |",
            f"| Созревшие к оценке интервью (30 дней) | {overall['matured_for_interview_outcomes_30d']} |",
            "| Приглашения на интервью | "
            + (str(overall["interview_invitations"]) if overall["interview_invitations"] is not None else "n/a")
            + " |",
            "| Назначенные интервью | "
            + (str(overall["scheduled_interviews"]) if overall["scheduled_interviews"] is not None else "n/a")
            + " |",
            "| Завершённые первые интервью | "
            + (str(overall["completed_first_interviews"]) if overall["completed_first_interviews"] is not None else "n/a")
            + " |",
            f"| Доля завершённых первых интервью | {scorecard_rate_label(overall['completed_first_interview_rate_30d'])} |",
            "| Более поздние раунды интервью | "
            + (str(overall["later_interview_rounds"]) if overall["later_interview_rounds"] is not None else "n/a")
            + " |",
            "| Предложения | "
            + (str(overall["offers"]) if overall["offers"] is not None else "n/a")
            + " |",
            f"| Покрытие поиска контактов | {scorecard_rate_label(overall['contact_search_coverage'])} |",
            f"| Покрытие проверенного пути к человеку | {scorecard_rate_label(overall['verified_human_path_coverage'])} |",
            f"| Полнота полей | {scorecard_rate_label(overall['field_completeness'])} |",
            "",
        ]
    )
    grouping_labels = {
        "campaign_id": "Кампания",
        "role_family": "Семейство ролей",
        "source_stream": "Поток источника",
        "source_channel": "Канал источника",
        "employer_account": "Аккаунт работодателя",
        "actual_resume_version": "Фактически отправленное резюме",
        "message_variant": "Вариант сообщения",
        "application_month": "Месяц отклика",
    }
    for grouping, title in grouping_labels.items():
        rows = scorecard["breakdowns"].get(grouping, [])
        if not rows:
            continue
        lines.extend(
            [
                f"## {title}",
                "",
                "| Значение | Отклики | Ответы за 14 дней | Завершённые первые интервью за 30 дней | Предложения | Поиск контактов | Путь к человеку | Полнота полей |",
                "|---|---|---|---|---|---|---|---|",
            ]
        )
        for row in rows:
            grouping_value = report_group_value(grouping, row[grouping])
            lines.append(
                "| "
                + " | ".join(
                    [
                        md_escape(grouping_value),
                        str(row["confirmed_applications"]),
                        scorecard_rate_label(row["human_reply_rate_14d"]),
                        scorecard_rate_label(row["completed_first_interview_rate_30d"]),
                        str(row["offers"]) if row["offers"] is not None else "n/a",
                        scorecard_rate_label(row["contact_search_coverage"]),
                        scorecard_rate_label(row["verified_human_path_coverage"]),
                        scorecard_rate_label(row["field_completeness"]),
                    ]
                )
                + " |"
            )
        lines.append("")
    lines.extend(["## Методика и ограничения", ""])
    lines.extend(f"- {md_escape(item)}" for item in scorecard["caveats"])
    lines.append("")
    return "\n".join(lines)


def render_false_negative_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Аудит пропущенных возможностей",
        "",
        f"Активная политика: `{md_escape(report['policy_version'])}` от {md_escape(report['policy_effective_date'])}.",
        f"Детерминированная выборка: {report['sample_size']} из {report['population_size']}; ключ воспроизводимости: `{md_escape(report['seed'])}`.",
        "",
        "## Частота срабатывания правил",
        "",
        "| Правило | Срабатывания | Доля выборки | Требует проверки |",
        "|---|---|---|---|",
    ]
    for rule in report["rule_counts"]:
        denominator = int(rule["sample_size"])
        rate = f"{rule['count']}/{denominator}"
        lines.append(
            f"| {md_escape(rule['rule_key'])} | {rule['count']} | {rate} | "
            f"{'да' if rule['requires_review'] else 'нет'} |"
        )
    if not report["rule_counts"]:
        lines.append("| n/a | n/a | n/a | нет данных |")
    lines.extend(
        [
            "",
            "## Выборка",
            "",
            "| ID | Решение | Балл | Работодатель | Вакансия | Сработавшие правила | Более позднее положительное доказательство |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for item in report["sample"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item["vacancy_id"]),
                    md_escape(item["decision"]),
                    str(item["score"] if item["score"] is not None else "n/a"),
                    md_escape(item["company"]),
                    md_escape(item["title"]),
                    md_escape(", ".join(item["matched_rules"])),
                    "да" if item["later_positive_evidence"] else "нет",
                ]
            )
            + " |"
        )
    lines.extend(["", f"Методика: {md_escape(report['methodology'])}", ""])
    return "\n".join(lines)


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
        "Активная очередь проверки",
        ["ID", "дата", "канал", "балл", "жизненный цикл", "работодатель", "вакансия", "обоснование", "вопросы или действие"],
        active_rows,
        f"Сформировано: {snapshot['generated_at']}. Сортировка по баллу и свежести.",
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
        f"Свежие вакансии ({report_date})",
        ["дата", "канал", "балл", "жизненный цикл", "работодатель", "вакансия", "примечание"],
        today_rows,
        "Самая свежая дата импорта в текущем наборе данных.",
    )

    funnel_rows = []
    for channel, stages in sorted(snapshot["funnel_by_channel"].items()):
        row = [CHANNEL_LABELS.get(channel, channel)]
        row.extend(stages.get(stage, 0) for stage in FUNNEL_STAGES)
        funnel_rows.append(row)
    write_markdown_table(
        VIEWS_DIR / "funnel.md",
        "Воронка по каналам",
        ["канал"] + [STAGE_LABELS[s] for s in FUNNEL_STAGES],
        funnel_rows,
        f"Сформировано: {snapshot['generated_at']}. Первый столбец — все уникальные вакансии канала; остальные столбцы отражают доказанный жизненный цикл.",
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
                        CONTACT_SEARCH_STATUS_LABELS.get(
                            item.get("contact_search_status") or "",
                            item.get("contact_search_status") or "",
                        ),
                        item.get("contact_search_note") or "",
                    )
                    if part
                ),
                item.get("why_applied") or item.get("risks") or "",
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "followups.md",
        "Повторные обращения",
        [
            "дата",
            "канал источника",
            "балл",
            "работодатель",
            "вакансия",
            "состояние",
            "прямой контакт",
            "каналы контакта",
            "последнее обращение",
            "поиск контакта",
            "примечание",
        ],
        followup_rows,
        "Наступившие сроки повторных обращений с проверкой прямых контактов и историей каналов.",
    )

    contact_rows = []
    for contact in snapshot["employer_contacts"]:
        address = contact.get("contact_address") or ""
        profile_url = contact.get("profile_url") or ""
        evidence = contact.get("evidence_note") or ""
        if contact.get("evidence_url"):
            evidence = md_link(evidence or "источник", contact["evidence_url"])
        contact_rows.append(
            [
                contact.get("vacancy_id") or "",
                contact.get("company") or "",
                md_link(contact.get("title") or "", contact.get("url") or ""),
                contact.get("person_name") or "",
                contact.get("person_role") or "",
                CONTACT_RELATIONSHIP_LABELS.get(
                    contact.get("relationship") or "",
                    contact.get("relationship") or "",
                ),
                CONTACT_CONFIDENCE_LABELS.get(
                    contact.get("confidence") or "",
                    contact.get("confidence") or "",
                ),
                contact.get("channel") or "",
                md_link(address, profile_url) if profile_url else address,
                contact.get("verified_date") or "",
                "активен" if int(contact.get("is_active") or 0) else "неактивен",
                evidence,
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "outreach_contacts.md",
        "Контакты работодателей",
        [
            "ID вакансии",
            "работодатель",
            "вакансия",
            "человек",
            "должность",
            "связь с вакансией",
            "уверенность",
            "канал",
            "контакт",
            "дата проверки",
            "состояние",
            "доказательство",
        ],
        contact_rows,
        "Проверенные контакты рекрутеров и нанимающих руководителей. Наличие контакта не разрешает отправку сообщения.",
    )

    def pct_label(value: Any) -> str:
        return "n/a" if value is None else f"{float(value):.1f}%"

    def ratio_label(numerator: Any, denominator: Any, rate: Any) -> str:
        if numerator is None or rate is None:
            return "n/a"
        return f"{int(numerator)}/{int(denominator or 0)} ({pct_label(rate)})"

    sources_rows = []
    for item in snapshot["source_quality"]:
        sources_rows.append(
            [
                item.get("canonical_source_stream") or "неизвестно",
                item.get("seen") or 0,
                item.get("signals") or 0,
                pct_label(item.get("signal_rate")),
                item.get("applied") or 0,
                item.get("matured_for_reply") or 0,
                item.get("human_replies")
                if item.get("human_replies") is not None
                else "n/a",
                ratio_label(
                    item.get("human_replies"),
                    item.get("matured_for_reply"),
                    item.get("human_reply_rate"),
                ),
                item.get("matured_for_interview") or 0,
                item.get("interview_1") or 0,
                ratio_label(
                    item.get("interview_1"),
                    item.get("matured_for_interview"),
                    item.get("interview_rate"),
                ),
            ]
        )
    write_markdown_table(
        REPORTS_DIR / "source_quality.md",
        "Качество источников",
        [
            "канонический поток",
            "увидено",
            "сигналы",
            "доля сигналов",
            "отклики",
            "созрело для ответа, 14 дней",
            "ответы людей",
            "доля ответов",
            "созрело для интервью, 30 дней",
            "первые интервью завершены",
            "доля интервью",
        ],
        sources_rows,
        (
            "Просмотры и сигналы считаются по уникальным парам «вакансия — поток». "
            "Исходы относятся к первому касанию до отклика, если источник отклика не "
            "задан явно. Для каждой доли показаны числитель и знаменатель; небольшая "
            "выборка не доказывает превосходство потока."
        ),
    )

    stream_rows = [
        [
            item.get("raw_stream") or "неизвестно",
            ", ".join(item.get("canonical_labels") or ["неизвестно"]),
            "настроенный псевдоним" if item.get("mapped") else "тождественное сопоставление",
            item.get("hits") or 0,
        ]
        for item in snapshot["source_stream_diagnostics"]
    ]
    write_markdown_table(
        REPORTS_DIR / "source_streams.md",
        "Нормализация потоков источников",
        ["сырой поток", "канонические метки", "способ сопоставления", "попадания"],
        stream_rows,
        (
            "Сырые значения без изменений остаются в source_hits.source_stream. "
            "Текущие локальные псевдонимы применяются при пересборке; произвольные "
            "символы «плюс» и другие разделители автоматически не разбираются."
        ),
    )

    conversion = snapshot["conversion_report"]
    overall = conversion["overall"]
    conversion_lines = [
        "# Когорты исходов откликов",
        "",
        f"Состояние на {md_escape(conversion['as_of'])}.",
        "",
        "## Методика",
        "",
    ]
    conversion_lines.extend(f"- {md_escape(item)}" for item in conversion["caveats"])
    conversion_lines.extend(
        [
            "",
            "## Общий результат",
            "",
            "| показатель | значение |",
            "|---|---|",
            f"| уникальные подтверждённые отклики | {overall['applications_unique']} |",
            f"| когорты, созревшие для ответа за 14 дней | {overall['matured_applications_14d']} |",
            "| зафиксированные ответы людей | "
            + (
                "n/a |"
                if overall["human_replies"] is None
                else f"{overall['human_replies']} |"
            ),
            f"| запросы на скрининг | {overall['screening_requests'] if overall['screening_requests'] is not None else 'n/a'} |",
            "| доля ответов людей за 14 дней | "
            + ratio_label(
                overall["human_replies"],
                overall["matured_applications_14d"],
                overall["human_reply_rate_14d"],
            )
            + " |",
            f"| когорты, созревшие для исхода интервью за 30 дней | {overall['matured_applications_30d']} |",
            f"| завершённые первые интервью | {overall['interview_1_ever'] if overall['interview_1_ever'] is not None else 'n/a'} |",
            "| доля завершённых первых интервью за 30 дней | "
            + ratio_label(
                overall["interview_1_ever"],
                overall["matured_applications_30d"],
                overall["interview_1_rate_30d"],
            )
            + " |",
            "| медианное время до первого ответа человека, дней | "
            + (
                "n/a"
                if overall["median_time_to_first_human_reply_days"] is None
                else str(overall["median_time_to_first_human_reply_days"])
            )
            + " |",
            "| среднее время до первого ответа человека, дней | "
            + (
                "n/a"
                if overall["average_time_to_first_human_reply_days"] is None
                else str(overall["average_time_to_first_human_reply_days"])
            )
            + " |",
            "| покрытие проверенного пути к человеку | "
            + ratio_label(
                overall["verified_contacts"],
                overall["applications_unique"],
                overall["verified_contact_coverage"],
            )
            + " |",
            "| покрытие поиска контактов | "
            + ratio_label(
                overall["contact_search_completed"],
                overall["applications_unique"],
                overall["contact_search_coverage"],
            )
            + " |",
            "",
        ]
    )
    for grouping, title in (
        ("source_channel", "Канал источника"),
        ("source_stream", "Канонический поток источника"),
        ("application_month", "Месяц отклика"),
    ):
        conversion_lines.extend(
            [
                f"## {title}",
                "",
                "| значение | отклики | ответы за 14 дней | интервью за 30 дней | путь к человеку | поиск контактов |",
                "|---|---|---|---|---|---|",
            ]
        )
        for row in conversion["breakdowns"][grouping]:
            grouping_value = report_group_value(grouping, row[grouping])
            conversion_lines.append(
                "| "
                + " | ".join(
                    [
                        md_escape(grouping_value),
                        str(row["applications_unique"]),
                        ratio_label(
                            row["human_replies"],
                            row["matured_applications_14d"],
                            row["human_reply_rate_14d"],
                        ),
                        ratio_label(
                            row["interview_1_ever"],
                            row["matured_applications_30d"],
                            row["interview_1_rate_30d"],
                        ),
                        ratio_label(
                            row["verified_contacts"],
                            row["applications_unique"],
                            row["verified_contact_coverage"],
                        ),
                        ratio_label(
                            row["contact_search_completed"],
                            row["applications_unique"],
                            row["contact_search_coverage"],
                        ),
                    ]
                )
                + " |"
            )
        conversion_lines.append("")
    (REPORTS_DIR / "conversion_cohorts.md").write_text(
        "\n".join(conversion_lines), encoding="utf-8"
    )

    account_rows = []
    for account in snapshot["employer_accounts"]:
        latest = account.get("latest_signal") or {}
        latest_signal = ""
        if latest:
            signal_label = " · ".join(
                part
                for part in (
                    latest.get("observed_date") or "",
                    EMPLOYER_SIGNAL_TYPE_LABELS.get(
                        latest.get("signal_type") or "",
                        latest.get("signal_type") or "",
                    ),
                    EVIDENCE_CONFIDENCE_LABELS.get(
                        latest.get("confidence") or "",
                        latest.get("confidence") or "",
                    ),
                    latest.get("evidence_note") or "",
                )
                if part
            )
            latest_signal = md_link(signal_label, latest.get("evidence_url") or "")
        account_rows.append(
            [
                account.get("id") or "",
                account.get("canonical_name") or "",
                ACCOUNT_PRIORITY_LABELS.get(
                    account.get("priority") or "",
                    account.get("priority") or "",
                ),
                ACCOUNT_STATUS_LABELS.get(
                    account.get("status") or "",
                    account.get("status") or "",
                ),
                md_link("сайт", account.get("website") or ""),
                md_link("вакансии", account.get("careers_url") or ""),
                account.get("country_market") or "",
                account.get("linked_vacancies") or 0,
                account.get("active_target_vacancies") or 0,
                account.get("portfolio_limit_effective") or ACCOUNT_ACTIVE_PORTFOLIO_LIMIT,
                account.get("portfolio_overflow") or 0,
                account.get("review_cadence_days") or "",
                account.get("next_review_date") or "",
                "да" if account.get("review_overdue") else "нет",
                account.get("website_checked_date") or "",
                account.get("careers_checked_date") or "",
                ", ".join(account.get("target_campaigns") or []),
                ", ".join(account.get("target_role_families") or []),
                HUMAN_PATH_LABELS.get(
                    account.get("human_path_status") or "unknown",
                    account.get("human_path_status") or "неизвестно",
                ),
                account.get("owner_evidence") or "",
                account.get("sponsor_evidence") or "",
                account.get("governance_evidence") or "",
                latest_signal,
                account.get("last_checked_date") or "",
                account.get("notes") or "",
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "employer_accounts.md",
        "Портфель целевых работодателей",
        [
            "ID аккаунта",
            "работодатель",
            "приоритет",
            "состояние",
            "сайт",
            "вакансии",
            "рынок",
            "связанные вакансии",
            "активные цели",
            "лимит портфеля",
            "сверх лимита",
            "каденс, дней",
            "следующая проверка",
            "проверка просрочена",
            "сайт проверен",
            "карьерный сайт проверен",
            "целевые кампании",
            "целевые семейства ролей",
            "путь к человеку",
            "доказательство владельца",
            "доказательство спонсора",
            "доказательство управления",
            "последний доказательный сигнал",
            "дата проверки",
            "примечания",
        ],
        account_rows,
        "Сигналы работодателя, соответствие вакансии кандидату и разрешение связаться с человеком — независимые понятия. Сигнал не меняет балл вакансии.",
    )

    factor_rows = []
    for factor in snapshot["vacancy_factors"]:
        evidence = md_link(
            factor.get("evidence_note") or "доказательство",
            factor.get("evidence_url") or "",
        )
        factor_rows.append(
            [
                factor.get("vacancy_id") or "",
                factor.get("company") or "",
                md_link(factor.get("title") or "", factor.get("url") or ""),
                factor.get("factor_key") or "",
                factor.get("factor_value") or "",
                factor.get("observed_date") or "",
                EVIDENCE_CONFIDENCE_LABELS.get(
                    factor.get("confidence") or "",
                    factor.get("confidence") or "",
                ),
                evidence,
            ]
        )
    write_markdown_table(
        VIEWS_DIR / "vacancy_factors.md",
        "Доказательные факторы вакансий",
        [
            "ID вакансии",
            "работодатель",
            "вакансия",
            "фактор",
            "значение",
            "дата наблюдения",
            "уверенность",
            "доказательство",
        ],
        factor_rows,
        "Факторы являются только доказательными записями и никогда автоматически не меняют балл соответствия кандидату.",
    )

    coverage_rows: list[list[Any]] = []
    coverage_intro = "Манифест покрытия поиска ещё не записан."
    with connect_db(db_path) as conn:
        latest_runs = conn.execute(
            """
            SELECT current.*
            FROM search_runs current
            WHERE current.id = (
                SELECT candidate.id
                FROM search_runs candidate
                WHERE candidate.source = current.source
                ORDER BY candidate.run_date DESC, candidate.id DESC
                LIMIT 1
            )
            ORDER BY current.source COLLATE NOCASE
            """
        ).fetchall()
        summaries: list[str] = []
        for latest_run in latest_runs:
            run_status_label = {
                "completed": "завершён",
                "incomplete": "неполный",
                "blocked": "заблокирован",
            }.get(latest_run["status"], latest_run["status"])
            summaries.append(
                f"{latest_run['run_date']} / {latest_run['source']}: "
                f"{run_status_label}; уникальные={latest_run['total_unique']}, "
                f"известные={latest_run['known_count']}, новые={latest_run['new_count']}, "
                f"проблемы={latest_run['issue_count']}"
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
                        latest_run["run_date"],
                        latest_run["source"],
                        item["stream_key"],
                        {
                            "completed": "завершён",
                            "incomplete": "неполный",
                            "blocked": "заблокирован",
                        }.get(item["status"], item["status"]),
                        md_link("запрос", item["query_url"] or ""),
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
        if len(summaries) == 1:
            coverage_intro = "Последний запуск: " + summaries[0] + "."
        elif summaries:
            coverage_intro = "Последние запуски по источникам: " + "; ".join(summaries) + "."
    write_markdown_table(
        REPORTS_DIR / "search_coverage.md",
        "Покрытие поиска",
        [
            "дата запуска",
            "источник",
            "поток",
            "состояние",
            "запрос",
            "найдено",
            "размер страницы",
            "страницы",
            "извлечено",
            "уникальные",
            "известные",
            "новые",
            "ошибки",
        ],
        coverage_rows,
        coverage_intro,
    )

    checkpoint_rows: list[list[Any]] = []
    with connect_db(db_path) as conn:
        checkpoints = conn.execute(
            """
            SELECT source, stream_key, cursor_value, cursor_date,
                   initialized_at, last_completed_run_date,
                   last_manifest_file, updated_at
            FROM source_checkpoints
            ORDER BY source COLLATE NOCASE, stream_key COLLATE NOCASE
            """
        ).fetchall()
        for item in checkpoints:
            checkpoint_rows.append(
                [
                    item["source"],
                    item["stream_key"],
                    item["cursor_value"] or "",
                    item["cursor_date"] or "",
                    item["initialized_at"],
                    item["last_completed_run_date"],
                    item["last_manifest_file"] or "",
                    item["updated_at"],
                ]
            )
    write_markdown_table(
        REPORTS_DIR / "source_checkpoints.md",
        "Контрольные точки инкрементальных источников",
        [
            "источник",
            "поток",
            "курсор",
            "дата курсора",
            "инициализация",
            "последний завершённый запуск",
            "манифест",
            "обновление",
        ],
        checkpoint_rows,
        (
            "Курсоры продвигаются только после полного манифеста источника. Пустая "
            "таблица означает, что ни один инкрементальный источник не завершил первый проход."
        ),
    )

    scorecard_markdown = render_outcome_scorecard_markdown(
        snapshot["outcome_scorecard"]
    )
    (REPORTS_DIR / "outcome_scorecard.md").write_text(
        scorecard_markdown, encoding="utf-8"
    )
    # Compatibility path: the legacy cohort report now projects the same
    # evidence-first methodology rather than a mutable application row.
    (REPORTS_DIR / "conversion_cohorts.md").write_text(
        scorecard_markdown, encoding="utf-8"
    )

    with connect_db(db_path) as conn:
        for stale in VIEWS_DIR.glob("wip_queue_page_*.md"):
            stale.unlink()
        for stale in REPORTS_DIR.glob("quarantine_page_*.md"):
            stale.unlink()
        queue_first = build_wip_queue_data(
            conn,
            as_of=dt.date.today(),
            page=1,
            page_size=WIP_PAGE_SIZE,
        )
        queue_pages = queue_first["pagination"]["total_pages"]
        for page_number in range(1, queue_pages + 1):
            page_data = (
                queue_first
                if page_number == 1
                else build_wip_queue_data(
                    conn,
                    as_of=dt.date.today(),
                    page=page_number,
                    page_size=WIP_PAGE_SIZE,
                )
            )
            queue_rows = [
                [
                    item["vacancy_id"],
                    item["bucket_label"],
                    ACTION_STATE_LABELS.get(item["action_state"], item["action_state"]),
                    item["priority"],
                    item["age_days"],
                    item["due_date"],
                    "да" if item["overdue"] else "нет",
                    "да" if item["wip_overflow"] else "нет",
                    item["company"],
                    md_link(item["title"], item["url"]),
                    item["priority_reason"],
                ]
                for item in page_data["items"]
            ]
            intro = (
                f"Состояние на {page_data['as_of']}. Страница {page_number} из "
                f"{queue_pages}; всего записей: {page_data['pagination']['total_items']}; "
                f"активный WIP: {page_data['active_wip_total']}; сверх лимита: "
                f"{page_data['overflow_total']}; просрочено по SLA: {page_data['overdue_total']}. "
                "Старые строки не архивируются и не изменяются автоматически."
            )
            page_path = VIEWS_DIR / f"wip_queue_page_{page_number:04d}.md"
            write_markdown_table(
                page_path,
                "Очередь WIP и SLA",
                [
                    "ID",
                    "корзина",
                    "действие",
                    "приоритет",
                    "возраст, дней",
                    "срок",
                    "просрочено",
                    "сверх WIP",
                    "работодатель",
                    "вакансия",
                    "причина приоритета",
                ],
                queue_rows,
                intro,
            )
            if page_number == 1:
                (VIEWS_DIR / "wip_queue.md").write_text(
                    page_path.read_text(encoding="utf-8"), encoding="utf-8"
                )

        quarantine_first = quarantine_report_data(
            conn,
            page=1,
            page_size=50,
            status="",
            classification="",
        )
        quarantine_pages = quarantine_first["pagination"]["total_pages"]
        for page_number in range(1, quarantine_pages + 1):
            page_data = (
                quarantine_first
                if page_number == 1
                else quarantine_report_data(
                    conn,
                    page=page_number,
                    page_size=50,
                    status="",
                    classification="",
                )
            )
            quarantine_rows = [
                [
                    item["id"],
                    QUARANTINE_LABELS.get(item["classification"], item["classification"]),
                    QUARANTINE_STATUS_LABELS.get(item["status"], item["status"]),
                    item["source_name"],
                    item["source_stream"],
                    item["origin_file"],
                    item["line_no"],
                    item["retry_count"],
                    item["evidence_note"],
                ]
                for item in page_data["items"]
            ]
            quarantine_path = REPORTS_DIR / f"quarantine_page_{page_number:04d}.md"
            write_markdown_table(
                quarantine_path,
                "Карантин импорта",
                [
                    "ID",
                    "классификация",
                    "состояние",
                    "источник",
                    "сырой поток",
                    "файл",
                    "строка",
                    "повторы",
                    "причина",
                ],
                quarantine_rows,
                (
                    f"Страница {page_number} из {quarantine_pages}; всего записей: "
                    f"{page_data['pagination']['total_items']}. Записи исключены из "
                    "показателей вакансий до явной повторной обработки."
                ),
            )
            if page_number == 1:
                (REPORTS_DIR / "quarantine.md").write_text(
                    quarantine_path.read_text(encoding="utf-8"), encoding="utf-8"
                )

        false_negative = build_false_negative_audit_data(
            conn,
            as_of=dt.date.today(),
            sample_size=25,
            seed="find-dream-job-v6",
        )
        (REPORTS_DIR / "false_negative_audit.md").write_text(
            render_false_negative_markdown(false_negative), encoding="utf-8"
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
        "Качество данных",
        ["файл", "строка", "проблема", "сырой фрагмент"],
        issue_rows,
        f"Проблем импорта: {snapshot['kpis']['import_issues']}; записей в карантине: {snapshot['kpis']['quarantine_pending']}.",
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
    dashboard_locale = "ru"
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
    <div class="sub">Сформировано <span id="generated"></span> из <span id="dbPath"></span></div>
  </header>
  <main>
    <div class="kpis" id="kpis"></div>
    <div id="conversionCaveat" class="note" style="margin: -4px 0 14px;"></div>
    <section>
      <div class="panel">
        <h2>Портфель целевых работодателей</h2>
        <div id="accountSummary"></div>
      </div>
    </section>
    <div class="tabs">
      <button data-tab="review" class="active">Проверка</button>
      <button data-tab="wip">WIP и SLA</button>
      <button data-tab="funnel">Воронка</button>
      <button data-tab="today">Свежие</button>
      <button data-tab="followups">Повторные обращения</button>
    </div>
    <div class="toolbar">
      <input id="search" placeholder="Поиск по работодателю, роли или причине">
      <div class="multi-filter" id="channelPicker">
        <button id="channelTrigger" class="multi-trigger" type="button" aria-expanded="false" aria-controls="channelMenu">
          <span id="channelSummary" class="multi-trigger-label">Все каналы</span>
          <span class="multi-trigger-mark">v</span>
        </button>
        <div id="channelMenu" class="multi-menu hidden"></div>
      </div>
      <div class="multi-filter" id="stagePicker">
        <button id="stageTrigger" class="multi-trigger" type="button" aria-expanded="false" aria-controls="stageMenu">
          <span id="stageSummary" class="multi-trigger-label">Все этапы</span>
          <span class="multi-trigger-mark">v</span>
        </button>
        <div id="stageMenu" class="multi-menu hidden"></div>
      </div>
      <button id="reset">Сбросить фильтры</button>
    </div>

    <section id="tab-review">
      <div class="panel">
        <h2>Активная очередь проверки</h2>
        <div class="note" style="padding: 0 12px 10px;">Открытые вопросы и рабочие действия показаны отдельно от доказанного жизненного цикла вакансии.</div>
        <div id="reviewTable"></div>
      </div>
    </section>

    <section id="tab-wip" class="hidden">
      <div class="panel">
        <h2>Очередь WIP и SLA</h2>
        <div class="note" style="padding: 0 12px 10px;">Показана текущая страница. Полная очередь без обрезания разбита на страницы в views/wip_queue_page_*.md.</div>
        <div id="wipTable"></div>
      </div>
    </section>

    <section id="tab-funnel" class="hidden">
      <div class="panel">
        <h2>Воронка по каналам</h2>
        <div id="funnelTable" class="funnel"></div>
        <div id="funnelVisual" class="funnel-visual"></div>
        <div id="funnelVacancies" class="funnel-vacancies"></div>
      </div>
    </section>

    <section id="tab-today" class="hidden">
      <div class="panel">
        <h2>Свежие импортированные вакансии</h2>
        <div id="recentTable"></div>
      </div>
    </section>

    <section id="tab-followups" class="hidden">
      <div class="panel">
        <h2>Повторные обращения</h2>
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
    const ACTION_LABELS = {{
      review: 'проверка', needs_input: 'нужны данные', employer_reply: 'ответ работодателя',
      follow_up: 'повторное обращение', account_research: 'исследование работодателя',
      waiting: 'ожидание', none: 'действий нет'
    }};
    const HUMAN_PATH_LABELS = {{
      unknown: 'неизвестно', not_searched: 'поиск не проводился', researching: 'идёт поиск',
      verified: 'проверен', not_found: 'не найден', blocked: 'заблокирован'
    }};
    const ACCOUNT_PRIORITY_LABELS = {{
      critical: 'критический', high: 'высокий', medium: 'средний', low: 'низкий'
    }};
    const ACCOUNT_STATUS_LABELS = {{
      target: 'целевой', active: 'активный', watch: 'под наблюдением',
      paused: 'приостановлен', inactive: 'неактивный', archived: 'архивный'
    }};
    const SIGNAL_TYPE_LABELS = {{
      technology_adoption: 'внедрение технологий', ai_adoption: 'внедрение ИИ',
      hiring_growth: 'рост найма', restructuring: 'реструктуризация',
      leadership_change: 'смена руководства', culture: 'культура', other: 'другое'
    }};
    const CONFIDENCE_LABELS = {{
      unknown: 'неизвестна', low: 'низкая', medium: 'средняя', high: 'высокая',
      confirmed: 'подтверждена'
    }};
    const CONTACT_SEARCH_STATUS_LABELS = {{
      found: 'найден', reused_verified_contact: 'использован проверенный контакт',
      not_found: 'не найден', ambiguous: 'неоднозначный результат',
      unreachable: 'недоступен'
    }};
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
      const label = esc(title || 'ссылка');
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
      return `<span class="stage ${{esc(stage)}}">${{esc(STAGE_LABELS[stage] || stage || 'неизвестно')}}</span>`;
    }}
    function interviewStageLabel(summary) {{
      return STAGE_LABELS[summary.stage] || `Интервью ${{summary.interview_no || ''}}`.trim();
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
    function channelLabel(channel) {{ return CHANNEL_LABELS[channel] || channel || 'другой'; }}
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
      const percentMetric = value => value === null || value === undefined ? 'n/a' : `${{Number(value).toFixed(1)}}%`;
      const items = [
        ['Вакансии', DATA.kpis.vacancies],
        ['Каналы', DATA.kpis.channels],
        ['Открытые действия', DATA.kpis.active_review],
        ['Нужны данные', DATA.kpis.needs_user],
        ['Повторные обращения', DATA.kpis.followups],
        ['Прямые контакты', DATA.kpis.direct_contacts],
        ['Подтверждённые отклики', DATA.kpis.applied],
        ['Созрело за 14 дней', DATA.kpis.matured_applications_14d],
        ['Ответы людей', DATA.kpis.human_replies === null ? 'n/a' : DATA.kpis.human_replies],
        ['Запросы на скрининг', DATA.kpis.screening_requests === null ? 'n/a' : DATA.kpis.screening_requests],
        ['Доля ответов людей', percentMetric(DATA.kpis.human_reply_rate_14d)],
        ['Завершённые интервью', percentMetric(DATA.kpis.interview_1_rate_30d)],
        ['Покрытие контактов', percentMetric(DATA.kpis.verified_contact_coverage)],
        ['Аккаунты работодателей', DATA.kpis.employer_accounts],
        ['Активные цели аккаунтов', DATA.kpis.active_account_targets],
        ['Сверх WIP', DATA.kpis.wip_overflow],
        ['Просрочено по SLA', DATA.kpis.sla_overdue],
        ['Карантин', DATA.kpis.quarantine_pending],
        ['Отказы', DATA.kpis.rejected],
      ];
      $('kpis').innerHTML = items.map(([label, value]) => `
        <div class="kpi"><div class="label">${{esc(label)}}</div><div class="value">${{esc(value)}}</div></div>
      `).join('');
      $('conversionCaveat').textContent = DATA.conversion_report.interaction_history_available
        ? 'Ответы считаются только по записанным взаимодействиям; текст состояния не используется для догадок.'
        : 'Структурированная история ответов неполна. Показатели ответов равны n/a, а не нулю.';
    }}

    function renderAccountSummary() {{
      const rows = DATA.employer_accounts || [];
      const headers = ['Работодатель','Приоритет','Состояние','Последний доказательный сигнал','Связано','Активные цели','Лимит','Сверх лимита','Путь к человеку','Следующая проверка'];
      $('accountSummary').innerHTML = table(headers, rows, account => {{
        const signal = account.latest_signal || {{}};
        const signalText = [
          signal.observed_date,
          SIGNAL_TYPE_LABELS[signal.signal_type] || signal.signal_type,
          CONFIDENCE_LABELS[signal.confidence] || signal.confidence,
          signal.evidence_note,
        ].filter(Boolean).join(' · ');
        return `<tr>
          ${{cell(headers[0], link(account.canonical_name, account.website || account.careers_url))}}
          ${{cell(headers[1], esc(ACCOUNT_PRIORITY_LABELS[account.priority] || account.priority || ''))}}
          ${{cell(headers[2], esc(ACCOUNT_STATUS_LABELS[account.status] || account.status || ''))}}
          ${{cell(headers[3], signal.evidence_url ? link(signalText, signal.evidence_url) : esc(signalText), 'detail-cell')}}
          ${{cell(headers[4], esc(account.linked_vacancies || 0))}}
          ${{cell(headers[5], esc(account.active_target_vacancies || 0))}}
          ${{cell(headers[6], esc(account.portfolio_limit_effective || ''))}}
          ${{cell(headers[7], esc(account.portfolio_overflow || 0))}}
          ${{cell(headers[8], esc(HUMAN_PATH_LABELS[account.human_path_status] || account.human_path_status || 'неизвестно'))}}
          ${{cell(headers[9], esc(account.next_review_date || ''))}}
        </tr>`;
      }}, {{ limit: 20 }});
    }}

    function renderFilters() {{
      renderMultiMenu('channelMenu', 'channel', ALL_CHANNELS, state.channels, 'Все каналы', channelLabel);
      renderMultiMenu('stageMenu', 'stage', ALL_STAGES, state.stages, 'Все этапы', stage => STAGE_LABELS[stage] || stage);
      $('channelSummary').textContent = selectedSummary(state.channels, ALL_CHANNELS, 'Все каналы', 'Каналы не выбраны', 'канала', channelLabel);
      $('stageSummary').textContent = selectedSummary(state.stages, ALL_STAGES, 'Все этапы', 'Этапы не выбраны', 'этапа', stage => STAGE_LABELS[stage] || stage);
    }}

    function table(headers, rows, renderRow, options = {{}}) {{
      if (!rows.length) return '<div class="bars muted">Для текущих фильтров строк нет.</div>';
      const limit = options.limit || rows.length;
      const visible = rows.slice(0, limit);
      const more = rows.length > visible.length ? `<span>Показано ${{visible.length}} из ${{rows.length}}. Уточните фильтры.</span>` : '<span></span>';
      return `<div class="table-meta"><span>Строк: ${{rows.length}}</span>${{more}}</div><div class="table-wrap"><table><thead><tr>${{headers.map(h => `<th>${{esc(h)}}</th>`).join('')}}</tr></thead><tbody>${{visible.map(renderRow).join('')}}</tbody></table></div>`;
    }}

    function renderReview() {{
      const rows = filtered(DATA.active_review);
      const headers = ['ID','Балл','Жизненный цикл','Дата','Канал','Работодатель','Вакансия','Причина или вопросы'];
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
      const headers = ['Балл','Жизненный цикл','Дата','Канал','Работодатель','Вакансия','Примечание'];
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
      const headers = ['Дата','Источник','Балл','Работодатель','Вакансия','Состояние','Прямой контакт','Последнее обращение','Поиск контакта','Примечание'];
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
          ${{cell(headers[8], esc([
            r.contact_search_date,
            CONTACT_SEARCH_STATUS_LABELS[r.contact_search_status] || r.contact_search_status,
            r.contact_search_note,
          ].filter(Boolean).join(' · ')), 'detail-cell')}}
          ${{cell(headers[9], detailWithInterviews(r.why_applied || r.risks || '', r), 'detail-cell')}}
        </tr>
      `);
    }}

    function renderWip() {{
      const queue = DATA.wip_queue || {{items: [], pagination: {{}}}};
      const headers = ['ID','Корзина','Действие','Приоритет','Возраст','Срок','Просрочено','Сверх WIP','Работодатель','Вакансия','Причина'];
      const body = table(headers, queue.items || [], item => `<tr>
        ${{cell(headers[0], esc(item.vacancy_id || ''), 'id-cell')}}
        ${{cell(headers[1], esc(item.bucket_label || ''))}}
        ${{cell(headers[2], esc(ACTION_LABELS[item.action_state] || item.action_state || ''))}}
        ${{cell(headers[3], esc(item.priority || 0), 'score')}}
        ${{cell(headers[4], esc(`${{item.age_days || 0}} дн.`))}}
        ${{cell(headers[5], esc(item.due_date || ''))}}
        ${{cell(headers[6], esc(item.overdue ? `да, ${{item.overdue_days}} дн.` : 'нет'))}}
        ${{cell(headers[7], esc(item.wip_overflow ? 'да' : 'нет'))}}
        ${{cell(headers[8], esc(item.company || ''), 'company-cell')}}
        ${{cell(headers[9], link(item.title, item.url), 'vacancy-cell')}}
        ${{cell(headers[10], esc(item.priority_reason || ''), 'detail-cell')}}
      </tr>`);
      const p = queue.pagination || {{}};
      $('wipTable').innerHTML = `<div class="note" style="padding: 8px 12px;">Страница ${{esc(p.page || 1)}} из ${{esc(p.total_pages || 1)}}; всего ${{esc(p.total_items || 0)}}, сверх лимита ${{esc(queue.overflow_total || 0)}}, просрочено ${{esc(queue.overdue_total || 0)}}.</div>` + body;
    }}

    function renderFunnel() {{
      const visibleStages = currentFunnelStages();
      const channels = currentFunnelChannels();
      const headers = ['Канал', ...visibleStages.map(st => STAGE_LABELS[st] || st)];
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
        const reason = !channels.length ? 'Каналы не выбраны.' : 'Этапы не выбраны.';
        $('funnelVisual').innerHTML = `<div class="funnel-visual-head">
          <div class="funnel-visual-title">Визуальная воронка</div>
          <div class="funnel-visual-sub">Ничего не выбрано</div>
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
        const meta = stage === 'seen' ? '100% выбранных' : `${{rate}}% от всех выбранных`;
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
        <div class="funnel-visual-title">Визуальная воронка</div>
        <div class="funnel-visual-sub">Каналов: ${{esc(channels.length)}}, вакансий: ${{esc(totalSeen)}}</div>
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
      const headers = ['ID','Балл','Жизненный цикл','Дата','Канал','Работодатель','Вакансия','Примечание'];
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
      $('funnelVacancies').innerHTML = '<div class="funnel-vacancies-head">Отфильтрованные вакансии</div>' + body;
    }}

    function renderTab() {{
      document.querySelectorAll('section[id^="tab-"]').forEach(el => el.classList.add('hidden'));
      $(`tab-${{state.tab}}`).classList.remove('hidden');
      document.querySelectorAll('.tabs button').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === state.tab));
      renderReview();
      renderWip();
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
    renderAccountSummary();
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
        ensure_active_policy(conn)
        refresh_source_hit_labels(conn)
        conn.commit()
        snapshot = build_snapshot(conn, db_path)
    generate_views(snapshot, db_path)
    generate_dashboard(snapshot)
    return snapshot


def db_label(path: Path) -> str:
    return display_path(path, ROOT)


def print_render_summary(action: str, db_path: Path, snapshot: dict[str, Any]) -> None:
    print(action)
    print(f"  база данных: {db_label(db_path)}")
    print(f"  панель: {display_path(DASHBOARD_PATH, ROOT)}")
    print(f"  вакансии: {snapshot['kpis']['vacancies']}")
    print(f"  открытые действия: {snapshot['kpis']['active_review']}")
    print(f"  нужны данные: {snapshot['kpis']['needs_user']}")
    print(f"  повторные обращения: {snapshot['kpis']['followups']}")
    print(f"  прямые контакты: {snapshot['kpis']['direct_contacts']}")
    print(f"  подтверждённые отклики: {snapshot['kpis']['applied']}")
    print(f"  проблемы импорта: {snapshot['kpis']['import_issues']}")


def rebuild(args: argparse.Namespace) -> None:
    snapshot = render_outputs(args.db)
    if args.json:
        print(json.dumps({"kpis": snapshot["kpis"]}, ensure_ascii=False, indent=2))
    else:
        print_render_summary("Проекции поиска работы пересобраны из SQLite.", args.db, snapshot)


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
    print("Наблюдение за базой SQLite запущено. Для остановки нажмите Ctrl-C.")
    rebuild(argparse.Namespace(db=args.db, json=False))
    previous = file_signature([args.db])
    try:
        while True:
            time.sleep(args.interval)
            current = file_signature([args.db])
            if current != previous:
                print(f"\nИзменение обнаружено: {now_iso()}")
                rebuild(argparse.Namespace(db=args.db, json=False))
                previous = current
    except KeyboardInterrupt:
        print("\nНаблюдение остановлено.")


def print_stats(args: argparse.Namespace) -> None:
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        snapshot = build_snapshot(conn, args.db)
    print(json.dumps(snapshot["kpis"], ensure_ascii=False, indent=2))


def migrate_schema(args: argparse.Namespace) -> None:
    """Explicitly migrate an existing database with a recoverable backup."""

    if not args.db.exists():
        raise FileNotFoundError(f"База данных не найдена: {args.db}")
    with sqlite3.connect(args.db) as probe:
        current_version = int(probe.execute("PRAGMA user_version").fetchone()[0])
    if current_version not in {1, 2, 3, 4, 5, 6, SCHEMA_VERSION}:
        raise RuntimeError(
            f"Неподдерживаемый путь переноса: {current_version} → {SCHEMA_VERSION}."
        )

    backup_path: Path | None = None
    if current_version < SCHEMA_VERSION and not args.no_backup:
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = args.db.with_name(f"{args.db.name}.bak-schema-v{current_version}-{stamp}")
        with sqlite3.connect(args.db) as source_conn, sqlite3.connect(backup_path) as backup_conn:
            source_conn.backup(backup_conn)

    with connect_db(args.db) as conn:
        preserved_tables = (
            "vacancies",
            "source_hits",
            "applications",
            "stage_events",
            "employer_interactions",
            "employer_interaction_invalidations",
            "employer_accounts",
            "source_checkpoints",
        )
        present_before = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        row_counts_before = {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in preserved_tables
            if table in present_before
        }
        ensure_auxiliary_schema(conn)
        backfilled_aliases = backfill_canonical_external_aliases(conn)
        backfilled_source_streams = backfill_canonical_source_streams(conn)
        refreshed_source_labels = refresh_source_hit_labels(conn)
        v6_backfill = (
            backfill_v6_evidence(conn)
            if current_version < 6
            else {
                "lifecycle_applications": 0,
                "lifecycle_rejections": 0,
                "action_events": 0,
            }
        )
        if current_version < SCHEMA_VERSION:
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.execute(
                """
                INSERT OR IGNORE INTO migration_log (
                    from_version, to_version, applied_at, backup_path,
                    row_counts_json, notes
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    current_version,
                    SCHEMA_VERSION,
                    now_iso(),
                    db_label(backup_path) if backup_path else "",
                    json.dumps(row_counts_before, ensure_ascii=False, sort_keys=True),
                    (
                        "Добавлены аудируемые исправления взаимодействий и эффективные проекции; "
                        "исторические строки не изменялись."
                        if current_version == 6
                        else "Консервативный перенос: неизвестные исторические поля не заполнялись; "
                        "добавлены эффективные проекции без автоматических исправлений."
                    ),
                ),
            )
        conn.commit()
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        foreign_key_issues = len(conn.execute("PRAGMA foreign_key_check").fetchall())
        row_counts_after = {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in row_counts_before
        }
        if quick_check != "ok" or foreign_key_issues:
            raise RuntimeError(
                f"После переноса нарушена целостность: quick_check={quick_check}, "
                f"foreign_key_issues={foreign_key_issues}"
            )
        if row_counts_after != row_counts_before:
            raise RuntimeError(
                "Перенос изменил число строк, которые должны были сохраниться: "
                f"до={row_counts_before}, после={row_counts_after}"
            )

    snapshot = render_outputs(args.db)
    result = {
        "from_version": current_version,
        "to_version": SCHEMA_VERSION,
        "backup": db_label(backup_path) if backup_path else "",
        "backfilled_aliases": backfilled_aliases,
        "backfilled_source_streams": backfilled_source_streams,
        "refreshed_source_labels": refreshed_source_labels,
        "v6_backfill": v6_backfill,
        "row_counts_before": row_counts_before,
        "row_counts_after": row_counts_after,
        "quick_check": quick_check,
        "foreign_key_issues": foreign_key_issues,
        "already_current": current_version == SCHEMA_VERSION,
        "kpis": snapshot["kpis"],
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if current_version == SCHEMA_VERSION:
            print(f"Схема базы уже имеет версию {SCHEMA_VERSION}.")
        else:
            print(f"Схема базы обновлена: {current_version} → {SCHEMA_VERSION}.")
        if backup_path:
            print(f"  резервная копия: {db_label(backup_path)}")
        print(f"  восстановлено канонических псевдонимов: {backfilled_aliases}")
        print(f"  обновлено меток источников: {refreshed_source_labels}")
        print(f"  перенос доказательств v6: {v6_backfill}")


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
        print(f"План покрытия создан: {display_path(output_path.resolve(), ROOT)}")
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


def load_source_checkpoints(
    conn: sqlite3.Connection, source: str
) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT source, stream_key, cursor_value, cursor_date, initialized_at,
               last_completed_run_date, last_manifest_file, updated_at
        FROM source_checkpoints
        WHERE source = ?
        ORDER BY stream_key COLLATE NOCASE
        """,
        (source,),
    ).fetchall()
    return {str(row["stream_key"]): dict(row) for row in rows}


def load_telegram_vacancy_evidence(
    conn: sqlite3.Connection,
) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    rows = conn.execute(
        """
        SELECT a.external_id, a.url, a.vacancy_id, v.score, sh.source_stream
        FROM vacancy_external_aliases a
        JOIN vacancies v ON v.id = a.vacancy_id
        LEFT JOIN source_hits sh ON sh.vacancy_id = a.vacancy_id
        WHERE a.channel = 'telegram'
        ORDER BY a.external_id, sh.id
        """
    ).fetchall()
    for row in rows:
        external_id = str(row["external_id"])
        item = evidence.setdefault(
            external_id,
            {
                "vacancy_id": int(row["vacancy_id"]),
                "url": row["url"] or "",
                "score": row["score"],
                "source_streams": set(),
            },
        )
        if row["source_stream"]:
            item["source_streams"].add(str(row["source_stream"]))
    return evidence


def persist_source_checkpoints(
    conn: sqlite3.Connection,
    result: dict[str, Any],
    manifest_file: str,
) -> None:
    if not result["ok"]:
        raise ValueError("Контрольные точки источника можно сдвигать только после полного покрытия.")
    now = now_iso()
    for stream in result["streams"]:
        checkpoint = stream.get("checkpoint") or {}
        conn.execute(
            """
            INSERT INTO source_checkpoints (
                source, stream_key, cursor_value, cursor_date, initialized_at,
                last_completed_run_date, last_manifest_file, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, stream_key) DO UPDATE SET
                cursor_value = excluded.cursor_value,
                cursor_date = excluded.cursor_date,
                last_completed_run_date = excluded.last_completed_run_date,
                last_manifest_file = excluded.last_manifest_file,
                updated_at = excluded.updated_at
            """,
            (
                result["source"],
                stream["key"],
                clean_cell(str(checkpoint.get("cursor_value") or "")),
                clean_cell(str(checkpoint.get("cursor_date") or "")),
                now,
                result["run_date"],
                manifest_file,
                now,
                now,
            ),
        )


def build_telegram_plan_command(args: argparse.Namespace) -> None:
    if not TELEGRAM_ENABLED:
        raise RuntimeError(
            "Поиск в Telegram выключен; сначала включите раздел [telegram] в локальных настройках."
        )
    run_date = args.run_date or dt.date.today().isoformat()
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        checkpoints = load_source_checkpoints(conn, "telegram")
    plan = build_telegram_plan(
        run_date,
        TELEGRAM_CHANNELS,
        initial_lookback_days=TELEGRAM_INITIAL_LOOKBACK_DAYS,
        checkpoints=checkpoints,
    )
    output_path = args.output or ROOT / "tmp" / f"telegram_coverage_{run_date}.json"
    if not output_path.is_absolute():
        output_path = ROOT / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if args.json:
        print(
            json.dumps(
                {
                    "output": display_path(output_path.resolve(), ROOT),
                    "plan": plan,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        backfills = sum(
            stream["query"]["mode"] == "backfill" for stream in plan["streams"]
        )
        print(f"План покрытия Telegram создан: {display_path(output_path.resolve(), ROOT)}")
        print(
            f"  каналов: {len(plan['streams'])}; первичных загрузок: {backfills}; "
            f"добавочных проходов: {len(plan['streams']) - backfills}"
        )


def check_telegram_coverage(args: argparse.Namespace) -> None:
    if not TELEGRAM_ENABLED:
        raise RuntimeError(
            "Поиск в Telegram выключен; сначала включите раздел [telegram] в локальных настройках."
        )
    manifest_path = args.file if args.file.is_absolute() else ROOT / args.file
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_file = origin_for(manifest_path)
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        checkpoints = load_source_checkpoints(conn, "telegram")
        vacancy_evidence = load_telegram_vacancy_evidence(conn)
        result = validate_telegram_manifest(
            payload,
            TELEGRAM_CHANNELS,
            initial_lookback_days=TELEGRAM_INITIAL_LOOKBACK_DAYS,
            checkpoints=checkpoints,
            vacancy_evidence=vacancy_evidence,
        )
        if result.get("run_date") and result.get("source"):
            run_id = persist_coverage_result(conn, result, manifest_file)
            result["search_run_id"] = run_id
            if result["ok"]:
                persist_source_checkpoints(conn, result, manifest_file)
            conn.commit()
    if result.get("search_run_id"):
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
        print(f"Резервная копия: {db_label(backup_path)}")

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
    print("Старые этапы перенесены в совместимую компактную воронку.")
    print(f"  изменённые значения: {changed}")
    print(f"  строки повторных обращений: {followup_applications}")
    print(f"  вакансии с повторным обращением: {followup_vacancies}")
    print(f"  показатели: {snapshot['kpis']}")


def ingest_json(args: argparse.Namespace) -> None:
    payload = json.loads(args.file.read_text(encoding="utf-8"))
    items = payload_items(payload)
    if not items:
        raise SystemExit("Ожидался массив JSON либо объект с массивом vacancies, items, jobs или data.")
    origin_file = origin_for(args.file)
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        stats = {"ingested": 0, "quarantined": 0}
        for line_no, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                store_quarantine_record(
                    conn,
                    item=item,
                    origin_file=origin_file,
                    line_no=line_no,
                    source_name=args.channel or "unknown",
                    source_stream=args.source or "unknown",
                    classification="malformed",
                    evidence_note="Элемент JSON не является объектом вакансии.",
                )
                stats["quarantined"] += 1
                continue
            if ingest_item(
                conn,
                item,
                default_channel=args.channel or "",
                default_source=args.source or "",
                origin_file=origin_file,
                line_no=line_no,
                ingestion_stats=stats,
            ):
                pass
        conn.commit()
    snapshot = render_outputs(args.db)
    if args.json:
        print(
            json.dumps(
                {
                    "ingested": stats["ingested"],
                    "quarantined": stats["quarantined"],
                    "kpis": snapshot["kpis"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(
            f"В SQLite добавлено вакансий: {stats['ingested']}; "
            f"помещено в карантин: {stats['quarantined']}. "
            f"Обновлён файл {display_path(DASHBOARD_PATH, ROOT)}."
        )


def ingest_gmail_json(args: argparse.Namespace) -> None:
    payload = json.loads(args.file.read_text(encoding="utf-8"))
    items = payload_items(payload)
    if not items:
        raise SystemExit("Ожидался массив JSON либо объект с массивом vacancies.")
    origin_file = origin_for(args.file)
    provider_defaults = {
        "hh": {
            "channel": "gmail_hh",
            "source": "hh_gmail_digest",
            "status": "DISCOVERED_FROM_GMAIL",
            "label": "Gmail HH",
        },
        "linkedin": {
            "channel": "linkedin",
            "source": "linkedin_gmail_job_alert",
            "status": "DISCOVERED_FROM_LINKEDIN_EMAIL",
            "label": "Gmail LinkedIn",
        },
    }
    defaults = provider_defaults[args.provider]
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        stats = {"ingested": 0, "quarantined": 0}
        for line_no, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                store_quarantine_record(
                    conn,
                    item=item,
                    origin_file=origin_file,
                    line_no=line_no,
                    source_name=defaults["channel"],
                    source_stream=defaults["source"],
                    classification="malformed",
                    evidence_note="Элемент почтового импорта не является объектом вакансии.",
                )
                stats["quarantined"] += 1
                continue
            item = dict(item)
            item.setdefault("channel", defaults["channel"])
            item.setdefault("source", defaults["source"])
            item.setdefault("status", defaults["status"])
            item.setdefault("stage", "seen")
            if ingest_item(
                conn,
                item,
                default_channel=defaults["channel"],
                default_source=defaults["source"],
                origin_file=origin_file,
                line_no=line_no,
                ingestion_stats=stats,
            ):
                pass
        conn.commit()
    snapshot = render_outputs(args.db)
    if args.json:
        print(
            json.dumps(
                {
                    "ingested": stats["ingested"],
                    "quarantined": stats["quarantined"],
                    "provider": args.provider,
                    "kpis": snapshot["kpis"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(
            f"Из почтового источника добавлено вакансий: {stats['ingested']}; "
            f"в карантине: {stats['quarantined']}."
        )


def update_vacancy(args: argparse.Namespace) -> None:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Укажите вакансию через --id, --url или --external-id.")
    status = args.status or args.stage or "UPDATED"
    stage = args.stage or detect_stage(status)
    date = args.date or dt.date.today().isoformat()
    norm_url = normalize_url(args.url or "")
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        row = resolve_vacancy_row(conn, args, required=False)
        if not row and not (args.title or norm_url):
            raise SystemExit("Вакансия не найдена. Для создания укажите --title или --company вместе с --url.")

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
                external_id=args.external_id or "",
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
        action_state, action_bucket, action_due, action_priority = action_from_legacy_row(
            {
                "latest_stage": stage,
                "latest_status": status,
                "next_action": args.next_action or "",
                "open_questions": args.open_questions or "",
                "follow_up_date": args.follow_up_date or "",
                "score": args.score,
            }  # type: ignore[arg-type]
        )
        append_action_event(
            conn,
            vacancy_id=vacancy_id,
            action_state=action_state,
            bucket=action_bucket,
            event_at=date,
            due_date=action_due,
            priority=action_priority,
            reason=args.next_action or args.open_questions or args.reason or "",
            evidence_note=args.note,
            source="cli:update-vacancy",
        )
        if stage in {"rejected", "offer"} and clean_cell(args.note):
            append_lifecycle_event(
                conn,
                vacancy_id=vacancy_id,
                event_type="rejected" if stage == "rejected" else "offer_received",
                event_at=date,
                evidence_at=now_iso(),
                evidence_note=args.note,
                evidence_source="manual_update",
                origin="cli:update-vacancy",
                history_complete=True,
                authorization_status="not_applicable",
            )
        conn.commit()
    render_outputs(args.db)
    sync_note = (
        f"; синхронизирована совместимая запись отклика №{synced_application_id}"
        if synced_application_id is not None
        else ""
    )
    print(
        f"Вакансия №{vacancy_id} обновлена{sync_note}; пересобран файл "
        f"{display_path(DASHBOARD_PATH, ROOT)}."
    )


def resolve_external_id_vacancy_row(
    conn: sqlite3.Connection, external_id: str
) -> sqlite3.Row | None:
    external_id = clean_cell(external_id)
    if not external_id:
        return None
    row = conn.execute(
        "SELECT * FROM vacancies WHERE external_id = ?", (external_id,)
    ).fetchone()
    if row:
        return row
    rows = conn.execute(
        """
        SELECT DISTINCT v.*
        FROM vacancy_external_aliases a
        JOIN vacancies v ON v.id = a.vacancy_id
        WHERE a.external_id = ?
        ORDER BY v.id
        """,
        (external_id,),
    ).fetchall()
    if len(rows) > 1:
        raise SystemExit(
            f"Внешний идентификатор {external_id!r} неоднозначен для разных каналов; "
            "используйте --id или --url."
        )
    return rows[0] if rows else None


def resolve_alias_url_vacancy_row(
    conn: sqlite3.Connection, url: str
) -> sqlite3.Row | None:
    norm_url = normalize_url(url)
    if not norm_url:
        return None
    row = conn.execute(
        "SELECT * FROM vacancies WHERE url = ?", (norm_url,)
    ).fetchone()
    if row:
        return row
    rows = conn.execute(
        """
        SELECT DISTINCT v.*
        FROM vacancy_external_aliases a
        JOIN vacancies v ON v.id = a.vacancy_id
        WHERE a.url = ?
        ORDER BY v.id
        """,
        (norm_url,),
    ).fetchall()
    if len(rows) > 1:
        raise SystemExit(f"Адрес вакансии {norm_url!r} неоднозначен; используйте --id.")
    return rows[0] if rows else None


def resolve_vacancy_row(
    conn: sqlite3.Connection,
    args: argparse.Namespace,
    *,
    required: bool = True,
) -> sqlite3.Row | None:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Укажите вакансию через --id, --url или --external-id.")

    row = None
    if args.id is not None:
        row = conn.execute("SELECT * FROM vacancies WHERE id = ?", (args.id,)).fetchone()
        if not row:
            raise SystemExit(f"Вакансия №{args.id} не найдена.")
    if not row and args.external_id:
        row = resolve_external_id_vacancy_row(conn, args.external_id)
    if not row and args.url:
        row = resolve_alias_url_vacancy_row(conn, args.url)
    if not row and required:
        raise SystemExit("Вакансия не найдена.")
    return row


def validate_optional_external_url(value: str, *, label: str) -> str:
    normalized = normalize_url(value)
    if normalized and not safe_external_url(normalized):
        raise SystemExit(f"Адрес {label} должен использовать http или https.")
    return normalized


def record_employer_interaction(args: argparse.Namespace) -> None:
    evidence_note = clean_cell(args.evidence_note)
    if args.direction == "inbound" and not evidence_note:
        raise SystemExit("Для входящего взаимодействия с работодателем требуется --evidence-note.")
    if args.direction == "outbound" and not evidence_note:
        raise SystemExit("Исходящее взаимодействие требует --evidence-note.")
    is_human = args.humanity == "human"
    if args.event_type == "automated_ack" and is_human:
        raise SystemExit("Для automated_ack укажите --humanity automated.")
    if args.event_type == "human_reply" and not is_human:
        raise SystemExit("Для human_reply укажите --humanity human.")
    if args.actor_type == "system" and is_human:
        raise SystemExit("Действующее лицо типа system нельзя пометить как human.")
    parsed_at = parse_iso_datetime(args.at or now_iso(), label="--at")
    event_at = parsed_at.isoformat()
    evidence_url = validate_optional_external_url(
        args.evidence_url, label="--evidence-url"
    )
    external_reference = clean_cell(args.external_reference)

    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy = resolve_vacancy_row(conn, args)
        vacancy_id = int(vacancy["id"])
        external_action_id: int | None = None
        if args.direction == "outbound":
            if not clean_cell(args.external_action_key):
                raise SystemExit(
                    "Исходящее взаимодействие требует --external-action-key."
                )
            confirmed_action = conn.execute(
                """
                SELECT id FROM external_actions
                WHERE vacancy_id = ? AND action_key = ?
                  AND action_type IN ('message', 'follow_up')
                  AND state = 'visibly_confirmed'
                ORDER BY event_at DESC, id DESC LIMIT 1
                """,
                (vacancy_id, clean_cell(args.external_action_key)),
            ).fetchone()
            if not confirmed_action:
                raise SystemExit(
                    "Нет видимо подтверждённого внешнего действия для исходящего взаимодействия."
                )
            external_action_id = int(confirmed_action["id"])
        if external_reference:
            dedupe_material = {
                "vacancy_id": vacancy_id,
                "direction": args.direction,
                "channel": clean_cell(args.channel).casefold(),
                "external_reference": external_reference,
            }
        else:
            dedupe_material = {
                "vacancy_id": vacancy_id,
                "event_at": event_at,
                "direction": args.direction,
                "event_type": args.event_type,
                "channel": clean_cell(args.channel).casefold(),
                "actor_type": args.actor_type,
                "is_human": is_human,
                "evidence_note": evidence_note,
                "evidence_url": evidence_url,
            }
        dedupe_key = hashlib.sha256(
            json.dumps(dedupe_material, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        before = conn.total_changes
        conn.execute(
            """
            INSERT OR IGNORE INTO employer_interactions (
                vacancy_id, event_at, direction, event_type, channel,
                actor_type, is_human, evidence_note, evidence_url,
                external_reference, dedupe_key, created_at
                , external_action_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vacancy_id,
                event_at,
                args.direction,
                args.event_type,
                clean_cell(args.channel),
                args.actor_type,
                1 if is_human else 0,
                evidence_note,
                evidence_url,
                external_reference,
                dedupe_key,
                now_iso(),
                external_action_id,
            ),
        )
        created = conn.total_changes > before
        row = conn.execute(
            "SELECT id FROM employer_interactions WHERE dedupe_key = ?",
            (dedupe_key,),
        ).fetchone()
        interaction_id = int(row["id"])
        conn.commit()
    render_outputs(args.db)
    result = {
        "interaction_id": interaction_id,
        "vacancy_id": vacancy_id,
        "created": created,
        "stage_changed": False,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        state = "Записано" if created else "Сохранён существующий повтор"
        print(f"{state}: взаимодействие №{interaction_id}, вакансия №{vacancy_id}.")


def invalidate_employer_interaction(args: argparse.Namespace) -> None:
    reason = clean_cell(args.reason)
    evidence_note = clean_cell(args.evidence_note)
    source = clean_cell(args.source)
    operator_context = clean_cell(args.operator_context) or source
    if not reason:
        raise SystemExit("Для исправления требуется непустой --reason.")
    if not evidence_note:
        raise SystemExit("Для исправления требуется непустой --evidence-note.")
    if not source or not operator_context:
        raise SystemExit("Для исправления требуются источник и контекст оператора.")
    corrected_at = parse_iso_datetime(
        args.corrected_at or now_iso(), label="--corrected-at"
    ).isoformat()

    with connect_db(args.db) as conn:
        ensure_schema(conn)
        interaction = conn.execute(
            "SELECT id, vacancy_id FROM employer_interactions WHERE id = ?",
            (args.interaction_id,),
        ).fetchone()
        if not interaction:
            raise SystemExit(f"Взаимодействие №{args.interaction_id} не найдено.")
        vacancy_id = int(interaction["vacancy_id"])

        if args.vacancy_id is not None or args.vacancy_url or args.vacancy_external_id:
            expected_vacancy = resolve_vacancy_row(
                conn,
                argparse.Namespace(
                    id=args.vacancy_id,
                    url=args.vacancy_url,
                    external_id=args.vacancy_external_id,
                ),
            )
            if int(expected_vacancy["id"]) != vacancy_id:
                raise SystemExit(
                    "Указанное взаимодействие относится к другой вакансии; исправление отменено."
                )

        dedupe_key = dedupe_hash(
            {
                "correction_type": "invalidate_employer_interaction",
                "interaction_id": int(args.interaction_id),
            }
        )
        existing = conn.execute(
            """
            SELECT * FROM employer_interaction_invalidations
            WHERE interaction_id = ?
            """,
            (args.interaction_id,),
        ).fetchone()
        created = False
        if existing:
            existing_context = (
                clean_cell(existing["reason"]),
                clean_cell(existing["evidence_note"]),
                clean_cell(existing["source"]),
                clean_cell(existing["operator_context"]),
            )
            requested_context = (reason, evidence_note, source, operator_context)
            if existing_context != requested_context:
                raise SystemExit(
                    "Взаимодействие уже исправлено с другими метаданными; "
                    "неоднозначное повторное исправление отклонено."
                )
            invalidation_id = int(existing["id"])
            corrected_at = str(existing["corrected_at"])
            dedupe_key = str(existing["dedupe_key"])
        else:
            cursor = conn.execute(
                """
                INSERT INTO employer_interaction_invalidations (
                    interaction_id, vacancy_id, corrected_at, reason,
                    evidence_note, source, operator_context, dedupe_key, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(args.interaction_id),
                    vacancy_id,
                    corrected_at,
                    reason,
                    evidence_note,
                    source,
                    operator_context,
                    dedupe_key,
                    now_iso(),
                ),
            )
            invalidation_id = int(cursor.lastrowid)
            created = True
        conn.commit()

    render_outputs(args.db)
    result = {
        "invalidation_id": invalidation_id,
        "interaction_id": int(args.interaction_id),
        "vacancy_id": vacancy_id,
        "corrected_at": corrected_at,
        "dedupe_key": dedupe_key,
        "created": created,
        "effective": False,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        state = "Исправление записано" if created else "Исправление уже существовало"
        print(f"{state}: взаимодействие №{args.interaction_id} исключено из проекций.")


def conversion_report_command(args: argparse.Namespace) -> None:
    as_of = parse_iso_date(
        args.as_of or dt.date.today().isoformat(), label="--as-of"
    )
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        report, _ = build_conversion_report_data(conn, as_of)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    overall = report["overall"]
    print(f"Конверсия откликов на {report['as_of']}")
    print(f"  подтверждённых откликов: {overall['applications_unique']}")
    print(f"  созрело за 14 дней: {overall['matured_applications_14d']}")
    print(
        "  ответов людей: "
        + (str(overall["human_replies"]) if overall["human_replies"] is not None else "н/д")
    )
    print(f"  созрело за 30 дней: {overall['matured_applications_30d']}")
    print(
        "  завершённых первых интервью: "
        + (str(overall["interview_1_ever"]) if overall["interview_1_ever"] is not None else "н/д")
    )


def ratio_display(metric: dict[str, Any]) -> str:
    if not metric.get("available") or metric.get("numerator") is None:
        return "н/д"
    percent_value = metric.get("percent")
    suffix = "н/д" if percent_value is None else f"{float(percent_value):.1f}%"
    return f"{metric['numerator']}/{metric['denominator']} ({suffix})"


def outcome_scorecard_command(args: argparse.Namespace) -> None:
    as_of = parse_iso_date(args.as_of or dt.date.today().isoformat(), label="--as-of")
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        refresh_source_hit_labels(conn)
        conn.commit()
        scorecard, _ = build_outcome_scorecard_data(conn, as_of)
    if args.json:
        print(json.dumps(scorecard, ensure_ascii=False, indent=2))
        return
    overall = scorecard["overall"]
    print(f"Карта исходов на {scorecard['as_of']}")
    print(f"  подтверждённых откликов: {overall['confirmed_applications']}")
    print(
        "  ответы людей за 14 дней: "
        + ratio_display(overall["human_reply_rate_14d"])
    )
    print(
        "  завершённые первые интервью за 30 дней: "
        + ratio_display(overall["completed_first_interview_rate_30d"])
    )
    print(f"  приглашений на интервью: {overall['interview_invitations'] if overall['interview_invitations'] is not None else 'н/д'}")
    print(f"  назначенных интервью: {overall['scheduled_interviews'] if overall['scheduled_interviews'] is not None else 'н/д'}")
    print(f"  предложений: {overall['offers'] if overall['offers'] is not None else 'н/д'}")
    print(f"  полнота полей: {ratio_display(overall['field_completeness'])}")
    if scorecard["small_sample_warning"]:
        print(f"  предупреждение: {scorecard['small_sample_warning']}")


def wip_queue_command(args: argparse.Namespace) -> None:
    as_of = parse_iso_date(args.as_of or dt.date.today().isoformat(), label="--as-of")
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        queue = build_wip_queue_data(
            conn,
            as_of=as_of,
            page=args.page,
            page_size=args.page_size,
            bucket_filter=args.bucket,
        )
    if args.json:
        print(json.dumps(queue, ensure_ascii=False, indent=2))
        return
    pagination = queue["pagination"]
    print(
        f"Очередь WIP на {queue['as_of']}: страница {pagination['page']} из "
        f"{pagination['total_pages']}, записей {pagination['total_items']}."
    )
    print(
        f"  активный WIP: {queue['active_wip_total']}; "
        f"сверх лимита: {queue['overflow_total']}; "
        f"просрочено: {queue['overdue_total']}"
    )
    for item in queue["items"]:
        overdue = (
            f"просрочено на {item['overdue_days']} дн."
            if item["overdue"]
            else f"срок {item['due_date']}"
        )
        print(
            f"  #{item['vacancy_id']} · {item['bucket_label']} · {overdue} · "
            f"{item['company']} — {item['title']}"
        )


def metadata_from_cli(args: argparse.Namespace) -> dict[str, Any]:
    item: dict[str, Any] = {}
    for key in (
        "campaign_id",
        "role_family",
        "confidence",
        "master_resume_id",
        "planned_resume_id",
        "actual_resume_id",
        "message_variant",
        "human_path_status",
    ):
        value = getattr(args, key, None)
        if value not in (None, ""):
            item[key] = value
    hard_gates = getattr(args, "hard_gates_json", "")
    if hard_gates:
        item["hard_gates"] = json.loads(hard_gates)
    questions = getattr(args, "unresolved_questions_json", "")
    if questions:
        item["unresolved_questions"] = json.loads(questions)
    return extract_decision_metadata(item)


def record_lifecycle_event_command(args: argparse.Namespace) -> None:
    if args.event_type == "application_confirmed":
        raise SystemExit(
            "Подтверждённый отклик фиксируется только командой record-external-action "
            "со статусом visibly_confirmed."
        )
    metadata = metadata_from_cli(args)
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy = resolve_vacancy_row(conn, args)
        vacancy_id = int(vacancy["id"])
        event_id, created = append_lifecycle_event(
            conn,
            vacancy_id=vacancy_id,
            event_type=args.event_type,
            event_at=args.at or now_iso(),
            evidence_at=args.evidence_at or now_iso(),
            evidence_note=args.evidence_note,
            evidence_source=args.source,
            origin="cli:record-lifecycle-event",
            evidence_url=args.evidence_url,
            external_reference=args.external_reference,
            round_no=args.round_no,
            scheduled_at=args.scheduled_at,
            metadata=metadata,
            history_complete=True,
            authorization_status="not_applicable",
        )
        conn.commit()
    render_outputs(args.db)
    result = {
        "lifecycle_event_id": event_id,
        "vacancy_id": vacancy_id,
        "event_type": args.event_type,
        "created": created,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Событие жизненного цикла №{event_id} "
            + ("добавлено." if created else "уже существовало; повтор не создан.")
        )


def set_current_action_command(args: argparse.Namespace) -> None:
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy = resolve_vacancy_row(conn, args)
        vacancy_id = int(vacancy["id"])
        event_id, created = append_action_event(
            conn,
            vacancy_id=vacancy_id,
            action_state=args.action_state,
            bucket=args.bucket,
            event_at=args.at or now_iso(),
            due_date=args.due_date,
            priority=args.priority,
            reason=args.priority_reason,
            evidence_note=args.evidence_note,
            source=args.source,
        )
        conn.commit()
    render_outputs(args.db)
    result = {
        "action_event_id": event_id,
        "vacancy_id": vacancy_id,
        "created": created,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Текущее действие для вакансии №{vacancy_id} "
            + ("обновлено." if created else "не изменилось.")
        )


def record_external_action_command(args: argparse.Namespace) -> None:
    metadata = metadata_from_cli(args)
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy_id: int | None = None
        if args.id is not None or args.url or args.external_id:
            vacancy = resolve_vacancy_row(conn, args)
            vacancy_id = int(vacancy["id"])
        if args.action_type in {"application", "message", "follow_up"} and vacancy_id is None:
            raise SystemExit("Для этого внешнего действия требуется точная вакансия.")
        application_source_hit_id = args.application_source_hit_id
        if application_source_hit_id is not None:
            hit = conn.execute(
                "SELECT vacancy_id FROM source_hits WHERE id = ?",
                (application_source_hit_id,),
            ).fetchone()
            if not hit or int(hit["vacancy_id"]) != vacancy_id:
                raise SystemExit("Указанный источник отклика не относится к этой вакансии.")
        external_action_id, created = append_external_action(
            conn,
            vacancy_id=vacancy_id,
            action_key=args.action_key,
            action_type=args.action_type,
            state=args.state,
            event_at=args.at or now_iso(),
            authorization_note=args.authorization_note,
            evidence_note=args.evidence_note,
            evidence_url=args.evidence_url,
            source=args.source,
            external_reference=args.external_reference,
            metadata=metadata,
        )
        lifecycle_event_id: int | None = None
        lifecycle_created = False
        if args.action_type == "application" and args.state == "visibly_confirmed":
            lifecycle_event_id, lifecycle_created = append_lifecycle_event(
                conn,
                vacancy_id=int(vacancy_id),
                event_type="application_confirmed",
                event_at=args.at or now_iso(),
                evidence_at=args.evidence_at or now_iso(),
                evidence_note=args.evidence_note,
                evidence_source=args.source,
                origin="cli:record-external-action",
                evidence_url=args.evidence_url,
                external_reference=args.external_reference,
                external_action_id=external_action_id,
                application_source_hit_id=application_source_hit_id,
                metadata=metadata,
                history_complete=True,
                authorization_status="explicit",
            )
            lifecycle_row = conn.execute(
                "SELECT event_at FROM lifecycle_events WHERE id = ?",
                (lifecycle_event_id,),
            ).fetchone()
            application_date = parse_iso_datetime(
                lifecycle_row["event_at"], label="event_at"
            ).date().isoformat()
            insert_application_once(
                conn,
                vacancy_id=int(vacancy_id),
                applied_date=application_date,
                status="APPLIED_VISIBLY_CONFIRMED",
                stage="applied",
                score=None,
                resume_version=metadata.get("actual_resume_id") or "",
                cover_letter=metadata.get("message_variant") or "",
                why_applied="",
                risks="",
                follow_up_date="",
                origin_file="cli:record-external-action",
                line_no=0,
            )
            application = conn.execute(
                """
                SELECT id FROM applications
                WHERE vacancy_id = ? AND applied_date = ?
                  AND origin_file = 'cli:record-external-action'
                ORDER BY id DESC LIMIT 1
                """,
                (vacancy_id, application_date),
            ).fetchone()
            if application:
                conn.execute(
                    """
                    UPDATE applications SET lifecycle_event_id = ?,
                        application_source_hit_id = ?, campaign_id = ?,
                        role_family = ?, actual_resume_version = ?, message_variant = ?
                    WHERE id = ?
                    """,
                    (
                        lifecycle_event_id,
                        application_source_hit_id,
                        metadata.get("campaign_id"),
                        metadata.get("role_family"),
                        metadata.get("actual_resume_id"),
                        metadata.get("message_variant"),
                        int(application["id"]),
                    ),
                )
            upsert_decision_metadata(
                conn,
                vacancy_id=int(vacancy_id),
                metadata=metadata,
                application_source_hit_id=application_source_hit_id,
            )
            append_action_event(
                conn,
                vacancy_id=int(vacancy_id),
                action_state="waiting",
                bucket="backlog",
                event_at=args.at or now_iso(),
                priority=20,
                reason="Ждать подтверждённого ответа работодателя.",
                evidence_note=args.evidence_note,
                source="cli:record-external-action",
            )
        conn.commit()
    if vacancy_id is not None:
        render_outputs(args.db)
    result = {
        "external_action_id": external_action_id,
        "vacancy_id": vacancy_id,
        "state": args.state,
        "created": created,
        "lifecycle_event_id": lifecycle_event_id,
        "lifecycle_created": lifecycle_created,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Состояние внешнего действия №{external_action_id}: {args.state}; "
            + ("новая запись." if created else "идемпотентный повтор.")
        )


def quarantine_report_data(
    conn: sqlite3.Connection,
    *,
    page: int,
    page_size: int,
    status: str,
    classification: str,
) -> dict[str, Any]:
    if page < 1 or page_size < 1 or page_size > 500:
        raise ValueError("Номер страницы должен быть не меньше 1, а размер страницы — от 1 до 500.")
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        if status not in QUARANTINE_STATUSES:
            raise ValueError("Неподдерживаемое состояние записи карантина.")
        clauses.append("status = ?")
        params.append(status)
    if classification:
        if classification not in QUARANTINE_CLASSIFICATIONS:
            raise ValueError("Неподдерживаемая классификация записи карантина.")
        clauses.append("classification = ?")
        params.append(classification)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    total = int(
        conn.execute(
            "SELECT COUNT(*) FROM quarantine_records" + where, params
        ).fetchone()[0]
    )
    pages = max((total + page_size - 1) // page_size, 1)
    offset = (page - 1) * page_size
    rows = conn.execute(
        """
        SELECT id, origin_file, line_no, source_name, source_stream,
               classification, status, evidence_note, retry_context_json,
               retry_count, reprocessed_vacancy_id, created_at, updated_at
        FROM quarantine_records
        """
        + where
        + " ORDER BY id LIMIT ? OFFSET ?",
        [*params, page_size, offset],
    ).fetchall()
    counts = {
        str(row["classification"]): int(row["n"])
        for row in conn.execute(
            """
            SELECT classification, COUNT(*) AS n
            FROM quarantine_records
            WHERE status = 'pending'
            GROUP BY classification ORDER BY classification
            """
        ).fetchall()
    }
    return {
        "items": rows_to_dicts(rows),
        "pending_by_classification": counts,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": total,
            "total_pages": pages,
            "has_previous": page > 1,
            "has_next": page < pages,
        },
    }


def quarantine_report_command(args: argparse.Namespace) -> None:
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        report = quarantine_report_data(
            conn,
            page=args.page,
            page_size=args.page_size,
            status=args.status,
            classification=args.classification,
        )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    pagination = report["pagination"]
    print(
        f"Карантин импорта: страница {pagination['page']} из "
        f"{pagination['total_pages']}, записей {pagination['total_items']}."
    )
    for item in report["items"]:
        print(
            f"  №{item['id']} · {item['classification']} · {item['status']} · "
            f"{item['origin_file']}:{item['line_no']} · {item['evidence_note'] or ''}"
        )


def reprocess_quarantine_command(args: argparse.Namespace) -> None:
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        record = conn.execute(
            "SELECT * FROM quarantine_records WHERE id = ?", (args.quarantine_id,)
        ).fetchone()
        if not record:
            raise SystemExit("Запись карантина не найдена.")
        if record["status"] == "reprocessed":
            result = {
                "quarantine_id": args.quarantine_id,
                "status": "reprocessed",
                "vacancy_id": record["reprocessed_vacancy_id"],
                "created": False,
            }
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print("Запись уже была успешно обработана повторно.")
            return
        if args.replacement_json:
            replacement_payload = json.loads(
                args.replacement_json.read_text(encoding="utf-8")
            )
            replacement_items = payload_items(replacement_payload)
            if len(replacement_items) != 1 or not isinstance(replacement_items[0], dict):
                raise SystemExit("Файл замены должен содержать ровно один объект вакансии.")
            item = replacement_items[0]
        else:
            raw = json.loads(record["raw_payload_json"])
            if not isinstance(raw, dict):
                raise SystemExit("Для некорректного элемента требуется --replacement-json.")
            item = raw
        stats = {"ingested": 0, "quarantined": 0}
        vacancy_id = ingest_item(
            conn,
            item,
            default_channel=clean_cell(record["source_name"]),
            default_source=clean_cell(record["source_stream"]),
            origin_file=f"cli:reprocess-quarantine:{args.quarantine_id}",
            line_no=1,
            ingestion_stats=stats,
        )
        if vacancy_id:
            conn.execute(
                """
                UPDATE quarantine_records
                SET status = 'reprocessed', retry_count = retry_count + 1,
                    reprocessed_vacancy_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (vacancy_id, now_iso(), args.quarantine_id),
            )
            status = "reprocessed"
        else:
            conn.execute(
                """
                UPDATE quarantine_records
                SET retry_count = retry_count + 1, updated_at = ?
                WHERE id = ?
                """,
                (now_iso(), args.quarantine_id),
            )
            status = "pending"
        conn.commit()
    render_outputs(args.db)
    result = {
        "quarantine_id": args.quarantine_id,
        "status": status,
        "vacancy_id": vacancy_id,
        "created": bool(vacancy_id),
        "re_quarantined": stats["quarantined"],
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            "Повторная обработка завершена: "
            + (f"создана вакансия №{vacancy_id}." if vacancy_id else "запись осталась в карантине.")
        )


def legacy_classification_dry_run_command(args: argparse.Namespace) -> None:
    if not args.dry_run:
        raise SystemExit(
            "Классификация исторических строк доступна только как явный --dry-run; "
            "автоматическое массовое изменение запрещено."
        )
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        candidates: list[dict[str, Any]] = []
        for row in conn.execute("SELECT * FROM vacancies ORDER BY id").fetchall():
            item = {
                "title": row["title"],
                "company": row["company"],
                "url": row["url"],
                "status": row["latest_status"],
                "description": row["reason"],
            }
            classification, details = classify_ingestion_record(
                item, channel=clean_cell(row["channel"])
            )
            if classification:
                candidates.append(
                    {
                        "vacancy_id": int(row["id"]),
                        "classification": classification,
                        "details": details,
                    }
                )
    sample = candidates[: args.limit]
    result = {
        "dry_run": True,
        "selection_rule": "все исторические вакансии, не прошедшие текущую проверку обязательных полей источника",
        "candidate_count": len(candidates),
        "sample": sample,
        "mutation_performed": False,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Пробная классификация: кандидатов {len(candidates)}; "
            "изменения в базе не выполнялись."
        )


def build_false_negative_audit_data(
    conn: sqlite3.Connection,
    *,
    as_of: dt.date,
    sample_size: int,
    seed: str,
) -> dict[str, Any]:
    if sample_size < 1:
        raise ValueError("Размер выборки должен быть положительным числом.")
    if not clean_cell(seed):
        raise ValueError("Ключ воспроизводимости не должен быть пустым.")
    active = conn.execute(
        "SELECT version, effective_date FROM policy_versions WHERE is_active = 1"
    ).fetchall()
    if len(active) != 1:
        raise RuntimeError("Должна быть активна ровно одна версия политики отбора.")
    policy_version = str(active[0]["version"])
    rows = conn.execute(
        """
        SELECT current.*, v.company, v.title
        FROM screening_decisions current
        JOIN vacancies v ON v.id = current.vacancy_id
        WHERE current.policy_version = ?
          AND current.decision IN ('rejected', 'low_priority')
          AND current.evaluated_at <= ?
          AND current.id = (
              SELECT candidate.id FROM screening_decisions candidate
              WHERE candidate.vacancy_id = current.vacancy_id
                AND candidate.policy_version = current.policy_version
              ORDER BY candidate.evaluated_at DESC, candidate.id DESC LIMIT 1
          )
        """,
        (policy_version, as_of.isoformat() + "T23:59:59"),
    ).fetchall()
    ordered = sorted(
        rows,
        key=lambda row: (
            hashlib.sha256(
                f"{seed}:{int(row['vacancy_id'])}:{policy_version}".encode("utf-8")
            ).hexdigest(),
            int(row["vacancy_id"]),
        ),
    )
    selected = ordered[:sample_size]
    rule_counts: Counter[str] = Counter()
    sample: list[dict[str, Any]] = []
    for row in selected:
        rules = json.loads(row["rule_results_json"] or "[]")
        matched_rules = [
            clean_cell(str(rule.get("rule_key") or ""))
            for rule in rules
            if isinstance(rule, dict) and rule.get("result") == "matched"
        ]
        if not matched_rules:
            matched_rules = ["unknown_structured_rule"]
        rule_counts.update(matched_rules)
        later_positive = conn.execute(
            """
            SELECT 1 FROM lifecycle_events
            WHERE vacancy_id = ? AND event_at >= ?
              AND event_type IN (
                  'application_confirmed', 'interview_invited',
                  'interview_scheduled', 'interview_completed', 'offer_received'
              )
            LIMIT 1
            """,
            (int(row["vacancy_id"]), row["evaluated_at"]),
        ).fetchone()
        sample.append(
            {
                "vacancy_id": int(row["vacancy_id"]),
                "decision": row["decision"],
                "score": row["score"],
                "company": row["company"] or "",
                "title": row["title"] or "",
                "matched_rules": matched_rules,
                "later_positive_evidence": bool(later_positive),
            }
        )
    high_volume_threshold = max(2, (len(selected) + 4) // 5) if selected else 2
    rule_summary = [
        {
            "rule_key": key,
            "count": count,
            "sample_size": len(selected),
            "requires_review": count >= high_volume_threshold,
        }
        for key, count in sorted(rule_counts.items(), key=lambda pair: (-pair[1], pair[0]))
    ]
    return {
        "as_of": as_of.isoformat(),
        "policy_version": policy_version,
        "policy_effective_date": active[0]["effective_date"],
        "seed": seed,
        "population_size": len(rows),
        "sample_size": len(selected),
        "sample": sample,
        "rule_counts": rule_summary,
        "high_volume_threshold": high_volume_threshold,
        "methodology": "Детерминированная SHA-256 выборка отклонённых и низкоприоритетных вакансий по активной версии политики.",
    }


def false_negative_audit_command(args: argparse.Namespace) -> None:
    as_of = parse_iso_date(args.as_of or dt.date.today().isoformat(), label="--as-of")
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        report = build_false_negative_audit_data(
            conn,
            as_of=as_of,
            sample_size=args.sample_size,
            seed=args.seed,
        )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(
        f"Аудит пропущенных возможностей: политика {report['policy_version']}, "
        f"выборка {report['sample_size']}/{report['population_size']}."
    )
    for rule in report["rule_counts"]:
        marker = "требует проверки" if rule["requires_review"] else "наблюдение"
        print(f"  {rule['rule_key']}: {rule['count']}/{rule['sample_size']} · {marker}")


def resolve_employer_account(
    conn: sqlite3.Connection,
    *,
    account_id: int | None,
    account_name: str,
) -> sqlite3.Row:
    if account_id is not None:
        row = conn.execute(
            "SELECT * FROM employer_accounts WHERE id = ?", (account_id,)
        ).fetchone()
        if not row:
            raise SystemExit(f"Аккаунт работодателя №{account_id} не найден.")
        return row
    normalized = normalized_account_name(account_name)
    if not normalized:
        raise SystemExit("Укажите --account-id или --account-name.")
    row = conn.execute(
        "SELECT * FROM employer_accounts WHERE normalized_name = ?", (normalized,)
    ).fetchone()
    if not row:
        raise SystemExit(f"Аккаунт работодателя {account_name!r} не найден по точному нормализованному имени.")
    return row


def upsert_employer_account(args: argparse.Namespace) -> None:
    canonical_name = clean_cell(args.canonical_name)
    if not canonical_name:
        raise SystemExit("Требуется --canonical-name.")
    normalized_name = normalized_account_name(canonical_name)
    website = validate_optional_external_url(args.website or "", label="--website")
    careers_url = validate_optional_external_url(
        args.careers_url or "", label="--careers-url"
    )
    for value, label in (
        (args.last_checked_date, "--last-checked-date"),
        (args.next_review_date, "--next-review-date"),
        (args.website_checked_date, "--website-checked-date"),
        (args.careers_checked_date, "--careers-checked-date"),
    ):
        if value:
            parse_iso_date(value, label=label)
    if args.portfolio_limit is not None and args.portfolio_limit < 1:
        raise SystemExit("--portfolio-limit должен быть положительным числом.")
    if args.review_cadence_days is not None and args.review_cadence_days < 1:
        raise SystemExit("--review-cadence-days должен быть положительным числом.")
    target_campaigns = [
        configured_value(value, DECISION_CAMPAIGN_IDS, label="target_campaigns")
        for value in (args.target_campaigns or "").split(",")
        if value.strip()
    ]
    target_role_families = [
        configured_value(value, DECISION_ROLE_FAMILIES, label="target_role_families")
        for value in (args.target_role_families or "").split(",")
        if value.strip()
    ]
    if args.human_path_status and args.human_path_status not in HUMAN_PATH_STATUSES:
        raise SystemExit("Неподдерживаемый статус пути к человеку.")
    now = now_iso()
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        row = None
        if args.account_id is not None:
            row = conn.execute(
                "SELECT * FROM employer_accounts WHERE id = ?", (args.account_id,)
            ).fetchone()
            if not row:
                raise SystemExit(f"Аккаунт работодателя №{args.account_id} не найден.")
            conflict = conn.execute(
                "SELECT id FROM employer_accounts WHERE normalized_name = ? AND id != ?",
                (normalized_name, args.account_id),
            ).fetchone()
            if conflict:
                raise SystemExit("Другой аккаунт работодателя уже имеет такое нормализованное имя.")
        else:
            row = conn.execute(
                "SELECT * FROM employer_accounts WHERE normalized_name = ?",
                (normalized_name,),
            ).fetchone()
        created = row is None
        if created:
            cur = conn.execute(
                """
                INSERT INTO employer_accounts (
                    canonical_name, normalized_name, website, careers_url,
                    country_market, priority, status, last_checked_date,
                    notes, portfolio_limit, review_cadence_days, next_review_date,
                    website_checked_date, careers_checked_date,
                    target_campaigns_json, target_role_families_json,
                    owner_evidence, sponsor_evidence, governance_evidence,
                    human_path_status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    canonical_name,
                    normalized_name,
                    website,
                    careers_url,
                    clean_cell(args.country_market or ""),
                    clean_cell(args.priority or ""),
                    clean_cell(args.account_status or ""),
                    clean_cell(args.last_checked_date or ""),
                    clean_cell(args.notes or ""),
                    args.portfolio_limit or ACCOUNT_ACTIVE_PORTFOLIO_LIMIT,
                    args.review_cadence_days,
                    clean_cell(args.next_review_date or ""),
                    clean_cell(args.website_checked_date or ""),
                    clean_cell(args.careers_checked_date or ""),
                    json.dumps(target_campaigns, ensure_ascii=False),
                    json.dumps(target_role_families, ensure_ascii=False),
                    clean_cell(args.owner_evidence or ""),
                    clean_cell(args.sponsor_evidence or ""),
                    clean_cell(args.governance_evidence or ""),
                    clean_cell(args.human_path_status or "unknown"),
                    now,
                    now,
                ),
            )
            account_id = int(cur.lastrowid)
        else:
            account_id = int(row["id"])

            def selected(column: str, value: str | None) -> str:
                return clean_cell(value) if value is not None else clean_cell(row[column])

            conn.execute(
                """
                UPDATE employer_accounts
                SET canonical_name = ?, normalized_name = ?, website = ?,
                    careers_url = ?, country_market = ?, priority = ?, status = ?,
                    last_checked_date = ?, notes = ?, portfolio_limit = ?,
                    review_cadence_days = ?, next_review_date = ?,
                    website_checked_date = ?, careers_checked_date = ?,
                    target_campaigns_json = ?, target_role_families_json = ?,
                    owner_evidence = ?, sponsor_evidence = ?,
                    governance_evidence = ?, human_path_status = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    canonical_name,
                    normalized_name,
                    website if args.website is not None else row["website"],
                    careers_url if args.careers_url is not None else row["careers_url"],
                    selected("country_market", args.country_market),
                    selected("priority", args.priority),
                    selected("status", args.account_status),
                    selected("last_checked_date", args.last_checked_date),
                    selected("notes", args.notes),
                    args.portfolio_limit
                    if args.portfolio_limit is not None
                    else row["portfolio_limit"],
                    args.review_cadence_days
                    if args.review_cadence_days is not None
                    else row["review_cadence_days"],
                    selected("next_review_date", args.next_review_date),
                    selected("website_checked_date", args.website_checked_date),
                    selected("careers_checked_date", args.careers_checked_date),
                    json.dumps(target_campaigns, ensure_ascii=False)
                    if args.target_campaigns is not None
                    else row["target_campaigns_json"],
                    json.dumps(target_role_families, ensure_ascii=False)
                    if args.target_role_families is not None
                    else row["target_role_families_json"],
                    selected("owner_evidence", args.owner_evidence),
                    selected("sponsor_evidence", args.sponsor_evidence),
                    selected("governance_evidence", args.governance_evidence),
                    selected("human_path_status", args.human_path_status),
                    now,
                    account_id,
                ),
            )
        conn.commit()
    render_outputs(args.db)
    result = {"account_id": account_id, "created": created, "canonical_name": canonical_name}
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Аккаунт работодателя №{account_id} "
            f"{'создан' if created else 'обновлён'}: {canonical_name}."
        )


def record_employer_signal(args: argparse.Namespace) -> None:
    evidence_note = clean_cell(args.evidence_note)
    if not evidence_note:
        raise SystemExit("Требуется --evidence-note.")
    observed_date = args.observed_date or dt.date.today().isoformat()
    parse_iso_date(observed_date, label="--observed-date")
    evidence_url = validate_optional_external_url(
        args.evidence_url, label="--evidence-url"
    )
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        account = resolve_employer_account(
            conn, account_id=args.account_id, account_name=args.account_name
        )
        account_id = int(account["id"])
        before = conn.total_changes
        conn.execute(
            """
            INSERT OR IGNORE INTO employer_account_signals (
                account_id, signal_type, observed_date, confidence,
                evidence_url, evidence_note, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                args.signal_type,
                observed_date,
                args.confidence,
                evidence_url,
                evidence_note,
                now_iso(),
            ),
        )
        created = conn.total_changes > before
        signal = conn.execute(
            """
            SELECT id FROM employer_account_signals
            WHERE account_id = ? AND signal_type = ? AND observed_date = ?
              AND evidence_url = ? AND evidence_note = ?
            """,
            (account_id, args.signal_type, observed_date, evidence_url, evidence_note),
        ).fetchone()
        signal_id = int(signal["id"])
        conn.commit()
    render_outputs(args.db)
    result = {"signal_id": signal_id, "account_id": account_id, "created": created}
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Сигнал работодателя №{signal_id} "
            f"{'записан' if created else 'уже существовал'}."
        )


def link_vacancy_account(args: argparse.Namespace) -> None:
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy = resolve_vacancy_row(conn, args)
        account = resolve_employer_account(
            conn, account_id=args.account_id, account_name=args.account_name
        )
        vacancy_id = int(vacancy["id"])
        account_id = int(account["id"])
        existing = conn.execute(
            "SELECT account_id FROM vacancy_employer_accounts WHERE vacancy_id = ?",
            (vacancy_id,),
        ).fetchone()
        if existing and int(existing["account_id"]) != account_id:
            raise SystemExit(
                f"Вакансия №{vacancy_id} уже связана с аккаунтом №{existing['account_id']}; "
                "смена связи требует отдельного явного процесса."
            )
        created = existing is None
        if created:
            now = now_iso()
            conn.execute(
                """
                INSERT INTO vacancy_employer_accounts (
                    vacancy_id, account_id, link_method, evidence_note,
                    created_at, updated_at
                )
                VALUES (?, ?, 'explicit', ?, ?, ?)
                """,
                (vacancy_id, account_id, clean_cell(args.evidence_note), now, now),
            )
        conn.commit()
    render_outputs(args.db)
    result = {"vacancy_id": vacancy_id, "account_id": account_id, "created": created}
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Связь вакансии №{vacancy_id} с аккаунтом №{account_id} "
            f"{'создана' if created else 'сохранена без изменений'}."
        )


def record_vacancy_factor(args: argparse.Namespace) -> None:
    observed_date = args.observed_date or dt.date.today().isoformat()
    factor = {
        "factor_key": args.factor_key,
        "value": args.value,
        "observed_date": observed_date,
        "evidence_note": args.evidence_note,
        "evidence_url": args.evidence_url,
        "confidence": args.confidence,
    }
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        vacancy = resolve_vacancy_row(conn, args)
        vacancy_id = int(vacancy["id"])
        created = insert_vacancy_factor_once(
            conn,
            vacancy_id=vacancy_id,
            factor=factor,
            default_date=observed_date,
        )
        conn.commit()
    render_outputs(args.db)
    result = {"vacancy_id": vacancy_id, "factor_key": args.factor_key, "created": created}
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"Фактор {args.factor_key} для вакансии №{vacancy_id} "
            f"{'записан' if created else 'уже существовал'}."
        )


def upsert_employer_contact(args: argparse.Namespace) -> None:
    channel = normalize_outreach_channel(args.contact_channel)
    if channel not in DIRECT_OUTREACH_CHANNELS:
        allowed = ", ".join(DIRECT_OUTREACH_CHANNELS)
        raise SystemExit(f"Канал контакта должен иметь одно из значений: {allowed}.")
    if args.relationship not in CONTACT_RELATIONSHIPS:
        raise SystemExit("Неподдерживаемый тип связи с контактом.")
    if args.confidence not in CONTACT_CONFIDENCE:
        raise SystemExit("Неподдерживаемый уровень уверенности в контакте.")
    if not clean_cell(args.evidence_url) and not clean_cell(args.evidence_note):
        raise SystemExit("Для подтверждения личности контакта укажите --evidence-url или --evidence-note.")

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
        f"Контакт работодателя №{contact['id']} сохранён для вакансии №{vacancy_id} "
        f"({channel}, {args.confidence}). Обновлён файл "
        f"{display_path(DASHBOARD_PATH, ROOT)}."
    )


def record_contact_search(args: argparse.Namespace) -> None:
    channels_checked = unique_outreach_channels(args.channels_checked.split(","))
    if not channels_checked:
        allowed = ", ".join(DIRECT_OUTREACH_CHANNELS)
        raise SystemExit(f"В --channels-checked требуется хотя бы одно из значений: {allowed}.")
    if args.search_status not in CONTACT_SEARCH_STATUSES:
        raise SystemExit("Неподдерживаемое состояние поиска контакта.")

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
        f"Поиск контакта №{cur.lastrowid} записан для вакансии №{vacancy_id}. "
        f"Обновлён файл {display_path(DASHBOARD_PATH, ROOT)}."
    )


def load_outreach_payload(path: Path) -> dict[str, Any]:
    candidate = path if path.is_absolute() else ROOT / path
    if not candidate.exists():
        raise FileNotFoundError(f"JSON-файл с данными обращения не найден: {candidate}")
    payload = json.loads(candidate.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JSON с данными обращения должен быть объектом.")
    if not isinstance(payload.get("contact_search"), dict):
        raise ValueError("JSON с данными обращения должен содержать объект contact_search.")
    if not isinstance(payload.get("touchpoints"), list) or not payload["touchpoints"]:
        raise ValueError("JSON с данными обращения должен содержать непустой массив touchpoints.")
    return payload


def record_followup(args: argparse.Namespace) -> None:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Укажите вакансию через --id, --url или --external-id.")
    if not args.outreach_json:
        raise SystemExit(
            "Требуется --outreach-json, чтобы сохранить поиск контактов, каналы, "
            "точные сообщения и видимые доказательства доставки."
        )

    payload = load_outreach_payload(args.outreach_json)
    contact_search = payload["contact_search"]
    search_status = clean_cell(str(contact_search.get("status") or "")).lower()
    if search_status not in CONTACT_SEARCH_STATUSES:
        allowed = ", ".join(sorted(CONTACT_SEARCH_STATUSES))
        raise SystemExit(f"Поле contact_search.status должно иметь одно из значений: {allowed}.")
    raw_checked = contact_search.get("channels_checked") or []
    if not isinstance(raw_checked, list):
        raise SystemExit("Поле contact_search.channels_checked должно быть массивом.")
    channels_checked = unique_outreach_channels(
        [str(value) for value in raw_checked], FOLLOW_UP_CHANNELS
    )
    if not channels_checked:
        raise SystemExit(
            "Поле contact_search.channels_checked должно содержать хотя бы один "
            "настроенный канал обращения."
        )
    research_note = clean_cell(str(contact_search.get("note") or ""))
    if not research_note:
        raise SystemExit("Требуется поле contact_search.note.")

    event_date = args.date or dt.date.today().isoformat()
    with connect_db(args.db) as conn:
        ensure_schema(conn)
        row = resolve_vacancy_row(conn, args)

        vacancy_id = int(row["id"])
        current_stage = canonical_stage(row["latest_stage"])
        if current_stage not in {"applied", "follow_up"}:
            raise SystemExit(
                f"Вакансия №{vacancy_id} находится на этапе {current_stage}; "
                "команда record-followup допустима только для applied или follow_up."
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
                f"Вакансия №{vacancy_id} уже достигла лимита повторных обращений: {FOLLOW_UP_LIMIT}."
            )
        if any(date == event_date and number > 0 for date, number in numbered_events):
            raise SystemExit(
                f"Для вакансии №{vacancy_id} уже записано повторное обращение на {event_date}."
            )
        duplicate_round_date = conn.execute(
            "SELECT 1 FROM followup_rounds WHERE vacancy_id = ? AND sent_date = ? LIMIT 1",
            (vacancy_id, event_date),
        ).fetchone()
        if duplicate_round_date:
            raise SystemExit(
                f"Для вакансии №{vacancy_id} уже записан структурированный раунд повторного обращения на {event_date}."
            )

        prepared_touchpoints: list[dict[str, Any]] = []
        seen_channels: set[str] = set()
        sent_channels: list[str] = []
        sent_direct_count = 0
        for index, raw_touchpoint in enumerate(payload["touchpoints"], start=1):
            if not isinstance(raw_touchpoint, dict):
                raise SystemExit(f"Элемент touchpoints[{index}] должен быть объектом.")
            channel = normalize_outreach_channel(str(raw_touchpoint.get("channel") or ""))
            if channel not in FOLLOW_UP_CHANNELS:
                allowed = ", ".join(FOLLOW_UP_CHANNELS)
                raise SystemExit(f"Поле touchpoints[{index}].channel должно иметь одно из значений: {allowed}.")
            if channel in seen_channels:
                raise SystemExit(f"В одном раунде допускается только одна точка контакта для канала {channel}.")
            seen_channels.add(channel)

            delivery_status = clean_cell(
                str(raw_touchpoint.get("delivery_status") or "sent")
            ).lower()
            if delivery_status not in OUTREACH_DELIVERY_STATUSES:
                raise SystemExit(f"Неподдерживаемое состояние доставки: {delivery_status}.")
            message_text = str(
                raw_touchpoint.get("message_text") or raw_touchpoint.get("message") or ""
            ).strip()
            evidence_note = clean_cell(str(raw_touchpoint.get("evidence_note") or ""))
            if delivery_status == "sent" and (not message_text or not evidence_note):
                raise SystemExit(
                    f"Для отправленной точки контакта {channel} требуются точные message_text и evidence_note."
                )

            external_action_id: int | None = None
            if delivery_status == "sent":
                external_action_key = clean_cell(
                    str(raw_touchpoint.get("external_action_key") or "")
                )
                if not external_action_key:
                    raise SystemExit(
                        f"Отправленная точка контакта {channel} требует "
                        "external_action_key с отдельным разрешением и видимым подтверждением."
                    )
                external_action = conn.execute(
                    """
                    SELECT id FROM external_actions
                    WHERE vacancy_id = ? AND action_key = ?
                      AND action_type IN ('message', 'follow_up')
                      AND state = 'visibly_confirmed'
                    ORDER BY event_at DESC, id DESC LIMIT 1
                    """,
                    (vacancy_id, external_action_key),
                ).fetchone()
                if not external_action:
                    raise SystemExit(
                        f"Нет явно разрешённого и видимо подтверждённого внешнего "
                        f"действия {external_action_key!r} для вакансии №{vacancy_id}."
                    )
                external_action_id = int(external_action["id"])

            contact_id: int | None = None
            recipient_name = clean_cell(str(raw_touchpoint.get("recipient_name") or ""))
            recipient_address = clean_cell(str(raw_touchpoint.get("recipient_address") or ""))
            if channel in DIRECT_OUTREACH_CHANNELS:
                try:
                    contact_id = int(raw_touchpoint.get("contact_id"))
                except (TypeError, ValueError):
                    raise SystemExit(
                        f"Для прямой точки контакта {channel} требуется contact_id из upsert-contact."
                    )
                contact = conn.execute(
                    "SELECT * FROM employer_contacts WHERE id = ?",
                    (contact_id,),
                ).fetchone()
                if not contact or int(contact["vacancy_id"]) != vacancy_id:
                    raise SystemExit(f"Контакт №{contact_id} не относится к вакансии №{vacancy_id}.")
                if not int(contact["is_active"]):
                    raise SystemExit(f"Контакт №{contact_id} неактивен.")
                if contact["channel"] != channel:
                    raise SystemExit(
                        f"Контакт №{contact_id} сохранён для канала {contact['channel']}, а не {channel}."
                    )
                if delivery_status == "sent" and contact["confidence"] not in DIRECT_SEND_CONFIDENCE:
                    raise SystemExit(
                        f"У контакта №{contact_id} уровень уверенности {contact['confidence']}; "
                        "для отправки требуется confirmed или strong."
                    )
                recipient_name = contact["person_name"]
                recipient_address = contact["contact_address"]
                if delivery_status == "sent":
                    sent_direct_count += 1
            else:
                primary_label = CHANNEL_LABELS.get(
                    PRIMARY_OUTREACH_CHANNEL, PRIMARY_OUTREACH_CHANNEL.title()
                )
                recipient_name = recipient_name or f"Обсуждение вакансии в {primary_label}"
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
                    "external_action_id": external_action_id,
                }
            )

        if not sent_channels:
            raise SystemExit("Раунд повторного обращения должен содержать хотя бы одну отправленную точку контакта.")
        if sent_direct_count > MAX_DIRECT_MESSAGES_PER_ROUND:
            preferred = ", затем ".join(
                CHANNEL_LABELS.get(channel, channel.title())
                for channel in DIRECT_OUTREACH_CHANNELS
            )
            raise SystemExit(
                f"В одном раунде допускается не более {MAX_DIRECT_MESSAGES_PER_ROUND} "
                f"отправленных прямых каналов; настроенный порядок: {preferred}."
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
                    delivery_status, evidence_note, sent_at, external_action_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    touchpoint["external_action_id"],
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
        f"Раунд повторного обращения №{follow_up_number} записан для вакансии №{vacancy_id} "
        f"через {', '.join(sent_channels)}; поиск контакта №{contact_search_cur.lastrowid}. "
        f"Обновлён файл {display_path(DASHBOARD_PATH, ROOT)}."
    )


def attach_interview_summary(args: argparse.Namespace) -> None:
    if args.id is None and not args.url and not args.external_id:
        raise SystemExit("Укажите вакансию через --id, --url или --external-id.")

    file_path = project_relative_file_path(args.file)
    date = args.date or dt.date.today().isoformat()
    completion_note = clean_cell(args.completion_evidence_note or args.note)
    if args.confirms_completion:
        content = (ROOT / file_path).read_text(encoding="utf-8")
        meaningful_lines = [line.strip() for line in content.splitlines() if line.strip()]
        meaningful_chars = len(re.sub(r"\s+", "", content))
        if meaningful_chars < 80 or len(meaningful_lines) < 3:
            raise SystemExit(
                "Резюме интервью не соответствует правилу завершения: требуется "
                "не менее 80 непробельных знаков и трёх содержательных строк."
            )
        if not completion_note:
            raise SystemExit(
                "Для подтверждения завершения требуется --completion-evidence-note."
            )

    with connect_db(args.db) as conn:
        ensure_schema(conn)
        row = resolve_vacancy_row(conn, args)

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
            raise SystemExit("Параметр --interview-no должен быть положительным целым числом.")

        stage = args.stage or f"interview_{interview_no}"
        title = args.title or f"Интервью {interview_no}"
        now = now_iso()
        conn.execute(
            """
            INSERT INTO interview_summaries (
                vacancy_id, interview_no, stage, summary_date, title, file_path,
                note, confirms_completion, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(vacancy_id, interview_no, file_path) DO UPDATE SET
                stage = excluded.stage,
                summary_date = excluded.summary_date,
                title = excluded.title,
                note = excluded.note,
                confirms_completion = excluded.confirms_completion,
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
                1 if args.confirms_completion else 0,
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
        completion_lifecycle_event_id: int | None = None
        if args.confirms_completion:
            completion_lifecycle_event_id, _ = append_lifecycle_event(
                conn,
                vacancy_id=vacancy_id,
                event_type="interview_completed",
                event_at=date,
                evidence_at=now,
                evidence_note=completion_note,
                evidence_source="interview_summary",
                origin="cli:attach-interview-summary",
                external_reference=f"interview-summary:{file_path}:{interview_no}",
                round_no=interview_no,
                history_complete=True,
                authorization_status="not_applicable",
            )
            conn.execute(
                """
                UPDATE interview_summaries
                SET completion_lifecycle_event_id = ?
                WHERE id = ?
                """,
                (completion_lifecycle_event_id, int(summary_row["id"])),
            )
        conn.commit()

    render_outputs(args.db)
    completion_text = (
        f" Завершение подтверждено событием №{completion_lifecycle_event_id}."
        if args.confirms_completion
        else " Завершение интервью не заявлено."
    )
    print(
        f"Резюме интервью №{summary_row['id']} связано с вакансией №{vacancy_id}."
        f"{completion_text} Обновлён файл {display_path(DASHBOARD_PATH, ROOT)}."
    )


def initialize_workspace(args: argparse.Namespace) -> None:
    """Create local-only settings, profile templates, and an empty database."""

    created: list[str] = []
    kept: list[str] = []
    settings_template = CODE_ROOT / "config" / "settings.example.toml"
    if not settings_template.exists():
        raise FileNotFoundError(f"Не найден шаблон настроек: {settings_template}")

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
            raise FileNotFoundError(f"Не найден шаблон рабочей области: {source}")
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
    print(f"Рабочая область подготовлена: {ROOT}")
    if created:
        print("  создано: " + ", ".join(created))
    if kept:
        print("  сохранено без изменений: " + ", ".join(kept))
    print(f"  панель: {display_path(DASHBOARD_PATH, ROOT)}")


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
        else "Локальные настройки отсутствуют; действуют безопасные встроенные значения.",
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
            "файл доступен" if profile_path.is_file() else "файл отсутствует",
        )

    if DB_PATH.exists():
        try:
            uri = f"file:{DB_PATH.as_posix()}?mode=ro"
            with sqlite3.connect(uri, uri=True) as conn:
                quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
                schema_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
                missing_tables = missing_schema_tables(conn)
                missing_indexes = missing_schema_indexes(conn)
                alias_schema_issues = vacancy_external_alias_schema_issues(conn)
                v4_contract_issues = schema_v4_issues(conn)
                v5_contract_issues = schema_v5_issues(conn)
                v6_contract_issues = schema_v6_issues(conn)
                v7_contract_issues = schema_v7_issues(conn)
                foreign_key_issues = len(conn.execute("PRAGMA foreign_key_check").fetchall())
                missing_canonical_streams = (
                    int(
                        conn.execute(
                            """
                            SELECT COUNT(*) FROM source_hits
                            WHERE TRIM(COALESCE(canonical_source_stream, '')) = ''
                            """
                        ).fetchone()[0]
                    )
                    if not v4_contract_issues
                    else -1
                )
                canonical_aliases_missing = (
                    int(
                        conn.execute(
                            """
                            SELECT COUNT(*)
                            FROM vacancies v
                            LEFT JOIN vacancy_external_aliases a
                              ON a.vacancy_id = v.id
                             AND a.channel = v.channel
                             AND a.external_id = v.external_id
                            WHERE a.id IS NULL
                            """
                        ).fetchone()[0]
                    )
                    if "vacancy_external_aliases" not in missing_tables
                    else -1
                )
                ambiguous_aliases = (
                    int(
                        conn.execute(
                            """
                            SELECT COUNT(*)
                            FROM (
                                SELECT external_id
                                FROM vacancy_external_aliases
                                GROUP BY external_id
                                HAVING COUNT(DISTINCT vacancy_id) > 1
                            )
                            """
                        ).fetchone()[0]
                    )
                    if "vacancy_external_aliases" not in missing_tables
                    else -1
                )
            add(
                "database_integrity",
                quick_check == "ok",
                "Быстрая проверка SQLite пройдена."
                if quick_check == "ok"
                else f"Результат быстрой проверки SQLite: {quick_check}",
            )
            add(
                "database_schema",
                schema_version == SCHEMA_VERSION and not missing_tables,
                (
                    f"версия базы: {schema_version}; поддерживаемая версия: "
                    f"{SCHEMA_VERSION}; отсутствующие таблицы: "
                    f"{','.join(missing_tables) or 'нет'}"
                ),
            )
            add(
                "database_indexes",
                not missing_indexes,
                f"Отсутствующие индексы: {','.join(missing_indexes) or 'нет'}.",
            )
            add(
                "database_external_alias_schema",
                not alias_schema_issues,
                "; ".join(alias_schema_issues) or "Схема корректна.",
            )
            add(
                "database_schema_v4_contract",
                not v4_contract_issues,
                "; ".join(v4_contract_issues) or "Контракт v4 соблюдён.",
            )
            add(
                "database_schema_v5_contract",
                not v5_contract_issues,
                "; ".join(v5_contract_issues) or "Контракт v5 соблюдён.",
            )
            add(
                "database_schema_v6_contract",
                not v6_contract_issues,
                "; ".join(v6_contract_issues) or "Контракт v6 соблюдён.",
            )
            add(
                "database_schema_v7_contract",
                not v7_contract_issues,
                "; ".join(v7_contract_issues) or "Контракт v7 соблюдён.",
            )
            add(
                "database_canonical_source_streams",
                missing_canonical_streams == 0,
                f"Записей без канонического потока: {missing_canonical_streams}.",
            )
            add(
                "database_canonical_aliases",
                canonical_aliases_missing == 0 and ambiguous_aliases == 0,
                (
                    f"Вакансий без канонического псевдонима: {canonical_aliases_missing}; "
                    f"неоднозначных внешних идентификаторов: {ambiguous_aliases}."
                ),
            )
            add(
                "database_foreign_keys",
                foreign_key_issues == 0,
                f"Нарушений внешних ключей: {foreign_key_issues}.",
            )
        except sqlite3.Error as exc:
            add("database_integrity", False, str(exc))
    else:
        add(
            "database",
            False,
            f"База данных отсутствует: {display_path(DB_PATH, ROOT)}",
        )

    add(
        "automation_confirmation",
        SETTINGS.automation.require_visible_confirmation,
        "Видимое внешнее подтверждение обязательно."
        if SETTINGS.automation.require_visible_confirmation
        else "Требование видимого внешнего подтверждения выключено.",
    )
    add(
        "search_streams",
        bool(REQUIRED_SEARCH_STREAMS),
        ", ".join(REQUIRED_SEARCH_STREAMS) or "Обязательные потоки не настроены.",
    )
    telegram_handles = [channel.handle for channel in TELEGRAM_CHANNELS]
    add(
        "telegram_sources",
        not TELEGRAM_ENABLED or bool(telegram_handles),
        (
            "Источники Telegram выключены."
            if not TELEGRAM_ENABLED
            else (
                f"Источники Telegram включены; начальный период: "
                f"{TELEGRAM_INITIAL_LOOKBACK_DAYS} дн.; "
                + ", ".join(telegram_handles)
            )
        ),
    )

    failed = [check for check in checks if check["required"] and not check["ok"]]
    result = {
        "ok": not failed,
        "workspace": str(ROOT),
        "config": display_path(SETTINGS.config_path, ROOT),
        "auto_apply": SETTINGS.automation.auto_apply,
        "apply_threshold": SETTINGS.automation.apply_threshold,
        "scan_linkedin_inbox": SETTINGS.mail.scan_linkedin_inbox,
        "archive_processed_linkedin": SETTINGS.mail.archive_processed_linkedin,
        "telegram_enabled": TELEGRAM_ENABLED,
        "telegram_initial_lookback_days": TELEGRAM_INITIAL_LOOKBACK_DAYS,
        "telegram_channels": telegram_handles,
        "checks": checks,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        for check in checks:
            marker = "ПРОЙДЕНО" if check["ok"] else "ОШИБКА"
            print(f"[{marker}] {check['name']}: {check['detail']}")
        print(
            "Рабочая область готова."
            if not failed
            else "Рабочая область требует внимания."
        )
    if args.strict and failed:
        raise SystemExit(1)


def latest_completed_coverage(
    conn: sqlite3.Connection, source: str
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM search_runs
        WHERE source = ? AND status = 'completed'
        ORDER BY run_date DESC, id DESC LIMIT 1
        """,
        (source,),
    ).fetchone()


def operational_doctor(args: argparse.Namespace) -> None:
    """Separate structural health from fail-closed daily closeout readiness."""

    as_of = parse_iso_date(args.as_of or dt.date.today().isoformat(), label="--as-of")
    checks: list[dict[str, Any]] = []

    def add(
        name: str,
        status: str,
        detail: str,
        *,
        blocks_closeout: bool = False,
        technical: bool = False,
    ) -> None:
        if status not in {"pass", "warn", "fail"}:
            raise ValueError("Состояние проверки операционного доктора должно быть pass, warn или fail.")
        checks.append(
            {
                "name": name,
                "status": status,
                "detail": detail,
                "blocks_closeout": blocks_closeout,
                "technical": technical,
            }
        )

    required_paths = [SETTINGS.config_path, *SETTINGS.profile.all_files()]
    missing_paths = [display_path(path, ROOT) for path in required_paths if not path.is_file()]
    add(
        "required_workspace_files",
        "pass" if not missing_paths else "fail",
        "Все обязательные файлы рабочей области доступны."
        if not missing_paths
        else "Отсутствуют обязательные файлы: " + ", ".join(missing_paths),
        blocks_closeout=True,
        technical=True,
    )
    safe_defaults = (
        not SETTINGS.automation.auto_apply
        and SETTINGS.automation.require_visible_confirmation
    )
    add(
        "safe_external_action_defaults",
        "pass" if safe_defaults else "fail",
        "Автоматические внешние действия выключены; видимое подтверждение обязательно."
        if safe_defaults
        else "Небезопасная политика: требуется auto_apply=false и require_visible_confirmation=true.",
        blocks_closeout=True,
    )
    add(
        "configured_resume_identifiers",
        "pass" if DECISION_RESUME_IDS else "warn",
        "Настроенные идентификаторы резюме: "
        + (", ".join(DECISION_RESUME_IDS) if DECISION_RESUME_IDS else "не заданы; полнота исходов будет н/д"),
    )

    if not DB_PATH.exists():
        add(
            "database",
            "fail",
            f"База данных отсутствует: {display_path(DB_PATH, ROOT)}.",
            blocks_closeout=True,
            technical=True,
        )
    else:
        try:
            with connect_db(args.db) as conn:
                ensure_schema(conn)
                quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
                foreign_keys = len(conn.execute("PRAGMA foreign_key_check").fetchall())
                schema_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
                add(
                    "database_quick_check",
                    "pass" if quick_check == "ok" else "fail",
                    "Результат проверки целостности SQLite quick_check: " + quick_check + ".",
                    blocks_closeout=True,
                    technical=True,
                )
                add(
                    "database_foreign_keys",
                    "pass" if foreign_keys == 0 else "fail",
                    f"Нарушений внешних ключей: {foreign_keys}.",
                    blocks_closeout=True,
                    technical=True,
                )
                add(
                    "schema_version",
                    "pass" if schema_version == SCHEMA_VERSION else "fail",
                    f"Версия базы: {schema_version}; поддерживается: {SCHEMA_VERSION}.",
                    blocks_closeout=True,
                    technical=True,
                )

                orphan_applications = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM applications a
                        LEFT JOIN lifecycle_events le ON le.id = a.lifecycle_event_id
                        WHERE a.lifecycle_event_id IS NOT NULL
                          AND (le.id IS NULL OR le.vacancy_id != a.vacancy_id)
                        """
                    ).fetchone()[0]
                )
                confirmed_without_action = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM lifecycle_events
                        WHERE event_type = 'application_confirmed'
                          AND authorization_status = 'explicit'
                          AND external_action_id IS NULL
                        """
                    ).fetchone()[0]
                )
                add(
                    "application_event_reconciliation",
                    "pass"
                    if orphan_applications == 0 and confirmed_without_action == 0
                    else "fail",
                    f"Несогласованных откликов: {orphan_applications}; "
                    f"явных подтверждений без внешнего действия: {confirmed_without_action}.",
                    blocks_closeout=True,
                )
                invalid_actions = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM action_events
                        WHERE action_state NOT IN (
                            'review', 'needs_input', 'employer_reply', 'follow_up',
                            'account_research', 'waiting', 'none'
                        ) OR bucket NOT IN (
                            'urgent', 'due_follow_up', 'deep_review',
                            'account_research', 'backlog'
                        )
                        """
                    ).fetchone()[0]
                )
                invalid_lifecycle = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM lifecycle_events
                        WHERE event_type IN (
                            'interview_invited', 'interview_scheduled',
                            'interview_completed', 'interview_cancelled',
                            'interview_no_show_candidate', 'interview_no_show_employer'
                        ) AND (round_no IS NULL OR round_no < 1)
                        """
                    ).fetchone()[0]
                )
                add(
                    "valid_lifecycle_action_states",
                    "pass" if invalid_actions == 0 and invalid_lifecycle == 0 else "fail",
                    f"Некорректных рабочих состояний: {invalid_actions}; "
                    f"интервью без номера раунда: {invalid_lifecycle}.",
                    blocks_closeout=True,
                )

                scorecard, _ = build_outcome_scorecard_data(conn, as_of)
                completeness = scorecard["overall"]["field_completeness"]
                completeness_pct = completeness["percent"]
                if scorecard["overall"]["confirmed_applications"] == 0:
                    completeness_status = "warn"
                    completeness_detail = "Подтверждённых откликов нет; полнота полей пока н/д."
                elif completeness_pct is not None and completeness_pct >= 80:
                    completeness_status = "pass"
                    completeness_detail = f"Полнота полей исходов: {completeness_pct:.1f}%."
                else:
                    completeness_status = "warn"
                    completeness_detail = (
                        "Полнота полей исходов ниже 80%: "
                        + ("н/д" if completeness_pct is None else f"{completeness_pct:.1f}%")
                    )
                add("field_completeness", completeness_status, completeness_detail)

                quarantine_pending = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM quarantine_records WHERE status = 'pending'"
                    ).fetchone()[0]
                )
                add(
                    "quarantine_volume",
                    "pass" if quarantine_pending == 0 else "warn",
                    f"Необработанных записей карантина: {quarantine_pending}.",
                )
                wip = build_wip_queue_data(conn, as_of=as_of, page=1, page_size=1)
                add(
                    "wip_overflow",
                    "pass" if wip["overflow_total"] == 0 else "warn",
                    f"Записей сверх лимита WIP: {wip['overflow_total']}.",
                )
                add(
                    "sla_overflow",
                    "pass" if wip["overdue_total"] == 0 else "warn",
                    f"Просроченных записей по SLA: {wip['overdue_total']}.",
                )

                db_mtime = args.db.stat().st_mtime if args.db.exists() else 0
                generated_paths = [
                    DASHBOARD_PATH,
                    REPORTS_DIR / "outcome_scorecard.md",
                    VIEWS_DIR / "wip_queue.md",
                ]
                missing_generated = [
                    display_path(path, ROOT) for path in generated_paths if not path.is_file()
                ]
                stale_generated = [
                    display_path(path, ROOT)
                    for path in generated_paths
                    if path.is_file() and path.stat().st_mtime + 0.001 < db_mtime
                ]
                generated_ok = not missing_generated and not stale_generated
                add(
                    "generated_read_model_freshness",
                    "pass" if generated_ok else "fail",
                    "Модели чтения синхронизированы с SQLite."
                    if generated_ok
                    else "Требуется пересборка; отсутствуют: "
                    + (", ".join(missing_generated) or "нет")
                    + "; устарели: "
                    + (", ".join(stale_generated) or "нет"),
                    blocks_closeout=True,
                )

                coverage_fresh = False
                coverage_source = ""
                for run in conn.execute(
                    """
                    SELECT id, source FROM search_runs
                    WHERE run_date = ? AND status = 'completed'
                    ORDER BY id DESC
                    """,
                    (as_of.isoformat(),),
                ).fetchall():
                    completed_streams = {
                        str(row["stream_key"])
                        for row in conn.execute(
                            """
                            SELECT stream_key FROM search_coverage
                            WHERE search_run_id = ? AND status = 'completed'
                            """,
                            (int(run["id"]),),
                        ).fetchall()
                    }
                    if set(REQUIRED_SEARCH_STREAMS).issubset(completed_streams):
                        coverage_fresh = True
                        coverage_source = str(run["source"])
                        break
                add(
                    "required_search_coverage",
                    "pass" if coverage_fresh else "fail",
                    f"Все обязательные потоки поиска закрыты источником {coverage_source}."
                    if coverage_fresh
                    else "Нет завершённого обязательного покрытия поиска на "
                    + as_of.isoformat()
                    + ".",
                    blocks_closeout=True,
                )

                if PERSONAL_RECOMMENDATIONS_ENABLED:
                    personal = conn.execute(
                        """
                        SELECT sr.run_date, sr.status
                        FROM search_runs sr
                        JOIN search_coverage sc ON sc.search_run_id = sr.id
                        WHERE sc.stream_key = ? AND sc.status = 'completed'
                        ORDER BY sr.run_date DESC, sr.id DESC LIMIT 1
                        """,
                        (PERSONAL_RECOMMENDATION_STREAM,),
                    ).fetchone()
                    personal_ok = bool(
                        personal
                        and personal["status"] == "completed"
                        and personal["run_date"] == as_of.isoformat()
                    )
                    add(
                        "personal_recommendation_coverage",
                        "pass" if personal_ok else "fail",
                        "Персональные рекомендации покрыты отдельно."
                        if personal_ok
                        else f"Нет завершённого покрытия персонального потока {PERSONAL_RECOMMENDATION_STREAM!r} на {as_of.isoformat()}.",
                        blocks_closeout=True,
                    )
                else:
                    add(
                        "personal_recommendation_coverage",
                        "pass",
                        "Отдельный поток персональных рекомендаций выключен.",
                    )

                if TELEGRAM_ENABLED:
                    checkpoint_rows = conn.execute(
                        """
                        SELECT stream_key, last_completed_run_date
                        FROM source_checkpoints WHERE source = 'telegram'
                        """
                    ).fetchall()
                    checkpoint_map = {
                        str(row["stream_key"]): str(row["last_completed_run_date"])
                        for row in checkpoint_rows
                    }
                    missing_telegram = [
                        channel.stream_key
                        for channel in TELEGRAM_CHANNELS
                        if checkpoint_map.get(channel.stream_key) != as_of.isoformat()
                    ]
                    add(
                        "incremental_source_checkpoints",
                        "pass" if not missing_telegram else "fail",
                        "Все включённые Telegram-каналы закрыты на текущую дату."
                        if not missing_telegram
                        else "Нет свежей контрольной точки для: " + ", ".join(missing_telegram),
                        blocks_closeout=True,
                    )
                else:
                    add(
                        "incremental_source_checkpoints",
                        "pass",
                        "Инкрементальные Telegram-источники выключены.",
                    )
        except (sqlite3.Error, RuntimeError) as exc:
            add(
                "database_operational_read",
                "fail",
                f"Не удалось проверить операционное состояние: {exc}",
                blocks_closeout=True,
                technical=True,
            )

    technical_health = all(
        check["status"] != "fail" for check in checks if check["technical"]
    )
    ready = technical_health and all(
        not (check["blocks_closeout"] and check["status"] == "fail")
        for check in checks
    )
    result = {
        "as_of": as_of.isoformat(),
        "technical_health": technical_health,
        "ready_for_daily_closeout": ready,
        "overall_status": "pass"
        if ready
        else ("warn" if technical_health else "fail"),
        "checks": checks,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        marker = {"pass": "ПРОЙДЕНО", "warn": "ПРЕДУПРЕЖДЕНИЕ", "fail": "ОШИБКА"}
        for check in checks:
            print(f"[{marker[check['status']]}] {check['name']}: {check['detail']}")
        print(
            "Ежедневное закрытие разрешено."
            if ready
            else "Ежедневное закрытие не готово: устраните точные ошибки выше."
        )
    if args.strict and not ready:
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
                raise SystemExit("Для --config требуется путь.")
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
    argparse_messages = {
        "usage: ": "использование: ",
        "positional arguments": "позиционные аргументы",
        "options": "параметры",
        "show this help message and exit": "показать эту справку и выйти",
        "the following arguments are required: %s": "требуются следующие аргументы: %s",
        "unrecognized arguments: %s": "нераспознанные аргументы: %s",
        "expected one argument": "ожидается один аргумент",
        "not allowed with argument %s": "нельзя использовать вместе с аргументом %s",
        "argument %s: invalid choice: %(value)r (choose from %(choices)s)": (
            "аргумент %s: недопустимое значение %(value)r; выберите из %(choices)s"
        ),
    }
    argparse._ = lambda message: argparse_messages.get(message, message)
    parser = argparse.ArgumentParser(
        description="База поиска работы, отчёты и панель"
    )
    parser.set_defaults(func=rebuild)
    parser.add_argument(
        "--config",
        type=Path,
        default=SETTINGS.config_path,
        help="Путь к локальным настройкам TOML; также поддерживается JOB_SEARCH_CONFIG",
    )
    parser.add_argument("--db", type=Path, default=DB_PATH, help="Путь к базе SQLite")
    parser.add_argument("--json", action="store_true", help="Вывести машиночитаемый JSON")

    sub = parser.add_subparsers()

    init_parser = sub.add_parser(
        "init", help="Создать локальные настройки, приватные шаблоны и базу"
    )
    init_parser.add_argument("--json", action="store_true")
    init_parser.set_defaults(func=initialize_workspace)

    doctor_parser = sub.add_parser(
        "doctor", help="Проверить настройки, файлы профиля и базу"
    )
    doctor_parser.add_argument("--json", action="store_true")
    doctor_parser.add_argument(
        "--strict", action="store_true", help="Вернуть ненулевой код при обязательной ошибке"
    )
    doctor_parser.set_defaults(func=doctor)

    operational_doctor_parser = sub.add_parser(
        "operational-doctor",
        help="Проверить готовность ежедневного операционного закрытия",
    )
    operational_doctor_parser.add_argument("--db", type=Path, default=DB_PATH)
    operational_doctor_parser.add_argument("--as-of", default="")
    operational_doctor_parser.add_argument("--json", action="store_true")
    operational_doctor_parser.add_argument(
        "--strict", action="store_true", help="Вернуть ненулевой код, если закрытие не готово"
    )
    operational_doctor_parser.set_defaults(func=operational_doctor)

    rebuild_parser = sub.add_parser("rebuild", help="Пересобрать представления, отчёты и панель из SQLite")
    rebuild_parser.add_argument("--db", type=Path, default=DB_PATH)
    rebuild_parser.add_argument("--json", action="store_true")
    rebuild_parser.set_defaults(func=rebuild)

    stats_parser = sub.add_parser("stats", help="Вывести текущие показатели базы")
    stats_parser.add_argument("--db", type=Path, default=DB_PATH)
    stats_parser.set_defaults(func=print_stats)

    conversion_parser = sub.add_parser(
        "conversion-report",
        help="Рассчитать совместимые когорты исходов откликов",
    )
    conversion_parser.add_argument("--db", type=Path, default=DB_PATH)
    conversion_parser.add_argument("--as-of", default="")
    conversion_parser.add_argument("--json", action="store_true")
    conversion_parser.set_defaults(func=conversion_report_command)

    scorecard_parser = sub.add_parser(
        "outcome-scorecard",
        help="Сформировать карту исходов по подтверждённым откликам",
    )
    scorecard_parser.add_argument("--db", type=Path, default=DB_PATH)
    scorecard_parser.add_argument("--as-of", default="")
    scorecard_parser.add_argument("--json", action="store_true")
    scorecard_parser.set_defaults(func=outcome_scorecard_command)

    wip_parser = sub.add_parser(
        "wip-queue", help="Показать ограниченную очередь WIP с пагинацией и SLA"
    )
    wip_parser.add_argument("--db", type=Path, default=DB_PATH)
    wip_parser.add_argument("--as-of", default="")
    wip_parser.add_argument("--page", type=int, default=1)
    wip_parser.add_argument("--page-size", type=int, default=WIP_PAGE_SIZE)
    wip_parser.add_argument("--bucket", choices=sorted(WIP_BUCKET_KEYS), default="")
    wip_parser.add_argument("--json", action="store_true")
    wip_parser.set_defaults(func=wip_queue_command)

    lifecycle_parser = sub.add_parser(
        "record-lifecycle-event",
        help="Добавить доказательное событие жизненного цикла вакансии",
    )
    lifecycle_parser.add_argument("--db", type=Path, default=DB_PATH)
    lifecycle_parser.add_argument("--id", type=int, default=None)
    lifecycle_parser.add_argument("--url", default="")
    lifecycle_parser.add_argument("--external-id", default="")
    lifecycle_parser.add_argument(
        "--event-type", choices=sorted(LIFECYCLE_EVENT_TYPES), required=True
    )
    lifecycle_parser.add_argument("--at", default="")
    lifecycle_parser.add_argument("--evidence-at", default="")
    lifecycle_parser.add_argument("--evidence-note", required=True)
    lifecycle_parser.add_argument("--evidence-url", default="")
    lifecycle_parser.add_argument("--source", required=True)
    lifecycle_parser.add_argument("--external-reference", default="")
    lifecycle_parser.add_argument("--round-no", type=int, default=None)
    lifecycle_parser.add_argument("--scheduled-at", default="")
    lifecycle_parser.add_argument("--campaign-id", default="")
    lifecycle_parser.add_argument("--role-family", default="")
    lifecycle_parser.add_argument(
        "--confidence", choices=sorted(EVIDENCE_CONFIDENCE | {"unknown"}), default=""
    )
    lifecycle_parser.add_argument("--master-resume-id", default="")
    lifecycle_parser.add_argument("--planned-resume-id", default="")
    lifecycle_parser.add_argument("--actual-resume-id", default="")
    lifecycle_parser.add_argument("--message-variant", default="")
    lifecycle_parser.add_argument(
        "--human-path-status", choices=sorted(HUMAN_PATH_STATUSES), default=""
    )
    lifecycle_parser.add_argument("--hard-gates-json", default="")
    lifecycle_parser.add_argument("--unresolved-questions-json", default="")
    lifecycle_parser.add_argument("--json", action="store_true")
    lifecycle_parser.set_defaults(func=record_lifecycle_event_command)

    action_parser = sub.add_parser(
        "set-current-action",
        help="Добавить новое текущее рабочее действие, не меняя жизненный цикл",
    )
    action_parser.add_argument("--db", type=Path, default=DB_PATH)
    action_parser.add_argument("--id", type=int, default=None)
    action_parser.add_argument("--url", default="")
    action_parser.add_argument("--external-id", default="")
    action_parser.add_argument("--action-state", choices=sorted(ACTION_STATES), required=True)
    action_parser.add_argument("--bucket", choices=sorted(WIP_BUCKET_KEYS), required=True)
    action_parser.add_argument("--at", default="")
    action_parser.add_argument("--due-date", default="")
    action_parser.add_argument("--priority", type=int, default=0)
    action_parser.add_argument("--priority-reason", default="")
    action_parser.add_argument("--evidence-note", default="")
    action_parser.add_argument("--source", default="cli:set-current-action")
    action_parser.add_argument("--json", action="store_true")
    action_parser.set_defaults(func=set_current_action_command)

    external_action_parser = sub.add_parser(
        "record-external-action",
        help="Зафиксировать состояние внешнего действия с разрешением и доказательством",
    )
    external_action_parser.add_argument("--db", type=Path, default=DB_PATH)
    external_action_parser.add_argument("--id", type=int, default=None)
    external_action_parser.add_argument("--url", default="")
    external_action_parser.add_argument("--external-id", default="")
    external_action_parser.add_argument("--action-key", required=True)
    external_action_parser.add_argument(
        "--action-type", choices=sorted(EXTERNAL_ACTION_TYPES), required=True
    )
    external_action_parser.add_argument(
        "--state", choices=sorted(EXTERNAL_ACTION_STATES), required=True
    )
    external_action_parser.add_argument("--at", default="")
    external_action_parser.add_argument("--evidence-at", default="")
    external_action_parser.add_argument("--authorization-note", default="")
    external_action_parser.add_argument("--evidence-note", default="")
    external_action_parser.add_argument("--evidence-url", default="")
    external_action_parser.add_argument("--source", required=True)
    external_action_parser.add_argument("--external-reference", default="")
    external_action_parser.add_argument("--application-source-hit-id", type=int, default=None)
    external_action_parser.add_argument("--campaign-id", default="")
    external_action_parser.add_argument("--role-family", default="")
    external_action_parser.add_argument(
        "--confidence", choices=sorted(EVIDENCE_CONFIDENCE | {"unknown"}), default=""
    )
    external_action_parser.add_argument("--master-resume-id", default="")
    external_action_parser.add_argument("--planned-resume-id", default="")
    external_action_parser.add_argument("--actual-resume-id", default="")
    external_action_parser.add_argument("--message-variant", default="")
    external_action_parser.add_argument(
        "--human-path-status", choices=sorted(HUMAN_PATH_STATUSES), default=""
    )
    external_action_parser.add_argument("--hard-gates-json", default="")
    external_action_parser.add_argument("--unresolved-questions-json", default="")
    external_action_parser.add_argument("--json", action="store_true")
    external_action_parser.set_defaults(func=record_external_action_command)

    quarantine_parser = sub.add_parser(
        "quarantine-report", help="Показать аудит записей карантина"
    )
    quarantine_parser.add_argument("--db", type=Path, default=DB_PATH)
    quarantine_parser.add_argument("--page", type=int, default=1)
    quarantine_parser.add_argument("--page-size", type=int, default=50)
    quarantine_parser.add_argument("--status", choices=sorted(QUARANTINE_STATUSES), default="")
    quarantine_parser.add_argument(
        "--classification", choices=sorted(QUARANTINE_CLASSIFICATIONS), default=""
    )
    quarantine_parser.add_argument("--json", action="store_true")
    quarantine_parser.set_defaults(func=quarantine_report_command)

    reprocess_parser = sub.add_parser(
        "reprocess-quarantine", help="Повторно обработать одну точную запись карантина"
    )
    reprocess_parser.add_argument("--db", type=Path, default=DB_PATH)
    reprocess_parser.add_argument("--id", dest="quarantine_id", type=int, required=True)
    reprocess_parser.add_argument("--replacement-json", type=Path, default=None)
    reprocess_parser.add_argument("--json", action="store_true")
    reprocess_parser.set_defaults(func=reprocess_quarantine_command)

    legacy_classification_parser = sub.add_parser(
        "classify-legacy-records",
        help="Показать пробную классификацию исторических строк без изменений",
    )
    legacy_classification_parser.add_argument("--db", type=Path, default=DB_PATH)
    legacy_classification_parser.add_argument("--dry-run", action="store_true")
    legacy_classification_parser.add_argument("--limit", type=int, default=100)
    legacy_classification_parser.add_argument("--json", action="store_true")
    legacy_classification_parser.set_defaults(func=legacy_classification_dry_run_command)

    false_negative_parser = sub.add_parser(
        "false-negative-audit",
        help="Провести воспроизводимый аудит отклонённых и низкоприоритетных вакансий",
    )
    false_negative_parser.add_argument("--db", type=Path, default=DB_PATH)
    false_negative_parser.add_argument("--as-of", default="")
    false_negative_parser.add_argument("--sample-size", type=int, default=25)
    false_negative_parser.add_argument("--seed", default="find-dream-job-v6")
    false_negative_parser.add_argument("--json", action="store_true")
    false_negative_parser.set_defaults(func=false_negative_audit_command)

    migrate_parser = sub.add_parser("migrate-stages", help="Перенести старые этапы в компактную модель воронки")
    migrate_parser.add_argument("--db", type=Path, default=DB_PATH)
    migrate_parser.add_argument("--no-backup", action="store_true", help="Не создавать резервную копию перед переносом")
    migrate_parser.set_defaults(func=migrate_stages)

    schema_parser = sub.add_parser(
        "migrate-schema",
        help="Создать резервную копию и обновить схему SQLite",
    )
    schema_parser.add_argument("--db", type=Path, default=DB_PATH)
    schema_parser.add_argument("--no-backup", action="store_true")
    schema_parser.add_argument("--json", action="store_true")
    schema_parser.set_defaults(func=migrate_schema)

    plan_parser = sub.add_parser(
        "build-coverage-plan",
        help="Построить детерминированные URL HH и шаблон манифеста покрытия",
    )
    plan_parser.add_argument("file", type=Path, help="JSON-план со спецификациями потоков")
    plan_parser.add_argument("--output", type=Path, default=None)
    plan_parser.set_defaults(func=build_coverage_plan_command)

    coverage_parser = sub.add_parser(
        "check-coverage",
        help="Сохранить и строго проверить завершённый манифест покрытия",
    )
    coverage_parser.add_argument("file", type=Path, help="Завершённый JSON-манифест покрытия")
    coverage_parser.add_argument("--db", type=Path, default=DB_PATH)
    coverage_parser.set_defaults(func=check_coverage)

    telegram_plan_parser = sub.add_parser(
        "build-telegram-plan",
        help="Построить план первичного или инкрементального просмотра Telegram",
    )
    telegram_plan_parser.add_argument(
        "--run-date", default="", help="Дата запуска YYYY-MM-DD; по умолчанию сегодня"
    )
    telegram_plan_parser.add_argument("--output", type=Path, default=None)
    telegram_plan_parser.add_argument("--db", type=Path, default=DB_PATH)
    telegram_plan_parser.add_argument("--json", action="store_true")
    telegram_plan_parser.set_defaults(func=build_telegram_plan_command)

    telegram_coverage_parser = sub.add_parser(
        "check-telegram-coverage",
        help="Проверить покрытие Telegram, импорт и продвижение курсоров",
    )
    telegram_coverage_parser.add_argument(
        "file", type=Path, help="Завершённый JSON-манифест покрытия Telegram"
    )
    telegram_coverage_parser.add_argument("--db", type=Path, default=DB_PATH)
    telegram_coverage_parser.set_defaults(func=check_telegram_coverage)

    watch_parser = sub.add_parser("watch", help="Автоматически пересобирать вывод при изменении SQLite")
    watch_parser.add_argument("--db", type=Path, default=DB_PATH)
    watch_parser.add_argument("--interval", type=float, default=2.0, help="Интервал проверки в секундах")
    watch_parser.set_defaults(func=watch)

    ingest_parser = sub.add_parser("ingest-json", help="Импортировать структурированные записи вакансий в SQLite")
    ingest_parser.add_argument("file", type=Path, help="Массив JSON или объект с vacancies/items/jobs/data")
    ingest_parser.add_argument("--db", type=Path, default=DB_PATH)
    ingest_parser.add_argument("--channel", default="", help="Канал по умолчанию, если поле отсутствует")
    ingest_parser.add_argument("--source", default="", help="Источник по умолчанию, если поле отсутствует")
    ingest_parser.add_argument("--json", action="store_true")
    ingest_parser.set_defaults(func=ingest_json)

    gmail_parser = sub.add_parser(
        "ingest-gmail-json",
        help="Импортировать ссылки на вакансии HH или LinkedIn из Gmail",
    )
    gmail_parser.add_argument("file", type=Path, help="JSON-файл с вакансиями")
    gmail_parser.add_argument("--db", type=Path, default=DB_PATH)
    gmail_parser.add_argument(
        "--provider",
        choices=("hh", "linkedin"),
        default="hh",
        help="Почтовый источник по умолчанию для строк без channel/source",
    )
    gmail_parser.add_argument("--json", action="store_true")
    gmail_parser.set_defaults(func=ingest_gmail_json)

    update_parser = sub.add_parser("update-vacancy", help="Обновить изменяемые поля одной вакансии в SQLite")
    update_parser.add_argument("--db", type=Path, default=DB_PATH)
    update_parser.add_argument("--id", type=int, default=None, help="Внутренний ID вакансии из очереди проверки")
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
        help="Также синхронизировать состояние, этап и дату повторного обращения в последней строке отклика",
    )
    update_parser.set_defaults(func=update_vacancy)

    interaction_parser = sub.add_parser(
        "record-employer-interaction",
        help="Добавить доказательное взаимодействие с работодателем без изменения жизненного цикла",
    )
    interaction_parser.add_argument("--db", type=Path, default=DB_PATH)
    interaction_parser.add_argument("--id", type=int, default=None)
    interaction_parser.add_argument("--url", default="")
    interaction_parser.add_argument("--external-id", default="")
    interaction_parser.add_argument("--at", default="", help="Дата или время ISO 8601")
    interaction_parser.add_argument(
        "--direction", choices=("inbound", "outbound"), default="inbound"
    )
    interaction_parser.add_argument(
        "--event-type", choices=sorted(EMPLOYER_INTERACTION_TYPES), required=True
    )
    interaction_parser.add_argument("--channel", required=True)
    interaction_parser.add_argument(
        "--actor-type", choices=sorted(EMPLOYER_ACTOR_TYPES), default="unknown"
    )
    interaction_parser.add_argument(
        "--humanity", choices=("human", "automated"), required=True
    )
    interaction_parser.add_argument("--evidence-note", default="")
    interaction_parser.add_argument("--evidence-url", default="")
    interaction_parser.add_argument("--external-reference", default="")
    interaction_parser.add_argument("--external-action-key", default="")
    interaction_parser.add_argument("--json", action="store_true")
    interaction_parser.set_defaults(func=record_employer_interaction)

    invalidation_parser = sub.add_parser(
        "invalidate-employer-interaction",
        help="Добавить аудируемое исправление ошибочного взаимодействия",
    )
    invalidation_parser.add_argument("--db", type=Path, default=DB_PATH)
    invalidation_parser.add_argument("--interaction-id", type=int, required=True)
    vacancy_guard = invalidation_parser.add_mutually_exclusive_group()
    vacancy_guard.add_argument("--vacancy-id", type=int, default=None)
    vacancy_guard.add_argument("--vacancy-url", default="")
    vacancy_guard.add_argument("--vacancy-external-id", default="")
    invalidation_parser.add_argument("--corrected-at", default="")
    invalidation_parser.add_argument("--reason", required=True)
    invalidation_parser.add_argument("--evidence-note", required=True)
    invalidation_parser.add_argument("--source", required=True)
    invalidation_parser.add_argument("--operator-context", default="")
    invalidation_parser.add_argument("--json", action="store_true")
    invalidation_parser.set_defaults(func=invalidate_employer_interaction)

    account_parser = sub.add_parser(
        "upsert-employer-account", help="Создать или обновить точную карточку работодателя"
    )
    account_parser.add_argument("--db", type=Path, default=DB_PATH)
    account_parser.add_argument("--account-id", type=int, default=None)
    account_parser.add_argument("--canonical-name", required=True)
    account_parser.add_argument("--website", default=None)
    account_parser.add_argument("--careers-url", default=None)
    account_parser.add_argument("--country-market", default=None)
    account_parser.add_argument("--priority", default=None)
    account_parser.add_argument("--status", dest="account_status", default=None)
    account_parser.add_argument("--last-checked-date", default=None)
    account_parser.add_argument("--notes", default=None)
    account_parser.add_argument("--portfolio-limit", type=int, default=None)
    account_parser.add_argument("--review-cadence-days", type=int, default=None)
    account_parser.add_argument("--next-review-date", default=None)
    account_parser.add_argument("--website-checked-date", default=None)
    account_parser.add_argument("--careers-checked-date", default=None)
    account_parser.add_argument(
        "--target-campaigns", default=None, help="Список campaign_id через запятую"
    )
    account_parser.add_argument(
        "--target-role-families", default=None, help="Список role_family через запятую"
    )
    account_parser.add_argument("--owner-evidence", default=None)
    account_parser.add_argument("--sponsor-evidence", default=None)
    account_parser.add_argument("--governance-evidence", default=None)
    account_parser.add_argument(
        "--human-path-status", choices=sorted(HUMAN_PATH_STATUSES), default=None
    )
    account_parser.add_argument("--json", action="store_true")
    account_parser.set_defaults(func=upsert_employer_account)

    signal_parser = sub.add_parser(
        "record-employer-signal", help="Добавить доказательный сигнал работодателя"
    )
    signal_parser.add_argument("--db", type=Path, default=DB_PATH)
    signal_account = signal_parser.add_mutually_exclusive_group(required=True)
    signal_account.add_argument("--account-id", type=int, default=None)
    signal_account.add_argument("--account-name", default="")
    signal_parser.add_argument(
        "--signal-type", choices=sorted(EMPLOYER_SIGNAL_TYPES), required=True
    )
    signal_parser.add_argument("--observed-date", default="")
    signal_parser.add_argument(
        "--confidence", choices=sorted(EVIDENCE_CONFIDENCE), required=True
    )
    signal_parser.add_argument("--evidence-url", default="")
    signal_parser.add_argument("--evidence-note", required=True)
    signal_parser.add_argument("--json", action="store_true")
    signal_parser.set_defaults(func=record_employer_signal)

    link_parser = sub.add_parser(
        "link-vacancy-account", help="Явно связать вакансию с карточкой работодателя"
    )
    link_parser.add_argument("--db", type=Path, default=DB_PATH)
    link_parser.add_argument("--id", type=int, default=None)
    link_parser.add_argument("--url", default="")
    link_parser.add_argument("--external-id", default="")
    link_account = link_parser.add_mutually_exclusive_group(required=True)
    link_account.add_argument("--account-id", type=int, default=None)
    link_account.add_argument("--account-name", default="")
    link_parser.add_argument("--evidence-note", default="")
    link_parser.add_argument("--json", action="store_true")
    link_parser.set_defaults(func=link_vacancy_account)

    factor_parser = sub.add_parser(
        "record-vacancy-factor",
        help="Добавить доказательный фактор вакансии без изменения балла",
    )
    factor_parser.add_argument("--db", type=Path, default=DB_PATH)
    factor_parser.add_argument("--id", type=int, default=None)
    factor_parser.add_argument("--url", default="")
    factor_parser.add_argument("--external-id", default="")
    factor_parser.add_argument(
        "--factor-key",
        required=True,
        help="Ключ в lowercase snake_case; основные ключи: " + ", ".join(sorted(CORE_VACANCY_FACTORS)),
    )
    factor_parser.add_argument("--value", required=True)
    factor_parser.add_argument("--observed-date", default="")
    factor_parser.add_argument(
        "--confidence", choices=sorted(EVIDENCE_CONFIDENCE), required=True
    )
    factor_parser.add_argument("--evidence-note", required=True)
    factor_parser.add_argument("--evidence-url", default="")
    factor_parser.add_argument("--json", action="store_true")
    factor_parser.set_defaults(func=record_vacancy_factor)

    contact_parser = sub.add_parser(
        "upsert-contact",
        help="Сохранить или обновить проверенный контакт по одной вакансии",
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
        help="Зафиксировать поиск прямого контакта без отправки сообщения",
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
            "Настроенные прямые каналы через запятую: "
            + ",".join(DIRECT_OUTREACH_CHANNELS)
        ),
    )
    contact_search_parser.add_argument("--note", required=True)
    contact_search_parser.set_defaults(func=record_contact_search)

    followup_parser = sub.add_parser(
        "record-followup",
        help="Зафиксировать один раунд повторных обращений и назначить следующий",
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
        help="JSON с contact_search, точными сообщениями и доказательствами доставки",
    )
    followup_parser.add_argument("--note", default="")
    followup_parser.set_defaults(func=record_followup)

    interview_parser = sub.add_parser("attach-interview-summary", help="Связать Markdown-резюме интервью с вакансией")
    interview_parser.add_argument("--db", type=Path, default=DB_PATH)
    interview_parser.add_argument("--id", type=int, default=None, help="Внутренний ID вакансии из панели")
    interview_parser.add_argument("--url", default="")
    interview_parser.add_argument("--external-id", default="")
    interview_parser.add_argument("--file", type=Path, required=True, help="Файл Markdown внутри рабочей области")
    interview_parser.add_argument("--interview-no", type=int, default=None, help="Номер интервью: 1, 2, 3, 4 и далее")
    interview_parser.add_argument("--stage", default="", help="Код этапа; по умолчанию interview_<номер>")
    interview_parser.add_argument("--date", default="", help="Дата интервью или резюме; по умолчанию сегодня")
    interview_parser.add_argument("--title", default="", help="Краткая подпись ссылки; по умолчанию «Интервью <номер>»")
    interview_parser.add_argument("--note", default="")
    interview_parser.add_argument(
        "--confirms-completion",
        action="store_true",
        help="Явно подтвердить завершение интервью по проверенному резюме",
    )
    interview_parser.add_argument("--completion-evidence-note", default="")
    interview_parser.set_defaults(func=attach_interview_summary)

    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    raw_argv, config_path = extract_config_argument(raw_argv)
    try:
        configure_runtime(config_path)
    except (OSError, ValueError) as exc:
        print(f"Ошибка конфигурации jobctl: {exc}", file=sys.stderr)
        return 2
    parser = build_parser()
    args = parser.parse_args(raw_argv)
    if hasattr(args, "db"):
        args.db = args.db.resolve() if not args.db.is_absolute() else args.db
    try:
        args.func(args)
    except Exception as exc:  # Keep CLI errors readable for daily runs.
        print(f"Ошибка jobctl: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
