#!/usr/bin/env python3
"""Configuration loading for the local job-search workspace.

The source tree contains reusable product logic. Candidate data, preferences,
and generated artifacts live in a workspace selected with JOB_SEARCH_HOME.
All paths in settings.toml are resolved relative to that workspace.
"""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_CHANNEL_LABELS = {
    "hh": "HH",
    "company_site": "Сайт работодателя",
    "email": "Почта",
    "telegram": "Telegram",
    "max": "Max",
    "linkedin": "LinkedIn",
    "signal": "Signal",
    "whatsapp": "WhatsApp",
    "gmail_hh": "Gmail HH",
}


@dataclass(frozen=True)
class ProjectSettings:
    title: str
    locale: str
    timezone: str


@dataclass(frozen=True)
class PathSettings:
    database: Path
    dashboard: Path
    views: Path
    reports: Path
    archive: Path


@dataclass(frozen=True)
class ProfileSettings:
    files: tuple[Path, ...]
    preferences_file: Path
    scoring_file: Path
    answers_file: Path

    def all_files(self) -> tuple[Path, ...]:
        ordered = [
            *self.files,
            self.preferences_file,
            self.scoring_file,
            self.answers_file,
        ]
        return tuple(dict.fromkeys(ordered))


@dataclass(frozen=True)
class AutomationSettings:
    auto_apply: bool
    apply_threshold: int
    require_visible_confirmation: bool


@dataclass(frozen=True)
class FollowUpSettings:
    limit: int
    interval_business_days: int
    primary_channel: str
    direct_channels: tuple[str, ...]
    max_direct_messages_per_round: int


@dataclass(frozen=True)
class MailSettings:
    scan_linkedin_inbox: bool
    archive_processed_linkedin: bool


@dataclass(frozen=True)
class TelegramChannelSettings:
    handle: str
    url: str
    preview_url: str

    @property
    def stream_key(self) -> str:
        return f"telegram:{self.handle}"


@dataclass(frozen=True)
class TelegramSettings:
    enabled: bool
    initial_lookback_days: int
    channels: tuple[TelegramChannelSettings, ...]


@dataclass(frozen=True)
class SearchSettings:
    required_streams: tuple[str, ...]
    default_period_days: int
    items_per_page: int
    stream_aliases: dict[str, tuple[str, ...]]
    personal_recommendations_enabled: bool
    personal_recommendation_stream: str


@dataclass(frozen=True)
class DecisionSettings:
    campaign_ids: tuple[str, ...]
    role_families: tuple[str, ...]
    resume_ids: tuple[str, ...]
    message_variants: tuple[str, ...]


@dataclass(frozen=True)
class WipBucketSettings:
    key: str
    label: str
    limit: int
    sla_days: int
    active: bool


@dataclass(frozen=True)
class WipSettings:
    page_size: int
    buckets: tuple[WipBucketSettings, ...]


@dataclass(frozen=True)
class PolicySettings:
    active_version: str
    effective_date: str


@dataclass(frozen=True)
class AccountSettings:
    active_portfolio_limit: int


@dataclass(frozen=True)
class DailyRunGateSettings:
    key: str
    kind: str
    order: int
    depends_on: tuple[str, ...]
    required: bool
    enabled: bool
    require_remote_boundary: bool


@dataclass(frozen=True)
class DailyRunSettings:
    required_gates: tuple[DailyRunGateSettings, ...]


@dataclass(frozen=True)
class Settings:
    code_root: Path
    workspace_root: Path
    config_path: Path
    config_loaded: bool
    project: ProjectSettings
    paths: PathSettings
    profile: ProfileSettings
    automation: AutomationSettings
    follow_up: FollowUpSettings
    mail: MailSettings
    telegram: TelegramSettings
    search: SearchSettings
    decision: DecisionSettings
    wip: WipSettings
    policy: PolicySettings
    account: AccountSettings
    daily_run: DailyRunSettings
    channel_labels: dict[str, str]


def _table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"Раздел [{key}] должен быть таблицей TOML.")
    return value


def _string(table: dict[str, Any], key: str, default: str) -> str:
    value = table.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key}: требуется непустая строка.")
    return value.strip()


def _boolean(table: dict[str, Any], key: str, default: bool) -> bool:
    value = table.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{key}: требуется значение true или false.")
    return value


def _integer(table: dict[str, Any], key: str, default: int, minimum: int) -> int:
    value = table.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{key}: требуется целое число не меньше {minimum}.")
    return value


def _strings(table: dict[str, Any], key: str, default: list[str]) -> tuple[str, ...]:
    value = table.get(key, default)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key}: требуется массив строк.")
    normalized = [item.strip().lower() for item in value if item.strip()]
    if not normalized:
        raise ValueError(f"{key}: требуется хотя бы одно значение.")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{key}: повторяющиеся значения запрещены.")
    return tuple(normalized)


def _stream_names(table: dict[str, Any], key: str, default: list[str]) -> tuple[str, ...]:
    value = table.get(key, default)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key}: требуется массив строк.")
    normalized = [item.strip() for item in value if item.strip()]
    if not normalized:
        raise ValueError(f"{key}: требуется хотя бы одно значение.")
    folded = [item.casefold() for item in normalized]
    if len(folded) != len(set(folded)):
        raise ValueError(f"{key}: повторяющиеся значения запрещены.")
    return tuple(normalized)


def _stream_aliases(table: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    """Parse exact raw aliases without ever guessing separators.

    A scalar keeps backward compatibility.  An array provides explicit
    multi-label attribution; arbitrary plus signs and other punctuation are
    never split implicitly.
    """

    aliases: dict[str, tuple[str, ...]] = {}
    original_keys: dict[str, str] = {}
    for raw_alias, canonical_key in table.items():
        if not isinstance(raw_alias, str) or not raw_alias.strip():
            raise ValueError("Ключи source_stream_aliases должны быть непустыми строками.")
        if isinstance(canonical_key, str):
            values = [canonical_key]
        elif isinstance(canonical_key, list) and all(
            isinstance(item, str) for item in canonical_key
        ):
            values = canonical_key
        else:
            raise ValueError(
                "Значения source_stream_aliases должны быть каноническим ключом или массивом ключей."
            )
        cleaned = [item.strip() for item in values if item.strip()]
        if not cleaned:
            raise ValueError(
                "Значения source_stream_aliases должны содержать непустой канонический ключ."
            )
        folded_values = [item.casefold() for item in cleaned]
        if len(folded_values) != len(set(folded_values)):
            raise ValueError(
                "Значения source_stream_aliases не должны содержать повторяющиеся канонические ключи."
            )
        folded = raw_alias.strip().casefold()
        if folded in aliases:
            raise ValueError(
                "В source_stream_aliases есть ключи, совпадающие без учёта регистра: "
                f"{original_keys[folded]!r} и {raw_alias.strip()!r}."
            )
        aliases[folded] = tuple(
            value for _, value in sorted(zip(folded_values, cleaned), key=lambda pair: pair[0])
        )
        original_keys[folded] = raw_alias.strip()
    return aliases


def _configured_values(
    table: dict[str, Any], key: str, *, allow_empty: bool = True
) -> tuple[str, ...]:
    value = table.get(key, [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key}: требуется массив строк.")
    cleaned = [item.strip() for item in value if item.strip()]
    if not allow_empty and not cleaned:
        raise ValueError(f"{key}: требуется хотя бы одно значение.")
    folded = [item.casefold() for item in cleaned]
    if len(folded) != len(set(folded)):
        raise ValueError(f"{key}: повторяющиеся значения запрещены.")
    return tuple(cleaned)


DAILY_RUN_GATE_KEY_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,95}$")
DAILY_RUN_GATE_KIND_RE = re.compile(r"^[a-z][a-z0-9_]{1,63}$")


def _daily_run_gates(table: dict[str, Any]) -> tuple[DailyRunGateSettings, ...]:
    """Parse opaque workspace gates without teaching the Engine their meaning."""

    value = table.get("required_gates", [])
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(
            "Поле daily_run.required_gates должно быть массивом таблиц TOML."
        )
    gates: list[DailyRunGateSettings] = []
    seen: set[str] = set()
    for index, raw in enumerate(value):
        key = raw.get("key")
        kind = raw.get("kind", "workspace_gate")
        order = raw.get("order", 500)
        depends_on = raw.get("depends_on", [])
        required = raw.get("required", True)
        enabled = raw.get("enabled", True)
        require_remote_boundary = raw.get("require_remote_boundary", True)
        label = f"daily_run.required_gates[{index}]"
        if not isinstance(key, str) or not DAILY_RUN_GATE_KEY_RE.fullmatch(key.strip()):
            raise ValueError(
                f"{label}.key: требуется устойчивый ключ из строчных латинских букв, "
                "цифр, точки, дефиса или подчёркивания."
            )
        key = key.strip()
        if key in seen:
            raise ValueError(f"{label}.key: ключ {key!r} повторяется.")
        seen.add(key)
        if not isinstance(kind, str) or not DAILY_RUN_GATE_KIND_RE.fullmatch(kind.strip()):
            raise ValueError(
                f"{label}.kind: требуется непустой ключ вида lowercase_snake_case."
            )
        if not isinstance(order, int) or isinstance(order, bool) or order < 0:
            raise ValueError(f"{label}.order: требуется целое число не меньше 0.")
        if not isinstance(depends_on, list) or not all(
            isinstance(item, str) for item in depends_on
        ):
            raise ValueError(f"{label}.depends_on: требуется массив строк.")
        dependencies = tuple(item.strip() for item in depends_on if item.strip())
        if len(dependencies) != len(set(dependencies)):
            raise ValueError(f"{label}.depends_on: повторяющиеся зависимости запрещены.")
        for field_name, field_value in (
            ("required", required),
            ("enabled", enabled),
            ("require_remote_boundary", require_remote_boundary),
        ):
            if not isinstance(field_value, bool):
                raise ValueError(f"{label}.{field_name}: требуется true или false.")
        gates.append(
            DailyRunGateSettings(
                key=key,
                kind=kind.strip(),
                order=order,
                depends_on=dependencies,
                required=required,
                enabled=enabled,
                require_remote_boundary=require_remote_boundary,
            )
        )
    return tuple(sorted(gates, key=lambda gate: (gate.order, gate.key)))


DEFAULT_WIP_BUCKETS: tuple[tuple[str, str, int, int, bool], ...] = (
    ("urgent", "Срочный ответ работодателю или данные от пользователя", 20, 1, True),
    ("due_follow_up", "Наступивший срок повторного обращения", 30, 2, True),
    ("deep_review", "Углублённая проверка сильной возможности", 20, 5, True),
    ("account_research", "Исследование целевого работодателя", 10, 7, True),
    ("backlog", "Резерв вне активного лимита", 0, 30, False),
)


def _wip_settings(table: dict[str, Any]) -> WipSettings:
    page_size = _integer(table, "page_size", 50, 1)
    if page_size > 500:
        raise ValueError("Значение queue.page_size должно быть от 1 до 500.")
    limits = table.get("limits", {})
    sla_days = table.get("sla_days", {})
    labels = table.get("labels", {})
    if not isinstance(limits, dict) or not isinstance(sla_days, dict) or not isinstance(labels, dict):
        raise ValueError("Разделы queue.limits, queue.sla_days и queue.labels должны быть таблицами TOML.")
    buckets: list[WipBucketSettings] = []
    known = {item[0] for item in DEFAULT_WIP_BUCKETS}
    unknown = (set(limits) | set(sla_days) | set(labels)) - known
    if unknown:
        raise ValueError("В queue указаны неподдерживаемые ключи групп: " + ", ".join(sorted(unknown)))
    for key, default_label, default_limit, default_sla, active in DEFAULT_WIP_BUCKETS:
        limit = limits.get(key, default_limit)
        sla = sla_days.get(key, default_sla)
        label = labels.get(key, default_label)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
            raise ValueError(f"queue.limits.{key}: требуется целое число не меньше 0.")
        if not isinstance(sla, int) or isinstance(sla, bool) or sla < 1:
            raise ValueError(f"queue.sla_days.{key}: требуется целое число не меньше 1.")
        if not isinstance(label, str) or not label.strip():
            raise ValueError(f"queue.labels.{key}: требуется непустая строка.")
        buckets.append(
            WipBucketSettings(
                key=key,
                label=label.strip(),
                limit=limit,
                sla_days=sla,
                active=active,
            )
        )
    return WipSettings(page_size=page_size, buckets=tuple(buckets))


TELEGRAM_HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")


def _telegram_channels(table: dict[str, Any]) -> tuple[TelegramChannelSettings, ...]:
    value = table.get("channels", [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("Поле telegram.channels должно быть массивом публичных адресов t.me.")

    channels: list[TelegramChannelSettings] = []
    seen: set[str] = set()
    for raw_url in value:
        candidate = raw_url.strip()
        if not candidate:
            continue
        parsed = urlsplit(candidate)
        host = (parsed.hostname or "").casefold()
        if parsed.scheme.casefold() not in {"http", "https"} or host not in {
            "t.me",
            "www.t.me",
        }:
            raise ValueError(
                f"Канал Telegram должен использовать публичный адрес t.me: {candidate!r}."
            )
        if parsed.query or parsed.fragment:
            raise ValueError(
                f"Адрес канала Telegram не должен содержать запрос или фрагмент: {candidate!r}."
            )
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) == 2 and parts[0].casefold() == "s":
            parts = parts[1:]
        if len(parts) != 1 or not TELEGRAM_HANDLE_RE.fullmatch(parts[0]):
            raise ValueError(
                "Адрес Telegram должен указывать на один публичный канал, "
                f"а не на публикацию или приглашение: {candidate!r}."
            )
        handle = parts[0].casefold()
        if handle in seen:
            raise ValueError(f"В telegram.channels повторяется имя канала: {handle}.")
        seen.add(handle)
        channels.append(
            TelegramChannelSettings(
                handle=handle,
                url=f"https://t.me/{handle}",
                preview_url=f"https://t.me/s/{handle}",
            )
        )
    return tuple(channels)


def _path(workspace_root: Path, value: str) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = workspace_root / candidate
    # Keep configured generated-output paths stable when they are managed
    # symlinks into an atomically switched projection generation.
    return Path(os.path.abspath(candidate))


def _path_value(
    table: dict[str, Any], key: str, default: str, workspace_root: Path
) -> Path:
    return _path(workspace_root, _string(table, key, default))


def _profile_files(table: dict[str, Any], workspace_root: Path) -> tuple[Path, ...]:
    value = table.get("files", ["private/profile.md"])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("Поле profile.files должно быть массивом путей.")
    files = tuple(_path(workspace_root, item) for item in value if item.strip())
    if not files:
        raise ValueError("Поле profile.files должно содержать хотя бы один путь.")
    return files


def _workspace_root(code_root: Path) -> Path:
    configured = os.environ.get("JOB_SEARCH_HOME", "").strip()
    return Path(configured).expanduser().resolve() if configured else code_root.resolve()


def _config_path(workspace_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return _path(workspace_root, str(explicit))
    configured = os.environ.get("JOB_SEARCH_CONFIG", "").strip()
    if configured:
        return _path(workspace_root, configured)
    return workspace_root / "config" / "settings.toml"


def load_settings(code_root: Path, config_path: Path | None = None) -> Settings:
    """Load validated settings without requiring a local config file."""

    code_root = code_root.resolve()
    workspace_root = _workspace_root(code_root)
    resolved_config = _config_path(workspace_root, config_path)
    data: dict[str, Any] = {}
    if resolved_config.exists():
        with resolved_config.open("rb") as handle:
            loaded = tomllib.load(handle)
        if not isinstance(loaded, dict):
            raise ValueError("Файл settings.toml должен содержать таблицы TOML.")
        data = loaded

    project = _table(data, "project")
    paths = _table(data, "paths")
    profile = _table(data, "profile")
    automation = _table(data, "automation")
    follow_up = _table(data, "follow_up")
    mail = _table(data, "mail")
    telegram = _table(data, "telegram")
    search = _table(data, "search")
    decision = _table(data, "decision")
    queue = _table(data, "queue")
    policy = _table(data, "policy")
    account = _table(data, "account")
    daily_run = _table(data, "daily_run")
    source_stream_aliases = _table(data, "source_stream_aliases")
    labels = _table(data, "channel_labels")

    direct_channels = _strings(
        follow_up, "direct_channels", ["linkedin"]
    )
    primary_channel = _string(follow_up, "primary_channel", "email").lower()
    if primary_channel in direct_channels:
        raise ValueError("Канал follow_up.primary_channel не должен одновременно входить в прямые каналы.")

    channel_labels = dict(DEFAULT_CHANNEL_LABELS)
    for channel, label in labels.items():
        if not isinstance(channel, str) or not isinstance(label, str) or not label.strip():
            raise ValueError("Элементы channel_labels должны сопоставлять строки с непустыми строками.")
        channel_labels[channel.strip().lower()] = label.strip()

    mail_settings = MailSettings(
        scan_linkedin_inbox=_boolean(mail, "scan_linkedin_inbox", False),
        archive_processed_linkedin=_boolean(
            mail, "archive_processed_linkedin", False
        ),
    )
    if (
        mail_settings.archive_processed_linkedin
        and not mail_settings.scan_linkedin_inbox
    ):
        raise ValueError(
            "Для mail.archive_processed_linkedin требуется mail.scan_linkedin_inbox = true."
        )

    telegram_settings = TelegramSettings(
        enabled=_boolean(telegram, "enabled", False),
        initial_lookback_days=_integer(
            telegram, "initial_lookback_days", 30, 1
        ),
        channels=_telegram_channels(telegram),
    )
    if telegram_settings.initial_lookback_days > 366:
        raise ValueError("Значение telegram.initial_lookback_days должно быть от 1 до 366.")
    if telegram_settings.enabled and not telegram_settings.channels:
        raise ValueError(
            "Для telegram.enabled = true требуется хотя бы один адрес публичного канала."
        )

    settings = Settings(
        code_root=code_root,
        workspace_root=workspace_root,
        config_path=resolved_config,
        config_loaded=resolved_config.exists(),
        project=ProjectSettings(
            title=_string(project, "title", "Find Dream Job"),
            locale=_string(project, "locale", "ru"),
            timezone=_string(project, "timezone", "UTC"),
        ),
        paths=PathSettings(
            database=_path_value(paths, "database", "data/job_search.sqlite", workspace_root),
            dashboard=_path_value(paths, "dashboard", "dashboard/index.html", workspace_root),
            views=_path_value(paths, "views", "views", workspace_root),
            reports=_path_value(paths, "reports", "reports", workspace_root),
            archive=_path_value(paths, "archive", "archive", workspace_root),
        ),
        profile=ProfileSettings(
            files=_profile_files(profile, workspace_root),
            preferences_file=_path_value(
                profile, "preferences_file", "private/preferences.md", workspace_root
            ),
            scoring_file=_path_value(
                profile, "scoring_file", "private/scoring.md", workspace_root
            ),
            answers_file=_path_value(
                profile,
                "answers_file",
                "private/questions_and_answers.md",
                workspace_root,
            ),
        ),
        automation=AutomationSettings(
            auto_apply=_boolean(automation, "auto_apply", False),
            apply_threshold=_integer(automation, "apply_threshold", 85, 0),
            require_visible_confirmation=_boolean(
                automation, "require_visible_confirmation", True
            ),
        ),
        follow_up=FollowUpSettings(
            limit=_integer(follow_up, "limit", 3, 1),
            interval_business_days=_integer(
                follow_up, "interval_business_days", 5, 1
            ),
            primary_channel=primary_channel,
            direct_channels=direct_channels,
            max_direct_messages_per_round=_integer(
                follow_up, "max_direct_messages_per_round", 1, 0
            ),
        ),
        mail=mail_settings,
        telegram=telegram_settings,
        search=SearchSettings(
            required_streams=_stream_names(
                search,
                "required_streams",
                ["recommendations", "target_roles"],
            ),
            default_period_days=_integer(
                search, "default_period_days", 3, 1
            ),
            items_per_page=_integer(search, "items_per_page", 100, 1),
            stream_aliases=_stream_aliases(source_stream_aliases),
            personal_recommendations_enabled=_boolean(
                search, "personal_recommendations_enabled", False
            ),
            personal_recommendation_stream=_string(
                search,
                "personal_recommendation_stream",
                "personal_recommendations",
            ),
        ),
        decision=DecisionSettings(
            campaign_ids=_configured_values(decision, "campaign_ids"),
            role_families=_configured_values(decision, "role_families"),
            resume_ids=_configured_values(decision, "resume_ids"),
            message_variants=_configured_values(decision, "message_variants"),
        ),
        wip=_wip_settings(queue),
        policy=PolicySettings(
            active_version=_string(policy, "active_version", "engine-safe-default-v1"),
            effective_date=_string(policy, "effective_date", "2026-01-01"),
        ),
        account=AccountSettings(
            active_portfolio_limit=_integer(
                account, "active_portfolio_limit", 20, 1
            )
        ),
        daily_run=DailyRunSettings(required_gates=_daily_run_gates(daily_run)),
        channel_labels=channel_labels,
    )

    if settings.automation.apply_threshold > 100:
        raise ValueError("Значение automation.apply_threshold должно быть от 0 до 100.")
    if settings.search.items_per_page > 100:
        raise ValueError("Значение search.items_per_page должно быть от 1 до 100.")
    try:
        effective_date = __import__("datetime").date.fromisoformat(
            settings.policy.effective_date
        )
    except ValueError as exc:
        raise ValueError("Поле policy.effective_date должно быть датой в формате ГГГГ-ММ-ДД.") from exc
    if effective_date.year < 2000:
        raise ValueError("Год в policy.effective_date должен быть не раньше 2000.")
    try:
        ZoneInfo(settings.project.timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(
            "Поле project.timezone должно содержать имя часового пояса IANA, например Europe/Moscow."
        ) from exc
    return settings


def display_path(path: Path, workspace_root: Path) -> str:
    """Return a portable workspace-relative label when possible."""

    try:
        return path.relative_to(workspace_root).as_posix()
    except ValueError:
        return str(path)
