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


DEFAULT_CHANNEL_LABELS = {
    "hh": "HH",
    "email": "Email",
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
    stream_aliases: dict[str, str]


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
    channel_labels: dict[str, str]


def _table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"[{key}] must be a TOML table")
    return value


def _string(table: dict[str, Any], key: str, default: str) -> str:
    value = table.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value.strip()


def _boolean(table: dict[str, Any], key: str, default: bool) -> bool:
    value = table.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be true or false")
    return value


def _integer(table: dict[str, Any], key: str, default: int, minimum: int) -> int:
    value = table.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{key} must be an integer >= {minimum}")
    return value


def _strings(table: dict[str, Any], key: str, default: list[str]) -> tuple[str, ...]:
    value = table.get(key, default)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be an array of strings")
    normalized = [item.strip().lower() for item in value if item.strip()]
    if not normalized:
        raise ValueError(f"{key} must contain at least one value")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{key} must not contain duplicates")
    return tuple(normalized)


def _stream_names(table: dict[str, Any], key: str, default: list[str]) -> tuple[str, ...]:
    value = table.get(key, default)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be an array of strings")
    normalized = [item.strip() for item in value if item.strip()]
    if not normalized:
        raise ValueError(f"{key} must contain at least one value")
    folded = [item.casefold() for item in normalized]
    if len(folded) != len(set(folded)):
        raise ValueError(f"{key} must not contain duplicates")
    return tuple(normalized)


def _stream_aliases(table: dict[str, Any]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    original_keys: dict[str, str] = {}
    for raw_alias, canonical_key in table.items():
        if not isinstance(raw_alias, str) or not raw_alias.strip():
            raise ValueError("source_stream_aliases keys must be non-empty strings")
        if not isinstance(canonical_key, str) or not canonical_key.strip():
            raise ValueError(
                "source_stream_aliases values must be non-empty canonical keys"
            )
        folded = raw_alias.strip().casefold()
        if folded in aliases:
            raise ValueError(
                "source_stream_aliases contains case-insensitive duplicate keys: "
                f"{original_keys[folded]!r} and {raw_alias.strip()!r}"
            )
        aliases[folded] = canonical_key.strip()
        original_keys[folded] = raw_alias.strip()
    return aliases


TELEGRAM_HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")


def _telegram_channels(table: dict[str, Any]) -> tuple[TelegramChannelSettings, ...]:
    value = table.get("channels", [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("telegram.channels must be an array of public t.me URLs")

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
                f"telegram channel must use a public t.me URL: {candidate!r}"
            )
        if parsed.query or parsed.fragment:
            raise ValueError(
                f"telegram channel URL must not contain a query or fragment: {candidate!r}"
            )
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) == 2 and parts[0].casefold() == "s":
            parts = parts[1:]
        if len(parts) != 1 or not TELEGRAM_HANDLE_RE.fullmatch(parts[0]):
            raise ValueError(
                "telegram channel URL must identify one public channel handle, "
                f"not a post or invite link: {candidate!r}"
            )
        handle = parts[0].casefold()
        if handle in seen:
            raise ValueError(f"telegram.channels contains duplicate handle: {handle}")
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
    return candidate.resolve()


def _path_value(
    table: dict[str, Any], key: str, default: str, workspace_root: Path
) -> Path:
    return _path(workspace_root, _string(table, key, default))


def _profile_files(table: dict[str, Any], workspace_root: Path) -> tuple[Path, ...]:
    value = table.get("files", ["private/profile.md"])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("profile.files must be an array of paths")
    files = tuple(_path(workspace_root, item) for item in value if item.strip())
    if not files:
        raise ValueError("profile.files must contain at least one path")
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
            raise ValueError("settings.toml must contain TOML tables")
        data = loaded

    project = _table(data, "project")
    paths = _table(data, "paths")
    profile = _table(data, "profile")
    automation = _table(data, "automation")
    follow_up = _table(data, "follow_up")
    mail = _table(data, "mail")
    telegram = _table(data, "telegram")
    search = _table(data, "search")
    source_stream_aliases = _table(data, "source_stream_aliases")
    labels = _table(data, "channel_labels")

    direct_channels = _strings(
        follow_up, "direct_channels", ["linkedin"]
    )
    primary_channel = _string(follow_up, "primary_channel", "email").lower()
    if primary_channel in direct_channels:
        raise ValueError("follow_up.primary_channel must not also be a direct channel")

    channel_labels = dict(DEFAULT_CHANNEL_LABELS)
    for channel, label in labels.items():
        if not isinstance(channel, str) or not isinstance(label, str) or not label.strip():
            raise ValueError("channel_labels entries must map strings to non-empty strings")
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
            "mail.archive_processed_linkedin requires mail.scan_linkedin_inbox = true"
        )

    telegram_settings = TelegramSettings(
        enabled=_boolean(telegram, "enabled", False),
        initial_lookback_days=_integer(
            telegram, "initial_lookback_days", 30, 1
        ),
        channels=_telegram_channels(telegram),
    )
    if telegram_settings.initial_lookback_days > 366:
        raise ValueError("telegram.initial_lookback_days must be between 1 and 366")
    if telegram_settings.enabled and not telegram_settings.channels:
        raise ValueError(
            "telegram.enabled = true requires at least one public channel URL"
        )

    settings = Settings(
        code_root=code_root,
        workspace_root=workspace_root,
        config_path=resolved_config,
        config_loaded=resolved_config.exists(),
        project=ProjectSettings(
            title=_string(project, "title", "Find Dream Job"),
            locale=_string(project, "locale", "en"),
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
        ),
        channel_labels=channel_labels,
    )

    if settings.automation.apply_threshold > 100:
        raise ValueError("automation.apply_threshold must be between 0 and 100")
    if settings.search.items_per_page > 100:
        raise ValueError("search.items_per_page must be between 1 and 100")
    return settings


def display_path(path: Path, workspace_root: Path) -> str:
    """Return a portable workspace-relative label when possible."""

    try:
        return path.relative_to(workspace_root).as_posix()
    except ValueError:
        return str(path)
