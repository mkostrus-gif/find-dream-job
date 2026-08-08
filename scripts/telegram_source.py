#!/usr/bin/env python3
"""Build and validate fail-closed Telegram channel scan manifests."""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, urlsplit


TELEGRAM_SOURCE = "telegram"
TELEGRAM_POST_CLASSIFICATIONS = {"vacancy", "non_vacancy", "out_of_scope"}
TELEGRAM_STREAM_PREFIX = "telegram:"


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _iso_date(value: Any, label: str) -> dt.date:
    text = _clean_text(value)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        raise ValueError(f"{label} must use YYYY-MM-DD")
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{label} must be a valid calendar date") from exc


def _post_date(value: Any, label: str) -> dt.date:
    text = _clean_text(value)
    if not text:
        raise ValueError(f"{label} is required")
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return dt.date.fromisoformat(text)
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO date or datetime") from exc


def _positive_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{label} must be a positive integer")
    return value


def _non_negative_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _string_list(value: Any, label: str, *, required: bool = False) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{label} must be an array of strings")
    result = [_clean_text(item) for item in value if _clean_text(item)]
    if required and not result:
        raise ValueError(f"{label} must contain at least one value")
    if len(result) != len({item.casefold() for item in result}):
        raise ValueError(f"{label} must not contain duplicates")
    return result


def _checkpoint_cursor(checkpoint: Mapping[str, Any] | None) -> int | None:
    if not checkpoint:
        return None
    raw = _clean_text(checkpoint.get("cursor_value"))
    if not raw:
        return None
    if not raw.isdigit() or int(raw) < 1:
        raise ValueError("stored Telegram cursor must be a positive post ID")
    return int(raw)


def build_telegram_plan(
    run_date: str,
    channels: Sequence[Any],
    *,
    initial_lookback_days: int,
    checkpoints: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Build a per-channel first-backfill or delta manifest skeleton."""

    parsed_run_date = _iso_date(run_date, "run_date")
    if not isinstance(initial_lookback_days, int) or isinstance(
        initial_lookback_days, bool
    ) or not 1 <= initial_lookback_days <= 366:
        raise ValueError("initial_lookback_days must be between 1 and 366")
    if not channels:
        raise ValueError("at least one configured Telegram channel is required")

    required: list[str] = []
    streams: list[dict[str, Any]] = []
    for channel in channels:
        key = channel.stream_key
        required.append(key)
        checkpoint = checkpoints.get(key)
        if checkpoint:
            mode = "delta"
            after_post_id = _checkpoint_cursor(checkpoint)
            since_date = _clean_text(checkpoint.get("last_completed_run_date"))
            if not since_date:
                since_date = _clean_text(checkpoint.get("cursor_date"))
            if not since_date:
                since_date = run_date
            _iso_date(since_date, f"checkpoint {key} date")
        else:
            mode = "backfill"
            after_post_id = None
            since_date = (
                parsed_run_date - dt.timedelta(days=initial_lookback_days)
            ).isoformat()

        streams.append(
            {
                "key": key,
                "status": "pending",
                "query": {
                    "handle": channel.handle,
                    "channel_url": channel.url,
                    "url": channel.preview_url,
                    "mode": mode,
                    "since_date": since_date,
                    "after_post_id": after_post_id,
                },
                "pages": [],
                "posts": [],
                "boundary": {"reached": False, "kind": "", "value": ""},
                "found": None,
                "unique": None,
                "known": None,
                "new": None,
                "error": "",
            }
        )

    return {
        "manifest_version": 1,
        "run_date": run_date,
        "source": TELEGRAM_SOURCE,
        "required_streams": required,
        "streams": streams,
        "totals": {"unique": None, "known": None, "new": None},
    }


def _valid_preview_page_url(value: str, handle: str) -> bool:
    parts = urlsplit(value)
    if (
        parts.scheme.casefold() != "https"
        or (parts.hostname or "").casefold() != "t.me"
        or parts.path.rstrip("/").casefold() != f"/s/{handle}".casefold()
        or parts.fragment
    ):
        return False
    params = parse_qs(parts.query, keep_blank_values=True)
    if not params:
        return True
    if set(params) != {"before"} or len(params["before"]) != 1:
        return False
    before = params["before"][0]
    return before.isdigit() and int(before) > 0


def _telegram_external_id_ok(value: str, handle: str, post_id: int) -> bool:
    prefix = f"telegram:{handle}:{post_id}"
    if value == prefix:
        return True
    suffix = value.removeprefix(prefix + ":")
    return bool(suffix and re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", suffix))


def validate_telegram_manifest(
    payload: dict[str, Any],
    channels: Sequence[Any],
    *,
    initial_lookback_days: int,
    checkpoints: Mapping[str, Mapping[str, Any]],
    vacancy_evidence: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate coverage, post classification, ingest evidence, and cursor safety."""

    issues: list[str] = []
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "issues": ["manifest must be a JSON object"],
            "streams": [],
            "totals": {"unique": 0, "known": 0, "new": 0},
        }

    run_date = _clean_text(payload.get("run_date"))
    try:
        _iso_date(run_date, "run_date")
        expected_plan = build_telegram_plan(
            run_date,
            channels,
            initial_lookback_days=initial_lookback_days,
            checkpoints=checkpoints,
        )
    except ValueError as exc:
        issues.append(str(exc))
        expected_plan = {
            "required_streams": [channel.stream_key for channel in channels],
            "streams": [],
        }

    source = _clean_text(payload.get("source")).casefold()
    if source != TELEGRAM_SOURCE:
        issues.append("source must be telegram")

    configured_keys = list(expected_plan["required_streams"])
    configured_folded = {key.casefold(): key for key in configured_keys}
    try:
        declared_required = _string_list(
            payload.get("required_streams"), "required_streams", required=True
        )
    except ValueError as exc:
        declared_required = []
        issues.append(str(exc))
    declared_folded = {key.casefold() for key in declared_required}
    for key in configured_keys:
        if key.casefold() not in declared_folded:
            issues.append(f"required_streams omits configured Telegram channel: {key}")
    for key in declared_required:
        if key.casefold() not in configured_folded:
            issues.append(f"required_streams contains unconfigured Telegram channel: {key}")

    expected_streams = {
        stream["key"].casefold(): stream for stream in expected_plan.get("streams", [])
    }
    raw_streams = payload.get("streams")
    if not isinstance(raw_streams, list):
        raw_streams = []
        issues.append("streams must be an array")

    normalized_streams: list[dict[str, Any]] = []
    seen_streams: set[str] = set()
    all_external_ids: set[str] = set()
    all_canonical_ids: set[int] = set()

    for stream_index, raw in enumerate(raw_streams):
        if not isinstance(raw, dict):
            issues.append(f"streams[{stream_index}] must be an object")
            continue
        key = _clean_text(raw.get("key"))
        folded_key = key.casefold()
        if not key:
            issues.append(f"streams[{stream_index}].key is required")
            continue
        if folded_key in seen_streams:
            issues.append(f"duplicate stream key: {key}")
            continue
        seen_streams.add(folded_key)
        expected = expected_streams.get(folded_key)
        stream_issues: list[str] = []
        if expected is None:
            stream_issues.append("stream is not a configured Telegram channel")

        status = _clean_text(raw.get("status")).casefold()
        if status not in {"completed", "blocked"}:
            stream_issues.append("status must be completed or blocked")
        error = _clean_text(raw.get("error"))
        if status == "blocked":
            stream_issues.append("required Telegram channel is blocked")
            if not error:
                stream_issues.append("blocked stream must include error")

        query = raw.get("query")
        if not isinstance(query, dict):
            query = {}
            stream_issues.append("query must be an object")
        expected_query = expected.get("query", {}) if expected else {}
        for field in ("handle", "channel_url", "url", "mode", "since_date"):
            if _clean_text(query.get(field)) != _clean_text(expected_query.get(field)):
                stream_issues.append(f"query.{field} does not match the generated plan")
        if query.get("after_post_id") != expected_query.get("after_post_id"):
            stream_issues.append("query.after_post_id does not match the stored checkpoint")

        handle = _clean_text(expected_query.get("handle"))
        mode = _clean_text(expected_query.get("mode"))
        since_date_text = _clean_text(expected_query.get("since_date")) or run_date
        try:
            since_date = _iso_date(since_date_text, f"{key}.query.since_date")
        except ValueError as exc:
            stream_issues.append(str(exc))
            since_date = dt.date.min
        after_post_id = expected_query.get("after_post_id")

        pages = raw.get("pages")
        if not isinstance(pages, list):
            pages = []
            stream_issues.append("pages must be an array")
        if status == "completed" and not pages:
            stream_issues.append("completed stream must include at least one fetched page")
        page_urls: set[str] = set()
        page_post_ids: set[int] = set()
        base_page_present = False
        for page_index, page in enumerate(pages):
            if not isinstance(page, dict):
                stream_issues.append(f"pages[{page_index}] must be an object")
                continue
            page_url = _clean_text(page.get("url"))
            if not _valid_preview_page_url(page_url, handle):
                stream_issues.append(
                    f"pages[{page_index}].url must be the configured public preview or its before page"
                )
            if page_url in page_urls:
                stream_issues.append(f"duplicate fetched page URL: {page_url}")
            page_urls.add(page_url)
            if not urlsplit(page_url).query:
                base_page_present = True
            post_ids = page.get("post_ids")
            if not isinstance(post_ids, list):
                stream_issues.append(f"pages[{page_index}].post_ids must be an array")
                continue
            local_ids: set[int] = set()
            for post_index, raw_post_id in enumerate(post_ids):
                try:
                    post_id = _positive_int(
                        raw_post_id, f"pages[{page_index}].post_ids[{post_index}]"
                    )
                except ValueError as exc:
                    stream_issues.append(str(exc))
                    continue
                if post_id in local_ids:
                    stream_issues.append(
                        f"pages[{page_index}] contains duplicate post ID: {post_id}"
                    )
                local_ids.add(post_id)
                page_post_ids.add(post_id)
        if status == "completed" and not base_page_present:
            stream_issues.append("scan must start from the channel's current preview page")

        posts = raw.get("posts")
        if not isinstance(posts, list):
            posts = []
            stream_issues.append("posts must be an array")
        post_ids_seen: set[int] = set()
        post_dates: dict[int, dt.date] = {}
        in_scope_posts: set[int] = set()
        stream_external_ids: set[str] = set()
        stream_canonical_ids: set[int] = set()
        for post_index, post in enumerate(posts):
            if not isinstance(post, dict):
                stream_issues.append(f"posts[{post_index}] must be an object")
                continue
            try:
                post_id = _positive_int(post.get("post_id"), f"posts[{post_index}].post_id")
            except ValueError as exc:
                stream_issues.append(str(exc))
                continue
            if post_id in post_ids_seen:
                stream_issues.append(f"duplicate post checkpoint: {post_id}")
                continue
            post_ids_seen.add(post_id)
            try:
                posted_date = _post_date(
                    post.get("posted_at"), f"posts[{post_index}].posted_at"
                )
                post_dates[post_id] = posted_date
            except ValueError as exc:
                stream_issues.append(str(exc))
                posted_date = dt.date.min

            post_url = _clean_text(post.get("url"))
            expected_post_url = f"https://t.me/{handle}/{post_id}"
            if post_url.rstrip("/") != expected_post_url:
                stream_issues.append(
                    f"post {post_id} URL must be {expected_post_url}"
                )

            if mode == "backfill":
                in_scope = posted_date >= since_date
            elif isinstance(after_post_id, int):
                in_scope = post_id > after_post_id
            else:
                # A completed empty channel has no numeric cursor. Re-scan the
                # last completion date inclusively so same-day posts are safe.
                in_scope = posted_date >= since_date
            if in_scope:
                in_scope_posts.add(post_id)

            classification = _clean_text(post.get("classification")).casefold()
            if classification not in TELEGRAM_POST_CLASSIFICATIONS:
                stream_issues.append(
                    f"post {post_id} classification must be vacancy, non_vacancy, or out_of_scope"
                )
            if in_scope and classification == "out_of_scope":
                stream_issues.append(f"in-scope post {post_id} cannot be out_of_scope")
            if not in_scope and classification != "out_of_scope":
                stream_issues.append(f"boundary post {post_id} must be out_of_scope")

            try:
                external_ids = _string_list(
                    post.get("vacancy_external_ids", []),
                    f"post {post_id} vacancy_external_ids",
                )
            except ValueError as exc:
                stream_issues.append(str(exc))
                external_ids = []
            if classification == "vacancy" and in_scope and not external_ids:
                stream_issues.append(
                    f"vacancy post {post_id} must list every imported vacancy external ID"
                )
            if classification != "vacancy" and external_ids:
                stream_issues.append(
                    f"post {post_id} may list vacancy external IDs only when classified as vacancy"
                )

            for external_id in external_ids:
                if not _telegram_external_id_ok(external_id, handle, post_id):
                    stream_issues.append(
                        f"external ID {external_id!r} must use telegram:{handle}:{post_id}"
                        " with an optional stable item suffix"
                    )
                stream_external_ids.add(external_id)
                all_external_ids.add(external_id)
                evidence = vacancy_evidence.get(external_id)
                if not evidence:
                    stream_issues.append(
                        f"external ID {external_id!r} is not resolved in SQLite after ingest"
                    )
                    continue
                if _clean_text(evidence.get("url")).rstrip("/") != expected_post_url:
                    stream_issues.append(
                        f"external ID {external_id!r} does not preserve its Telegram post URL"
                    )
                score = evidence.get("score")
                if not isinstance(score, int) or isinstance(score, bool) or not 0 <= score <= 100:
                    stream_issues.append(
                        f"external ID {external_id!r} has no completed 0..100 score"
                    )
                source_streams = {
                    _clean_text(item).casefold()
                    for item in evidence.get("source_streams", [])
                }
                if key.casefold() not in source_streams:
                    stream_issues.append(
                        f"external ID {external_id!r} has no source hit for {key}"
                    )
                vacancy_id = evidence.get("vacancy_id")
                if isinstance(vacancy_id, int) and not isinstance(vacancy_id, bool):
                    stream_canonical_ids.add(vacancy_id)
                    all_canonical_ids.add(vacancy_id)

        if page_post_ids != post_ids_seen:
            missing_posts = sorted(page_post_ids - post_ids_seen)
            unproven_posts = sorted(post_ids_seen - page_post_ids)
            if missing_posts:
                stream_issues.append(
                    "fetched page posts lack classification checkpoints: "
                    + ", ".join(map(str, missing_posts))
                )
            if unproven_posts:
                stream_issues.append(
                    "post checkpoints are absent from fetched page evidence: "
                    + ", ".join(map(str, unproven_posts))
                )

        boundary = raw.get("boundary")
        if not isinstance(boundary, dict):
            boundary = {}
            if status == "completed":
                stream_issues.append("boundary must be an object")
        boundary_kind = _clean_text(boundary.get("kind")).casefold()
        boundary_value = boundary.get("value")
        if status == "completed":
            if boundary.get("reached") is not True:
                stream_issues.append("scan boundary was not reached")
            elif boundary_kind == "channel_start":
                pass
            elif isinstance(after_post_id, int):
                if boundary_kind != "post_id":
                    stream_issues.append("delta scan boundary.kind must be post_id or channel_start")
                else:
                    try:
                        boundary_post_id = _positive_int(
                            boundary_value, "boundary.value"
                        )
                        if boundary_post_id > after_post_id:
                            stream_issues.append(
                                "delta boundary post must be at or before the stored cursor"
                            )
                        if boundary_post_id not in post_ids_seen:
                            stream_issues.append(
                                "delta boundary post must appear in fetched post evidence"
                            )
                    except ValueError as exc:
                        stream_issues.append(str(exc))
            else:
                if boundary_kind != "date":
                    stream_issues.append("date scan boundary.kind must be date or channel_start")
                else:
                    try:
                        boundary_date = _iso_date(boundary_value, "boundary.value")
                        if boundary_date >= since_date:
                            stream_issues.append(
                                "date boundary must be older than the inclusive scan start"
                            )
                        if boundary_date not in post_dates.values():
                            stream_issues.append(
                                "date boundary must appear in fetched post evidence"
                            )
                    except ValueError as exc:
                        stream_issues.append(str(exc))

        def checked_count(name: str, expected_count: int | None = None) -> int:
            try:
                count = _non_negative_int(raw.get(name), name)
            except ValueError as exc:
                if status == "completed":
                    stream_issues.append(str(exc))
                return 0
            if status == "completed" and expected_count is not None and count != expected_count:
                stream_issues.append(f"{name} must equal {expected_count}")
            return count

        found_count = checked_count("found", len(in_scope_posts))
        unique_count = checked_count("unique", len(stream_canonical_ids))
        known_count = checked_count("known")
        new_count = checked_count("new")
        if status == "completed" and known_count + new_count != unique_count:
            stream_issues.append("known + new must equal unique")

        previous = checkpoints.get(key) or {}
        try:
            previous_cursor = _checkpoint_cursor(previous)
        except ValueError as exc:
            stream_issues.append(str(exc))
            previous_cursor = None
        observed_cursor = max(post_ids_seen) if post_ids_seen else None
        checkpoint_cursor = max(
            [cursor for cursor in (previous_cursor, observed_cursor) if cursor is not None],
            default=None,
        )
        checkpoint_date = _clean_text(previous.get("cursor_date"))
        if checkpoint_cursor in post_dates:
            checkpoint_date = post_dates[checkpoint_cursor].isoformat()

        query_metadata = {
            "mode": mode,
            "since_date": since_date_text,
            "after_post_id": after_post_id,
            "boundary": {
                "kind": boundary_kind,
                "value": boundary_value,
            },
            "checkpoint_post_id": checkpoint_cursor,
            "checkpoint_post_date": checkpoint_date,
        }
        normalized = {
            "key": key,
            "status": status,
            "query_url": _clean_text(expected_query.get("url")),
            "query_text": json.dumps(query_metadata, ensure_ascii=False, sort_keys=True),
            "search_period_days": initial_lookback_days if mode == "backfill" else 0,
            "page_size": 0,
            "found": found_count,
            "pages_expected": len(pages),
            "pages_visited": len(page_urls),
            "extracted": len(post_ids_seen),
            "unique": unique_count,
            "known": known_count,
            "new": new_count,
            "error": error,
            "issues": stream_issues,
            "checkpoint": {
                "cursor_value": str(checkpoint_cursor or ""),
                "cursor_date": checkpoint_date,
            },
        }
        normalized_streams.append(normalized)
        issues.extend(f"{key}: {issue}" for issue in stream_issues)

    for key in configured_keys:
        if key.casefold() not in seen_streams:
            issues.append(f"missing configured Telegram channel: {key}")

    totals = payload.get("totals")
    normalized_totals = {"unique": 0, "known": 0, "new": 0}
    if not isinstance(totals, dict):
        issues.append("totals must be an object")
    else:
        for name in normalized_totals:
            try:
                normalized_totals[name] = _non_negative_int(
                    totals.get(name), f"totals.{name}"
                )
            except ValueError as exc:
                issues.append(str(exc))
        if normalized_totals["known"] + normalized_totals["new"] != normalized_totals["unique"]:
            issues.append("totals.known + totals.new must equal totals.unique")
        if normalized_totals["unique"] != len(all_canonical_ids):
            issues.append(
                f"totals.unique must equal {len(all_canonical_ids)} resolved canonical vacancies"
            )

    return {
        "ok": not issues,
        "manifest_version": 1,
        "run_date": run_date,
        "source": TELEGRAM_SOURCE,
        "required_streams": configured_keys,
        "issues": issues,
        "totals": normalized_totals,
        "streams": normalized_streams,
        "vacancy_external_ids": sorted(all_external_ids),
    }
