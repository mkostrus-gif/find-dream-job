#!/usr/bin/env python3
"""Build source queries and validate fail-closed search coverage manifests."""

from __future__ import annotations

import hashlib
import html
import math
import re
import unicodedata
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit


HH_SEARCH_URL = "https://hh.ru/search/vacancy"
HH_FIELDS = {"NAME", "DESCRIPTION"}
STREAM_STATUSES = {"completed", "blocked"}


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalized_string_list(value: Any, label: str, *, required: bool) -> list[str]:
    if value is None and not required:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{label}: требуется массив строк.")
    result = [_clean_text(item) for item in value if _clean_text(item)]
    if required and not result:
        raise ValueError(f"{label}: требуется хотя бы одно значение.")
    if len({item.casefold() for item in result}) != len(result):
        raise ValueError(f"{label}: повторяющиеся значения запрещены.")
    return result


def _positive_int(value: Any, label: str, *, maximum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{label}: требуется положительное целое число.")
    if maximum is not None and value > maximum:
        raise ValueError(f"{label}: значение не должно превышать {maximum}.")
    return value


def _non_negative_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{label}: требуется неотрицательное целое число.")
    return value


def normalize_fingerprint_text(value: Any) -> str:
    """Normalize vacancy text conservatively for exact semantic repost matching."""

    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = unicodedata.normalize("NFKC", text).casefold().replace("ё", "е")
    text = re.sub(r"[^\w]+", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def semantic_vacancy_fingerprint(company: Any, title: Any, description: Any) -> str:
    """Return a stable fingerprint only when all three semantic inputs exist."""

    normalized = [
        normalize_fingerprint_text(company),
        normalize_fingerprint_text(title),
        normalize_fingerprint_text(description),
    ]
    if not all(normalized) or len(normalized[2]) < 80:
        return ""
    digest = hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()
    return f"semantic:v1:{digest}"


def _quote_hh_term(term: str) -> str:
    escaped = term.replace('"', '\\"')
    return escaped if re.fullmatch(r"[\w.+#/-]+", escaped, re.UNICODE) else f'"{escaped}"'


def build_hh_query(query: dict[str, Any]) -> dict[str, Any]:
    """Build one deterministic HH query from declarative OR/AND term groups."""

    if not isinstance(query, dict):
        raise ValueError("Поле query должно быть объектом.")
    any_terms = _normalized_string_list(query.get("any_terms"), "query.any_terms", required=True)
    all_terms = _normalized_string_list(query.get("all_terms", []), "query.all_terms", required=False)
    fields = _normalized_string_list(
        query.get("fields", ["NAME", "DESCRIPTION"]),
        "query.fields",
        required=True,
    )
    fields = [field.upper() for field in fields]
    unsupported = sorted(set(fields) - HH_FIELDS)
    if unsupported:
        raise ValueError("Поле query.fields содержит неподдерживаемые поля HH: " + ", ".join(unsupported))

    search_period_days = _positive_int(
        query.get("search_period_days"), "query.search_period_days"
    )
    items_per_page = _positive_int(
        query.get("items_per_page"), "query.items_per_page", maximum=100
    )

    def fielded(term: str) -> str:
        alternatives = [f"{field}:{_quote_hh_term(term)}" for field in fields]
        return alternatives[0] if len(alternatives) == 1 else "(" + " OR ".join(alternatives) + ")"

    any_clause = "(" + " OR ".join(fielded(term) for term in any_terms) + ")"
    all_clauses = [fielded(term) for term in all_terms]
    query_text = " AND ".join([any_clause, *all_clauses])
    params = {
        "text": query_text,
        "search_period": str(search_period_days),
        "items_on_page": str(items_per_page),
        "order_by": "publication_time",
    }
    return {
        "query_text": query_text,
        "url": f"{HH_SEARCH_URL}?{urlencode(params)}",
        "search_period_days": search_period_days,
        "items_per_page": items_per_page,
        "any_terms": any_terms,
        "all_terms": all_terms,
        "fields": fields,
    }


def _same_hh_url(actual: str, expected: str) -> bool:
    actual_parts = urlsplit(actual)
    expected_parts = urlsplit(expected)
    return (
        actual_parts.scheme == expected_parts.scheme
        and actual_parts.netloc == expected_parts.netloc
        and actual_parts.path.rstrip("/") == expected_parts.path.rstrip("/")
        and parse_qs(actual_parts.query, keep_blank_values=True)
        == parse_qs(expected_parts.query, keep_blank_values=True)
    )


def build_coverage_plan(
    payload: dict[str, Any],
    configured_required_streams: tuple[str, ...],
    *,
    default_period_days: int,
    default_items_per_page: int,
) -> dict[str, Any]:
    """Build deterministic HH URLs and an auditable manifest skeleton."""

    if not isinstance(payload, dict):
        raise ValueError("План покрытия должен быть объектом JSON.")
    source = _clean_text(payload.get("source") or "hh").lower()
    if source != "hh":
        raise ValueError("Встроенный планировщик запросов сейчас поддерживает только source=hh.")
    run_date = _clean_text(payload.get("run_date"))
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date):
        raise ValueError("Поле run_date должно иметь формат ГГГГ-ММ-ДД.")
    required = list(configured_required_streams)
    requested = payload.get("required_streams")
    if requested is not None:
        declared = _normalized_string_list(requested, "required_streams", required=True)
        missing = [stream for stream in required if stream.casefold() not in {x.casefold() for x in declared}]
        if missing:
            raise ValueError("В required_streams отсутствуют настроенные потоки: " + ", ".join(missing))
        required = declared
    streams = payload.get("streams")
    if not isinstance(streams, list):
        raise ValueError("Поле streams должно быть массивом.")
    built: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for index, raw in enumerate(streams):
        if not isinstance(raw, dict):
            raise ValueError(f"Элемент streams[{index}] должен быть объектом.")
        key = _clean_text(raw.get("key"))
        if not key:
            raise ValueError(f"Требуется поле streams[{index}].key.")
        folded = key.casefold()
        if folded in seen_keys:
            raise ValueError(f"Повторяется ключ потока: {key}.")
        seen_keys.add(folded)
        query = dict(raw.get("query") or {})
        query.setdefault("search_period_days", default_period_days)
        query.setdefault("items_per_page", default_items_per_page)
        generated = build_hh_query(query)
        built.append(
            {
                "key": key,
                "status": "pending",
                "query": {**query, "query_text": generated["query_text"], "url": generated["url"]},
                "found": None,
                "pages": [],
                "unique": None,
                "known": None,
                "new": None,
                "error": "",
            }
        )
    missing_specs = [stream for stream in required if stream.casefold() not in seen_keys]
    if missing_specs:
        raise ValueError("В streams отсутствуют обязательные настроенные потоки: " + ", ".join(missing_specs))
    return {
        "run_date": run_date,
        "source": source,
        "required_streams": required,
        "streams": built,
        "totals": {"unique": None, "known": None, "new": None},
    }


def validate_coverage_manifest(
    payload: dict[str, Any], configured_required_streams: tuple[str, ...]
) -> dict[str, Any]:
    """Validate every required stream, page, query parameter, and lazy-load count."""

    issues: list[str] = []
    if not isinstance(payload, dict):
        return {"ok": False, "issues": ["Манифест должен быть объектом JSON."], "streams": []}

    run_date = _clean_text(payload.get("run_date"))
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date):
        issues.append("Поле run_date должно иметь формат ГГГГ-ММ-ДД.")
    source = _clean_text(payload.get("source") or "hh").lower()
    try:
        declared_required = _normalized_string_list(
            payload.get("required_streams"), "required_streams", required=True
        )
    except ValueError as exc:
        declared_required = []
        issues.append(str(exc))
    required = list(configured_required_streams) or declared_required
    declared_folded = {item.casefold() for item in declared_required}
    for stream in required:
        if stream.casefold() not in declared_folded:
            issues.append(f"В required_streams отсутствует настроенный поток: {stream}.")

    raw_streams = payload.get("streams")
    if not isinstance(raw_streams, list):
        raw_streams = []
        issues.append("Поле streams должно быть массивом.")
    normalized_streams: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(raw_streams):
        if not isinstance(raw, dict):
            issues.append(f"Элемент streams[{index}] должен быть объектом.")
            continue
        key = _clean_text(raw.get("key"))
        if not key:
            issues.append(f"Требуется поле streams[{index}].key.")
            continue
        folded = key.casefold()
        if folded in by_key:
            issues.append(f"Повторяется ключ потока: {key}.")
            continue
        stream_issues: list[str] = []
        status = _clean_text(raw.get("status")).lower()
        if status not in STREAM_STATUSES:
            stream_issues.append("Поле status должно иметь значение completed или blocked.")
        if status == "blocked":
            stream_issues.append("Обязательный поток заблокирован.")
            if not _clean_text(raw.get("error")):
                stream_issues.append("Для заблокированного потока требуется поле error.")

        query_result: dict[str, Any] = {}
        query = raw.get("query")
        if source == "hh":
            try:
                query_result = build_hh_query(query)
                actual_url = _clean_text(query.get("url")) if isinstance(query, dict) else ""
                if not actual_url:
                    stream_issues.append("Для доказательства выполнения требуется query.url.")
                else:
                    actual_params = parse_qs(urlsplit(actual_url).query)
                    if "period" in actual_params:
                        stream_issues.append("В query.url используется устаревший параметр period; используйте search_period.")
                    if not _same_hh_url(actual_url, query_result["url"]):
                        stream_issues.append("Адрес query.url не соответствует декларативному плану OR/search_period.")
            except ValueError as exc:
                stream_issues.append(str(exc))
        elif not isinstance(query, dict) or not _clean_text(query.get("url")):
            stream_issues.append("Требуется query.url.")

        found = raw.get("found")
        page_size = int(query_result.get("items_per_page") or 0)
        pages_expected = 0
        extracted_total = 0
        pages_visited = 0
        if status == "completed":
            try:
                found = _non_negative_int(found, "found")
            except ValueError as exc:
                stream_issues.append(str(exc))
                found = 0
            if page_size:
                pages_expected = max(1, math.ceil(found / page_size))
            pages = raw.get("pages")
            if not isinstance(pages, list):
                pages = []
                stream_issues.append("Поле pages должно быть массивом.")
            page_map: dict[int, int] = {}
            for page_index, page in enumerate(pages):
                if not isinstance(page, dict):
                    stream_issues.append(f"Элемент pages[{page_index}] должен быть объектом.")
                    continue
                try:
                    number = _non_negative_int(page.get("page"), f"pages[{page_index}].page")
                    extracted = _non_negative_int(
                        page.get("extracted"), f"pages[{page_index}].extracted"
                    )
                except ValueError as exc:
                    stream_issues.append(str(exc))
                    continue
                if number in page_map:
                    stream_issues.append(f"Повторяется контрольная точка страницы: {number}.")
                    continue
                page_map[number] = extracted
            pages_visited = len(page_map)
            extracted_total = sum(page_map.values())
            if page_size:
                expected_numbers = set(range(pages_expected))
                if set(page_map) != expected_numbers:
                    stream_issues.append(
                        "Контрольные точки страниц должны точно покрывать диапазон 0.."
                        + str(max(pages_expected - 1, 0)) + "."
                    )
                for number in sorted(expected_numbers & set(page_map)):
                    remaining = max(found - number * page_size, 0)
                    expected_count = min(page_size, remaining) if remaining else 0
                    if found == 0:
                        expected_count = 0
                    if page_map[number] != expected_count:
                        stream_issues.append(
                            f"На странице {number} извлечено {page_map[number]}; после полной подгрузки ожидалось {expected_count}."
                        )

        def count_field(name: str) -> int:
            try:
                return _non_negative_int(raw.get(name), name)
            except ValueError as exc:
                if status == "completed":
                    stream_issues.append(str(exc))
                return 0

        unique_count = count_field("unique")
        known_count = count_field("known")
        new_count = count_field("new")
        if status == "completed" and known_count + new_count != unique_count:
            stream_issues.append("Сумма known и new должна равняться unique.")
        if status == "completed" and unique_count > extracted_total:
            stream_issues.append("Значение unique не может превышать число извлечённых карточек.")

        normalized = {
            "key": key,
            "status": status,
            "query_url": query_result.get("url") or (_clean_text(query.get("url")) if isinstance(query, dict) else ""),
            "query_text": query_result.get("query_text") or "",
            "search_period_days": query_result.get("search_period_days") or 0,
            "page_size": page_size,
            "found": int(found or 0),
            "pages_expected": pages_expected,
            "pages_visited": pages_visited,
            "extracted": extracted_total,
            "unique": unique_count,
            "known": known_count,
            "new": new_count,
            "error": _clean_text(raw.get("error")),
            "issues": stream_issues,
        }
        normalized_streams.append(normalized)
        by_key[folded] = normalized
        issues.extend(f"{key}: {issue}" for issue in stream_issues)

    for stream in required:
        if stream.casefold() not in by_key:
            issues.append(f"Отсутствует обязательный поток: {stream}.")

    totals = payload.get("totals")
    normalized_totals = {"unique": 0, "known": 0, "new": 0}
    if not isinstance(totals, dict):
        issues.append("Поле totals должно быть объектом.")
    else:
        for name in normalized_totals:
            try:
                normalized_totals[name] = _non_negative_int(totals.get(name), f"totals.{name}")
            except ValueError as exc:
                issues.append(str(exc))
        if normalized_totals["known"] + normalized_totals["new"] != normalized_totals["unique"]:
            issues.append("Сумма totals.known и totals.new должна равняться totals.unique.")
        stream_unique_sum = sum(item["unique"] for item in normalized_streams)
        if normalized_totals["unique"] > stream_unique_sum:
            issues.append("Значение totals.unique не может превышать сумму unique по потокам.")

    return {
        "ok": not issues,
        "run_date": run_date,
        "source": source,
        "required_streams": required,
        "issues": issues,
        "totals": normalized_totals,
        "streams": normalized_streams,
    }
