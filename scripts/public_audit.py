#!/usr/bin/env python3
"""Audit exactly the files Git would consider for a public repository."""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCAL_DENY_FILE = ROOT / "config" / "public-audit.local.toml"
MAX_PUBLIC_FILE_BYTES = 1_000_000

PRIVATE_ROOTS = {
    "archive",
    "dashboard",
    "data",
    "output",
    "outputs",
    "private",
    "reports",
    "tmp",
    "views",
}
PRIVATE_BASENAMES = {
    ".DS_Store",
    ".env",
    "settings.toml",
    "public-audit.local.toml",
}
PRIVATE_SUFFIXES = {
    ".db",
    ".doc",
    ".docx",
    ".eml",
    ".gif",
    ".gz",
    ".html",
    ".ipynb",
    ".jpeg",
    ".jpg",
    ".key",
    ".mbox",
    ".mobileconfig",
    ".p12",
    ".pdf",
    ".pem",
    ".png",
    ".ppt",
    ".pptx",
    ".pyc",
    ".sqlite",
    ".sqlite3",
    ".tar",
    ".webp",
    ".xls",
    ".xlsx",
    ".zip",
}
PUBLIC_TEXT_FIXTURE_PREFIXES = ("tests/fixtures/",)

SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
    "OpenAI-style key": re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    "GitHub token": re.compile(
        r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"
    ),
    "Slack token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "Anthropic-style key": re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b"),
    "Google API key": re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b"),
    "GitLab token": re.compile(r"\bglpat-[A-Za-z0-9_-]{20,}\b"),
    "Hugging Face token": re.compile(r"\bhf_[A-Za-z0-9]{20,}\b"),
    "npm token": re.compile(r"\bnpm_[A-Za-z0-9]{20,}\b"),
    "Stripe live key": re.compile(r"\b[rs]k_live_[A-Za-z0-9]{16,}\b"),
    "Telegram bot token": re.compile(r"\b\d{8,12}:[A-Za-z0-9_-]{30,}\b"),
    "credential in URL": re.compile(
        r"(?i)https?://[^\s/:@]+:[^\s/@]+@[^\s/]+"
    ),
    "bearer credential": re.compile(
        r"(?i)\bauthorization\s*:\s*bearer\s+[A-Za-z0-9._~+/-]{20,}"
    ),
    "assigned secret": re.compile(
        r"(?i)\b(?:api[_-]?key|client[_-]?secret|password|passwd|access[_-]?token)"
        r"\s*[:=]\s*['\"][^'\"\n]{8,}['\"]"
    ),
}
HOME_PATH_RE = re.compile(r"/(?:Users|home)/([A-Za-z0-9._-]+)/")
WINDOWS_HOME_PATH_RE = re.compile(
    r"\b[A-Z]:[\\/](?:Users|Documents and Settings)[\\/]([^\\/\s]+)[\\/]",
    re.I,
)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@([A-Z0-9.-]+\.[A-Z]{2,})\b", re.I)
PHONE_CANDIDATE_RE = re.compile(r"(?<![\w])\+?\d[\d \t().-]{7,}\d(?![\w])")
IPV4_RE = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")


def run_git(args: list[str], git_dir: Path | None = None) -> subprocess.CompletedProcess[bytes]:
    command = ["git"]
    if git_dir is not None:
        command.extend([f"--git-dir={git_dir}", f"--work-tree={ROOT}"])
    else:
        command.extend(["-C", str(ROOT)])
    command.extend(args)
    return subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def candidate_files() -> list[Path]:
    """Use Git's own ignore engine, even before this folder is a repository."""

    top = run_git(["rev-parse", "--show-toplevel"])
    if top.returncode == 0 and Path(top.stdout.decode().strip()).resolve() == ROOT:
        result = run_git(["ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    else:
        with tempfile.TemporaryDirectory(prefix="find-dream-job-audit-") as temp:
            temp_root = Path(temp)
            initialized = subprocess.run(
                ["git", "init", "--quiet", str(temp_root)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if initialized.returncode != 0:
                raise RuntimeError(initialized.stderr.decode(errors="replace").strip())
            result = run_git(
                ["ls-files", "--others", "--exclude-standard", "-z"],
                temp_root / ".git",
            )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode(errors="replace").strip())
    names = [name for name in result.stdout.decode(errors="surrogateescape").split("\0") if name]
    return sorted((ROOT / name for name in names), key=lambda path: path.as_posix())


def local_deny_literals(path: Path = LOCAL_DENY_FILE) -> list[str]:
    if not path.exists():
        return []
    with path.open("rb") as handle:
        data = tomllib.load(handle)
    values = data.get("deny_literals", [])
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise ValueError(f"{path} deny_literals must be strings")
    return [value for value in values if value]


def is_placeholder_home(user: str) -> bool:
    return user.casefold() in {"your-account", "yourname", "username", "user", "example"}


def is_example_email(domain: str) -> bool:
    domain = domain.casefold()
    return domain in {"example.com", "example.org", "example.net"}


def looks_like_phone(value: str) -> bool:
    digits = re.sub(r"\D", "", value)
    if not 10 <= len(digits) <= 15:
        return False
    return value.startswith("+") or bool(re.search(r"[ \t().-]", value))


def is_safe_example_ipv4(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return True
    if address.is_loopback or address.is_unspecified:
        return True
    documentation_networks = (
        ipaddress.ip_network("192.0.2.0/24"),
        ipaddress.ip_network("198.51.100.0/24"),
        ipaddress.ip_network("203.0.113.0/24"),
    )
    return any(address in network for network in documentation_networks)


def audit_file(path: Path, deny_literals: list[str], max_bytes: int) -> list[str]:
    relative = path.relative_to(ROOT).as_posix()
    issues: list[str] = []
    parts = Path(relative).parts

    if path.is_symlink():
        issues.append("symlinks are not allowed in the public tree")
        return issues
    if parts and parts[0] in PRIVATE_ROOTS:
        issues.append(f"private runtime root is public: {parts[0]}")
    if path.name in PRIVATE_BASENAMES:
        issues.append(f"private filename is public: {path.name}")
    synthetic_text_fixture = (
        path.suffix.casefold() == ".html"
        and relative.startswith(PUBLIC_TEXT_FIXTURE_PREFIXES)
    )
    if path.suffix.casefold() in PRIVATE_SUFFIXES and not synthetic_text_fixture:
        issues.append(f"private/binary file type is public: {path.suffix}")
    if not path.is_file():
        issues.append("candidate path is not a regular file")
        return issues
    size = path.stat().st_size
    if size > max_bytes:
        issues.append(f"file is too large for the public tree: {size} bytes")

    raw = path.read_bytes()
    if b"\0" in raw:
        issues.append("binary content detected")
        return issues
    text = raw.decode("utf-8", errors="replace")
    if "\ufffd" in text:
        issues.append("file is not valid UTF-8")

    combined = f"{relative}\n{text}"
    for label, pattern in SECRET_PATTERNS.items():
        if pattern.search(combined):
            issues.append(f"possible {label}")
    for match in HOME_PATH_RE.finditer(combined):
        if not is_placeholder_home(match.group(1)):
            issues.append("absolute home path")
    for match in WINDOWS_HOME_PATH_RE.finditer(combined):
        if not is_placeholder_home(match.group(1)):
            issues.append("absolute Windows home path")
    for match in EMAIL_RE.finditer(combined):
        if not is_example_email(match.group(1)):
            issues.append("non-example email address")
    if any(looks_like_phone(match.group(0)) for match in PHONE_CANDIDATE_RE.finditer(combined)):
        issues.append("possible phone number")
    if any(not is_safe_example_ipv4(match.group(0)) for match in IPV4_RE.finditer(combined)):
        issues.append("non-example IPv4 address")
    for literal in deny_literals:
        if literal.casefold() in combined.casefold():
            issues.append("local deny literal found")
    return sorted(set(issues))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on any issue")
    parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    parser.add_argument(
        "--max-bytes", type=int, default=MAX_PUBLIC_FILE_BYTES, help="Maximum public file size"
    )
    parser.add_argument(
        "--deny-file",
        type=Path,
        default=LOCAL_DENY_FILE,
        help="Private TOML file containing deny_literals (may live outside the checkout)",
    )
    args = parser.parse_args(argv)
    if args.max_bytes < 1:
        parser.error("--max-bytes must be positive")

    try:
        files = candidate_files()
        deny_literals = local_deny_literals(args.deny_file.expanduser().resolve())
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"public audit error: {exc}", file=sys.stderr)
        return 2

    findings: dict[str, list[str]] = {}
    for path in files:
        issues = audit_file(path, deny_literals, args.max_bytes)
        if issues:
            findings[path.relative_to(ROOT).as_posix()] = issues

    result = {
        "ok": not findings,
        "candidate_count": len(files),
        "candidates": [path.relative_to(ROOT).as_posix() for path in files],
        "findings": findings,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"Public candidate files ({len(files)}):")
        for path in result["candidates"]:
            print(f"  {path}")
        if findings:
            print("Findings:")
            for path, issues in findings.items():
                for issue in issues:
                    print(f"  {path}: {issue}")
        else:
            print("No public-tree privacy or secret findings.")
    return 1 if args.strict and findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
