# Changelog

## Unreleased

- Made repository use agent-first with a canonical `AGENTS.md`, an executable
  first-time onboarding prompt, an operational runbook, copy-paste delegation
  prompts, completion contracts, and documentation consistency tests.
- Separated the reusable engine from candidate-specific settings and data.
- Added external private workspaces through `JOB_SEARCH_HOME`.
- Added non-destructive initialization and workspace diagnostics.
- Made follow-up and automation policy configurable with safe public defaults.
- Added default-deny Git rules, a publication audit, synthetic examples, tests,
  public documentation, and MIT licensing.
- Removed one-off legacy import adapters and candidate-specific channel labels
  from the reusable engine.
- Expanded the publication audit with redacted PII findings and broader secret,
  phone, home-path, and public-IP detection.
- Updated the CI checkout action to the already validated v7 release.
- Hardened SQLite schema version checks and inline dashboard JSON escaping.
