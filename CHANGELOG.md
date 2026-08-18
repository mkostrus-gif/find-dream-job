# Changelog

## Unreleased

- Добавлена схема v9: долговечные SQLite-планы ежедневного запуска,
  нормализованные шаги и элементы работы, типизированные версионированные
  манифесты, неизменяемая история переходов, компактный статус, безопасные
  приостановка и возобновление с новым `lease`, контроль изменения конфигурации,
  сверка неопределённых внешних действий и программная проверка полного объёма
  очередей, источников и дополнительных условий. Финализация по-прежнему
  публикует ровно одно атомарное поколение P0; доказательства v8 и поколения
  проекций сохраняются миграцией с резервной копией.
- Added schema v8 projection revisions and expiring daily-run leases; global
  deferred/no-render writes; bounded OS-backed writer/render locks; immutable
  staged projection generations with atomic publication; single-pass actionable
  WIP pagination; and a bounded dashboard projection. Immediate rendering stays
  the compatibility default, and v1–v7 evidence rows are preserved by migration.
- Added schema v7 append-only employer-interaction invalidations, effective
  interaction metrics, deterministic one-row-per-vacancy application state for
  follow-up projections, a fail-closed idempotent correction CLI, and backed-up
  migration from schema v6.
- Added schema v6 append-only lifecycle, current-action, and external-action
  evidence; precise interview progression; configurable decision metadata;
  multi-label source attribution; import quarantine and reprocessing; outcome,
  WIP/SLA, operational-health, account-portfolio, and false-negative-audit
  workflows; plus backed-up idempotent migrations from every supported prior
  schema.
- Added schema v5 generic source checkpoints and configurable public Telegram
  discovery with a per-channel 30-day initial backfill, post-ID deltas,
  fail-closed page/post/score/SQLite reconciliation, and cursors that advance
  only after complete coverage.
- Added schema v4 employer interactions, vacancy-level 14/30-day conversion
  cohorts, canonical source-stream aliases, downstream source-quality metrics,
  explicit employer account radar/signals/links, and evidence-backed vacancy
  factors that never alter score automatically.
- Added LinkedIn Gmail job-alert ingestion with stable LinkedIn job IDs and a
  fail-closed daily mail workflow that scores every recommended vacancy and
  archives each fully processed message only with authorization and verification.
- Added schema v3 external-ID aliases so semantic repost deduplication keeps one
  canonical vacancy while every imported source ID and URL remains resolvable.
- Added fail-closed daily search coverage with configured required streams,
  deterministic HH OR/search-period query generation, page and lazy-load
  checkpoints, SQLite history, and a generated coverage report.
- Added conservative company/title/description fingerprints for exact vacancy
  reposts with new external IDs and an explicit backed-up schema-v2 migration.
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
