# Job Search System Contract

This is the persistence and CLI reference. An operating agent must first read
[`AGENTS.md`](AGENTS.md) and [`PROJECT_RULES.md`](PROJECT_RULES.md). For a new
candidate, complete [`prompts/onboarding.md`](prompts/onboarding.md) before
treating the workspace as ready for live search.

## Source of truth

The configured SQLite database is authoritative. Generated Markdown and HTML
are read models only.

Default local paths:

- `data/job_search.sqlite` — durable state;
- `dashboard/index.html` — interactive static dashboard;
- `views/review_active.md` — review inbox;
- `views/today.md` — newest vacancies;
- `views/funnel.md` — funnel by source channel;
- `views/followups.md` — due follow-ups and contact-search state;
- `views/wip_queue.md` and numbered `wip_queue_page_*.md` files — capped,
  deterministic WIP/SLA queue with explicit overflow;
- `views/outreach_contacts.md` — verified direct contacts;
- `views/employer_accounts.md` — account radar and latest employer signals;
- `views/vacancy_factors.md` — structured evidence factors;
- `reports/source_quality.md` — screening and downstream source performance;
- `reports/source_streams.md` — raw-to-canonical stream diagnostics;
- `reports/source_checkpoints.md` — durable cursors for incremental sources;
- `reports/conversion_cohorts.md` — vacancy-level application conversion;
- `reports/outcome_scorecard.md` — evidence-first 14/30-day outcome scorecard;
- `reports/quarantine.md` and numbered pages — auditable non-vacancy and
  technical ingestion results;
- `reports/false_negative_audit.md` — deterministic review of rejected and
  low-priority screening decisions;
- `reports/data_quality.md` — import diagnostics.
- `.jobctl/projections/generations/` — complete immutable generated sets;
  `views/`, `reports/`, and `dashboard/index.html` are compatibility links
  through one atomically switched `current` generation.

All paths can be moved with `JOB_SEARCH_HOME` or `config/settings.toml`.

## Bootstrap and health

```bash
python3 scripts/jobctl.py init --json
python3 scripts/jobctl.py doctor --strict --json
python3 scripts/jobctl.py rebuild --json
python3 scripts/jobctl.py stats
```

`init` creates only missing files. `rebuild` also creates an empty database when
none exists. These checks establish technical health; they do not prove that
candidate templates are factually complete. The onboarding evidence review is
still required.

`doctor --strict` checks structural health. `operational-doctor --strict`
separately checks whether all exact daily-closeout preconditions are current.
A structurally healthy database can correctly report
`ready_for_daily_closeout = false` when required source coverage is stale or
incomplete.

Never ingest the public example vacancies into a candidate's real database.
Use a disposable `JOB_SEARCH_HOME` for smoke tests.

## Data model

- `vacancies` — canonical vacancy and latest state;
- `source_hits` — raw discovery events whose `source_stream` is never rewritten;
- `source_labels` / `source_hit_labels` — normalized many-to-many attribution
  derived from current exact aliases;
- `search_runs` / `search_coverage` — fail-closed daily-run manifests and
  per-stream page/query checkpoints;
- `source_checkpoints` — per-source/per-stream cursors that advance only after
  a complete fail-closed manifest;
- `vacancy_external_aliases` — every observed `(channel, external_id)` mapped
  to one canonical vacancy, with its own URL and first/last observation dates;
- `vacancy_fingerprints` — conservative semantic repost aliases based on
  company, title, and normalized description;
- `evaluations` — screening/review decisions;
- `applications` — backward-compatible application rows linked to lifecycle
  evidence when known;
- `lifecycle_events` — append-only durable application, rejection, interview,
  no-show, cancellation, and offer facts;
- `action_events` — append-only current review/input/follow-up/WIP state,
  independent from lifecycle;
- `external_actions` — drafted/authorized/attempted/confirmed/blocked/failed
  evidence for actions affecting external systems;
- `vacancy_decision_metadata` — configured campaigns, role families, evidence
  confidence, hard gates, open questions, resume IDs, message variant, and
  human-path state;
- `stage_events` — legacy append-only stage/status compatibility evidence;
- `employer_interactions` — append-only inbound/outbound employer interactions,
  separate from funnel stage changes;
- `employer_interaction_invalidations` — append-only, evidence-backed
  invalidations of exact erroneous interaction rows;
- `interview_summaries` — links to private local notes;
- `employer_contacts` — evidence-backed recruiter/hiring contacts;
- `contact_searches` — positive, negative, and ambiguous lookup history;
- `followup_rounds` — numbered follow-up waves and next date;
- `outreach_messages` — exact per-channel message and delivery evidence;
- `employer_accounts` / `employer_account_signals` — account radar and
  evidence-backed employer observations;
- `vacancy_employer_accounts` — explicit vacancy-to-account links;
- `vacancy_factors` — structured evidence that never changes score automatically;
- `quarantine_records` — raw technical/non-vacancy evidence and retry state;
- `policy_versions` / `screening_decisions` — one active dated policy plus
  reproducible rule-level screening history;
- `migration_log` — auditable schema-upgrade record;
- `import_issues` — ingestion diagnostics.

Supported funnel stages are `seen`, `needs_input`, `follow_up`, `applied`,
`interview_1`, `interview_2`, `interview_3`, `offer`, and `rejected`.

The evidence vocabulary is intentionally non-interchangeable:

| Concept | Durable evidence | What it does not prove |
|---|---|---|
| Screening signal | `source_hits.raw_status` / evaluation evidence | Application or employer interest |
| Current work item | Latest `action_events` row | Application, rejection, interview, or offer |
| Application | Earliest `application_confirmed` lifecycle event linked to a visibly confirmed action | Human response |
| Automated acknowledgment | Inbound automated `employer_interactions` row | Human reply |
| Human reply | Inbound human `employer_interactions` row after application | Interview or stage change |
| Interview invitation | `interview_invited` lifecycle event and round | Scheduled or completed interview |
| Scheduled interview | `interview_scheduled` event with scheduled time | Completed interview |
| Completed interview | Explicit `interview_completed` event or a summary that passes the documented evidence rule | Offer or candidate fit by itself |
| Verified contact | Active confirmed/strong `employer_contacts` row | Reply or hiring authority beyond stored evidence |
| Employer signal | Evidence-backed `employer_account_signals` row | Candidate experience or fit |
| Candidate evidence | Configured private profile/Q&A source | Employer quality or hiring reality |

## Structured ingestion

```bash
python3 scripts/jobctl.py ingest-json examples/vacancies.json \
  --channel company_site --source official_board
```

Accepted top-level JSON forms are an array or an object containing
`vacancies`, `items`, `jobs`, or `data`. Useful row fields include:

```json
{
  "date": "2026-01-15",
  "channel": "company_site",
  "source": "official_board",
  "source_stream": "product_roles",
  "external_id": "company_site:example-product-operations",
  "kind": "screening",
  "title": "Head of Product Operations",
  "company": "Example Labs",
  "url": "https://example.com/careers/product-operations",
  "status": "NEEDS_REVIEW",
  "stage": "seen",
  "score": 78,
  "role_type": "Product Operations",
  "reason": "Relevant scope; ownership needs confirmation",
  "risks": "Location is not listed",
  "open_questions": "Confirm team and reporting line",
  "next_action": "manual_review",
  "factors": [
    {
      "factor_key": "hiring_reality",
      "value": "active requisition",
      "observed_date": "2026-01-15",
      "confidence": "confirmed",
      "evidence_note": "Role is present on the official careers page",
      "evidence_url": "https://example.com/careers/product-operations"
    }
  ]
}
```

The command deduplicates, writes the appropriate history rows, and regenerates
the read models.

Decision metadata fields (`campaign_id`, `role_family`, `master_resume_id`,
`planned_resume_id`, `actual_resume_id`, and `message_variant`) accept only
values declared in the local `[decision]` configuration. `confidence` uses the
controlled vocabulary `low`, `medium`, `high`, `confirmed`, or `unknown`.
`hard_gates` and `unresolved_questions` are structured arrays. Missing history
stays unknown; ingestion never guesses a campaign, resume, gate result, or
human path.

CAPTCHA pages, logged-out or access-error pages, malformed URLs/payloads, and
source-aware missing required fields are written to `quarantine_records` with
raw evidence and retry context. They do not create `vacancies`, enter WIP, or
affect vacancy/source/outcome denominators. Use `quarantine-report`, an exact
`reprocess-quarantine --id ...`, or the non-mutating
`classify-legacy-records --dry-run`; no age-based bulk mutation exists.

When a source provides a description or snippet, include it in the JSON row.
The engine resolves a row in deterministic order: canonical
`vacancies.external_id`, stored external-ID alias in the same channel,
conservative semantic fingerprint, then creation of a new canonical vacancy.
After any successful resolution it stores the incoming external ID in
`vacancy_external_aliases`, including exact matches, new vacancies, and semantic
reposts.

`vacancies.external_id` and `vacancies.url` are the stable canonical source
identity and URL. A semantic repost does not replace them. Each repost URL is
stored beside its external ID in `vacancy_external_aliases`; the row with the
newest `last_seen_date` is the latest observed repost. Re-ingesting an alias
updates its last-observed metadata without creating another alias or vacancy.

For scan-to-SQLite reconciliation, compare every input `(channel, external_id)`
against the union of canonical identities and aliases. Do not compare only with
`vacancies.external_id`:

```sql
SELECT channel, external_id FROM vacancies
UNION
SELECT channel, external_id FROM vacancy_external_aliases;
```

A completed ingest has zero scan identities missing from that result. The
canonical vacancy count may be lower than the scan identity count when semantic
reposts were correctly merged.

Optional `factors` accept a lowercase snake-case key, scalar `value` or
`level`, observed date, confidence, evidence note, and optional evidence URL.
Core keys are `technology_adoption_maturity`, `work_content_risk`,
`hiring_reality`, and `human_access`; additional generic keys are allowed.
Factors are evidence records only. Ingestion never changes a vacancy score
because of a factor, an AI label, or an employer signal.

## Canonical source streams

`source_hits.source_stream` always preserves the raw source value. Optional
local `[source_stream_aliases]` entries map one exact raw alias
case-insensitively to one canonical reporting key or an explicit array of
several keys. The engine never splits arbitrary plus signs or other
punctuation. `source_hit_labels` stores the derived many-to-many links, while
the raw hit remains one row. Reports refresh links from the current aliases on
rebuild, so a mapping change is predictable and does not rewrite history.
Downstream application cohorts still receive one deterministic first touch:
the first configured label of the selected raw hit. An unmapped value keeps its
raw identity. Inspect `reports/source_streams.md` before consolidating streams.

Search coverage still validates the exact configured
`search.required_streams` manifest fail-closed. Stream aliases consolidate
outcome reporting; they do not excuse a missing required checkpoint.

## Telegram channel discovery

Public Telegram channel URLs are private, candidate-specific configuration:

```toml
[telegram]
enabled = true
initial_lookback_days = 30
channels = ["https://t.me/example_exec_jobs"]
```

The public Engine contains no real channel list. It accepts only public `t.me`
handles, canonicalizes URLs, rejects post/invite URLs and duplicates, and uses
one stream key per channel: `telegram:<handle>`.

Build the daily plan from SQLite rather than choosing a date range manually:

```bash
python3 scripts/jobctl.py build-telegram-plan --run-date 2026-01-15 \
  --output tmp/telegram_coverage_2026-01-15.json --json
```

A channel without a completed `source_checkpoints` row receives an inclusive
initial backfill beginning `initial_lookback_days` before the run date. A
completed channel receives a delta plan from its last observed numeric post ID.
Adding another channel does not reset established channels: the new stream
backfills independently.

The operating agent starts at `https://t.me/s/<handle>`, records every fetched
page and post ID/date, and paginates backward until it proves the generated
date/post boundary or the actual start of the channel. Every in-scope post is
classified as vacancy-bearing or non-vacancy. Every vacancy is scored and
ingested before coverage closes, using:

- `channel = telegram`;
- `source = telegram_public_channel`;
- `source_stream = telegram:<handle>`;
- exact post URL;
- `external_id = telegram:<handle>:<post_id>` for a single vacancy, or the same
  prefix plus a stable lowercase item suffix for a multi-vacancy post.

Then validate and persist:

```bash
python3 scripts/jobctl.py check-telegram-coverage \
  tmp/telegram_coverage_2026-01-15.json
```

The validator fails closed on a missing configured channel, unproven boundary,
unclassified fetched post, unstable/mismatched ID, absent post URL, missing
SQLite alias/source hit, or missing 0–100 score. Incomplete attempts are kept in
`search_runs` / `search_coverage`, but `source_checkpoints` advances only after
the entire Telegram manifest succeeds. This is read-only discovery authority;
it never authorizes joining, messaging, applying, or another external action.

## Search coverage contract

Prepare a private plan whose stream keys match local
`search.required_streams`, then let the engine generate deterministic HH URLs:

```bash
python3 scripts/jobctl.py build-coverage-plan tmp/search_plan_2026-01-15.json \
  --output tmp/search_coverage_2026-01-15.json
```

The generated manifest uses explicit OR groups, `NAME`/`DESCRIPTION`,
`search_period`, and the configured page size. After the scan, fill every page
checkpoint and de-duplicated total, then persist and validate it:

```bash
python3 scripts/jobctl.py check-coverage tmp/search_coverage_2026-01-15.json
```

The command exits non-zero for an omitted/blocked stream, a modified or stale
query URL, incomplete pagination, a partially lazy-loaded page, or inconsistent
known/new totals. Its durable read model is `reports/search_coverage.md`.

## Employer interactions and conversion

Record an employer event against an exact canonical ID, stored external-ID
alias, canonical URL, or alias URL:

```bash
python3 scripts/jobctl.py record-employer-interaction \
  --external-id company_site:example-product-operations \
  --at 2026-01-20T11:30:00 --direction inbound \
  --event-type human_reply --channel email --actor-type recruiter \
  --humanity human --evidence-note "Reply visible in the authorized mailbox" \
  --json
```

Supported interaction types are `human_reply`, `automated_ack`,
`screening_request`, `interview_invite`, `rejection`, and `other`. An inbound
event requires an evidence note. `automated_ack` must be automated and never
counts as a human reply. Exact repeats are idempotent; an external reference,
when supplied, is the strongest duplicate key. Recording an interaction never
changes lifecycle or current action state. Historical interactions are never
inferred from free-form status text.

If a stored interaction is later proven erroneous, preserve the original row
and append an explicit invalidation:

```bash
python3 scripts/jobctl.py invalidate-employer-interaction \
  --interaction-id 42 \
  --reason "The event was recorded against evidence that does not exist" \
  --evidence-note "Operator rechecked the authorized source and found no message" \
  --source operator_review \
  --operator-context manual_reconciliation \
  --json
```

The target interaction must exist. Optional `--vacancy-id`, `--vacancy-url`, or
`--vacancy-external-id` acts as an exact guard and fails when the interaction
belongs to another vacancy. Reason, evidence, and source are mandatory. An
exact repeat returns the existing invalidation; a repeat with conflicting
metadata fails closed. Raw audit queries continue to use
`employer_interactions`; KPI and generated-output queries use
`effective_employer_interactions`, which excludes invalidated rows. See
[`docs/evidence-corrections.md`](docs/evidence-corrections.md).

Use `record-lifecycle-event` for `rejected`, `interview_invited`,
`interview_scheduled`, `interview_completed`, `interview_cancelled`, candidate
or employer no-show, later rounds, and `offer_received`. Interview events
require a positive round number; a scheduled event also requires its event
time. An invitation or scheduled slot never counts as completed. The only
summary shortcut is `attach-interview-summary --confirms-completion`: the file
must contain at least 80 non-whitespace characters and three meaningful lines,
and the command requires a separate completion-evidence note.

Run a reproducible cohort calculation with:

```bash
python3 scripts/jobctl.py conversion-report --as-of 2026-02-28 --json
python3 scripts/jobctl.py outcome-scorecard --as-of 2026-02-28 --json
```

`conversion-report` remains a compatibility projection. The supported
first-class report is `outcome-scorecard`, also generated as
`reports/outcome_scorecard.md`. Its grain is one canonical vacancy plus its
earliest `application_confirmed` lifecycle event; mutable application/action
rows cannot multiply or erase the cohort. The 14-day human-reply denominator
contains applications at least 14 calendar days old at `as_of`; automated
acknowledgments are excluded. The 30-day completed-first-interview numerator
requires explicit completion evidence, not an invite or scheduled slot.

Source-stream attribution is deterministic first touch: the earliest
`source_hit` on or before the first application date, with `source_hits.id` as
the tie-breaker, unless an application source is explicitly linked. Missing
pre-application hits are unknown. Breakdowns cover configured campaign, role
family, normalized source stream, source channel, employer account, actual
resume, message variant, and application month. The scorecard also reports
invitations, scheduled/completed/later interviews, offers, contact-search and
verified-human-path coverage, and field completeness. Every rate carries its
numerator and denominator. Incomplete migrated history is `n/a`, not a
fabricated zero, and samples below ten receive an explicit warning.

## Employer account radar and vacancy factors

Accounts are exact, explicit entities; the engine performs no fuzzy employer
matching:

```bash
python3 scripts/jobctl.py upsert-employer-account \
  --canonical-name "Example Labs" --website https://example.com \
  --careers-url https://example.com/careers --priority high --status target --json
python3 scripts/jobctl.py link-vacancy-account \
  --id 42 --account-name "Example Labs" --evidence-note "Explicit operator link" --json
python3 scripts/jobctl.py record-employer-signal \
  --account-name "Example Labs" --signal-type technology_adoption \
  --observed-date 2026-01-20 --confidence confirmed \
  --evidence-note "Official product announcement" \
  --evidence-url https://example.com/news --json
python3 scripts/jobctl.py record-vacancy-factor \
  --id 42 --factor-key human_access --value "verified recruiter" \
  --observed-date 2026-01-20 --confidence confirmed \
  --evidence-note "Current recruiting contact on the official page" --json
```

An account name is matched only by case/Unicode/whitespace-normalized equality;
a vacancy link is always explicit. Employer signals do not prove candidate
fit, growth, or culture beyond their exact evidence. In particular, employer
AI adoption and a candidate's use of AI tools do not prove enterprise AI
transformation experience. Candidate claims require private candidate evidence,
and candidate-relative scores remain the result of private policy.

Accounts also support a configured active-portfolio limit, account status and
priority, review cadence/next date, website and careers-page freshness,
configured campaign/role-family targets, owner/sponsor/governance evidence, and
human-path status. Signals have dates and controlled confidence. Account
signals, vacancy fit, candidate score, and authorization to contact a person
remain four separate concepts.

## Schema upgrades

Schema upgrades are explicit and create a timestamped backup by default:

```bash
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py migrate-schema --json
```

Схема v9 обновляет все поддерживаемые версии v1–v8. Она сохраняет полную модель
доказательств и поколения проекций v8, добавляет для `lease` корректное
состояние `released` и создаёт только таблицы долговечной оркестрации ежедневных
запусков. Схема v8 сохранила модель доказательств v7 и добавила ревизии
актуальности проекций и ограниченный по времени `lease`. Схема v7 сохранила
модель v6 и добавила неизменяемые исправления взаимодействий с работодателями,
а также эффективные проекции взаимодействий и откликов. Схема v6 сохранила
инкрементальные контрольные точки v5 и добавила события жизненного цикла и
действий, доказательства внешних действий, метаданные решений, множественные
метки источников, карантин, версионированную политику отбора и журнал миграций.
Существующие подтверждённые отклики становятся событиями
`application_confirmed` с `history_complete = 0` и
`authorization_status = legacy_unknown`: доказательство сохраняется без
выдумывания прежнего разрешения или полной истории ответов и интервью.
Подтверждённые старые отказы сохраняются, а неоднозначным историческим строкам
автоматически не назначаются карантин, кампании, резюме, ответы, интервью,
офферы, полнота источника или путь к человеку.

Do not use `--no-backup` on a live candidate database. After migration, use the
same workspace selection for:

```bash
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py doctor --strict --json
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py rebuild --json
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py stats
```

Первая успешная пересборка v8 переносит прежние каталоги результатов в хранилище
поколений и заменяет публичные пути совместимыми символическими ссылками. Эту
одноразовую операцию выполняют при остановленных читателях; последующие
публикации атомарно переключают один указатель `current`. Откат выполняется из
копии `job_search.sqlite.bak-schema-v<old>-<timestamp>`: остановите процессы
записи, сохраните текущую базу для диагностики, восстановите резервную копию по
настроенному пути базы и используйте прежнюю совместимую версию Engine.
Разрушающей обратной миграции нет. После обновления до v9 нельзя открывать базу
старой версией Engine: она не знает долговечный план запуска и может нарушить
координацию. Повторный `migrate-schema` для схемы v9 идемпотентен и не создаёт
новую резервную копию.

## Долговечная оркестрация ежедневного запуска

Одна компактная команда статуса восстанавливает контекст после смены процесса
или задачи:

```bash
python3 scripts/jobctl.py daily-run-status --json
python3 scripts/jobctl.py resume-daily-run --run-id <run_id> --json
```

Запуск существует дольше своего `lease`. Команда `pause-daily-run` освобождает
`lease`, но сохраняет SQLite и загрязнённое состояние проекций для продолжения.
Истечение `lease` также не удаляет план, манифесты или историю переходов. Второй
незавершённый запуск запрещён.

Схема v9 хранит текущий снимок запуска в `daily_runs`, неизменяемые ревизии
плана в `daily_run_plan_revisions`, граф в `daily_run_steps` и
`daily_run_step_dependencies`, точные единицы работы в `daily_run_work_items`,
доказательства в `daily_run_manifests`, а переходы — в
`daily_run_transitions`.

Полный цикл CLI:

```text
begin-daily-run
daily-run-status [--run-id ID] [--verbose]
resume-daily-run --run-id ID
start-daily-run-work --step-key KEY [--item-key KEY]
checkpoint-daily-run-work --manifest FILE
complete-daily-run-work --manifest FILE
block-daily-run-work --code CODE --reason TEXT --retryable|--not-retryable
mark-daily-run-work-uncertain --reason TEXT
invalidate-daily-run-work --reason TEXT
refresh-daily-run-plan --reason TEXT
pause-daily-run --reason TEXT
finalize-daily-run
```

Все изменения после `begin-daily-run` требуют точные флаги
`--defer-render --run-lease <token>`, проходят блокировку записи P0 и сразу
фиксируются в SQLite. Они не запускают `render`. Обычный статус ограничен
агрегатами, причинами блокировки, последними контрольными точками и следующими
безопасными единицами работы; `--verbose` явно добавляет полный план, манифесты
и историю.

Манифест v1 (`manifest_version = 1`) содержит точные `run_id`, `step_key`,
необязательный `item_key`, `kind`, `observed_at`, `captured_scope`, необязательные
счётчики (`raw`, `unique`, `known`, `new`, `processed`, `reconciled`, `blocked`),
`completion_boundary`, причины блокировки и `remote_boundary_verified`.
Подробное доказательство задаётся относительным от рабочей области `path` и
SHA-256. Engine проверяет контракт, но не утверждает, что сам видел полноту
удалённого Inbox или браузерной страницы. Без видимо подтверждённой границы
удалённый шаг не завершается.

Типизированные результаты HH и Telegram автоматически связываются с элементами
зафиксированного плана. Успешный поток или канал закрывается, частичный результат
сохраняет последнюю проверенную страницу или курсор, а заблокированный остаётся
незавершённым с точной причиной. Попытка внешнего действия без видимого успеха
переходит в `needs_verification`; безопасное продолжение по умолчанию — сверка,
а не повторная отправка.

`finalize-daily-run` заново перечисляет всю обязательную очередь наступивших
повторных обращений без ограничения WIP и квоты сообщений, сверяет
нетерминальные внешние действия, покрытие источников, дополнительные условия,
отпечаток конфигурации, `PRAGMA quick_check` и внешние ключи. Только после этого
запуск входит в `finalizing`, публикует одно поколение P0 и становится
`completed`. Если обязательные входные данные изменились во время прерванной
финализации, код с записью в журнал возвращает запуск в `running`, `blocked` или
`needs_verification` и требует повторной проверки. Повторная финализация уже
завершённого запуска ничего не меняет и не выполняет второй `render`.

## Lifecycle, current action, and external-action evidence

Use the internal ID when available:

```bash
python3 scripts/jobctl.py set-current-action \
  --id 42 --action-state needs_input --bucket urgent \
  --due-date 2026-01-21 --priority 100 \
  --priority-reason "Questionnaire needs one verified answer"
```

`--url` or `--external-id` may be used when the ID is unknown. All commands
accepting `--external-id` resolve both the canonical ID and any stored alias;
stored alias URLs are also resolvable through `--url`. Resolve the target before
writing. A current action never erases a lifecycle fact. A rejection remains
terminal for that canonical vacancy; later legacy cleanup to `seen` cannot
regress it.

Record an external action as an append-only state sequence. The score and
`auto_apply` configuration are irrelevant to authorization:

```bash
python3 scripts/jobctl.py record-external-action \
  --id 42 --action-key application-42-v1 --action-type application \
  --state authorized --authorization-note "Exact current authorization" \
  --source operator
python3 scripts/jobctl.py record-external-action \
  --id 42 --action-key application-42-v1 --action-type application \
  --state attempted --evidence-note "Submission attempted" --source operator
python3 scripts/jobctl.py record-external-action \
  --id 42 --action-key application-42-v1 --action-type application \
  --state visibly_confirmed --evidence-note "Acceptance page visible" \
  --external-reference synthetic-confirmation-42 --source operator \
  --campaign-id "Campaign Alpha" --actual-resume-id "Resume Alpha"
```

Only the last command may create `application_confirmed`. Replaying the same
visible reference is idempotent. `record-lifecycle-event` deliberately refuses
direct `application_confirmed` writes.

## Deferred writes and one-render daily runs

Existing commands remain backward compatible: without another option, a write
commits SQLite and renders immediately. A batch or daily run can instead use
either spelling below on every mutating command:

```bash
python3 scripts/jobctl.py update-vacancy ... --defer-render
python3 scripts/jobctl.py update-vacancy ... --no-render
```

The global flag also applies to `init` and `migrate-schema`. A deferred `init`
creates the durable schema with dirty projections but intentionally does not
claim bootstrap completion until a later successful `rebuild`.

The domain transaction and `projection_state.dirty_revision` commit together.
Generated files are not opened, removed, or rewritten. Inspect the resumable
state with `projection-status --json`; `rebuild --json` publishes one complete
generation and advances `rendered_revision` only after success.

A live daily run must also hold one durable lease:

```bash
python3 scripts/jobctl.py begin-daily-run --run-id daily-YYYY-MM-DD-agent --json
# pass both flags to every later write:
python3 scripts/jobctl.py set-current-action ... \
  --defer-render --run-lease <token>
python3 scripts/jobctl.py finalize-daily-run --run-lease <token> --json
```

Only one unexpired lease may exist. Writes during it require the exact token and
heartbeat the bounded expiry. The Engine rejects a non-deferred write or an
ordinary `rebuild` while that lease is active. `finalize-daily-run` performs the
run's only full render and releases the lease only after atomic publication. Repeating a
successfully finalized command is idempotent and does not render again. A lock
timeout or interruption leaves SQLite durable, projections dirty, the lease
resumable, and the previous generated set intact. `--lock-timeout` controls the
bounded writer/render wait (30 seconds by default).

## WIP, SLA, quarantine, and operational closeout

```bash
python3 scripts/jobctl.py wip-queue --page 1 --page-size 100 --json
python3 scripts/jobctl.py quarantine-report --page 1 --json
python3 scripts/jobctl.py false-negative-audit --as-of 2026-02-28 --json
python3 scripts/jobctl.py operational-doctor --as-of 2026-02-28 --strict --json
```

The actionable queue orders configured buckets, overdue state, overdue age,
priority, due date, event time, and vacancy ID deterministically. Its latest
action set is materialized once per request/render and then sliced into pages.
`action_state=none`, inactive backlog buckets, and terminal rejected/offer
lifecycle rows are excluded from actionable WIP; their append-only history
remains in SQLite. Limits mark overflow and never mutate history. Numbered
Markdown pages cover the whole actionable result rather than truncating it.
Use `wip-queue --page ... --json` for supported pagination and read
`action_events`/`lifecycle_events` with read-only SQLite queries for the full
audit history.

`operational-doctor` returns per-check `pass`, `warn`, or `fail`, independent
`technical_health`, and `ready_for_daily_closeout`. Current exact search
coverage and enabled personal/incremental source checkpoints are closeout
gates. A green structural `doctor` alone is not closeout permission.

## Contacts and follow-ups

Store a contact only with identity evidence:

```bash
python3 scripts/jobctl.py upsert-contact \
  --id 42 --person-name "Example Recruiter" --person-role "Recruiter" \
  --relationship recruiter --confidence confirmed \
  --contact-channel linkedin --contact-address "profile-handle" \
  --profile-url "https://example.com/profile" \
  --evidence-note "Named contact on the official vacancy page"
```

If no usable contact exists, preserve that result:

```bash
python3 scripts/jobctl.py record-contact-search \
  --id 42 --status not_found --channels-checked linkedin \
  --note "Checked the vacancy, company directory, and current recruiting team"
```

`record-followup` accepts a local JSON payload with `contact_search` and
`touchpoints`. A sent touchpoint requires exact text and visible delivery
evidence plus an `external_action_key` whose matching `message` or `follow_up`
action is already `visibly_confirmed`. Direct touchpoints also require a stored
contact ID. Follow-up limit, interval, primary channel, and preferred direct
channels come from local settings.

## Private profile and prompts

The paths to candidate profile, preferences, scoring calibration, and reusable
Q&A are defined in ignored `config/settings.toml`. Public prompts refer to those
configured files rather than embedding candidate facts.

Unknown facts always transition to `needs_input` with the exact question. After
the candidate answers, semantically merge the reusable fact into the configured
private Q&A file.

The primary agent flows are `prompts/onboarding.md` for first setup and
`prompts/daily_run.md` for recurring operation. Source-specific tools may
discover or submit data, but their result becomes durable system state only
after an evidence-backed `jobctl.py` write.

Gmail vacancy extraction supports both HH and LinkedIn. The legacy command
defaults to HH; pass `--provider linkedin` for LinkedIn job mail. LinkedIn rows
default to channel `linkedin`, source `linkedin_gmail_job_alert`, and stable
`linkedin:<job-id>` identity when the canonical job URL exposes that ID:

```bash
python3 scripts/jobctl.py ingest-gmail-json tmp/linkedin_mail_YYYY-MM-DD.json \
  --provider linkedin --json
```

This discovery import does not replace full screening. Open and score every
vacancy, then ingest the completed screening rows with `ingest-json`. Mailbox
archiving remains an external action: perform it only within explicit scope,
after per-message reconciliation, and verify removal of the `INBOX` label.

## Generated output

Immediate render remains the compatibility default. Deferred writes only mark
the projections dirty. A rebuild renders all views, reports, and the compact
dashboard into a staging generation, fsyncs and validates the set, then
publishes it through one atomic `current` pointer. Failed or interrupted
renders never delete the last complete generation, and obsolete numbered pages
disappear only when a new complete generation becomes current. Manual changes
to generated files will be lost.

The dashboard embeds only bounded operational vacancy fields; full vacancy and
action history stays in SQLite and paginated CLI/read-model paths. This keeps a
25,000-row workspace from turning one HTML file into a database copy. The core
does not upload or fetch the snapshot and adds no network dependency. Use
`watch` only when another process writes the SQLite database directly:

Generated follow-up rows read `effective_applications`, which selects the
highest application row ID for each canonical vacancy. Compatibility rows stay
auditable, while an older duplicate cannot restore stale status, questions, or
follow-up dates after the latest row has been reconciled. Durable application
counts continue to come from the earliest visibly confirmed
`application_confirmed` lifecycle event per canonical vacancy.

```bash
python3 scripts/jobctl.py watch
```

Direct SQL is appropriate for read-only analysis. Schema or state changes
should go through the CLI or an explicit migration with a backup.
