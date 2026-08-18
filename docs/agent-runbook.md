# Agent Runbook

This is the detailed operational reference for agents. Start with
[`AGENTS.md`](../AGENTS.md); use this file when executing or recovering a
bootstrap or job-search run. Human operators can follow the same commands and
completion checks.

## Lifecycle at a glance

| State | How to recognize it | Agent action | Exit condition |
|---|---|---|---|
| Uninitialized | No local settings or database | Run `init`, then onboarding | Private templates and database exist |
| Needs context | Templates exist but facts or policy are incomplete | Extract supplied evidence and batch missing questions | Material search inputs are confirmed or explicitly unresolved |
| Ready | `doctor --strict --json` is healthy | Start `prompts/daily_run.md` | Sources scanned or exact access blocker recorded |
| Operating | Vacancies or application history exist | Reconcile inbound state, discover, score, act within policy | SQLite and read models agree; run report delivered |
| Blocked | A required fact, authorization, or login is missing | Preserve work and request the narrow missing input | User supplies input or blocker remains explicit |
| Development | User requested product changes | Use a temporary synthetic workspace | Tests and public audit pass |

## 1. Resolve code and private workspace

The code root is the directory containing `scripts/jobctl.py`. The private
workspace is resolved in this order:

1. `JOB_SEARCH_HOME`, when set;
2. otherwise the code root.

The config path is resolved from `--config`, then `JOB_SEARCH_CONFIG`, then
`<workspace>/config/settings.toml`. Relative paths inside TOML are relative to
the private workspace.

For a zero-configuration start in the checkout:

```bash
python3 scripts/jobctl.py init --json
```

For stronger separation:

```bash
JOB_SEARCH_HOME="/absolute/private/path" \
  python3 scripts/jobctl.py init --json
```

Use the same environment selection on every later command. Do not silently
switch workspaces between commands.

## 2. Discover capabilities

Before declaring a source unavailable, inspect the tools and sessions actually
present in the environment. Classify each capability as:

- available and authorized;
- available but requiring a user-only login, MFA, or CAPTCHA;
- technically available but outside the requested scope;
- unavailable.

Prefer official APIs and career pages where practical. Browser sessions,
mailboxes, credentials, and source-specific automation are external
capabilities; this repository neither installs nor authenticates them.

Reading public vacancies or data the user placed in scope is a discovery
action. Submitting a form, messaging a person, mutating mail, scheduling an
event, or changing an account is an external action and requires matching
authorization.

## 3. Onboard candidate context

Follow [`prompts/onboarding.md`](../prompts/onboarding.md). Keep the mapping
between evidence and private files explicit:

| Context | Private destination | Evidence standard |
|---|---|---|
| Identity, experience, metrics, education, languages | configured profile files | supplied resume/profile or candidate confirmation |
| Roles, geography, compensation, exclusions | preferences file | candidate choice or explicit existing policy |
| Weights, bands, hard caps | scoring file | calibrated candidate preferences |
| Reusable form answers | Q&A file | exact candidate answer or verified profile fact |
| Automation and outreach permissions | local settings | explicit current authorization |

Do not write candidate data into `README.md`, public examples, prompts, source
code, tests, issues, or commit messages.

## 4. Validate without polluting live state

Run health checks in the selected real workspace:

```bash
python3 scripts/jobctl.py doctor --strict --json
python3 scripts/jobctl.py rebuild --json
python3 scripts/jobctl.py stats
python3 scripts/jobctl.py operational-doctor --strict --json
```

Use synthetic vacancy data only for a separate smoke test. On POSIX systems:

```bash
SMOKE_WORKSPACE="$(mktemp -d)"
JOB_SEARCH_HOME="$SMOKE_WORKSPACE" python3 scripts/jobctl.py init --json
JOB_SEARCH_HOME="$SMOKE_WORKSPACE" \
  python3 scripts/jobctl.py ingest-json examples/vacancies.json --json
JOB_SEARCH_HOME="$SMOKE_WORKSPACE" python3 scripts/jobctl.py doctor --strict --json
```

The temporary database proves mechanics, not live source access or candidate
fit. Do not report its vacancies as search results.

Structural health and operational closeout are intentionally separate. A new
or stale workspace may pass `doctor --strict` while
`operational-doctor --strict` fails because current required coverage is
absent. Do not weaken or skip that closeout gate.

### Upgrade an existing private workspace

Run schema upgrades separately from development and keep the default backup:

```bash
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py migrate-schema --json
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py doctor --strict --json
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py rebuild --json
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py stats
```

Никогда не используйте `--no-backup` для рабочей базы. Схема v9 обновляет
поддерживаемые версии v1–v8 и сохраняет исходные находки, каноническую
идентичность вакансий, псевдонимы, отклики, взаимодействия, карточки
работодателей, факторы и контрольные точки. Старый подтверждённый отклик
становится долговечным событием с явно неполной историей и неизвестным прежним
разрешением; миграция не выдумывает кампанию, резюме, ответ, интервью, оффер,
причину карантина или полноту источника. Схема v9 добавляет только структуры
плана, шагов, единиц работы, манифестов и переходов, а также корректное состояние
освобождённого `lease`; доказательства и поколения проекций v8 не
перезаписываются. Схема v8 добавила транзакционную актуальность проекций,
ограниченные блокировки записи и `render`, атомарные поколения результатов и
один истекающий `lease` ежедневного запуска, не изменяя доказательства v7.
Схема v7 добавила инвалидации и эффективные состояния взаимодействий и
откликов; см. [`evidence-corrections.md`](evidence-corrections.md).
После v9 не открывайте обновлённую базу старой версией Engine. Для отката
восстановите резервную копию с отметкой времени и используйте предыдущую
совместимую версию; разрушающей обратной миграции нет. Инструкция по
восстановлению для оператора приведена в
[`operations-v6.ru.md`](operations-v6.ru.md).

## 5. Execute one search cycle

Use [`prompts/daily_run.md`](../prompts/daily_run.md) as the canonical order:

1. load settings and private evidence;
2. выполнить `daily-run-status --json`; возобновить незавершённый долговечный
   запуск и обрабатывать только ожидающие или инвалидированные элементы либо,
   если открытого запуска нет, выполнить `projection-status`, а затем
   `begin-daily-run`;
3. reconcile inbound replies and external statuses; record each automated or
   human employer event with `record-employer-interaction`, then append precise
   lifecycle evidence separately;
4. process authorized Gmail HH mail and every LinkedIn message currently in
   Inbox; score every recommended vacancy and verify each authorized archive;
5. when Telegram is enabled, build its SQLite-backed backfill/delta plan,
   inspect every configured channel to the generated boundary, score/import
   vacancy posts, and pass `check-telegram-coverage` without manually advancing
   a cursor;
6. build the configured source-stream coverage plan, search every generated
   query through its final fully loaded page, and deduplicate against SQLite;
7. normalize results into ignored JSON and ingest them; every mutation from
   inbound reconciliation through coverage uses
   `--defer-render --run-lease <token>`;
8. score the real mandate, hard constraints, and open questions;
9. prepare only in-scope actions; persist an exact `authorized` action record
   before any attempt—score and `auto_apply` are not authorization;
10. append attempted/blocked/failed evidence and verify visible success before
    adding `visibly_confirmed`;
11. сохранять каждую проверенную частичную единицу через
    `checkpoint-daily-run-work`; при внешней блокировке чисто приостановить
    запуск;
12. выполнить все включённые проверки источников с отказом при неполноте и теми
    же флагами отложенного `render`, один раз вызвать
    `finalize-daily-run --run-lease <token>`, проверить точный запуск через
    `operational-doctor --run-id <run_id>` и подготовить отчёт.

Отложенные записи фиксируют SQLite и её ревизию `dirty` одной транзакцией, не
затрагивая ранее опубликованную панель. Если финализация прервана или истекло
ограниченное ожидание блокировки, повторите ту же финализацию с тем же токеном
либо после истечения `lease` получите новый токен через `resume-daily-run`.
Срок жизни запуска не зависит от срока жизни `lease`. Не начинайте второй
запуск, не очищайте `lease` вручную и не принимайте состояние `dirty` за потерю
доказательств. При активном `lease` CLI отклоняет обычный `rebuild` и любое
изменение без отложенного `render`.

При блокировке входом, CAPTCHA или источником зафиксируйте точную причину через
`block-daily-run-work` или `mark-daily-run-work-uncertain`, затем выполните
`pause-daily-run --reason ...`. Следующая задача начинает с компактного статуса
и не повторяет завершённую работу. Если изменился отпечаток конфигурации, только
`refresh-daily-run-plan --reason ...` может создать новую аудируемую ревизию
плана; молчаливое сокращение объёма запрещено.

Use [`prompts/scan_channel.md`](../prompts/scan_channel.md) when the task is
limited to one company or source. Use
[`prompts/ats_application_playbook.md`](../prompts/ats_application_playbook.md)
for resumes, forms, applications, and outreach.

Use [`prompts/gmail_hh_digest.md`](../prompts/gmail_hh_digest.md) for the mail
pass. A LinkedIn message is complete only after all vacancy/reply content is
reconciled into SQLite or classified as non-actionable. When archiving is in
scope, verify the `INBOX` label was removed; an unverified archive or omitted
message keeps the mail pass incomplete.

## 6. Normalize source results

Store source captures and normalized batches in the private workspace, normally
under `tmp/`. Import an array or an object containing `vacancies`, `items`,
`jobs`, or `data`:

```bash
python3 scripts/jobctl.py ingest-json tmp/scan_YYYY-MM-DD.json \
  --channel <channel> --source <source> --json
```

Preserve source URL, source identity, discovery date, title, company, mandate
evidence, score, risks, open questions, and next action. Do not manufacture a
URL or employer identity to make a row importable.

CAPTCHA, logged-out/access-error pages, malformed payloads, and source-aware
missing required fields belong in quarantine, not among low-fit vacancies.
Inspect `quarantine-report`; reprocess one exact ID only after correcting its
raw/replacement payload. `classify-legacy-records --dry-run` never mutates
ambiguous history.

Preserve raw `source_stream`. Local `[source_stream_aliases]` may map one whole
raw label to one or several normalized labels, but arbitrary punctuation is
never split. Unmapped values retain their raw identity and remain visible in
`reports/source_streams.md`. Alias changes refresh only derived links on
rebuild; they do not alter the fail-closed `search.required_streams` manifest.

After ingest, reconcile scan identities by `(channel, external_id)` against
both `vacancies` and `vacancy_external_aliases`. A semantic repost is healthy
when every scan identity resolves but several identities map to one canonical
vacancy. Comparing only with `vacancies.external_id` produces false missing-ID
reports.

For HH, build queries from a private plan instead of hand-assembling URLs:

```bash
python3 scripts/jobctl.py build-coverage-plan tmp/search_plan_YYYY-MM-DD.json \
  --output tmp/search_coverage_YYYY-MM-DD.json
```

After visiting all generated pages, record the raw card count for every page.
With `items_per_page = 100`, an initial DOM count near 20 is an intermediate
lazy-load state. Scroll until the page reaches 100 or the exact final-page
remainder. Fill run-level de-duplicated totals and run:

```bash
python3 scripts/jobctl.py check-coverage tmp/search_coverage_YYYY-MM-DD.json
```

Do not claim a complete run after a non-zero exit. The SQLite checkpoint and
`reports/search_coverage.md` preserve the resumable incomplete state.

For configured public Telegram channels, build a plan before browsing:

```bash
python3 scripts/jobctl.py build-telegram-plan --run-date YYYY-MM-DD \
  --output tmp/telegram_coverage_YYYY-MM-DD.json --json
```

Never-completed channels receive the local initial lookback (30 days by
default); completed channels receive their post-ID delta. Start at each current
public preview, paginate to the generated date/post boundary, classify every
fetched post, and ingest every scored vacancy with the exact post URL,
`source_stream = telegram:<handle>`, and a stable
`telegram:<handle>:<post_id>` identity (plus a stable item suffix when a post
contains several vacancies). Then run:

```bash
python3 scripts/jobctl.py check-telegram-coverage \
  tmp/telegram_coverage_YYYY-MM-DD.json
```

The cursor is stored in `source_checkpoints` only after the whole manifest
passes. Inspect `reports/source_checkpoints.md` when resuming; an incomplete
attempt in `search_runs` is evidence of a blocker, not permission to skip it.

### Reconcile downstream outcomes

An automated acknowledgment is an inbound automated employer interaction, not
a human reply. A recruiter, hiring-manager, or founder response is a human
interaction only when the actor/humanity classification and evidence note are
stored. Neither event changes the funnel automatically. Record interview or
rejection state separately after visible evidence.

Use `outcome-scorecard --as-of YYYY-MM-DD --json` or the generated
`reports/outcome_scorecard.md`. The cohort grain is one canonical vacancy at
its earliest confirmed-application lifecycle event. Human-reply maturity is 14
calendar days; completed-interview maturity is 30. Invitations and scheduled
slots are separate and do not count as completed. First-touch source
attribution uses the explicitly linked application source, otherwise the
earliest hit on or before application date and then `source_hits.id`. Never
infer historical outcomes from status text; incomplete history must display
`n/a`.

### Maintain employer accounts and factors

Create an account explicitly, link a vacancy explicitly, and append signals
only with evidence. Exact normalized account-name matching is allowed; fuzzy
company matching is not. `ai_adoption` is an employer signal only. It does not
prove candidate fit, growth, culture, or enterprise AI transformation
experience. Vacancy factors such as `hiring_reality` and `human_access` inform
action planning but never replace strategic fit or change score automatically.
Maintain the configured active-account portfolio limit, review cadence,
website/careers freshness, target campaign/role metadata, governance evidence,
and human-path status. These remain separate from vacancy fit and permission to
contact a person.

## 7. Handle blockers

| Blocker | Preserve | Ask or report |
|---|---|---|
| Unknown factual form field | Current external draft and exact field/options | One precise candidate question |
| Login, MFA, or CAPTCHA | Local progress and current page/state when safe | Exact user-only step needed |
| Ambiguous vacancy or person | Candidate matches and evidence inspected | Do not write or contact until exact target exists |
| No source capability | Healthy local workspace | Missing connector, session, or authorized URL |
| Conflicting profile facts | Both sources and affected decision | Ask which fact is current |
| External success not visible | Attempt details without sent status | Report attempted/unconfirmed; do not mark sent |
| SQLite health failure | Database untouched and diagnostic output | Repair only with a backup and within task scope |

Never solve a blocker by inventing data, weakening privacy rules, or recording a
desired external outcome as though it happened.

## 8. Handoff and resumability

Every operational handoff should let the next agent resume without replaying
work. Report:

- the exact workspace and config used;
- commands/checks completed and final health state;
- database counts and affected vacancy IDs;
- source coverage and checkpoint/date range;
- external actions with visible evidence state;
- unresolved factual questions and access blockers;
- the next safe action.

SQLite and configured private files carry durable state. Commentary or chat
history alone is not a substitute for recording an authorized, verified state
change.

## 9. Development and publication mode

When changing the reusable engine, follow [`CONTRIBUTING.md`](../CONTRIBUTING.md)
and test only against temporary synthetic workspaces. Before declaring the tree
ready for publication, run the tests and strict public audit. Preparing a tree
does not authorize Git initialization, a commit, a remote, or a push; those are
separate user-directed actions.
