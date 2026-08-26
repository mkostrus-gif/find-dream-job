# Find Dream Job

Find Dream Job is an agent-first system that turns an AI agent into a persistent
personal job-search operator. It learns your experience, achievements, target
roles, constraints, preferences, and reusable application answers from your
resumes and other verified materials. Using authorized APIs or connectors, or
your logged-in browser session, it can search HeadHunter, other job boards,
company career pages, and email vacancy digests. It deduplicates and ranks
vacancies, explains the fit and risks, and keeps the complete funnel in SQLite
with readable reports and a dashboard. For a promising role, the agent can
tailor a truthful resume, write a vacancy-specific cover letter, and complete
application forms without inventing missing facts. Once you explicitly enable
submission, provide enough verified profile context, and give it access to the
appropriate logged-in service, it can apply automatically and verify that the
application was actually accepted. It can also reconcile employer replies,
track interviews, manage follow-ups, find evidence-backed contacts, and prepare
or send authorized outreach. All personal data, credentials, messages, and job
history remain local; the public repository contains only the reusable engine.
To begin, share the repository with an agent, provide your resume and search
preferences, complete the guided onboarding, and let the agent run the first
review-only search.

**AI agent: start with [`AGENTS.md`](AGENTS.md), then execute
[`prompts/onboarding.md`](prompts/onboarding.md).** Do not put candidate data in
public files and do not take external actions without matching authorization.

[Русская версия](docs/README.ru.md)

## Give this repository to an agent

The primary way to use Find Dream Job is to share the repository URL with an AI
agent that can work with files and a terminal. The agent should inspect its
available web, browser, email, and job-board capabilities; initialize the local
system; build a verified private candidate profile; and start a review-only
search. It should ask only for information or access it cannot safely recover.

Copy this prompt together with the repository URL:

```text
Set up Find Dream Job for me and start my job search. Clone or open the
repository, read AGENTS.md first, and follow prompts/onboarding.md. Work
directly in the repository when safe: create the private workspace
non-destructively, extract verified facts from the materials I provide, keep
personal data out of public files, validate with doctor --strict --json, and run
the first review-only search using the sources and tools you are authorized to
access. Do not submit applications, contact people, mutate mail, or publish
anything unless I explicitly authorize that action. Ask only for genuinely
missing facts or user-only login steps. Finish with validation results, search
coverage, imported vacancies, strongest matches, external-action evidence, and
precise blockers.
```

The agent's canonical execution path is:

```text
AGENTS.md -> onboarding -> private profile -> doctor -> first search -> report
```

If an external source needs login, MFA, CAPTCHA, or an unavailable connector,
the agent should still finish local deployment and identify the exact blocker.
It must not fabricate live search results or silently substitute sample data.

## What the agent deploys

Find Dream Job keeps vacancy discovery, evaluation, applications, follow-ups,
and interview links in SQLite, then generates readable Markdown views and a
single-file dashboard. The reusable engine can be public; resumes, contact
details, preferences, messages, credentials, and job-search history stay in an
ignored private workspace.

It provides:

- non-destructive private-workspace initialization;
- normalized JSON ingestion from any authorized source;
- canonical external-ID plus persisted repost aliases, conservative
  semantic-repost deduplication, and source/evaluation history;
- deterministic HH query plans and fail-closed stream/page/lazy-load coverage;
- безопасное инкрементальное получение вакансий HH в схемах v10–v11: версионированный
  DOM-адаптер только для чтения, режимы `full`, `shadow`, `delta`, `resume` и
  `audit`, observer- или timer-based доказательство стабильности видимого DOM
  в авторизованном встроенном браузере Codex, программная сверка канонических
  ID и псевдонимов в SQLite, отдельные персональные рекомендации, ограниченная
  очередь деталей, периодические полные аудиты и выключенное по умолчанию
  доказательное восстановление точного повторяющегося transient-хвоста;
- configurable public Telegram channels with a per-channel 30-day first
  backfill, success-gated post cursors, incremental daily deltas, and
  fail-closed post/ingest reconciliation;
- Gmail HH and LinkedIn job-mail ingestion, with per-message reconciliation and
  authorized archive verification in the daily workflow;
- append-only durable lifecycle events independent from current work actions;
- explicit drafted/authorized/attempted/visibly-confirmed/blocked external
  action evidence, with no score-based permission semantics;
- a capped, paginated WIP/SLA queue with overflow and overdue evidence;
- evidence-backed recruiter and hiring-manager contacts;
- structured employer interactions that distinguish automated acknowledgments
  from human replies without changing funnel stage;
- vacancy-level 14/30-day outcome cohorts with precise interview states,
  deterministic first-touch attribution, field completeness, and configured
  campaign/resume/message breakdowns;
- preserved raw source hits with explicit many-to-many normalized labels;
- quarantine and explicit reprocessing for technical/non-vacancy records;
- separate structural and fail-closed operational closeout doctors;
- an explicit employer account radar, evidence-backed employer signals, and
  structured vacancy factors that remain separate from candidate-relative score;
- generated `views/*.md`, `reports/*.md`, and `dashboard/index.html`;
- deferred durable writes plus one atomic closeout render for large daily runs;
- ограниченные межпроцессные блокировки записи и `render`, долговечный план
  ежедневного запуска в SQLite, возобновляемые контрольные точки и сменяемые
  истекающие `lease`;
- agent workflows for onboarding, discovery, scoring, ATS documents,
  applications, and reconciliation;
- safe review-only defaults and a default-deny public Git allowlist.

The repository intentionally does not bundle a universal scraper, credentials,
or an unattended submission bot. Live discovery and external actions use the
agent's authorized tools and sessions and remain subject to each service's
rules.

## Agent entry points

| Task | Start here |
|---|---|
| First setup from a repository URL | [`AGENTS.md`](AGENTS.md), then [`prompts/onboarding.md`](prompts/onboarding.md) |
| Full recurring search cycle | [`prompts/daily_run.md`](prompts/daily_run.md) |
| One company or hiring source | [`prompts/scan_channel.md`](prompts/scan_channel.md) |
| Score a vacancy | [`prompts/scoring.md`](prompts/scoring.md) |
| Resume, ATS form, application, or outreach | [`prompts/ats_application_playbook.md`](prompts/ats_application_playbook.md) |
| Understand or recover an operation | [`docs/agent-runbook.md`](docs/agent-runbook.md) |
| Change the reusable product | [`CONTRIBUTING.md`](CONTRIBUTING.md) |
| Prepare a fork for publication | [`docs/publishing-checklist.md`](docs/publishing-checklist.md) |

## Safe manual bootstrap

Humans and agents can initialize the zero-configuration local workspace with:

```bash
python3 scripts/jobctl.py init --json
python3 scripts/jobctl.py doctor --strict --json
python3 scripts/jobctl.py rebuild --json
python3 scripts/jobctl.py stats
```

`init` creates only missing files and never overwrites an existing profile,
configuration, or database. Before live searching, replace the private
templates by following [`prompts/onboarding.md`](prompts/onboarding.md).

Do not ingest `examples/vacancies.json` into a real job-search database. To
smoke-test mechanics, use a disposable workspace:

```bash
SMOKE_WORKSPACE="$(mktemp -d)"
JOB_SEARCH_HOME="$SMOKE_WORKSPACE" python3 scripts/jobctl.py init --json
JOB_SEARCH_HOME="$SMOKE_WORKSPACE" \
  python3 scripts/jobctl.py ingest-json examples/vacancies.json --json
JOB_SEARCH_HOME="$SMOKE_WORKSPACE" \
  python3 scripts/jobctl.py doctor --strict --json
```

After a real rebuild, open the configured `dashboard/index.html`.

## Stronger private-data separation

The default workspace lives in the checkout and is protected by the
default-deny `.gitignore`. For stronger isolation, point the public engine at a
separate private directory and use that selection consistently:

```bash
export JOB_SEARCH_HOME="/absolute/path/to/private-job-search"
python3 scripts/jobctl.py init --json
python3 scripts/jobctl.py doctor --strict --json
```

Code and synthetic examples stay in this repository. The database, profile,
generated dashboard, temporary source captures, and application artifacts live
under `JOB_SEARCH_HOME`.

## Safety contract

The public template is deliberately review-only:

- automatic submission is disabled;
- a request to search does not authorize applying or contacting people;
- unknown candidate facts must be escalated instead of guessed;
- visible external success is required before recording a sent action;
- direct outreach requires an exact person, a professional connection to the
  vacancy, and stored evidence;
- secrets stay in connector or browser credential stores, never repository
  files;
- SQLite is authoritative; Markdown and dashboard files are rebuildable read
  models.
- employer AI adoption, an AI word in a vacancy title, and a candidate's use of
  AI tools never prove candidate fit or enterprise AI transformation experience;
  candidate claims still require private evidence.

Candidate-specific behavior belongs in ignored `config/settings.toml` and the
private files it references. See [`PROJECT_RULES.md`](PROJECT_RULES.md) for the
invariants and [`JOB_SYSTEM.md`](JOB_SYSTEM.md) for the persistence contract.

## Requirements

- Python 3.11 or newer;
- SQLite 3, normally bundled with Python;
- optional Node.js and Playwright for browser-level dashboard QA;
- for live search, at least one authorized source available to the operating
  agent.

The core CLI uses only the Python standard library.

## Main commands

```text
init                      Create local settings, profile templates, and DB
doctor --strict --json    Validate config, profile paths, and SQLite health
operational-doctor        Check technical health and daily-closeout readiness
projection-status         Show dirty/fresh projection revision and active lease
begin-daily-run           Acquire the single daily-run orchestration lease
daily-run-status          Показать план, блокировки, контрольные точки и следующий шаг
resume-daily-run          Получить новый lease для незавершённого запуска
pause-daily-run           Освободить lease без завершения запуска
checkpoint-daily-run-work Сохранить проверенный частичный результат без render
complete-daily-run-work   Закрыть единицу работы по проверенному манифесту
cancel-due-followup-obligation Отменить точную frozen follow-up обязанность
resolve-due-followup-from-reverified-inbound Разрешить её свежей проверкой старого human inbound
refresh-daily-run-plan    Зафиксировать изменение конфигурации и расширить план
finalize-daily-run        Publish one atomic final generation and release lease
hh-browser-adapter        Найти версионированный DOM-адаптер HH только для чтения
plan-hh-acquisition       Выбрать безопасный режим full/shadow/delta/resume/audit
record-hh-page            Проверить снимок, сверить ID и сохранить контрольную точку P1
record-hh-detail          Сохранить подробности только для ограниченной очереди
next-hh-work              Показать точное безопасное продолжение потока HH
finalize-hh-stream        Завершить обычный поток HH по манифесту v2
finalize-hh-personal-recommendations Завершить отдельный персональный источник
inspect-hh-checkpoint     Показать курсор и доказательства shadow/audit
record-hh-transient-error Сохранить независимую попытку точной хвостовой ошибки
record-hh-rollover-page   Сохранить страницу полной новой HH-сессии от page 0
resolve-hh-recovery       Зафиксировать точное проверенное решение восстановления
invalidate-hh-zero-evidence-plan Перепланировать только пустой frozen HH-план
invalidate-hh-checkpoint  Явно отключить небезопасный инкрементальный курсор
ingest-json FILE          Import structured vacancy/evaluation rows
ingest-gmail-json FILE    Import HH or LinkedIn vacancy links extracted from Gmail
build-coverage-plan FILE  Generate deterministic HH URLs and manifest skeleton
check-coverage FILE       Persist and fail-closed validate daily-run coverage
build-telegram-plan       Generate per-channel first-backfill or delta plan
check-telegram-coverage   Validate post/ingest evidence and advance cursors
migrate-schema            Back up and upgrade an existing SQLite workspace
update-vacancy            Change one vacancy and optionally its application
set-current-action        Append current WIP/action state without lifecycle loss
record-lifecycle-event    Append evidence-backed outcome/interview state
record-external-action    Append authorization, attempt, and visible evidence
record-employer-interaction Append a human or automated employer event
conversion-report         Calculate vacancy-level 14/30-day cohorts
outcome-scorecard         Calculate the first-class evidence outcome scorecard
wip-queue                 Read the capped, paginated WIP/SLA queue
quarantine-report         Audit quarantined ingestion records
reprocess-quarantine      Retry one exact quarantine record
false-negative-audit      Deterministically sample rejected/low-priority rows
upsert-employer-account   Create or update an exact employer account
record-employer-signal    Append an evidence-backed account signal
link-vacancy-account      Explicitly link a vacancy to an account
record-vacancy-factor     Append a structured evidence factor without scoring
upsert-contact            Store an evidence-backed employer contact
record-contact-search     Record a negative or ambiguous contact search
record-followup           Record one structured multi-channel follow-up round
attach-interview-summary  Link a private Markdown summary to a vacancy
rebuild                    Regenerate views, reports, and dashboard
stats                      Print funnel KPIs as JSON
watch                      Rebuild when the SQLite file changes
```

Run `python3 scripts/jobctl.py COMMAND --help` for exact fields.

`cancel-due-followup-obligation` фиксирует append-only resolution
`user_cancelled_followup_obligation`; он не означает отзыв отклика, отказ,
входящий ответ или доставку сообщения.

`resolve-due-followup-from-reverified-inbound` — отдельная evidence-backed
операция. Она требует точный диалог, исходный `human_reply`, свежую удалённую
границу и манифест `reverified_historical_inbound_v1`, но не создаёт новый
inbound и не меняет его исходный `event_at`. Успех фиксируется отдельным
переходом `reverified_historical_inbound_due_resolution`, а не пользовательской
отменой.

All existing writes still render immediately by default. For a batch, add
`--defer-render` (or `--no-render`) to commit SQLite and mark projections dirty
without touching generated files, then run one `rebuild`. A live daily run uses
`begin-daily-run`, passes its `--run-lease` together with `--defer-render` to
every mutation, and ends with `finalize-daily-run`.

Перед созданием нового запуска агент обязан вызвать `daily-run-status --json`.
Незавершённый план возобновляется через `resume-daily-run`; уже выполненная
работа не повторяется после смены процесса. Блокировка входом, CAPTCHA или
источником фиксируется в соответствующей единице работы и приводит к чистому
`pause-daily-run`, а не к ложному завершению.

The reproducible performance fixture is synthetic and never opens an existing
workspace:

```bash
python3 scripts/benchmark_daily_run.py --rows 25000 --workflow deferred
python3 scripts/benchmark_hh_incremental.py --streams 10 --cards 3000
```

Тест производительности HH создаёт только одноразовые синтетические рабочие
области, сравнивает `full` и `delta` по числу страниц, повторным загрузкам,
объёму снимков и времени сверки, проверяет все ожидаемые новые и изменённые ID,
а также подтверждает, что нарушение порядка и конфликтующие повторные снимки
приводят к полному обходу или блокировке.

## Repository map

```text
AGENTS.md     canonical instructions for an operating agent
config/       public settings templates; local settings are ignored
docs/         agent runbook, architecture, privacy, and release guidance
examples/     synthetic profile and import examples, never live candidate data
prompts/      executable agent workflows without candidate-specific facts
scripts/      SQLite CLI, config loader, dashboard QA, publication audit
tests/        isolated tests using temporary private workspaces
```

Runtime directories such as `data/`, `private/`, `views/`, `reports/`,
`dashboard/`, `tmp/`, and `output/` are intentionally excluded from the public
tree.

## Documentation

- [Agent operating contract](AGENTS.md)
- [Agent runbook](docs/agent-runbook.md)
- [System contract](JOB_SYSTEM.md)
- [Project rules](PROJECT_RULES.md)
- [Architecture](docs/architecture.md)
- [Evidence corrections](docs/evidence-corrections.md)
- [Configuration](docs/configuration.md)
- [Russian v6 operator guide](docs/operations-v6.ru.md)
- [Privacy model](docs/privacy.md)
- [Roadmap](docs/roadmap.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

Before publishing a fork, run the tests and strict public audit, then follow
the [publishing checklist](docs/publishing-checklist.md). Preparing a tree does
not authorize an agent to create a remote or push it.

## License

MIT. See [LICENSE](LICENSE).
