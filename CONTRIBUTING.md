# Contributing

Contributions are welcome after the repository is published.

AI agents working on the reusable product are in **develop** mode. Read
[`AGENTS.md`](AGENTS.md), [`PROJECT_RULES.md`](PROJECT_RULES.md), and
[`docs/architecture.md`](docs/architecture.md) before editing. A request to
operate a candidate's search is not authorization to change product code.

## Development

Use Python 3.11 or newer. The core has no third-party runtime dependencies.
Never use a real candidate workspace as a test fixture.

```bash
python3 -m py_compile scripts/jobctl.py scripts/daily_run_orchestration.py scripts/hh_acquisition.py scripts/jobsearch_config.py scripts/search_coverage.py scripts/telegram_source.py scripts/public_audit.py scripts/benchmark_hh_incremental.py
node --check scripts/hh_browser_adapter.js
python3 -m unittest discover -s tests -v
python3 scripts/public_audit.py --strict
python3 scripts/benchmark_daily_run.py --rows 25000 --workflow deferred
python3 scripts/benchmark_hh_incremental.py --streams 10 --cards 3000
```

Dashboard browser QA is optional and requires Playwright:

```bash
node scripts/qa_dashboard.mjs
```

Agents must execute the applicable checks, inspect failures, and report exact
results. Do not run development tests against the live workspace or repair a
test by weakening privacy, evidence, or review-only defaults.

Schema contributions must also run isolated synthetic migrations from every
supported prior version with the default backup enabled, followed by
`rebuild --json`, `stats`, and `doctor --strict --json`. Verify row counts,
`PRAGMA integrity_check`, `PRAGMA foreign_key_check`, backup creation, and
idempotent repeated migration. When operational state is affected, also run
`operational-doctor`, the outcome scorecard, WIP pagination, generated-output
language QA, and dashboard QA at desktop and narrow widths. Never use a real
candidate database as migration proof.

Projection/render changes must additionally verify that deferred mutations do
not change generated files, an interrupted render preserves the prior current
generation, concurrent renderers obey bounded OS locks, and the 25,000-row
synthetic benchmark reports dashboard bytes and WIP file count. A schema v8
migration changes only projection-control and run-lease structures; evidence
tables and external-action gates must remain row-count stable.

Изменения оркестрации схемы v9 дополнительно требуют синтетических сценариев
аварийного продолжения: миграции v8 с резервной копией, детерминированного
отпечатка плана, частичной контрольной точки источника, приостановки и нового
`lease`, истечения `lease`, изменения конфигурации, неопределённого внешнего
действия, полной очереди наступивших повторных обращений, прерывания финальной
публикации, ровно одного `render`, идемпотентной повторной финализации, точного
`operational-doctor` для запуска и ограниченного статуса на 25 000 строках. Все
фикстуры должны находиться в одноразовом `JOB_SEARCH_HOME`; реальную базу
кандидата использовать запрещено.

Изменения HH P2 схемы v10 дополнительно проверяют миграцию v9→v10 с резервной
копией, все режимы `full`, `shadow`, `delta`, `resume` и `audit`, минимальное
перекрытие, две известные страницы и защитную страницу, переход к полному
обходу, плановый аудит и его расхождения, устойчивые повторные снимки при
недостатке или избытке карточек, изменение наборов ID, индикатор загрузки,
страницу входа, CAPTCHA, аномалии порядка и сессии, пакетную сверку канонических
ID и псевдонимов, ограниченный вывод новых и изменённых ID, отдельный
персональный источник, манифесты v1 и v2, приостановку с продолжением в новом
процессе, монотонность курсора при ошибке и ровно одну финальную публикацию.
Браузерные фикстуры могут содержать только синтетические DOM и снимки.
Обязательны `node --check` для адаптера, тест производительности на 3 000
карточек без пропущенных ожидаемых ID и враждебные сценарии, которые приводят
только к полному обходу или блокировке.

## Pull requests

- Keep changes focused and explain schema or privacy impact.
- Add isolated tests for behavior changes.
- Preserve review-only and visible-confirmation safety defaults.
- Do not include real resumes, databases, recruiter details, messages,
  screenshots, or tokens in issues, fixtures, or PR descriptions.
- For schema changes, include an explicit migration and downgrade/backup notes.

By contributing, you agree that your contribution is licensed under MIT.
