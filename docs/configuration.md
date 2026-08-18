# Configuration

This is the agent and human reference for local policy. Read
[`AGENTS.md`](../AGENTS.md) first. For first-time setup, follow
[`prompts/onboarding.md`](../prompts/onboarding.md); do not edit the public
example with a candidate's values.

Run `python3 scripts/jobctl.py init --json` to copy the public template into the
private workspace. Local settings are intentionally ignored by Git. An agent
must review the returned `created` and `kept` paths and preserve every existing
file.

## Project and paths

`[project]` задаёт заголовок панели, локаль HTML и часовой пояс IANA `timezone`,
который фиксируется в долговечном плане ежедневного запуска. `[paths]` задаёт
пути к базе и генерируемым результатам. Относительные пути отсчитываются от
`JOB_SEARCH_HOME`, а при отсутствии этой переменной — от корня checkout.

From schema v8, configured generated-output paths remain stable compatibility
paths while their contents resolve through `.jobctl/projections/current`.
Do not point `views`, `reports`, or `dashboard` into candidate source folders;
the first v8 rebuild replaces only the configured generated paths with managed
links after copying the legacy generated set.

## Profile

`[profile]` lists private Markdown sources:

- `files` — resumes and factual candidate profiles;
- `preferences_file` — target roles and constraints;
- `scoring_file` — candidate-specific scoring and calibration;
- `answers_file` — semantically merged reusable form answers.

These files are instructions/evidence for an agent. `jobctl` checks their
presence but never publishes or embeds their contents in generated reports.
File presence is not evidence completeness: the onboarding agent must remove or
explicitly mark unresolved template placeholders and ground material claims in
candidate sources.

## Automation

`automation.auto_apply` is `false` in the public template. Enabling it is a
local workflow policy, not authorization for any particular action and not a
scoring shortcut. Every external action still needs its own durable
`authorized` record. Even when enabled, `require_visible_confirmation = true`
means the system must observe external success before recording a submission.

`apply_threshold` is available only for review prioritization and diagnostic
output. It never authorizes an application, message, follow-up, mailbox change,
or publication. The CLI stores scores but does not itself click an application
button.

An agent may edit `auto_apply` only when the user explicitly authorizes
application submission. A general request to deploy, search, score, recommend,
or draft leaves it `false`. Keep `require_visible_confirmation = true`.

## Follow-up

`[follow_up]` controls the maximum number of rounds, business-day interval,
primary channel, ordered direct channels, and maximum direct messages per round.
Only configured direct channels are accepted by contact and follow-up commands.

Changing the list does not install or authenticate a messaging connector. It
only changes validation and recording policy.

## Mail

`[mail]` makes LinkedIn Inbox processing an explicit local policy:

- `scan_linkedin_inbox = true` requires each daily run to inspect every current
  LinkedIn message in Gmail Inbox and process all vacancy/reply content;
- `archive_processed_linkedin = true` records authorization to archive each
  fully reconciled LinkedIn message and verify removal of the `INBOX` label.

Both settings default to `false`. Archiving cannot be enabled unless scanning
is enabled. These settings do not install or authenticate Gmail and do not
authorize deletion, sending, or unrelated mailbox mutations.

## Telegram sources

`[telegram]` enables public Telegram channels as mandatory daily discovery
sources:

```toml
[telegram]
enabled = true
initial_lookback_days = 30
channels = ["https://t.me/example_exec_jobs"]
```

Real channel URLs belong only in the ignored local settings file. The public
template stays disabled with an empty list. URLs are normalized to public
handles; post links, invite links, query strings, invalid handles, and
case-insensitive duplicates are rejected.

On the first successful scan of each channel, `initial_lookback_days` defines
the backfill window. Later plans use that channel's durable numeric Telegram
post cursor and process only the delta. A cursor advances only after
`check-telegram-coverage` proves complete pagination/classification and
evidence-backed SQLite ingest for every vacancy post. Enabling a channel grants
read-only discovery scope; it does not join a channel, authenticate Telegram,
send a message, or authorize an application.

## Search coverage

`[search]` makes daily discovery completeness machine-checkable:

- `required_streams` is the local, candidate-specific list that every daily run
  must cover;
- `default_period_days` becomes HH's `search_period` value;
- `items_per_page` controls both generated HH URLs and lazy-load validation and
  cannot exceed 100;
- `personal_recommendations_enabled` adds a separate fail-closed operational
  check for `personal_recommendation_stream` rather than treating it as normal
  search coverage.

Keep role names and candidate-specific stream definitions in the ignored local
configuration and preferences. The public template contains generic examples.
`build-coverage-plan` refuses plans that omit a configured stream, while
`check-coverage` exits non-zero when a stream, page, or expected card count is
missing.

## Дополнительные обязательные условия ежедневного запуска

`[daily_run]` позволяет приватной рабочей области объявить дополнительные
обязательные условия без изменения публичного Engine. Ключи непрозрачны для
Engine и поэтому в публичном шаблоне не должны содержать частные факты.

```toml
[daily_run]

[[daily_run.required_gates]]
key = "additional_source"
kind = "workspace_gate"
order = 100
depends_on = ["hh_coverage"]
required = true
enabled = true
require_remote_boundary = true
```

`key` стабилен в пределах рабочей области; `kind` выбирает типизированный
контракт доказательства; `order` задаёт детерминированный порядок;
`depends_on` ссылается на встроенный ключ шага либо ключ другого условия.
`enabled = false` фиксируется в снимке плана как доказанное
`not_applicable`. Включённое обязательное условие нельзя вручную обойти через
`skip`. Если настройка меняется во время запуска, статус показывает изменение,
а финализация отказывает до явного `refresh-daily-run-plan --reason ...` с
записью причины в журнал.

`require_remote_boundary = true` требует в завершающем манифесте одновременно
`remote_boundary_verified = true` и точную `completion_boundary`. Это контракт
предоставленного доказательства, а не утверждение, что Engine сам проверил
удалённую систему.

## Source stream aliases

Optional `[source_stream_aliases]` entries map one whole raw historical stream
label to one or several canonical reporting keys. Matching is case-insensitive,
but punctuation is never split implicitly:

```toml
[source_stream_aliases]
"Legacy Product Stream" = "product_roles"
"Campaign Alpha + Campaign Beta" = ["campaign_alpha", "campaign_beta"]
```

The engine always preserves `source_hits.source_stream`. Generated source and
conversion reports apply the current mapping; unmapped values retain their raw
key and appear in `reports/source_streams.md`. Changing the mapping changes
future report grouping on rebuild but does not rewrite historical raw values.
For a multi-label hit, the raw row remains one discovery event and the derived
many-to-many links are refreshed. The first configured label is the stable
compatibility/first-touch label. Reordering an alias is therefore an explicit
attribution change.
This mapping does not alter the exact fail-closed `search.required_streams`
coverage contract.

## Decision metadata

`[decision]` supplies controlled local vocabularies for `campaign_id`,
`role_family`, resume identifiers, and message variants. Empty arrays are the
safe default: missing data remains unknown, and any non-empty unconfigured
value is rejected. `confidence` uses the built-in evidence vocabulary `low`,
`medium`, `high`, `confirmed`, and `unknown`. The Engine never assumes a number
of resumes, campaigns, role families, languages, locations, or preferences.

## WIP and SLA

`[queue]` configures page size plus the exact `limits`, `sla_days`, and Russian
`labels` for `urgent`, `due_follow_up`, `deep_review`, `account_research`, and
`backlog`. Limits mark overflow; they do not archive, delete, or downgrade old
rows. `backlog` is outside active WIP by design. Page size must be 1–500.

## Screening policy and employer portfolio

`[policy]` declares one `active_version` and ISO `effective_date`. Changing a
screening rule requires an intentional version/date update, so obsolete policy
rows cannot silently compete in false-negative audits.

`[account].active_portfolio_limit` caps the number of employer accounts whose
status is `target` or `active`. Individual accounts may also store an exact
portfolio limit, review cadence, freshness dates, configured campaign/role
targets, governance evidence, and human-path status. None of these fields
changes vacancy score or authorizes contact.

An agent should configure only channels both chosen by the candidate and
available in the intended workflow. Configuration is policy, not proof of a
working connector or permission to send.

## Multiple private workspaces

Use one code checkout with separate workspaces:

```bash
JOB_SEARCH_HOME="/path/to/workspace-a" python3 scripts/jobctl.py stats
JOB_SEARCH_HOME="/path/to/workspace-b" python3 scripts/jobctl.py stats
```

Never share a local config between candidates when it references private files
or a shared SQLite database.

## Agent validation

After every configuration change, run:

```bash
python3 scripts/jobctl.py doctor --strict --json
python3 scripts/jobctl.py rebuild --json
python3 scripts/jobctl.py operational-doctor --strict --json
```

Report the selected workspace, config path, `auto_apply` state, threshold,
LinkedIn mail policy, Telegram enabled/channel count/lookback, and any failed
check. Never weaken validation merely to make the command pass.
