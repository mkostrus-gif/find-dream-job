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

Schema v6 upgrades every supported schema v1–v5. It retains the v5 incremental
checkpoints and adds lifecycle/action events, external-action evidence,
decision metadata, many-to-many source labels, quarantine, versioned screening
policy, and migration audit state. Existing confirmed application rows become
`application_confirmed` events marked `history_complete = 0` and
`authorization_status = legacy_unknown`; this preserves evidence without
inventing prior authorization or complete reply/interview history. Evidence-
backed legacy rejections are preserved, while ambiguous historical rows are
not auto-quarantined or assigned campaigns, resumes, replies, interviews,
offers, source completion, or human paths.

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

Rollback is recovery from the timestamped
`job_search.sqlite.bak-schema-v<old>-<timestamp>` copy: stop writers, preserve
the failed/current database for diagnosis, restore the backup to the configured
database path, and use the prior compatible Engine version. There is no
destructive reverse migration. Repeating `migrate-schema` on schema v6 is
idempotent and creates no additional backup.

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

## WIP, SLA, quarantine, and operational closeout

```bash
python3 scripts/jobctl.py wip-queue --page 1 --page-size 100 --json
python3 scripts/jobctl.py quarantine-report --page 1 --json
python3 scripts/jobctl.py false-negative-audit --as-of 2026-02-28 --json
python3 scripts/jobctl.py operational-doctor --as-of 2026-02-28 --strict --json
```

The queue orders configured buckets, overdue state, overdue age, priority, due
date, event time, and vacancy ID deterministically. Limits mark overflow; they
never archive or mutate old backlog rows. Numbered Markdown pages cover the
whole result rather than truncating the first 200 records.

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

Every write command regenerates the dashboard and reports. Manual changes to
generated files will be lost. Use `watch` only when another process writes the
SQLite database directly:

```bash
python3 scripts/jobctl.py watch
```

Direct SQL is appropriate for read-only analysis. Schema or state changes
should go through the CLI or an explicit migration with a backup.
