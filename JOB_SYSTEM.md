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
- `views/outreach_contacts.md` — verified direct contacts;
- `views/employer_accounts.md` — account radar and latest employer signals;
- `views/vacancy_factors.md` — structured evidence factors;
- `reports/source_quality.md` — screening and downstream source performance;
- `reports/source_streams.md` — raw-to-canonical stream diagnostics;
- `reports/conversion_cohorts.md` — vacancy-level application conversion;
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

Never ingest the public example vacancies into a candidate's real database.
Use a disposable `JOB_SEARCH_HOME` for smoke tests.

## Data model

- `vacancies` — canonical vacancy and latest state;
- `source_hits` — discovery events by source and stream;
- `search_runs` / `search_coverage` — fail-closed daily-run manifests and
  per-stream page/query checkpoints;
- `vacancy_external_aliases` — every observed `(channel, external_id)` mapped
  to one canonical vacancy, with its own URL and first/last observation dates;
- `vacancy_fingerprints` — conservative semantic repost aliases based on
  company, title, and normalized description;
- `evaluations` — screening/review decisions;
- `applications` — application-level history;
- `stage_events` — append-only stage/status evidence;
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
- `import_issues` — ingestion diagnostics.

Supported funnel stages are `seen`, `needs_input`, `follow_up`, `applied`,
`interview_1`, `interview_2`, `interview_3`, `offer`, and `rejected`.

The evidence vocabulary is intentionally non-interchangeable:

| Concept | Durable evidence | What it does not prove |
|---|---|---|
| Screening signal | `source_hits.raw_status` / evaluation evidence | Application or employer interest |
| Application | Confirmed `applications` row after visible submission success | Human response |
| Automated acknowledgment | Inbound automated `employer_interactions` row | Human reply |
| Human reply | Inbound human `employer_interactions` row after application | Interview or stage change |
| Interview | Evidence-backed `interview_1` stage event | Offer or candidate fit by itself |
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
local `[source_stream_aliases]` entries map raw aliases case-insensitively to a
canonical reporting key. New hits also store the canonical key effective at
ingest time; reports resolve every raw value against the current local mapping,
so a mapping change takes effect on rebuild without rewriting history. An
unmapped value keeps its raw key. Inspect `reports/source_streams.md` before
assuming that a legacy spelling or case variant belongs to another stream.

Search coverage still validates the exact configured
`search.required_streams` manifest fail-closed. Stream aliases consolidate
outcome reporting; they do not excuse a missing required checkpoint.

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

Supported types are `human_reply`, `automated_ack`, `screening_request`,
`interview_invite`, `rejection`, and `other`. An inbound event requires an
evidence note. `automated_ack` must be automated and never counts as a human
reply. Exact repeats are idempotent; an external reference, when supplied, is
the strongest duplicate key. Recording an interaction never changes
`vacancies.latest_stage`. Record `interview_1` or `rejected` separately with
`update-vacancy` and a non-empty evidence note after external state is visibly
confirmed. Historical interactions are never inferred from free-form status
text.

Run a reproducible cohort calculation with:

```bash
python3 scripts/jobctl.py conversion-report --as-of 2026-02-28 --json
```

The grain is one unique vacancy. Its application date is the earliest
confirmed application row; repeated rows for that vacancy do not multiply the
cohort. The 14-day human-reply denominator contains applications at least 14
calendar days old at `as_of`. Its numerator contains cohort vacancies with an
inbound human interaction after the application date. Automated
acknowledgments are excluded. The 30-day interview denominator uses the same
rule at 30 days; its numerator requires an `interview_1` stage event with a
non-empty evidence note. Verified-contact and completed-contact-search
coverage use all unique applications as their denominator.

Source-stream attribution is deterministic first touch: the earliest
`source_hit` on or before the first application date, with `source_hits.id` as
the tie-breaker. Missing pre-application hits are attributed to `unknown`.
Breakdowns are produced by source channel, canonical source stream, and
application month. Every rate is shown with numerator and denominator; a tiny
sample is not ranked as a proven winner. If no structured employer interaction
exists, human-reply counts and rates are `n/a`, not a false zero.

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

## Schema upgrades

Schema upgrades are explicit and create a timestamped backup by default:

```bash
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py migrate-schema --json
```

Schema v4 retains the schema-v3 external-ID model and adds canonical stream
keys, employer interactions, account radar tables, and vacancy factors. A v3
workspace is upgraded without rewriting raw stream values or inventing any
historical replies, accounts, signals, links, or factors. Existing source hits
receive only a deterministic canonical key from the current local alias
configuration, or their raw identity when unmapped.

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
destructive reverse migration. Repeating `migrate-schema` on schema v4 is
idempotent and creates no additional backup.

## Exact state changes

Use the internal ID when available:

```bash
python3 scripts/jobctl.py update-vacancy \
  --id 42 --stage applied --status APPLIED_CONFIRMED \
  --note "Visible confirmation captured" --sync-application
```

`--url` or `--external-id` may be used when the ID is unknown. All commands
accepting `--external-id` resolve both the canonical ID and any stored alias;
stored alias URLs are also resolvable through `--url`. Resolve the target before
writing. Never infer external success from this command itself.

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
evidence. Direct touchpoints also require a stored contact ID. Follow-up limit,
interval, primary channel, and preferred direct channels come from local
settings.

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
