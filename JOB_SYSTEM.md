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
- `reports/source_quality.md` — source performance;
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
- `interview_summaries` — links to private local notes;
- `employer_contacts` — evidence-backed recruiter/hiring contacts;
- `contact_searches` — positive, negative, and ambiguous lookup history;
- `followup_rounds` — numbered follow-up waves and next date;
- `outreach_messages` — exact per-channel message and delivery evidence;
- `import_issues` — ingestion diagnostics.

Supported funnel stages are `seen`, `needs_input`, `follow_up`, `applied`,
`interview_1`, `interview_2`, `interview_3`, `offer`, and `rejected`.

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
  "next_action": "manual_review"
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

## Schema upgrades

Schema upgrades are explicit and create a timestamped backup by default:

```bash
JOB_SEARCH_HOME="/path/to/private-workspace" \
  python3 scripts/jobctl.py migrate-schema --json
```

Schema v3 adds `vacancy_external_aliases` and backfills each existing canonical
external ID. It does not invent historical repost aliases; re-ingest the
corresponding retained scan artifacts after migration to recover those IDs.
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

## Generated output

Every write command regenerates the dashboard and reports. Manual changes to
generated files will be lost. Use `watch` only when another process writes the
SQLite database directly:

```bash
python3 scripts/jobctl.py watch
```

Direct SQL is appropriate for read-only analysis. Schema or state changes
should go through the CLI or an explicit migration with a backup.
