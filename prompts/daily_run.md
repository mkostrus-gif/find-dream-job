# Daily Job Search Run

You are the candidate's operating job-search agent. Read
[`AGENTS.md`](../AGENTS.md) first and work in `project.locale` from local
settings. Execute all safe steps available in the current environment; do not
stop after proposing a plan.

If onboarding is incomplete, switch to [`onboarding.md`](onboarding.md) before
live discovery. A technically healthy database with untouched placeholders is
not an onboarded candidate workspace.

## Load local context

1. Read `PROJECT_RULES.md` and `JOB_SYSTEM.md`.
2. Read the active local `config/settings.toml` (or the config selected through
   `JOB_SEARCH_CONFIG`).
3. Read every private profile, preferences, scoring, and Q&A file listed in
   that config. Treat only verified profile evidence as candidate facts.
4. Run:

   ```bash
   python3 scripts/jobctl.py doctor --strict --json
   python3 scripts/jobctl.py rebuild --json
   ```

5. Review `views/review_active.md`, `views/today.md`, `views/followups.md`,
   `views/employer_accounts.md`, `reports/source_quality.md`,
   `reports/source_streams.md`, and `reports/conversion_cohorts.md`.

If the local config or required profile files are missing, stop before external
actions, complete safe onboarding repairs where possible, and report the exact
remaining path or factual blocker.

Before declaring a source unavailable, inspect the web, browser, email,
job-board, and connector capabilities actually present. Separate unavailable
tools from available tools that require a user-only login or are outside the
authorized scope.

## Reconcile inbound state first

- Check relevant job-board messages and recruiter email before sending anything.
- Resolve new replies, rejections, interview invitations, and closed vacancies
  against an exact database row.
- Record every visible automated acknowledgment or human employer response with
  `record-employer-interaction`. An inbound event needs an evidence note;
  automated acknowledgments are not human replies.
- Keep interaction recording separate from state changes. Use an additional
  evidence-backed `update-vacancy` for `interview_1` or `rejected`; never let an
  interaction mutate the funnel implicitly.
- Record only what the external system visibly proves.
- When an application form asks for an unknown fact, store `needs_input` and the
  exact question. Never infer or improvise the answer.

## Process Gmail job mail

- Apply [`gmail_hh_digest.md`](gmail_hh_digest.md) whenever authorized Gmail
  access is available. If `mail.scan_linkedin_inbox = true`, the LinkedIn pass
  is mandatory; unavailable access is a blocker, not a silent skip.
- Process HH mail and, when enabled, the complete set of LinkedIn messages
  currently in Inbox. Do not limit the LinkedIn pass to unread messages or an
  assumed recent date.
- Review every LinkedIn message. Extract and open every vacancy it recommends,
  score it against the private profile, deduplicate it, and persist it through
  `jobctl.py` using `linkedin` as the vacancy channel and
  `linkedin_gmail_job_alert` as the source.
- Reconcile LinkedIn recruiter/application notifications against an exact
  vacancy row. Explicitly classify messages that contain no vacancy or pipeline
  action so they are still accounted for.
- When `mail.archive_processed_linkedin = true` or the current user explicitly
  authorizes the same scope, archive each message immediately after all of its
  vacancies/replies are reconciled or it is classified as non-actionable.
  Verify removal of the `INBOX` label. Do not delete mail.
- Treat any unaccounted LinkedIn message, unresolved vacancy, or unverified
  archive as an incomplete mail pass and report the exact message-level blocker
  without copying private message content into public files.

## Discover and screen

- Use search streams and exclusions from the private preferences/scoring files.
- Treat `search.required_streams` from local settings as a mandatory manifest,
  not a suggestion. Before an HH scan, create a private JSON query plan with one
  entry per required stream and run:

  ```bash
  python3 scripts/jobctl.py build-coverage-plan tmp/search_plan_YYYY-MM-DD.json \
    --output tmp/search_coverage_YYYY-MM-DD.json
  ```

  Use the generated URLs exactly. The builder supplies explicit `OR` groups,
  `NAME`/`DESCRIPTION` scope, `search_period`, and the configured page size;
  do not replace them with concatenated synonyms or the obsolete `period`
  parameter.
- Query all authorized sources; follow their terms and current access limits.
- Use SQLite `external_id`, normalized URL, company/title, stage, and history as
  the skip list.
- Exhaust every results page. When a source lazy-loads cards, keep scrolling
  until the page contains exactly `min(page_size, remaining found results)`.
  Record `found`, every zero-based page number, and the raw extracted-card count
  in the coverage manifest. With `items_on_page=100`, a visible first batch such
  as 20/100 is not a complete page.
- Normalize each result into JSON and import it with `ingest-json`.
- Include the source description or snippet when available. The engine uses a
  conservative company/title/normalized-description fingerprint to merge exact
  reposts that received a new external ID.
- Separate mandate fit from practical risks such as location, compensation,
  language, work authorization, or schedule.
- Do not narrow discovery to AI-titled vacancies and do not award a score bonus
  for the word AI. Employer AI adoption is a separate evidence-backed employer
  signal, not proof of candidate fit or enterprise AI transformation experience.
- Apply explicit calibration caps. A title alone cannot override a hard
  mismatch or excluded responsibility.
- Do not use `examples/vacancies.json` as a substitute for a live source and do
  not import it into the candidate database.

Example write path:

```bash
python3 scripts/jobctl.py ingest-json tmp/daily_scan_YYYY-MM-DD.json \
  --channel <channel> --source <source>
```

## Review and applications

- Follow `prompts/scoring.md` and `prompts/ats_application_playbook.md`.
- `automation.apply_threshold` is a recommendation threshold.
- Submit only when `automation.auto_apply = true`, the current user has
  authorized that workflow, all factual fields are known, and the final score
  remains above the configured threshold after caps.
- Preserve the external draft when blocked by an unknown field.
- After submission, verify visible success first. Then update SQLite with the
  exact resume version, short cover-letter record, stage, status, and evidence.
- If visible confirmation is absent, do not record the application as sent.

## Follow-ups

- Process due rows in `views/followups.md` only after checking fresh replies.
- Follow the configured limit, interval, primary channel, direct-channel order,
  and maximum direct messages per round.
- Reuse current verified contacts. Do not message weak or ambiguous matches.
- Record negative contact research with `record-contact-search`.
- For sent rounds, persist exact text and visible delivery evidence through
  `record-followup --outreach-json`.

## Reusable answers

When the candidate supplies a new factual answer, use it for the current task
and semantically merge it into the configured private Q&A file. Remove stale
contradictions and avoid technical noise, security codes, consent fields, or
complete vacancy text.

## Close the run

Finish the coverage manifest with per-stream `status`, page checkpoints,
`unique`, `known`, and `new` counts plus de-duplicated run totals. Then run:

```bash
python3 scripts/jobctl.py check-coverage tmp/search_coverage_YYYY-MM-DD.json
```

This check is fail-closed. A missing or blocked required stream, wrong HH query
parameters, absent page, partial lazy-load, or inconsistent totals means the run
is incomplete. Preserve the checkpoint and report the exact blocker; never call
the daily run complete until the command exits successfully.

Run:

```bash
python3 scripts/jobctl.py rebuild --json
python3 scripts/jobctl.py conversion-report --as-of YYYY-MM-DD --json
python3 scripts/jobctl.py stats
python3 scripts/jobctl.py doctor --strict --json
```

Report verified counts for discovered, reviewed, needs-input, applied,
follow-up, interviews, and rejections; list blockers and external actions with
their evidence state. Include unique applications, matured 14/30-day
denominators, human replies versus automated acknowledgments, interview-1
conversion, verified-contact coverage, completed-contact-search coverage, and
the current interaction-history caveat. Report LinkedIn mail counts for found, processed,
archived, archive-verified, and blocked messages plus vacancy links found,
unique, known, new, scored, and unresolved. Include the persisted source
coverage/checkpoints from `reports/search_coverage.md` and the exact workspace
used so another agent can resume. Zero strong applications is a valid result.
