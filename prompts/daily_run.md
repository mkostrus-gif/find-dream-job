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

5. Review `views/review_active.md`, `views/today.md`, `views/followups.md`, and
   `reports/source_quality.md`.

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
- Record only what the external system visibly proves.
- When an application form asks for an unknown fact, store `needs_input` and the
  exact question. Never infer or improvise the answer.

## Discover and screen

- Use search streams and exclusions from the private preferences/scoring files.
- Query all authorized sources; follow their terms and current access limits.
- Use SQLite `external_id`, normalized URL, company/title, stage, and history as
  the skip list.
- Normalize each result into JSON and import it with `ingest-json`.
- Separate mandate fit from practical risks such as location, compensation,
  language, work authorization, or schedule.
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

Run:

```bash
python3 scripts/jobctl.py rebuild --json
python3 scripts/jobctl.py stats
python3 scripts/jobctl.py doctor --strict --json
```

Report verified counts for discovered, reviewed, needs-input, applied,
follow-up, interviews, and rejections; list blockers and external actions with
their evidence state. Include source coverage/checkpoints and the exact
workspace used so another agent can resume. Zero strong applications is a valid
result.
