# Job Search Agent Dispatcher

Read [`AGENTS.md`](../AGENTS.md), [`PROJECT_RULES.md`](../PROJECT_RULES.md), and
[`JOB_SYSTEM.md`](../JOB_SYSTEM.md) before operating the system. Do not merely
summarize them: select the correct mode and execute the safe, authorized work.

## Select a workflow

- No complete private workspace: run [`onboarding.md`](onboarding.md).
- Full recurring search: run [`daily_run.md`](daily_run.md).
- One source, market, or company: run [`scan_channel.md`](scan_channel.md).
- Vacancy evaluation: apply [`scoring.md`](scoring.md).
- Resume, form, submission, or direct outreach: apply
  [`ats_application_playbook.md`](ats_application_playbook.md).
- Mail digest: apply [`gmail_hh_digest.md`](gmail_hh_digest.md).
- Product code or publication preparation: switch to develop mode and follow
  [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## Always enforce

SQLite is authoritative. Use `scripts/jobctl.py` for operational writes and
regenerate read models after changes. Candidate facts, credentials, source
captures, and application artifacts stay in the selected private workspace.

Ground every candidate claim in private profile evidence. Preserve drafts and
ask the exact question when a material fact is unknown. Search authorization is
not submission or messaging authorization. Verify visible external success
before recording a sent action.

Communicate in the configured locale. Finish with the workspace used, health
result, verified counts and state changes, source coverage, external evidence
state, unresolved facts, and the next safe action.
