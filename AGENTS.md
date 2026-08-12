# Agent Operating Contract

This is the primary entry point for an AI agent operating Find Dream Job. The
normal user journey is agent-first: a person shares this repository, the agent
prepares a private workspace, learns the candidate's verified profile, and
starts a safe job-search cycle. Do the work when tools and permissions allow;
do not merely repeat these instructions back to the user.

Humans can use the same flow. The main product overview is in [README.md](README.md).

## Mission

Turn verified candidate information and authorized vacancy sources into a
traceable job-search pipeline:

```text
private candidate context + authorized sources -> SQLite -> generated views
                                            external success -> evidence -> SQLite
```

The configured SQLite database is the source of truth. Personal data stays in
the private workspace. Public source and documentation must remain generic.

## Instruction order

Use the following repository documents in this order:

1. `AGENTS.md` — operating contract and safety boundaries;
2. `PROJECT_RULES.md` — invariant data, privacy, and evidence rules;
3. `JOB_SYSTEM.md` — CLI and persistence contract;
4. `prompts/onboarding.md` — first-time private setup;
5. `prompts/daily_run.md` — recurring search workflow;
6. task-specific files under `prompts/`.

The user's current request and explicit permissions define the task scope.
Local private settings define candidate-specific policy. If instructions
conflict, choose the safer interpretation, preserve user data, and identify the
conflict instead of silently guessing.

## Select the operating mode

- **Bootstrap** — no complete local workspace exists. Follow the first-run
  protocol below and `prompts/onboarding.md`.
- **Operate** — local settings, private profile files, and a database exist.
  Run health checks, then follow `prompts/daily_run.md` or the requested
  task-specific prompt.
- **Develop** — the user asked to change the reusable product. Use synthetic
  temporary workspaces, preserve safe defaults, and follow `CONTRIBUTING.md`.

Never mix development fixtures with a candidate's live database.

## First-run protocol

When given only this repository and a request to start a job search:

1. If only a repository URL is available, obtain a local working copy using the
   environment's normal read/clone capability. Then find the root containing
   `scripts/jobctl.py` and read the documents in the instruction order above.
2. Inspect the environment before asking questions: Python version, existing
   local workspace, user-provided resumes/profile files, and available web,
   browser, email, job-board, or messaging capabilities.
3. Require Python 3.11 or newer. The core has no third-party runtime
   dependencies.
4. Reuse `JOB_SEARCH_HOME` when it is already set. Otherwise use the checkout
   root for a zero-configuration local setup; its default-deny `.gitignore`
   protects runtime data. Offer an external private workspace when the user
   wants stronger isolation or the checkout is temporary.
5. Initialize non-destructively:

   ```bash
   python3 scripts/jobctl.py init --json
   ```

6. Follow `prompts/onboarding.md`. Extract facts from materials already
   supplied before asking the user. Store candidate facts only in the private
   files named by local settings. Batch genuinely missing questions.
7. Keep `automation.auto_apply = false` unless the user explicitly authorizes
   submissions. Permission to search, screen, or draft is not permission to
   apply, send messages, alter a mailbox, or publish anything.
8. Validate the workspace:

   ```bash
   python3 scripts/jobctl.py doctor --strict --json
   python3 scripts/jobctl.py rebuild --json
   python3 scripts/jobctl.py stats
   ```

9. Start the first review-only search cycle with `prompts/daily_run.md`, using
   only sources that are available and authorized. If source access is absent,
   finish local setup and report the exact missing capability or login instead
   of pretending that discovery ran.

Do not import `examples/vacancies.json` into a real candidate workspace. It is
synthetic smoke-test data and belongs only in a temporary test workspace.

## Autonomy and stop conditions

Proceed without asking when an action is local, reversible, within the stated
task, and supported by verified evidence. Prefer inspecting existing files and
available tools over asking the user to repeat information.

Stop and ask only when at least one of these is true:

- a material candidate fact cannot be verified from supplied sources;
- authentication, CAPTCHA, MFA, or another user-only step blocks access;
- the requested scope does not authorize an external or destructive action;
- two sources conflict on a fact that would affect scoring or an application;
- a privacy boundary cannot be maintained.

Keep drafts intact when blocked. Capture the exact unanswered field or failed
precondition. Never convert uncertainty into a candidate claim.

## External-action gate

Searching public or user-authorized sources and preparing drafts are distinct
from actions that affect other people or systems. Before an application,
message, follow-up, mailbox mutation, or similar external action, verify all of
the following:

1. the action is within the user's current authorization and local policy;
2. every submitted factual field is backed by private profile evidence;
3. the target vacancy and, where applicable, recipient identity are exact;
4. the final content is known and any required review is complete;
5. visible external success can be checked.

Before execution, persist the exact action's `authorized` state with its
authorization note. After execution, append `attempted`, `visibly_confirmed`,
`blocked`, or `failed` evidence as applicable. Only `visibly_confirmed` may
create a confirmed application or sent-message fact. A score, recommendation
band, draft, `automation.auto_apply`, or CLI write is never authorization and
is never evidence that an external action happened.

## Completion contracts

Bootstrap is complete only when:

- local settings and all configured private profile files exist;
- known candidate facts and preferences have been written to private files;
- unresolved material facts are listed explicitly, not hidden in placeholders;
- the automation and outreach policy reflects actual authorization;
- `doctor --strict --json` reports `"ok": true`;
- the database and generated dashboard exist;
- a first review-only search ran, or its precise external-access blocker is
  reported.

A search run is complete only when:

- inbound replies and existing state were reconciled before new outreach;
- every locally configured search stream has a successful persisted coverage
  checkpoint; missing streams, incomplete pagination, and partial lazy-loads
  are fail-closed conditions;
- every enabled incremental source has a successful manifest for every
  configured stream; a first scan must complete its configured backfill and
  later scans must begin from the last success-gated cursor;
- discovered vacancies were deduplicated and written through `jobctl.py`;
- scores cite verified evidence and hard constraints;
- external actions distinguish drafted, attempted, visibly confirmed, and
  blocked states;
- read models were rebuilt and final health checks passed;
- `operational-doctor --strict --json` reports
  `"ready_for_daily_closeout": true`;
- the user receives counts, strongest matches, actions, unresolved questions,
  and blockers.

## Data and repository boundaries

- Write operational state through `scripts/jobctl.py`, not generated Markdown
  or dashboard HTML.
- Candidate profiles, resumes, Q&A, contact details, messages, databases,
  screenshots, exports, and research artifacts are private.
- Never put credentials in TOML or repository files. Use the credential store
  of the authorized connector or browser session.
- Do not change `.gitignore` to expose a runtime file.
- Do not initialize Git, create a remote, commit, push, or open a pull request
  unless the user explicitly requests that separate action. Cloning an existing
  public repository for the requested setup is a read/acquisition step, not
  publication authorization.
- Before any publication-related commit, run:

  ```bash
  python3 scripts/public_audit.py --strict --json
  python3 -m unittest discover -s tests -v
  ```

## Useful task entry points

- First private setup: `prompts/onboarding.md`
- Full recurring cycle: `prompts/daily_run.md`
- One source or company: `prompts/scan_channel.md`
- Vacancy scoring: `prompts/scoring.md`
- Resume, form, application, or outreach: `prompts/ats_application_playbook.md`
- Gmail HH and LinkedIn job mail: `prompts/gmail_hh_digest.md`
- Detailed operational reference: `docs/agent-runbook.md`

When handing control back, lead with the outcome. Include the workspace used,
validation result, verified state changes, external evidence state, and any
specific input the user must provide next.
