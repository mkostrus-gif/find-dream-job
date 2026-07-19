# Project Rules

These are invariant rules for humans and agents working with Find Dream Job.
Agents must read [`AGENTS.md`](AGENTS.md) first; that file defines the execution
order, while this file defines constraints that apply in every operating mode.

## Working posture

- Execute safe, in-scope local work instead of returning setup instructions to
  the user.
- Inspect existing workspace state and available tools before asking for input.
- Preserve existing files, drafts, database rows, and external state unless a
  requested change is exact and authorized.
- Batch genuinely missing questions and state the decision each answer affects.
- Finish with verified results and resumable state, not an optimistic claim of
  completion.

## Data contract

- SQLite is the durable source of truth.
- Write vacancy, application, contact, and follow-up state through
  `scripts/jobctl.py`; do not edit generated Markdown or dashboard data.
- Treat `views/`, generated `reports/`, and `dashboard/index.html` as disposable
  read models that may be rebuilt at any time.
- Resolve an exact vacancy ID, URL, or external ID before changing its state.

## Public/private boundary

- Reusable logic, examples, prompts, tests, and documentation may be public.
- Candidate profiles, resumes, Q&A, contact details, databases, messages,
  interview notes, browser captures, exports, and research artifacts are private.
- Store private material only in configured local paths or under
  `JOB_SEARCH_HOME`. Do not weaken the default-deny `.gitignore` to make a
  command more convenient.
- Run `python3 scripts/public_audit.py --strict` before any commit intended for
  publication.
- Git initialization, commits, remotes, pushes, and releases are separate
  actions and require an explicit user request; preparing the tree is not
  authorization to publish it.

## Evidence and external actions

- Never invent candidate facts or form answers. Stop at the exact unknown field.
- Do not mark an application, message, or follow-up as sent without visible
  success evidence from the external system.
- A database write is not proof of an external action; record external evidence
  first, then synchronize SQLite.
- Do not contact a person on an ambiguous identity match. Direct outreach needs
  a current professional relationship to the vacancy and stored evidence.
- Respect the local `automation.auto_apply` setting. Review-only is the public
  default and must remain the fallback when configuration is absent.

## Configuration

- Candidate-specific policy comes from ignored `config/settings.toml` and the
  private profile files listed there.
- Product defaults belong in code or public examples. Do not encode one
  candidate's roles, salary, location, languages, citizenship, or channel
  preferences in reusable prompts or source code.
- When behavior is configurable, tests must cover the safe default and at least
  one customized workspace.
- Do not change public examples to match the active candidate. Personal
  calibration belongs only in the selected private workspace.

## Quality

- Keep the CLI usable with Python's standard library only.
- Preserve backward compatibility for the SQLite schema or provide an explicit,
  backed-up migration.
- Escape untrusted vacancy data before embedding it in HTML or Markdown.
- Verify changes with an isolated temporary workspace; do not use the live
  candidate database as a test fixture.
