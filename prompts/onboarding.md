# First-Time Agent Onboarding

Use this workflow when a candidate has shared the repository but does not yet
have a complete private workspace. Read [`AGENTS.md`](../AGENTS.md),
[`PROJECT_RULES.md`](../PROJECT_RULES.md), and
[`JOB_SYSTEM.md`](../JOB_SYSTEM.md) first.

## Required outcome

Prepare a working, private, review-only job-search workspace; convert supplied
candidate materials into verified structured context; validate the system; and
start the first search with the sources and tools currently available.

Do the setup directly when permissions allow. Ask the candidate only for facts,
choices, authentication, or materials that cannot be recovered safely from the
current environment.

## Phase 1: Inspect before asking

1. Confirm that the repository root contains `scripts/jobctl.py` and that
   Python 3.11 or newer is available.
2. Check whether `JOB_SEARCH_HOME`, local settings, profile files, or a database
   already exist. Never overwrite them.
3. Inventory candidate materials already supplied or available in scope:
   resumes, portfolio, profile pages, prior Q&A, search preferences, and
   application history.
4. Inventory usable capabilities: web search, authorized browser sessions,
   email, job boards, ATS sites, calendars, and messaging connectors. Access to
   one service does not imply permission to mutate it.

If a usable workspace already exists, do not re-onboard from scratch. Run
`doctor --strict --json`, identify only missing or conflicting context, and
continue in operate mode.

## Phase 2: Initialize privately

Reuse an existing `JOB_SEARCH_HOME`. If none exists, use the checkout root for
the zero-configuration setup unless the candidate requested a separate path or
the checkout is clearly temporary. For stronger isolation, use an explicitly
chosen durable directory outside the source checkout and prefix every command
with the same `JOB_SEARCH_HOME` value.

Run:

```bash
python3 scripts/jobctl.py init --json
```

`init` is non-destructive. Review its `created` and `kept` lists. Do not use
`examples/vacancies.json` in this real workspace.

## Phase 3: Build verified private context

Read `config/settings.toml` in the selected workspace. Populate only its
configured private files:

- profile files — identity, contact details, location, authorization,
  experience, education, languages, and defensible achievements;
- preferences — target mandates, geography, work format, compensation,
  exclusions, priorities, and source scope;
- scoring — candidate-specific weights, decision bands, penalties, and hard
  caps;
- reusable Q&A — confirmed answers to recurring factual application questions.

Extract first, then ask. Preserve exact dates, titles, metrics, language levels,
citizenship, work authorization, and compensation constraints. When sources
conflict, record the conflict and ask one precise question.

Do not leave a template placeholder looking like a confirmed answer. Mark
unknown material fields explicitly as unresolved and keep them out of external
forms until answered.

## Phase 4: Confirm the smallest necessary policy set

Infer nothing about external authorization. If not already clear, batch the
remaining questions around:

- target role families and priority order;
- allowed locations, remote/hybrid/on-site constraints, relocation, and work
  authorization;
- languages and minimum acceptable compensation;
- excluded roles, tasks, industries, employers, or contract types;
- sources the agent may search;
- whether the agent may only review and draft or may also submit applications;
- allowed follow-up channels and limits.

Keep `automation.auto_apply = false` unless the candidate explicitly authorizes
submission. Even after authorization, retain
`require_visible_confirmation = true`. Do not interpret a request to “start
searching” as permission to apply or contact people.

Set `project.locale` to the language the candidate wants the agent to use.
Change only the ignored local settings file, never the public example.

## Phase 5: Validate readiness

Inspect all configured private files once more for unresolved template markers,
unsupported claims, and contradictions. Then run:

```bash
python3 scripts/jobctl.py doctor --strict --json
python3 scripts/jobctl.py rebuild --json
python3 scripts/jobctl.py stats
```

`doctor` validates paths, configuration, and database health; it cannot prove
that profile prose is true or complete. That evidence review is the agent's
responsibility.

If validation fails, fix local reversible issues and rerun it. Never weaken a
check merely to obtain a green result.

## Phase 6: Start the first search

Follow `prompts/daily_run.md` in review-only mode:

1. reconcile any existing replies and application history available in scope;
2. search all currently authorized and accessible sources;
3. save normalized source results under the ignored workspace `tmp/` directory;
4. score against verified profile evidence and private constraints;
5. ingest with `scripts/jobctl.py`, rebuild, and verify counts;
6. prepare recommendations or drafts, but take no unapproved external action.

If no live vacancy source is accessible, do not substitute synthetic results.
Finish the local deployment and report the exact access, login, connector, or
source URL needed to begin discovery.

## Handoff format

Return a concise onboarding report containing:

- private workspace path and whether files were created or preserved;
- profile sources used and unresolved factual questions;
- effective locale, target scope, and review/submission policy;
- `doctor` result and database/dashboard paths;
- first-search coverage, vacancies found/imported, strongest matches, and
  needs-input count;
- external actions separated into drafted, attempted, visibly confirmed, and
  blocked;
- the single clearest next input or action for the candidate, if any.
