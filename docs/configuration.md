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

`[project]` controls the dashboard title and HTML locale. `[paths]` controls the
database and generated output. Relative paths resolve against `JOB_SEARCH_HOME`
or the checkout root when that variable is absent.

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
local authorization decision, not a scoring shortcut. Even when enabled,
`require_visible_confirmation = true` means the system must observe external
success before recording a submission.

`apply_threshold` is available to prompts and diagnostic output. The CLI stores
scores but does not itself click an application button.

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

## Search coverage

`[search]` makes daily discovery completeness machine-checkable:

- `required_streams` is the local, candidate-specific list that every daily run
  must cover;
- `default_period_days` becomes HH's `search_period` value;
- `items_per_page` controls both generated HH URLs and lazy-load validation and
  cannot exceed 100.

Keep role names and candidate-specific stream definitions in the ignored local
configuration and preferences. The public template contains generic examples.
`build-coverage-plan` refuses plans that omit a configured stream, while
`check-coverage` exits non-zero when a stream, page, or expected card count is
missing.

## Source stream aliases

Optional `[source_stream_aliases]` entries map a raw historical stream label to
a canonical reporting key. Matching is case-insensitive, so one entry can
consolidate case variants:

```toml
[source_stream_aliases]
"Legacy Product Stream" = "product_roles"
```

The engine always preserves `source_hits.source_stream`. Generated source and
conversion reports apply the current mapping; unmapped values retain their raw
key and appear in `reports/source_streams.md`. Changing the mapping changes
future report grouping on rebuild but does not rewrite historical raw values.
This mapping does not alter the exact fail-closed `search.required_streams`
coverage contract.

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
```

Report the selected workspace, config path, `auto_apply` state, threshold,
LinkedIn mail policy, and any failed check. Never weaken validation merely to
make the command pass.
