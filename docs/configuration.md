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

Report the selected workspace, config path, `auto_apply` state, threshold, and
any failed check. Never weaken validation merely to make the command pass.
