# Scan One Hiring Channel

Read [`AGENTS.md`](../AGENTS.md) first. Execute this workflow against the exact
authorized scope and checkpoint the coverage so another agent can resume
without repeating the scan.

## Inputs

- company or market:
- channel:
- authorized source URL:
- scope and date range:

## Process

1. Read local settings and all configured private profile/scoring files.
2. Use the authorized official source. Expand scope only with user permission.
3. Collect all in-scope roles and preserve source URLs.
4. Deduplicate against SQLite and within the batch.
5. Score the real mandate using `prompts/scoring.md`; record hard requirements,
   evidence, practical risks, and open questions.
6. Save normalized rows to ignored JSON and ingest them:

   ```bash
   python3 scripts/jobctl.py ingest-json tmp/channel_scan_YYYY-MM-DD.json \
     --channel <channel> --source <source>
   ```

7. Rebuild and verify targeted rows and counts.

For a configured Telegram source, do not invent a date window. Use
`build-telegram-plan` so a never-completed channel receives its initial
lookback and an established channel receives only its SQLite-backed delta.
Record every fetched page/post and boundary, ingest scored vacancy rows with
stable `telegram:<handle>:<post_id>` identities, then run
`check-telegram-coverage`. The channel cursor must not advance after partial
history, unresolved post classification, or missing SQLite ingest evidence.

Do not create channel-specific Markdown journals. Do not apply merely because a
scan found a high score. Local settings and the current request define the
available workflow, but every exact application still needs its own durable
`authorized` record before any attempt.

## Report

State source coverage, roles found, duplicates, score bands, strongest matches,
hard rejects, needs-input questions, imported row count, and dashboard path.
