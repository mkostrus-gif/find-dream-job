# Gmail Job Mail Workflow

Read [`AGENTS.md`](../AGENTS.md) and confirm that mailbox access and the
requested read/mutation scope are authorized. A connected mailbox is a
capability, not blanket permission to archive, label, or send.

Read local `[mail]` policy. `scan_linkedin_inbox = true` makes the complete
LinkedIn Inbox pass required. `archive_processed_linkedin = true` authorizes
archiving only after the per-message reconciliation gate below; it does not
authorize deletion or any other mailbox mutation.

Use an authorized mail connector to inspect the complete in-scope Inbox set,
not only unread messages or messages newer than an assumed checkpoint. For
LinkedIn, query all messages currently in Inbox from LinkedIn-controlled
senders. Review every matching message, including job alerts, recommendations,
recruiter/application notifications, and messages with no vacancy content.

For each message:

1. Classify it as vacancy discovery, inbound pipeline state, or no actionable
   job-search content.
2. Extract every vacancy link. Resolve tracking redirects and retain the stable
   vacancy URL and verified external job ID when available.
3. Open and inspect every vacancy page, then score it with [`scoring.md`](scoring.md).
   Reconcile closed or inaccessible roles explicitly instead of silently
   dropping them.
4. Resolve each vacancy against canonical IDs and aliases in SQLite, import its
   final screening state, and verify the exact row. A message is not processed
   merely because its links were extracted.
5. If mailbox archiving is authorized, archive the message only after every
   vacancy/reply in it is reconciled or the message is explicitly classified as
   having no actionable content. Verify that the message no longer has the
   `INBOX` label. Archive is not delete.

If exact vacancy resolution or mailbox verification fails, keep the message in
Inbox, preserve the private checkpoint, and report the blocker. Do not expose
mailbox credentials, private message bodies, or message IDs in repository
files.

Extract vacancy links into a local ignored JSON file:

```json
{
  "vacancies": [
    {
      "date": "YYYY-MM-DD",
      "source": "mail_digest",
      "title": "",
      "company": "",
      "url": ""
    }
  ]
}
```

Import and screen them:

```bash
python3 scripts/jobctl.py ingest-gmail-json tmp/mail_digest_YYYY-MM-DD.json
python3 scripts/jobctl.py ingest-json tmp/mail_screening_YYYY-MM-DD.json \
  --channel gmail_hh --source mail_digest
```

For LinkedIn job mail, use LinkedIn as the vacancy channel so the same job ID
deduplicates against direct LinkedIn discovery:

```bash
python3 scripts/jobctl.py ingest-gmail-json tmp/linkedin_mail_YYYY-MM-DD.json \
  --provider linkedin --json
python3 scripts/jobctl.py ingest-json tmp/linkedin_mail_screening_YYYY-MM-DD.json \
  --channel linkedin --source linkedin_gmail_job_alert
```

Use the private profile and `prompts/ats_application_playbook.md`. Unknown form
answers go to `needs_input`. In the run report, include messages found,
processed, archived, archive-verified, and blocked; vacancy links found, unique,
known, new, scored, and unresolved. A LinkedIn Inbox pass is incomplete while
any matching message is unaccounted for or its authorized archive is
unverified.
