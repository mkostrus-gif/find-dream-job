# Mail Digest Import

Read [`AGENTS.md`](../AGENTS.md) and confirm that mailbox access and the
requested read/mutation scope are authorized. A connected mailbox is a
capability, not blanket permission to archive, label, or send.

Use an authorized mail connector to find recent vacancy digests from the target
job board. Do not expose mailbox credentials or message bodies in repository
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

Use the private profile and `prompts/ats_application_playbook.md`. Unknown form
answers go to `needs_input`. Archive or label mail only when the user authorized
mailbox changes and the vacancy/reply was fully reconciled into SQLite.
