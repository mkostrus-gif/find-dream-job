# Security Policy

Agents that encounter a credential or unexpected personal artifact in a public
candidate file must stop publication work, avoid echoing the value into logs or
chat, and report the affected path and secret type with the value redacted.

## Supported versions

Until the first stable release, security fixes target the latest `main` branch.

## Reporting

Do not open a public issue containing credentials, resumes, personal contact
details, application history, recruiter messages, or a real SQLite database.
After publication, use GitHub's private vulnerability-reporting feature if it
is enabled; otherwise contact the repository owner through the private channel
listed in the GitHub profile.

Include a minimal synthetic reproduction, affected commit, impact, and proposed
mitigation. Remove all candidate and employer identifiers.

## If private data was committed

Stop publication, rotate exposed credentials, remove the material from the
entire Git history, and rerun both a history-aware credential scanner and
`scripts/public_audit.py`. Deleting the file in a later commit is not enough.
