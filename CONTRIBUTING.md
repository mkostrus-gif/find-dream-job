# Contributing

Contributions are welcome after the repository is published.

AI agents working on the reusable product are in **develop** mode. Read
[`AGENTS.md`](AGENTS.md), [`PROJECT_RULES.md`](PROJECT_RULES.md), and
[`docs/architecture.md`](docs/architecture.md) before editing. A request to
operate a candidate's search is not authorization to change product code.

## Development

Use Python 3.11 or newer. The core has no third-party runtime dependencies.
Never use a real candidate workspace as a test fixture.

```bash
python3 -m py_compile scripts/jobctl.py scripts/jobsearch_config.py scripts/search_coverage.py scripts/public_audit.py
python3 -m unittest discover -s tests -v
python3 scripts/public_audit.py --strict
```

Dashboard browser QA is optional and requires Playwright:

```bash
node scripts/qa_dashboard.mjs
```

Agents must execute the applicable checks, inspect failures, and report exact
results. Do not run development tests against the live workspace or repair a
test by weakening privacy, evidence, or review-only defaults.

## Pull requests

- Keep changes focused and explain schema or privacy impact.
- Add isolated tests for behavior changes.
- Preserve review-only and visible-confirmation safety defaults.
- Do not include real resumes, databases, recruiter details, messages,
  screenshots, or tokens in issues, fixtures, or PR descriptions.
- For schema changes, include an explicit migration and downgrade/backup notes.

By contributing, you agree that your contribution is licensed under MIT.
