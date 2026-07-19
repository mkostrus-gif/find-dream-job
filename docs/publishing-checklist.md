# Publishing Checklist

This checklist is executable by a human or agent, but it prepares only a local
tree. It does not authorize Git initialization, commits, remote creation, push,
or release. An agent must stop before those actions unless the user explicitly
requests them.

1. Run isolated tests:

   ```bash
   python3 -m unittest discover -s tests -v
   ```

2. Run the strict privacy audit in machine-readable mode:

   ```bash
   python3 scripts/public_audit.py --strict --json
   ```

   If the private identifier list lives outside the checkout, add
   `--deny-file /path/to/public-audit.local.toml`.

3. Review the audit's complete candidate-file manifest. Expected public files
   are source, docs, prompts, examples, tests, and repository metadata only.

4. Confirm these are absent: SQLite databases and sidecars, resumes, PDFs,
   DOC/DOCX files, screenshots, exports, transcripts, messages, generated HTML,
   local TOML, `.env` files, cookies, tokens, and absolute user paths.

5. Initialize Git locally only when ready. Inspect `git status --short` before
   staging, then inspect the staged diff and file list again.

6. Run a credential scanner on the staged tree and, before the first push, on
   the full history. Rotate any credential that was ever exposed.

7. Confirm the MIT license, project description, support/security policy, and
   copyright holder are acceptable.

8. Create the remote repository and push only in a separate, explicitly
   authorized step.

## Agent handoff

Report the test result, audit `candidate_count`, exact candidate-file manifest,
any finding, and whether Git/history review was possible. State explicitly that
nothing was published. Never describe a clean working tree as safe if the
candidate manifest or history was not actually inspected.
