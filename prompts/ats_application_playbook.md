# ATS and Application Playbook

Read [`AGENTS.md`](../AGENTS.md) before using this gate. This workflow prepares
and validates application materials; it does not itself grant permission to
submit, upload, message, or mutate an external system.

Use this gate for resumes, cover letters, application forms, recruiter chats,
and direct outreach.

## 1. Extract the vacancy language

Before writing, capture the exact title, must-have skills, tools, domain terms,
years and level of experience, management scope, metrics, location, work
authorization, sponsorship, relocation, schedule, and repeated phrases.

Do not treat the title as proof of the real mandate. Identify reporting line,
decision authority, team, budget, business outcome, and success metrics.

## 2. Select verified evidence

Read the configured private profile and Q&A. Build a small evidence map from
each vacancy requirement to a verified candidate fact. Missing facts are
questions, not opportunities to infer.

Education, dates, titles, languages, citizenship, work authorization, domain
experience, team size, revenue, and other metrics must remain exact.

## 3. Resume gate

- Start from the closest truthful master resume.
- Match headline, summary, skills, and vocabulary to the vacancy only where the
  evidence map supports it.
- Keep experience in reverse chronological order.
- Prefer measurable outcome bullets with context, action, and result.
- Use one column, ordinary text, standard headings, and predictable reading
  order. Avoid hidden keywords, graphics as text, complex tables, and layout
  tricks that break parsers.
- For a standalone PDF/DOCX, include a practical contact header and keep the
  length appropriate to the market and role.

## 4. Technical document QA

Before uploading a final file:

1. Extract text from that exact file with an independent parser.
2. Verify name, target title, contact fields, headings, employers, roles, dates,
   required keywords, and key metrics.
3. Verify reading order and reverse chronology.
4. Check bullets, punctuation, currency, percentages, and non-Latin glyphs.
5. Render every page and inspect clipping, overlap, empty pages, broken wraps,
   and unbalanced pagination.
6. Rebuild and repeat if either parsing or rendering fails.

## 5. Cover letter

Use a short human structure: specific hook, two or three evidence-backed
matches, motivation for this role/company, and a simple call to action. Avoid
generic AI language, a full resume recap, and unsupported enthusiasm claims.

## 6. Forms

Use the configured private Q&A file. When a factual field is unknown, preserve
the draft, capture the exact field and options, store `needs_input`, and ask the
candidate. Do not translate uncertainty into a confident answer.

## 7. Direct outreach

- Confirm identity, current employer, and professional connection to the role.
- Respect configured channels and per-round limits.
- State only the relationship evidence actually known.
- Keep the message specific and short; do not attach files without a reason.
- Verify visible delivery before recording `sent`.

## 8. Final application check

- Every claim maps to private profile evidence.
- Hard requirements and practical constraints are explicit.
- Unknown facts remain unresolved rather than guessed.
- The final document passed parse and render QA.
- The exact action has a durable `authorized` record with the current
  authorization evidence; a score, draft, or `auto_apply` setting is not
  authorization.
- Record `attempted`, `blocked`, or `failed` after the attempt as applicable.
- Visible success exists before a `visibly_confirmed` action creates the
  durable `application_confirmed` event.
- Record the actual submitted resume and message variant; do not replace them
  with the planned versions.
