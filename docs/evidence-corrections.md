# Evidence corrections

Find Dream Job preserves append-only evidence. A factual correction therefore
does not delete or rewrite the original event. Schema v7 adds an explicit,
auditable invalidation layer for erroneous employer interactions.

## Correction model

`employer_interactions` remains the raw historical ledger.
`employer_interaction_invalidations` records one invalidation for one exact
interaction, including:

- the target interaction and its canonical vacancy;
- correction timestamp;
- reason and evidence note;
- source and operator context;
- a deterministic deduplication key and creation timestamp.

The composite foreign key requires the interaction and vacancy to match. Each
interaction may have at most one invalidation. `effective_employer_interactions`
contains only raw interactions without an invalidation and is the supported
input for reply, screening-request, conversion, outcome, and dashboard metrics.

The original interaction remains queryable and its invalidation can be joined
for audit:

```sql
SELECT interaction.*, invalidation.corrected_at, invalidation.reason,
       invalidation.evidence_note, invalidation.source,
       invalidation.operator_context
FROM employer_interactions interaction
LEFT JOIN employer_interaction_invalidations invalidation
  ON invalidation.interaction_id = interaction.id
WHERE interaction.id = ?;
```

## Supported CLI

```bash
python3 scripts/jobctl.py invalidate-employer-interaction \
  --interaction-id 42 \
  --reason "The recorded interaction was disproved" \
  --evidence-note "The authorized source was rechecked and contains no message" \
  --source operator_review \
  --operator-context manual_reconciliation \
  --json
```

Use one optional vacancy guard when additional target confirmation is useful:
`--vacancy-id`, `--vacancy-url`, or `--vacancy-external-id`. The command fails
closed when the interaction is absent, the guard resolves to another vacancy,
reason or evidence is empty, or an existing invalidation has conflicting
metadata. An exact repeat returns the existing row with `created: false` and
the same deduplication key.

Invalidating an employer interaction changes no lifecycle event, application
confirmation, action event, vacancy field, or application row. Current state
must be reconciled separately with the existing explicit commands, for example
`update-vacancy --sync-application` and `set-current-action`.

## Duplicate application compatibility rows

Application lifecycle truth comes from `lifecycle_events`, reduced to the
earliest confirmed application per canonical vacancy. The legacy
`applications` table may contain duplicate or reconciliation rows. Schema v7
adds `effective_applications`, which deterministically selects the highest row
ID for each vacancy as the current compatibility state. Generated follow-ups
join that view, so they contain at most one row per canonical vacancy and an
older row cannot reintroduce stale status, questions, or a follow-up date.

No application row is deleted. A visibly confirmed application lifecycle event
continues to count once even when its related employer interaction is
invalidated.

## Migration and recovery

`migrate-schema` upgrades schema v1 through v7 to v8 and creates the normal
timestamped database backup by default. The migration is additive: it creates
any missing invalidation/effective structures plus v8 projection-control and
run-lease tables without creating any invalidation automatically. Recovery uses the pre-migration backup and the
prior compatible Engine version; there is no destructive reverse migration.
