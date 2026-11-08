# HybridRecall review fixes (2026-11-08)

## Collatable document IDs (F02)

The executor takes the ID collation from the actual text-branch tuple descriptor
and requires the vector branch to agree. Collatable types require a valid
collation; noncollatable types may retain InvalidOid. The self-describing hash
key carries that same collation for hashing and equality, and the final ID
ordering uses it too. The field is runtime state, so no Plan serialization or
copy format changes are needed. Handmade-plan result descriptors also retain
the ID collation.

`src/test/regress/sql/hybrid_recall.sql` now includes an independent two-row
`text COLLATE "C"` primary-key fixture with opposite text/vector rankings.
It specifies the two branch orders, equal-weight RRF ID order, OFF/FORCE modes,
LIMIT 1, and candidate caps 4 with LIMIT 4 returning only two deduplicated rows.
Expected results are recorded in `src/test/regress/expected/hybrid_recall.out`.

## Extended LIMIT forms (F03)

HybridRecall V1 supports an absolute finite LIMIT only. WITH TIES and PERCENT
are rejected by the shape whitelist before any marker expression or sort clause
is changed. Their ordinary plans keep the marker and sorting; executing an
unsupported shape retains the marker's existing diagnostic instead of silently
returning a fixed-K result. No WITH TIES expansion or percent conversion is
implemented by this change.

The existing plan-properties regression helper covers both forms with zero RRF
weights: no HybridRecall node or task stream is admitted, and the marker Sort
remains present.

## Validation boundary

Only static code review and `git diff --check` were performed in this cloud
repair. The regression sources and expected output were updated but **not run**.
No compilation, build, dependency installation, or service startup was performed.
Historical performance and test results elsewhere in this directory do not
validate these new cases.
